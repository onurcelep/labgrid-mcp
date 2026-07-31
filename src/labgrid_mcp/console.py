"""Interactive serial-console sessions over MCP (DESIGN §11.10).

A :class:`ConsoleRegistry` owns one session per place. Each session holds a
labgrid ``SerialDriver`` (bound + activated by :meth:`TargetManager.console_driver`
on the place's cached Target), a background reader thread pumping the driver's
UNDECORATED ``_read`` into a bounded byte ring, and lifecycle state. An idle-TTL
sweeper closes sessions that go untouched.

**The load-bearing constraint (DESIGN §11.10): never touch the public,
``@step``-decorated ``read``/``write``.** labgrid's ``@step`` instrumentation is
a process-global, thread-unsafe stack; a long-lived reader on the public API
corrupts it for every other driver call in the process (reproduced crash). The
reader and :meth:`send` therefore use ``driver._read`` / ``driver._write``
exclusively. Those bypass ``@Driver.check_active`` too, so this registry owns the
open/closed guard and the stop-reader-BEFORE-deactivate ordering.

**Reader mechanism — a dedicated ``threading.Thread``, not ``asyncio.to_thread``.**
A console reader blocks for the whole session lifetime (minutes to hours). A
``to_thread`` task would pin a slot in the shared default ``ThreadPoolExecutor``
that every other ``to_thread`` driver op (power/io/mux, target build/teardown)
draws from; a handful of open consoles would starve it and stall unrelated
places. A dedicated per-session thread is owned outright, stays cancel-responsive
via a ``threading.Event`` + the short (0.1 s) ``_read`` timeout, and is joined
deterministically (bounded) on close. It is a daemon as a backstop so a wedged
reader can never block interpreter exit.

Verified against labgrid 26.0. Re-verify when the pin moves.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pexpect  # type: ignore[import-untyped]  # untyped labgrid dependency (§11.10)

from labgrid_mcp.ringlog import ByteRing, Sweeper

if TYPE_CHECKING:
    from labgrid_mcp.config import Config
    from labgrid_mcp.coordinator import CoordinatorClient
    from labgrid_mcp.target import TargetManager

# Bounded ring per session; overflow drops OLDEST and flags truncation (§3.4).
CONSOLE_RING_BYTES = 65536
# Idle sessions older than this are swept closed; sweep runs on this cadence.
CONSOLE_TTL_S = 600.0
CONSOLE_SWEEP_INTERVAL_S = 30.0

# Reader pump params (§11.10 recommended): drain up to _READ_MAX_SIZE bytes,
# returning within _READ_TIMEOUT so the stop-event stays responsive.
_READ_SIZE = 1
_READ_TIMEOUT = 0.1
_READ_MAX_SIZE = 4096
# Bounded join so a wedged reader can never hang close/shutdown.
_JOIN_TIMEOUT_S = 5.0

# Clock seams (patched in tests to drive TTL deterministically), mirroring
# session.py's convention.
_sleep = asyncio.sleep
_monotonic = time.monotonic

_logger = logging.getLogger(__name__)


class ConsoleError(Exception):
    """A console-session operation failed (unknown/closed session, etc.)."""


@dataclass
class _Session:
    """One console session: driver + reader thread + bounded byte ring.

    ``lock`` guards every field the reader thread and the API touch
    concurrently (``ring``, ``state``, ``error``, ``last_used``/``last_used_at``).
    ``created``/``last_used`` are monotonic (interval math, TTL sweep);
    ``created_at``/``last_used_at`` are the wall-clock (epoch seconds)
    counterparts captured at the same instants, for display/logging.
    """

    id: str
    place: str
    driver: Any
    created: float
    created_at: float
    last_used: float
    last_used_at: float
    stop: threading.Event = field(default_factory=threading.Event)
    ring: ByteRing = field(default_factory=lambda: ByteRing(CONSOLE_RING_BYTES))
    thread: threading.Thread | None = None
    state: str = "open"  # "open" | "error"
    error: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class ConsoleRegistry:
    """Owns console sessions keyed by session id, one per place (§11.10).

    All structural mutations (open/close/close_place/shutdown/sweep) are
    serialized by a single ``asyncio.Lock`` so the one-session-per-place
    invariant holds even across the ``console_driver`` await. :meth:`read` and
    :meth:`sessions` are lock-free snapshots (dict lookups are atomic; each
    session's own ``threading.Lock`` guards its contents).
    """

    def __init__(self, targets: TargetManager, client: CoordinatorClient, config: Config) -> None:
        self._targets = targets
        self._client = client
        self._config = config
        self._sessions: dict[str, _Session] = {}
        self._by_place: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._sweeper: asyncio.Task[None] | None = None
        # interval_fn/sweep_fn are looked up FRESH each iteration (not captured
        # here), so monkeypatching CONSOLE_SWEEP_INTERVAL_S or an instance's
        # bound _sweep_once after construction still takes effect (§ringlog).
        self._sweep_helper = Sweeper(
            interval_fn=lambda: CONSOLE_SWEEP_INTERVAL_S,
            sweep_fn=lambda: self._sweep_once(),
            sleep=_sleep,
            logger=_logger,
            log_message="console TTL sweep failed; retrying next interval",
        )

    # ---- public API -----------------------------------------------------

    async def open(self, place: str) -> dict[str, object]:
        """Open a console on ``place`` and start its reader. One per place.

        Held under ``self._lock`` across the ``console_driver`` await so two
        concurrent opens on the same place cannot both pass the existence
        check. ``console_driver`` re-checks ownership and raises ``TargetError``
        for an unknown/unacquired place (propagated to the caller).
        """
        async with self._lock:
            existing = self._by_place.get(place)
            if existing is not None:
                raise ConsoleError(
                    f"place {place!r} already has console session {existing!r}; "
                    "close it before opening another"
                )
            driver = await self._targets.console_driver(place)
            session_id = uuid4().hex[:12]
            now = _monotonic()
            now_wall = time.time()
            s = _Session(
                id=session_id,
                place=place,
                driver=driver,
                created=now,
                created_at=now_wall,
                last_used=now,
                last_used_at=now_wall,
            )
            s.thread = threading.Thread(
                target=self._pump, args=(s,), name=f"console-{session_id}", daemon=True
            )
            self._sessions[session_id] = s
            self._by_place[place] = session_id
            s.thread.start()
            self._ensure_sweeper()
            return {"session": session_id, "place": place}

    def read(self, session: str, max_bytes: int | None = None) -> dict[str, object]:
        """Drain up to ``max_bytes`` (default all) buffered bytes, consuming them.

        The ring stores bytes and decoding happens here with
        ``errors="replace"``, so the reader's arbitrary chunk boundaries never
        corrupt a full drain. A ``max_bytes`` partial drain, however, cuts at a
        byte count and CAN split a multibyte UTF-8 sequence — the split
        character decodes as U+FFFD on each side of the boundary. Resets the
        truncated flag it reports. When the reader has failed, surfaces
        ``"state": "error"`` + the message ALONGSIDE any bytes still buffered.
        """
        s = self._require(session)
        with s.lock:
            chunk, truncated = s.ring.drain(max_bytes)
            s.last_used = _monotonic()
            s.last_used_at = time.time()
            state, error = s.state, s.error
        result: dict[str, object] = {
            "session": session,
            "data": chunk.decode("utf-8", errors="replace"),
            "bytes": len(chunk),
            "truncated": truncated,
        }
        if state == "error":
            result["state"] = "error"
            if error is not None:
                result["error"] = error
        return result

    async def send(self, session: str, data: str, newline: bool = False) -> dict[str, object]:
        """Write ``data`` (plus ``"\\n"`` when ``newline``) to the console.

        Uses the undecorated ``_write`` (bypasses ``@step``), off the loop. An
        errored or closed session refuses the write.
        """
        s = self._require(session)
        if s.state == "error":
            raise ConsoleError(f"console session {session!r} is in error state: {s.error}")
        payload = (data + "\n") if newline else data
        try:
            n = await asyncio.to_thread(s.driver._write, payload.encode("utf-8"))
        except Exception as exc:
            # A write racing close/close_place/shutdown/the TTL sweep hits a
            # deactivated driver and raises a raw pyserial/socket error; any
            # driver failure must surface as ConsoleError, cause attached.
            if self._sessions.get(session) is not s:
                raise ConsoleError(
                    f"console session {session!r} was closed during send: {exc}"
                ) from exc
            raise ConsoleError(f"console send on session {session!r} failed: {exc}") from exc
        with s.lock:
            s.last_used = _monotonic()
            s.last_used_at = time.time()
        return {"session": session, "bytes_written": int(n)}

    async def close(self, session: str) -> dict[str, object]:
        """Stop + join the reader, deactivate the driver, drop the session.

        Unknown session → :class:`ConsoleError`. The teardown itself is
        idempotent: :meth:`close_place`, :meth:`shutdown`, and the sweeper all
        route through the same path and never double-deactivate a session that
        is already gone.
        """
        async with self._lock:
            if session not in self._sessions:
                raise ConsoleError(f"unknown console session {session!r}")
            await self._close_locked(session)
        return {"closed": session}

    async def close_place(self, place: str) -> None:
        """Close ``place``'s console if it has one; no-op otherwise."""
        async with self._lock:
            session = self._by_place.get(place)
            if session is not None:
                await self._close_locked(session)

    def sessions(self) -> list[dict[str, object]]:
        """``labgrid://sessions`` payload: one dict per live session.

        ``created``/``last_used`` are monotonic seconds (interval math);
        ``created_at``/``last_used_at`` are the epoch-seconds wall-clock
        counterparts captured at the same instants (additive fields).
        """
        out: list[dict[str, object]] = []
        for s in list(self._sessions.values()):
            with s.lock:
                out.append(
                    {
                        "session": s.id,
                        "place": s.place,
                        "state": s.state,
                        "created": s.created,
                        "created_at": s.created_at,
                        "last_used": s.last_used,
                        "last_used_at": s.last_used_at,
                        "buffered_bytes": len(s.ring),
                    }
                )
        return out

    async def shutdown(self) -> None:
        """Cancel the sweeper and close every session. Idempotent."""
        task = self._sweeper
        self._sweeper = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            for session in list(self._sessions):
                await self._close_locked(session)

    # ---- internals ------------------------------------------------------

    def _require(self, session: str) -> _Session:
        s = self._sessions.get(session)
        if s is None:
            raise ConsoleError(f"unknown console session {session!r}")
        return s

    async def _close_locked(self, session: str) -> None:
        """Tear a session down. Caller holds ``self._lock``.

        Ordering is load-bearing (§11.10): stop the reader and JOIN it before
        deactivating the driver, because deactivate closes the socket and a
        reader mid-``_read`` on a closed socket raises.
        """
        s = self._sessions.pop(session, None)
        if s is None:
            return
        if self._by_place.get(s.place) == session:
            del self._by_place[s.place]
        await asyncio.to_thread(self._stop_and_join, s)
        with contextlib.suppress(Exception):
            await self._targets.release_console_driver(s.place)

    @staticmethod
    def _stop_and_join(s: _Session) -> None:
        s.stop.set()
        if s.thread is not None:
            s.thread.join(timeout=_JOIN_TIMEOUT_S)

    def _pump(self, s: _Session) -> None:
        """Reader thread: pump ``driver._read`` into the ring until stopped.

        Uses the undecorated ``_read`` (never the ``@step`` public ``read``,
        §11.10). ``pexpect.TIMEOUT`` is the idle signal → continue; any other
        exception (socket died, place lost) puts the session in ``"error"``
        state and ends the thread — buffered bytes stay readable.
        """
        driver = s.driver
        while not s.stop.is_set():
            try:
                data = driver._read(size=_READ_SIZE, timeout=_READ_TIMEOUT, max_size=_READ_MAX_SIZE)
            except pexpect.TIMEOUT:
                continue
            except Exception as exc:  # noqa: BLE001 - any read failure -> error state
                with s.lock:
                    s.state = "error"
                    s.error = str(exc) or type(exc).__name__
                return
            with s.lock:
                s.ring.extend(data)

    def _ensure_sweeper(self) -> None:
        self._sweeper = self._sweep_helper.ensure_running(self._sweeper)

    async def _sweep_once(self) -> None:
        """Close every session idle for at least ``CONSOLE_TTL_S`` (like close)."""
        async with self._lock:
            now = _monotonic()
            expired = [
                sid for sid, s in self._sessions.items() if now - s.last_used >= CONSOLE_TTL_S
            ]
            for sid in expired:
                await self._close_locked(sid)
