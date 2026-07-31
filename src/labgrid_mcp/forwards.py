"""Local port-forward tunnels over an acquired place's SSH transport (§11.13).

A :class:`ForwardRegistry` owns forward tunnels keyed by a forward id. Each
tunnel binds an ``SSHDriver`` on the place's cached Target (via
:meth:`TargetManager.ssh_driver`) and drives labgrid's
``SSHDriver.forward_local_port`` to open an ``ssh -O forward`` local forward
through the driver's ControlMaster; a background idle-TTL sweeper closes tunnels
older than :data:`FORWARD_TTL_S`.

**API shape discrepancy (verified against labgrid 26.0).** The plan assumed a
``forward_local_port`` *open* call paired with a separate *cancel* method. In
the installed source ``SSHDriver.forward_local_port`` is instead a
``@contextlib.contextmanager``: ``__enter__`` runs ``ssh -O forward`` (via the
ControlMaster) and yields the chosen local port; ``__exit__`` runs the matching
``ssh -O cancel`` (best-effort, ``stderr`` suppressed). There is no standalone
cancel method. This registry therefore drives the context manager MANUALLY --
it holds the returned context-manager object per tunnel and calls ``__enter__``
on open and ``__exit__`` on close. ``localport=None`` (our ``local_port == 0``)
lets labgrid pick a free port via ``get_free_port()`` and returns it.

**Multiple forwards per place are allowed** (unlike console.py's one-session-
per-place rule): tunnels are cheap ``ssh -O forward`` registrations on the
shared ControlMaster, a place commonly needs several at once (e.g. a web UI plus
a debug port), and each is closed independently by its own id. ``close_place``
closes ALL of a place's tunnels (release-time cleanup).

**Remote forward (-R), §11.14.** :meth:`ForwardRegistry.open_remote` drives
``SSHDriver.forward_remote_port(remoteport, localport)`` -- verified against
labgrid 26.0 the same way as ``forward_local_port``: a
``@contextlib.contextmanager`` whose ``__enter__`` runs ``ssh -O forward -R…``
and ``__exit__`` runs the matching ``ssh -O cancel`` (both share the private
``_forward`` helper, so open/close/TTL-sweep/shutdown all drive it identically
to a local tunnel -- no separate teardown path needed). Unlike the local (-L)
direction, **both ports are required**: labgrid has no ``get_free_port()``
auto-assign for ``-R`` (a connection to ``remoteport`` on the DUT is forwarded
to ``localhost:localport`` on this host, so a local listener must already
exist there -- there is nothing to auto-pick). Each tunnel's ``direction``
(``"local"`` or ``"remote"``) is recorded on its entry and reported by
:meth:`sessions`.

Mirrors :mod:`labgrid_mcp.console`'s registry conventions: a single
``asyncio.Lock`` serializes structural mutations, the ``ringlog.Sweeper`` runs
the TTL loop, ``_sleep``/``_monotonic`` are patchable clock seams, and
``shutdown`` is idempotent.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from labgrid_mcp.ringlog import Sweeper

if TYPE_CHECKING:
    from labgrid_mcp.config import Config
    from labgrid_mcp.coordinator import CoordinatorClient
    from labgrid_mcp.target import TargetManager

# Tunnels untouched for this long are swept closed; sweep runs on this cadence
# (mirrors console.py's TTL/sweep pair). Forwards carry no per-op "touch"
# (there is no read/send), so a tunnel effectively ages from creation.
FORWARD_TTL_S = 3600.0
FORWARD_SWEEP_INTERVAL_S = 30.0

# Clock seams (patched in tests to drive TTL deterministically), mirroring
# console.py's convention.
_sleep = asyncio.sleep
_monotonic = time.monotonic

_logger = logging.getLogger(__name__)


class ForwardError(Exception):
    """A forward-tunnel operation failed (unknown tunnel, open failure, etc.)."""


@dataclass
class _Forward:
    """One forward tunnel: its ports + the driving context manager.

    ``created``/``last_used`` are monotonic (interval math, TTL sweep);
    ``created_at``/``last_used_at`` are the wall-clock (epoch seconds)
    counterparts captured at the same instants, for display/logging. ``cm`` is
    labgrid's ``forward_local_port`` context manager, held open between
    :meth:`ForwardRegistry.open` (``__enter__``) and the eventual close
    (``__exit__``).
    """

    id: str
    place: str
    local_port: int
    remote_port: int
    direction: Literal["local", "remote"]
    created: float
    created_at: float
    last_used: float
    last_used_at: float
    cm: Any


class ForwardRegistry:
    """Owns forward tunnels keyed by forward id (§11.13). Multiple per place.

    All structural mutations (open/close/close_place/shutdown/sweep) are
    serialized by a single ``asyncio.Lock`` so the tunnel map and the driver's
    ControlMaster stay consistent across the ``ssh_driver`` await. :meth:`sessions`
    is a lock-free snapshot (dict iteration over immutable-after-open entries).

    Review note: out-of-band ``TargetManager`` driver deactivation --
    ``invalidate()``'s ``deactivate_all_drivers()`` (target.py), which can run
    for reasons unrelated to forwards (e.g. a stale-place reconnect edge case)
    -- tears down the SSHDriver's ControlMaster underneath any tunnels open on
    it, silently killing them at the OS level. This registry has no hook into
    that path, so the affected entries linger here until their TTL sweep or an
    explicit :meth:`close` call. That staleness is harmless: :meth:`_exit_forward`
    already suppresses every exception from the held context manager's
    ``__exit__``, so cancelling a tunnel whose master is already dead is a
    no-op, not an error -- the entry is simply removed a bit later than the
    tunnel actually died.
    """

    def __init__(self, targets: TargetManager, client: CoordinatorClient, config: Config) -> None:
        self._targets = targets
        self._client = client
        self._config = config
        self._forwards: dict[str, _Forward] = {}
        self._lock = asyncio.Lock()
        self._sweeper: asyncio.Task[None] | None = None
        # interval_fn/sweep_fn are looked up FRESH each iteration (see
        # console.py) so monkeypatching FORWARD_SWEEP_INTERVAL_S or an instance's
        # bound _sweep_once after construction still takes effect (§ringlog).
        self._sweep_helper = Sweeper(
            interval_fn=lambda: FORWARD_SWEEP_INTERVAL_S,
            sweep_fn=lambda: self._sweep_once(),
            sleep=_sleep,
            logger=_logger,
            log_message="forward TTL sweep failed; retrying next interval",
        )

    # ---- public API -----------------------------------------------------

    async def open(self, place: str, remote_port: int, local_port: int = 0) -> dict[str, object]:
        """Open a local forward to ``remote_port`` on ``place``; return its handle.

        ``local_port == 0`` (the default) lets labgrid pick a free local port.
        Held under ``self._lock`` across the ``ssh_driver`` await. ``ssh_driver``
        re-checks ownership and raises ``TargetError`` for an unknown/unacquired
        place or an unset keyfile (propagated to the caller). A failure to
        actually establish the tunnel (``ssh -O forward`` non-zero) surfaces as
        :class:`ForwardError`, cause attached.
        """
        async with self._lock:
            driver = await self._targets.ssh_driver(place)
            try:
                cm, chosen = await asyncio.to_thread(
                    self._open_forward, driver, remote_port, local_port
                )
            except Exception as exc:
                raise ForwardError(
                    f"opening forward to remote port {remote_port} on place {place!r} failed: {exc}"
                ) from exc
            fid = uuid4().hex[:12]
            now = _monotonic()
            now_wall = time.time()
            self._forwards[fid] = _Forward(
                id=fid,
                place=place,
                local_port=chosen,
                remote_port=remote_port,
                direction="local",
                created=now,
                created_at=now_wall,
                last_used=now,
                last_used_at=now_wall,
                cm=cm,
            )
            self._ensure_sweeper()
            return {
                "forward": fid,
                "place": place,
                "local_port": chosen,
                "remote_port": remote_port,
            }

    async def open_remote(self, place: str, remote_port: int, local_port: int) -> dict[str, object]:
        """Open a REMOTE (``-R``) forward from ``remote_port`` on ``place`` to
        ``local_port`` on this host; return its handle (§11.14).

        Unlike :meth:`open`, **both ports are required** -- there is no
        ``local_port=0`` auto-assign for ``-R`` (module docstring). Otherwise
        mirrors :meth:`open` exactly: held under ``self._lock`` across the
        ``ssh_driver`` await, ownership/keyfile errors from ``ssh_driver``
        propagate as ``TargetError``, and a failure to establish the tunnel
        wraps into :class:`ForwardError`.
        """
        async with self._lock:
            driver = await self._targets.ssh_driver(place)
            try:
                cm = await asyncio.to_thread(
                    self._open_remote_forward, driver, remote_port, local_port
                )
            except Exception as exc:
                raise ForwardError(
                    f"opening remote forward from target port {remote_port} to local port "
                    f"{local_port} on place {place!r} failed: {exc}"
                ) from exc
            fid = uuid4().hex[:12]
            now = _monotonic()
            now_wall = time.time()
            self._forwards[fid] = _Forward(
                id=fid,
                place=place,
                local_port=local_port,
                remote_port=remote_port,
                direction="remote",
                created=now,
                created_at=now_wall,
                last_used=now,
                last_used_at=now_wall,
                cm=cm,
            )
            self._ensure_sweeper()
            return {
                "forward": fid,
                "place": place,
                "direction": "remote",
                "remote_port": remote_port,
                "local_port": local_port,
            }

    async def close(self, forward: str) -> dict[str, object]:
        """Close a tunnel by id (``ssh -O cancel``, best-effort). Idempotent path.

        Unknown id → :class:`ForwardError`. :meth:`close_place`, :meth:`shutdown`,
        and the sweeper all route through the same teardown and never
        double-cancel a tunnel that is already gone.
        """
        async with self._lock:
            if forward not in self._forwards:
                raise ForwardError(f"unknown forward {forward!r}")
            await self._close_locked(forward)
        return {"closed": forward}

    async def close_place(self, place: str) -> None:
        """Close EVERY tunnel on ``place`` (release-time cleanup); no-op if none."""
        async with self._lock:
            for fid in [fid for fid, f in self._forwards.items() if f.place == place]:
                await self._close_locked(fid)

    def sessions(self) -> list[dict[str, object]]:
        """``labgrid://sessions`` "forwards" payload: one dict per live tunnel.

        ``created``/``last_used`` are monotonic seconds (interval math);
        ``created_at``/``last_used_at`` are the epoch-seconds wall-clock
        counterparts captured at the same instants.
        """
        return [
            {
                "forward": f.id,
                "place": f.place,
                "local_port": f.local_port,
                "remote_port": f.remote_port,
                "direction": f.direction,
                "created": f.created,
                "created_at": f.created_at,
                "last_used": f.last_used,
                "last_used_at": f.last_used_at,
            }
            for f in list(self._forwards.values())
        ]

    async def shutdown(self) -> None:
        """Cancel the sweeper and close every tunnel. Idempotent."""
        task = self._sweeper
        self._sweeper = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            for fid in list(self._forwards):
                await self._close_locked(fid)

    # ---- internals ------------------------------------------------------

    @staticmethod
    def _open_forward(driver: Any, remote_port: int, local_port: int) -> tuple[Any, int]:
        """Drive labgrid's ``forward_local_port`` context manager's ``__enter__``.

        Runs off the loop (labgrid shells out to ``ssh -O forward``). Returns the
        held context-manager object (for the later ``__exit__``) and the local
        port actually bound (labgrid-chosen when ``local_port == 0``).

        Review note: when ``local_port == 0``, labgrid picks the port via its
        own ``get_free_port()`` (close-then-hand-off) before handing it to
        ``ssh -O forward``'s ``-L`` bind -- there is an inherent TOCTOU window
        between "port observed free" and "port actually bound" in which
        another process on the host could steal it, causing the forward to
        fail to bind. This is upstream labgrid behavior, not something this
        registry can close; a caller that hits it should simply retry
        ``forward_open`` (a fresh ``local_port=0`` picks again).
        """
        cm = driver.forward_local_port(remote_port, local_port or None)
        chosen = cm.__enter__()
        return cm, int(chosen)

    @staticmethod
    def _open_remote_forward(driver: Any, remote_port: int, local_port: int) -> Any:
        """Drive labgrid's ``forward_remote_port`` context manager's ``__enter__``.

        Runs off the loop (labgrid shells out to ``ssh -O forward -R…``).
        Returns the held context-manager object (for the later ``__exit__``).
        Unlike :meth:`_open_forward`, there is no "chosen port" to report --
        ``forward_remote_port`` yields nothing (§11.14: the real source
        contradicts its own docstring, which claims a return value) and both
        ports are caller-supplied, never auto-picked.
        """
        cm = driver.forward_remote_port(remote_port, local_port)
        cm.__enter__()
        return cm

    async def _close_locked(self, forward: str) -> None:
        """Tear one tunnel down (``__exit__`` -> ``ssh -O cancel``). Holds the lock."""
        f = self._forwards.pop(forward, None)
        if f is None:
            return
        await asyncio.to_thread(self._exit_forward, f)

    @staticmethod
    def _exit_forward(f: _Forward) -> None:
        # labgrid's __exit__ already suppresses cancel errors (the master socket
        # may be gone), but a torn-down/racing driver can still raise here --
        # close is best-effort, so swallow anything.
        with contextlib.suppress(Exception):
            f.cm.__exit__(None, None, None)

    def _ensure_sweeper(self) -> None:
        self._sweeper = self._sweep_helper.ensure_running(self._sweeper)

    async def _sweep_once(self) -> None:
        """Close every tunnel untouched for at least ``FORWARD_TTL_S`` (like close)."""
        async with self._lock:
            now = _monotonic()
            expired = [
                fid for fid, f in self._forwards.items() if now - f.last_used >= FORWARD_TTL_S
            ]
            for fid in expired:
                await self._close_locked(fid)
