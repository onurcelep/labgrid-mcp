"""Tests for ConsoleRegistry against a fake driver + fake TargetManager.

No gRPC / exporter / real ``SerialDriver``: :meth:`TargetManager.console_driver`
is faked to return a :class:`FakeDriver` whose ``_read``/``_write`` are scripted,
so the reader thread, ring buffer, TTL sweep, and lifecycle are exercised in
isolation (DESIGN §11.10). Clocks are patched via the ``_monotonic`` seam.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from collections.abc import Callable

import pexpect  # type: ignore[import-untyped]  # untyped labgrid dependency (§11.10)
import pytest

from labgrid_mcp import console as console_mod
from labgrid_mcp.console import CONSOLE_TTL_S, ConsoleError, ConsoleRegistry


class FakeDriver:
    """Scripted console driver: ``_read`` emits queued bytes then idles/fails."""

    def __init__(self) -> None:
        self._queue: deque[bytes] = deque()
        self._lock = threading.Lock()
        self._error: BaseException | None = None
        self.writes: list[bytes] = []

    def feed(self, data: bytes) -> None:
        with self._lock:
            self._queue.append(data)

    def fail(self, exc: BaseException) -> None:
        with self._lock:
            self._error = exc

    def _read(self, size: int = 1, timeout: float = 0.0, max_size: int | None = None) -> bytes:
        with self._lock:
            if self._queue:
                return self._queue.popleft()
            err = self._error
        if err is not None:
            raise err
        time.sleep(min(timeout, 0.01))  # idle: don't busy-spin
        raise pexpect.TIMEOUT("idle")

    def _write(self, data: bytes) -> int:
        self.writes.append(data)
        return len(data)


class FakeTargets:
    """Stands in for TargetManager: hands out FakeDrivers, records releases."""

    def __init__(self) -> None:
        self.drivers: dict[str, FakeDriver] = {}
        self.released: dict[str, int] = {}
        self.open_error: BaseException | None = None

    async def console_driver(self, place: str) -> FakeDriver:
        if self.open_error is not None:
            raise self.open_error
        drv = self.drivers.get(place)
        if drv is None:
            drv = FakeDriver()
            self.drivers[place] = drv
        return drv

    async def release_console_driver(self, place: str) -> None:
        self.released[place] = self.released.get(place, 0) + 1


def make_registry(targets: FakeTargets) -> ConsoleRegistry:
    # client/config are unused by the registry (ownership is enforced inside
    # console_driver); pass inert placeholders.
    return ConsoleRegistry(targets, object(), object())  # type: ignore[arg-type]


async def wait_true(pred: Callable[[], bool], timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.005)
    return pred()


async def open_session(reg: ConsoleRegistry, place: str) -> str:
    """``reg.open(place)``, narrowed to the session id string.

    ``open()`` returns ``dict[str, object]``; every caller below immediately
    needs the "session" value as ``str`` for ``reg.read``/``send``/``close``/
    ``buffered``/``_sessions`` indexing -- narrowed here ONCE instead of an
    ``isinstance`` assert at each of this file's call sites.
    """
    session = (await reg.open(place))["session"]
    assert isinstance(session, str)
    return session


def buffered(reg: ConsoleRegistry, session: str) -> int:
    return len(reg._sessions[session].ring)


# ---- open + reader --------------------------------------------------------


async def test_open_returns_handle_and_reader_captures_bytes() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    handle = await reg.open("p")
    assert set(handle) == {"session", "place"}
    assert handle["place"] == "p"
    session = handle["session"]
    assert isinstance(session, str) and len(session) == 12

    targets.drivers["p"].feed(b"hello-console")
    assert await wait_true(lambda: buffered(reg, session) > 0)
    assert reg.read(session)["data"] == "hello-console"

    await reg.shutdown()


async def test_second_open_same_place_errors_naming_existing() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    with pytest.raises(ConsoleError, match=session):
        await reg.open("p")

    await reg.shutdown()


async def test_open_propagates_console_driver_error() -> None:
    targets = FakeTargets()
    targets.open_error = RuntimeError("not acquired by this server")
    reg = make_registry(targets)

    with pytest.raises(RuntimeError, match="not acquired"):
        await reg.open("p")
    assert reg.sessions() == []  # nothing registered on failure

    await reg.shutdown()


# ---- ring buffer / read draining ------------------------------------------


async def test_ring_overflow_drops_oldest_and_sets_then_clears_truncated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(console_mod, "CONSOLE_RING_BYTES", 8)
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    targets.drivers["p"].feed(b"0123456789")  # 10 bytes into an 8-byte ring
    assert await wait_true(lambda: buffered(reg, session) == 8)

    first = reg.read(session)
    assert first["data"] == "23456789"  # oldest two dropped
    assert first["truncated"] is True
    assert reg.read(session)["truncated"] is False  # flag cleared after read

    await reg.shutdown()


async def test_read_drains_then_second_read_is_empty() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    targets.drivers["p"].feed(b"hello")
    assert await wait_true(lambda: buffered(reg, session) == 5)
    assert reg.read(session)["data"] == "hello"

    empty = reg.read(session)
    assert empty["data"] == "" and empty["bytes"] == 0

    await reg.shutdown()


async def test_read_max_bytes_partial_drain_leaves_remainder() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    targets.drivers["p"].feed(b"hello")
    assert await wait_true(lambda: buffered(reg, session) == 5)

    part = reg.read(session, max_bytes=2)
    assert part["data"] == "he" and part["bytes"] == 2
    assert reg.read(session)["data"] == "llo"

    await reg.shutdown()


# ---- send -----------------------------------------------------------------


async def test_send_writes_bytes_and_newline_variant() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    plain = await reg.send(session, "abc")
    assert plain == {"session": session, "bytes_written": 3}

    line = await reg.send(session, "cmd", newline=True)
    assert line["bytes_written"] == 4
    assert targets.drivers["p"].writes == [b"abc", b"cmd\n"]

    await reg.shutdown()


async def test_send_driver_failure_wraps_console_error() -> None:
    class BoomWriteDriver(FakeDriver):
        def _write(self, data: bytes) -> int:
            raise RuntimeError("serial gone")

    targets = FakeTargets()
    targets.drivers["p"] = BoomWriteDriver()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    with pytest.raises(ConsoleError, match="serial gone") as excinfo:
        await reg.send(session, "x")
    assert isinstance(excinfo.value.__cause__, RuntimeError)  # cause attached

    await reg.shutdown()


# ---- unknown / closed sessions --------------------------------------------


async def test_read_and_send_on_unknown_session_error() -> None:
    reg = make_registry(FakeTargets())
    with pytest.raises(ConsoleError, match="unknown console session"):
        reg.read("nope")
    with pytest.raises(ConsoleError, match="unknown console session"):
        await reg.send("nope", "x")


async def test_read_and_send_on_closed_session_error() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")
    await reg.close(session)

    with pytest.raises(ConsoleError, match="unknown console session"):
        reg.read(session)
    with pytest.raises(ConsoleError, match="unknown console session"):
        await reg.send(session, "x")

    await reg.shutdown()


# ---- reader failure -> error state ----------------------------------------


async def test_reader_error_surfaces_data_and_refuses_send() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")
    drv = targets.drivers["p"]

    drv.feed(b"partial")
    drv.fail(RuntimeError("bridge died"))
    assert await wait_true(lambda: reg._sessions[session].state == "error")

    result = reg.read(session)
    assert result["data"] == "partial"  # buffered bytes still surface
    assert result["state"] == "error"
    assert "bridge died" in str(result["error"])

    with pytest.raises(ConsoleError, match="error state"):
        await reg.send(session, "x")

    await reg.shutdown()


# ---- TTL sweep ------------------------------------------------------------


async def test_ttl_sweep_closes_idle_but_not_recently_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(console_mod, "_monotonic", lambda: clock[0])
    targets = FakeTargets()
    reg = make_registry(targets)

    idle = await open_session(reg, "idle")
    active = await open_session(reg, "active")

    clock[0] = 100.0
    reg.read(active)  # bumps last_used -> stays alive past the sweep

    clock[0] = CONSOLE_TTL_S + 50.0  # idle age 650, active age 550
    await reg._sweep_once()

    assert idle not in reg._sessions
    assert targets.released.get("idle") == 1
    assert active in reg._sessions  # recently used -> survived
    assert targets.released.get("active") is None

    await reg.shutdown()


async def test_sweeper_survives_a_failing_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising _sweep_once must not kill the sweeper loop (fix round 1)."""
    monkeypatch.setattr(console_mod, "CONSOLE_SWEEP_INTERVAL_S", 0.005)
    clock = [0.0]
    monkeypatch.setattr(console_mod, "_monotonic", lambda: clock[0])
    targets = FakeTargets()
    reg = make_registry(targets)

    calls = {"n": 0}
    real_sweep = reg._sweep_once

    async def flaky_sweep() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("sweep boom")
        await real_sweep()

    monkeypatch.setattr(reg, "_sweep_once", flaky_sweep)

    session = await open_session(reg, "p")
    clock[0] = CONSOLE_TTL_S + 1.0  # idle past TTL
    # First sweep raises; the loop must survive and a later sweep still closes
    # (wait for the driver release, the LAST step of the close path).
    assert await wait_true(lambda: targets.released.get("p") == 1)
    assert calls["n"] >= 2
    assert session not in reg._sessions

    await reg.shutdown()


async def test_send_also_refreshes_last_used(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [0.0]
    monkeypatch.setattr(console_mod, "_monotonic", lambda: clock[0])
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")

    clock[0] = 100.0
    await reg.send(session, "keepalive")
    clock[0] = CONSOLE_TTL_S + 50.0  # age since last_used = 550 < TTL
    await reg._sweep_once()

    assert session in reg._sessions

    await reg.shutdown()


# ---- close / close_place / shutdown ---------------------------------------


async def test_close_joins_reader_deactivates_once_idempotent() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    session = await open_session(reg, "p")
    thread = reg._sessions[session].thread

    assert await reg.close(session) == {"closed": session}
    assert targets.released["p"] == 1
    assert thread is not None and not thread.is_alive()  # reader joined
    assert reg.sessions() == []

    # Idempotent teardown: shutdown / close_place after close do not re-release.
    await reg.shutdown()
    await reg.close_place("p")
    assert targets.released["p"] == 1

    # A genuinely unknown (already-closed) session id -> error.
    with pytest.raises(ConsoleError, match="unknown console session"):
        await reg.close(session)


async def test_close_place_targets_only_that_place() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    keep = await open_session(reg, "keep")
    await reg.open("drop")

    await reg.close_place("drop")
    assert targets.released.get("drop") == 1
    assert targets.released.get("keep") is None
    assert [s["session"] for s in reg.sessions()] == [keep]

    await reg.close_place("never-opened")  # no-op, no error

    await reg.shutdown()


async def test_shutdown_closes_all_and_is_idempotent() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open("p")
    await reg.open("q")

    await reg.shutdown()
    assert reg.sessions() == []
    assert targets.released == {"p": 1, "q": 1}
    assert reg._sweeper is None

    await reg.shutdown()  # idempotent: no error, nothing to do
    assert targets.released == {"p": 1, "q": 1}


async def test_sessions_payload_shape_includes_wall_clock_fields() -> None:
    """Additive wall-clock fields (created_at/last_used_at) alongside the
    existing monotonic created/last_used, captured at the same instants."""
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open("p")

    entry = reg.sessions()[0]
    assert set(entry) == {
        "session",
        "place",
        "state",
        "created",
        "created_at",
        "last_used",
        "last_used_at",
        "buffered_bytes",
    }
    assert isinstance(entry["created_at"], float) and isinstance(entry["last_used_at"], float)

    await reg.shutdown()


def test_module_constants_match_spec() -> None:
    # Guardrails from the plan's Global Constraints (§3.4).
    assert console_mod.CONSOLE_RING_BYTES == 65536
    assert console_mod.CONSOLE_TTL_S == 600.0
    assert console_mod.CONSOLE_SWEEP_INTERVAL_S == 30.0
