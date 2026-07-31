"""Tests for ForwardRegistry against a fake SSHDriver + fake TargetManager.

No gRPC / exporter / real ``SSHDriver``: :meth:`TargetManager.ssh_driver` is
faked to return a :class:`FakeDriver` whose ``forward_local_port`` is a
context manager (mirroring labgrid 26.0's real ``@contextmanager`` shape, see
forwards.py) that records enter/exit, so the tunnel map, TTL sweep, and
lifecycle are exercised in isolation (§11.13). Clocks are patched via the
``_monotonic`` seam, exactly like test_console.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable, Iterator

import pytest

from labgrid_mcp import forwards as forwards_mod
from labgrid_mcp.forwards import FORWARD_TTL_S, ForwardError, ForwardRegistry


class FakeDriver:
    """Scripted SSH driver: ``forward_local_port``/``forward_remote_port`` are
    context managers.

    Records every (remote_port, local_port) it opens and every one it closes,
    so open/close ordering and auto-assignment are verifiable. ``next_port``
    stands in for labgrid's ``get_free_port()`` when the caller passes no
    explicit local port (``forward_local_port`` only -- ``forward_remote_port``
    has no auto-assign, §11.14). Remote opens/closes are tracked separately
    (``remote_opened``/``remote_closed``) so tests can distinguish direction.
    """

    def __init__(self) -> None:
        self.opened: list[tuple[int, int]] = []
        self.closed: list[tuple[int, int]] = []
        self.remote_opened: list[tuple[int, int]] = []
        self.remote_closed: list[tuple[int, int]] = []
        self.next_port = 40000
        self.open_error: BaseException | None = None

    @contextlib.contextmanager
    def forward_local_port(self, remoteport: int, localport: int | None = None) -> Iterator[int]:
        if self.open_error is not None:
            raise self.open_error
        if localport is None:
            localport = self.next_port
            self.next_port += 1
        self.opened.append((remoteport, localport))
        try:
            yield localport
        finally:
            self.closed.append((remoteport, localport))

    @contextlib.contextmanager
    def forward_remote_port(self, remoteport: int, localport: int) -> Iterator[None]:
        if self.open_error is not None:
            raise self.open_error
        self.remote_opened.append((remoteport, localport))
        try:
            yield  # forward_remote_port yields nothing (§11.14)
        finally:
            self.remote_closed.append((remoteport, localport))


class FakeTargets:
    """Stands in for TargetManager: hands out FakeDrivers per place."""

    def __init__(self) -> None:
        self.drivers: dict[str, FakeDriver] = {}
        self.ssh_error: BaseException | None = None

    async def ssh_driver(self, place: str) -> FakeDriver:
        if self.ssh_error is not None:
            raise self.ssh_error
        drv = self.drivers.get(place)
        if drv is None:
            drv = FakeDriver()
            self.drivers[place] = drv
        return drv


def make_registry(targets: FakeTargets) -> ForwardRegistry:
    # client/config are unused by the registry (ownership is enforced inside
    # ssh_driver); pass inert placeholders, like test_console.py.
    return ForwardRegistry(targets, object(), object())  # type: ignore[arg-type]


async def wait_true(pred: Callable[[], bool], timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.005)
    return pred()


async def open_forward(
    reg: ForwardRegistry, place: str, remote_port: int, local_port: int = 0
) -> str:
    handle = await reg.open(place, remote_port, local_port)
    fid = handle["forward"]
    assert isinstance(fid, str)
    return fid


# ---- open -----------------------------------------------------------------


async def test_open_returns_handle_and_drives_context_manager() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    handle = await reg.open("p", 8080)
    assert set(handle) == {"forward", "place", "local_port", "remote_port"}
    assert handle["place"] == "p"
    assert handle["remote_port"] == 8080
    fid = handle["forward"]
    assert isinstance(fid, str) and len(fid) == 12
    local = handle["local_port"]
    assert local == 40000  # auto-assigned (local_port=0 -> None -> get_free_port)
    assert targets.drivers["p"].opened == [(8080, 40000)]

    await reg.shutdown()


async def test_open_honours_explicit_local_port() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    handle = await reg.open("p", 22, local_port=2222)
    assert handle["local_port"] == 2222
    assert targets.drivers["p"].opened == [(22, 2222)]

    await reg.shutdown()


async def test_multiple_forwards_per_place_allowed() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    a = await open_forward(reg, "p", 8080)
    b = await open_forward(reg, "p", 9090)

    assert a != b
    assert {f["forward"] for f in reg.sessions()} == {a, b}
    assert targets.drivers["p"].opened == [(8080, 40000), (9090, 40001)]

    await reg.shutdown()


async def test_open_propagates_ssh_driver_error() -> None:
    targets = FakeTargets()
    targets.ssh_error = RuntimeError("not acquired by this server")
    reg = make_registry(targets)

    with pytest.raises(RuntimeError, match="not acquired"):
        await reg.open("p", 8080)
    assert reg.sessions() == []  # nothing registered on failure

    await reg.shutdown()


async def test_open_wraps_forward_failure_in_forward_error() -> None:
    targets = FakeTargets()
    drv = FakeDriver()
    drv.open_error = RuntimeError("ssh -O forward exited 255")
    targets.drivers["p"] = drv
    reg = make_registry(targets)

    with pytest.raises(ForwardError, match="ssh -O forward exited 255") as excinfo:
        await reg.open("p", 8080)
    assert isinstance(excinfo.value.__cause__, RuntimeError)  # cause attached
    assert reg.sessions() == []

    await reg.shutdown()


# ---- close / close_place / shutdown ---------------------------------------


async def test_close_cancels_tunnel_and_is_reported() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    fid = await open_forward(reg, "p", 8080)

    assert await reg.close(fid) == {"closed": fid}
    assert targets.drivers["p"].closed == [(8080, 40000)]
    assert reg.sessions() == []

    # A genuinely unknown (already-closed) id -> error.
    with pytest.raises(ForwardError, match="unknown forward"):
        await reg.close(fid)

    await reg.shutdown()


async def test_close_unknown_forward_errors() -> None:
    reg = make_registry(FakeTargets())
    with pytest.raises(ForwardError, match="unknown forward"):
        await reg.close("nope")


async def test_close_place_closes_all_tunnels_of_that_place_only() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open("drop", 80)
    await reg.open("drop", 443)
    keep = await open_forward(reg, "keep", 22)

    await reg.close_place("drop")

    assert [f["forward"] for f in reg.sessions()] == [keep]
    assert len(targets.drivers["drop"].closed) == 2
    assert targets.drivers["keep"].closed == []

    await reg.close_place("never-opened")  # no-op, no error

    await reg.shutdown()


async def test_shutdown_closes_all_and_is_idempotent() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open("p", 8080)
    await reg.open("q", 9090)

    await reg.shutdown()
    assert reg.sessions() == []
    assert targets.drivers["p"].closed == [(8080, 40000)]
    assert targets.drivers["q"].closed == [(9090, 40000)]
    assert reg._sweeper is None

    await reg.shutdown()  # idempotent: no error, nothing to do


async def test_exit_swallows_cancel_errors() -> None:
    """A raising __exit__ (torn-down master) must not break close."""

    class BadExitDriver(FakeDriver):
        @contextlib.contextmanager
        def forward_local_port(
            self, remoteport: int, localport: int | None = None
        ) -> Iterator[int]:
            port = localport if localport is not None else self.next_port
            self.opened.append((remoteport, port))
            try:
                yield port
            finally:
                raise RuntimeError("master socket gone")

    targets = FakeTargets()
    targets.drivers["p"] = BadExitDriver()
    reg = make_registry(targets)
    fid = await open_forward(reg, "p", 8080)

    assert await reg.close(fid) == {"closed": fid}  # swallowed, still reported
    assert reg.sessions() == []

    await reg.shutdown()


# ---- TTL sweep ------------------------------------------------------------


async def test_ttl_sweep_closes_tunnels_past_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [0.0]
    monkeypatch.setattr(forwards_mod, "_monotonic", lambda: clock[0])
    targets = FakeTargets()
    reg = make_registry(targets)

    old = await open_forward(reg, "old", 80)
    clock[0] = FORWARD_TTL_S - 100.0
    fresh = await open_forward(reg, "fresh", 443)  # opened later -> younger

    clock[0] = FORWARD_TTL_S + 50.0  # old age > TTL, fresh age 150 < TTL
    await reg._sweep_once()

    assert old not in {f["forward"] for f in reg.sessions()}
    assert targets.drivers["old"].closed == [(80, 40000)]
    assert fresh in {f["forward"] for f in reg.sessions()}
    assert targets.drivers["fresh"].closed == []

    await reg.shutdown()


async def test_sweeper_survives_a_failing_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising _sweep_once must not kill the sweeper loop (mirrors console.py)."""
    monkeypatch.setattr(forwards_mod, "FORWARD_SWEEP_INTERVAL_S", 0.005)
    clock = [0.0]
    monkeypatch.setattr(forwards_mod, "_monotonic", lambda: clock[0])
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

    await reg.open("p", 8080)
    clock[0] = FORWARD_TTL_S + 1.0  # past TTL
    # First sweep raises; the loop must survive and a later sweep still closes.
    assert await wait_true(lambda: targets.drivers["p"].closed == [(8080, 40000)])
    assert calls["n"] >= 2
    assert reg.sessions() == []

    await reg.shutdown()


# ---- sessions payload -----------------------------------------------------


async def test_sessions_payload_shape() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open("p", 8080, local_port=1234)

    entry = reg.sessions()[0]
    assert set(entry) == {
        "forward",
        "place",
        "local_port",
        "remote_port",
        "direction",
        "created",
        "created_at",
        "last_used",
        "last_used_at",
    }
    assert entry["local_port"] == 1234
    assert entry["remote_port"] == 8080
    assert entry["direction"] == "local"
    assert isinstance(entry["created_at"], float) and isinstance(entry["last_used_at"], float)

    await reg.shutdown()


# ---- open_remote (-R, §11.14) ---------------------------------------------


async def test_open_remote_returns_handle_and_drives_context_manager() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    handle = await reg.open_remote("p", 8080, 8081)
    assert set(handle) == {"forward", "place", "direction", "remote_port", "local_port"}
    assert handle["place"] == "p"
    assert handle["direction"] == "remote"
    assert handle["remote_port"] == 8080
    assert handle["local_port"] == 8081
    fid = handle["forward"]
    assert isinstance(fid, str) and len(fid) == 12
    assert targets.drivers["p"].remote_opened == [(8080, 8081)]
    # A local-forward-only fake was NOT touched.
    assert targets.drivers["p"].opened == []

    await reg.shutdown()


async def test_open_remote_entry_carries_remote_direction() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    await reg.open_remote("p", 8080, 8081)

    entry = reg.sessions()[0]
    assert entry["direction"] == "remote"
    assert entry["local_port"] == 8081
    assert entry["remote_port"] == 8080

    await reg.shutdown()


async def test_open_remote_propagates_ssh_driver_error() -> None:
    targets = FakeTargets()
    targets.ssh_error = RuntimeError("not acquired by this server")
    reg = make_registry(targets)

    with pytest.raises(RuntimeError, match="not acquired"):
        await reg.open_remote("p", 8080, 8081)
    assert reg.sessions() == []

    await reg.shutdown()


async def test_open_remote_wraps_forward_failure_in_forward_error() -> None:
    targets = FakeTargets()
    drv = FakeDriver()
    drv.open_error = RuntimeError("ssh -O forward -R exited 255")
    targets.drivers["p"] = drv
    reg = make_registry(targets)

    with pytest.raises(ForwardError, match="ssh -O forward -R exited 255") as excinfo:
        await reg.open_remote("p", 8080, 8081)
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert reg.sessions() == []

    await reg.shutdown()


async def test_close_closes_a_remote_tunnel_via_the_same_path() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    handle = await reg.open_remote("p", 8080, 8081)
    fid = handle["forward"]
    assert isinstance(fid, str)

    assert await reg.close(fid) == {"closed": fid}
    assert targets.drivers["p"].remote_closed == [(8080, 8081)]
    assert reg.sessions() == []

    await reg.shutdown()


async def test_local_and_remote_forwards_coexist_on_one_place() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    local = await open_forward(reg, "p", 80)
    remote_handle = await reg.open_remote("p", 443, 8443)
    remote = remote_handle["forward"]
    assert isinstance(remote, str)

    directions = {f["forward"]: f["direction"] for f in reg.sessions()}
    assert directions == {local: "local", remote: "remote"}

    await reg.shutdown()


def test_module_constants_match_spec() -> None:
    # Guardrails from the plan's Global Constraints (§11.13).
    assert forwards_mod.FORWARD_TTL_S == 3600.0
    assert forwards_mod.FORWARD_SWEEP_INTERVAL_S == 30.0
