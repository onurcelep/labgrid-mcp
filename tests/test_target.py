"""Tests for TargetManager against faked labgrid seams (no gRPC/exporter).

The labgrid seams `TargetManager` touches are module attributes in
``labgrid_mcp.target`` (``Target``, ``RemotePlace``, ``_remote_place_manager``,
``_DRIVERS``); these tests patch them with fakes, so no real coordinator,
exporter, or ``ClientSession`` is ever constructed (DESIGN §11.9).
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest
from labgrid.step import step as lg_step

from labgrid_mcp import target as target_mod
from labgrid_mcp.config import Config
from labgrid_mcp.target import TargetError, TargetManager

IDENTITY = "testhost/tester"


def make_config(ssh_keyfile: str | None = None) -> Config:
    return Config(
        coordinator="127.0.0.1:20408",
        hostname="testhost",
        username="tester",
        readonly=False,
        allow=None,
        acquire_timeout=120.0,
        ssh_keyfile=ssh_keyfile,
    )


class FakeInfo:
    """Stands in for ``CoordinatorInfo``: only ``.connected`` is consulted here."""

    def __init__(self, connected: bool) -> None:
        self.connected = connected


class FakeClient:
    """Snapshot surface TargetManager/adapter consume: place() + resources().

    ``connected`` defaults True so every pre-existing test (none of which set
    up connectivity) keeps exercising the "genuinely unknown place" branch.
    """

    def __init__(self) -> None:
        self._places: dict[str, dict[str, Any]] = {}
        self._resources: list[dict[str, Any]] = []
        self.connected = True

    def place(self, name: str) -> dict[str, Any] | None:
        p = self._places.get(name)
        return dict(p) if p is not None else None

    def resources(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self._resources]

    def info(self) -> FakeInfo:
        return FakeInfo(self.connected)


class FakeManager:
    def __init__(self) -> None:
        self.session: Any = None
        self.loop: Any = None
        self.env: Any = "unset-sentinel"
        self.resources: list[Any] = []
        self.unmanaged_resources: list[Any] = []


def owned_client(name: str = "p") -> FakeClient:
    client = FakeClient()
    client._places[name] = {"name": name, "acquired": IDENTITY, "acquired_resources": []}
    return client


class _Wiring:
    def __init__(self) -> None:
        self.targets: list[Any] = []
        self.places: list[Any] = []
        self.manager = FakeManager()
        self.manager_calls = 0


def wire(monkeypatch: pytest.MonkeyPatch, *, drivers: dict[str, Any]) -> _Wiring:
    w = _Wiring()

    class FakeTarget:
        def __init__(self, name: str) -> None:
            self.name = name
            self.activated: list[Any] = []
            self.deactivated_drivers: list[Any] = []
            self.deactivated = 0
            self.binding_maps: list[dict[str, str]] = []
            w.targets.append(self)

        def set_binding_map(self, mapping: dict[str, str]) -> None:
            self.binding_maps.append(mapping)

        def activate(self, drv: Any) -> None:
            self.activated.append(drv)

        def deactivate(self, drv: Any) -> None:
            self.deactivated_drivers.append(drv)

        def deactivate_all_drivers(self) -> None:
            self.deactivated += 1

    class FakeRemotePlace:
        def __init__(self, target: Any, name: str | None = None) -> None:
            self.target = target
            self.name = name
            self.parent = None
            w.places.append(self)

    def fake_rpm() -> FakeManager:
        w.manager_calls += 1
        return w.manager

    monkeypatch.setattr(target_mod, "Target", FakeTarget)
    monkeypatch.setattr(target_mod, "RemotePlace", FakeRemotePlace)
    monkeypatch.setattr(target_mod, "_remote_place_manager", fake_rpm)
    monkeypatch.setattr(target_mod, "_DRIVERS", drivers)
    return w


class FakePowerDriver:
    def __init__(self, target: Any, name: str | None = None) -> None:
        self.state: bool | None = None

    def on(self) -> None:
        self.state = True

    def off(self) -> None:
        self.state = False

    def cycle(self) -> None:
        self.state = True

    def get(self) -> bool:
        return bool(self.state)


# ---- adapter shapes (§11.9 members 2 & 3) ---------------------------------


def test_adapter_get_place_reads_tags_and_acquired_resources() -> None:
    client = FakeClient()
    client._places["p"] = {
        "name": "p",
        "tags": {"board": "x"},
        "acquired": IDENTITY,
        "acquired_resources": [["exp", "grp", "NetworkPowerPort", "port0"]],
    }
    view = target_mod._CoordinatorAdapter(client).get_place("p")
    assert view.name == "p"
    assert view.tags == {"board": "x"}
    assert view.acquired_resources == [["exp", "grp", "NetworkPowerPort", "port0"]]


def test_adapter_get_place_missing_is_empty() -> None:
    view = target_mod._CoordinatorAdapter(FakeClient()).get_place("nope")
    assert view.tags == {}
    assert view.acquired_resources == []


def test_adapter_get_target_resources_keyed_with_args_and_extra() -> None:
    client = FakeClient()
    client._places["p"] = {
        "acquired": IDENTITY,
        "acquired_resources": [["exp", "grp", "NetworkPowerPort", "port0"]],
    }
    client._resources = [
        {
            "exporter": "exp",
            "group": "grp",
            "name": "port0",
            "cls": "NetworkPowerPort",
            "avail": True,
            "params": {
                "index": 0,
                "host": "http://h/{index}",
                "model": "rest",
                "extra": {"proxy_required": False},
            },
        }
    ]
    adapter = target_mod._CoordinatorAdapter(client)
    entries = adapter.get_target_resources(adapter.get_place("p"))
    entry = entries[("port0", "NetworkPowerPort")]
    assert entry.cls == "NetworkPowerPort"
    # extra is dug out of params, not left in args (§11.9).
    assert entry.args == {"index": 0, "host": "http://h/{index}", "model": "rest"}
    assert entry.extra == {"proxy_required": False}
    assert entry.avail is True


# ---- Target caching / build sequence --------------------------------------


async def test_target_built_once_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(owned_client(), make_config())

    await tm.power_state("p")
    await tm.power("p", "on")

    assert len(w.targets) == 1  # second call reused the cached Target
    assert w.manager_calls == 1  # manager not re-invoked (no rebuild)
    # §11.9 wiring: adapter/loop/env set before RemotePlace construction.
    assert w.manager.session is tm._adapter
    assert w.manager.loop.is_running() is True
    assert w.manager.env is None
    assert len(w.places) == 1


async def test_invalidate_drops_cache_and_rebuild_is_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(owned_client(), make_config())

    await tm.power_state("p")
    first = w.targets[0]
    rp = w.places[0]
    # Simulate what labgrid's manager accumulates for this place (§11.9 leak
    # trap): the RemotePlace itself, a managed child (parent=rp) in
    # ``resources``, an unmanaged child (parent=rp), plus a foreign entry that
    # must survive the prune.
    managed_child = type("Res", (), {"parent": rp})()
    unmanaged_child = type("Res", (), {"parent": rp})()
    foreign = type("Res", (), {"parent": None})()
    w.manager.resources = [rp, managed_child, foreign]
    w.manager.unmanaged_resources = [unmanaged_child]

    await tm.invalidate("p")
    assert first.deactivated == 1  # best-effort deactivate
    assert "p" not in tm._targets
    assert w.manager.resources == [foreign]  # rp + its children pruned
    assert w.manager.unmanaged_resources == []

    await tm.power_state("p")
    assert len(w.targets) == 2  # a fresh Target was built
    assert w.targets[1] is not first


# ---- driver execution: thread, mapping, wrapping --------------------------


async def test_driver_call_runs_off_the_loop_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    idents: list[int] = []

    class ThreadProbeDriver:
        def __init__(self, target: Any, name: str | None = None) -> None:
            pass

        def get(self) -> bool:
            idents.append(threading.get_ident())
            return True

    wire(monkeypatch, drivers={"power": ThreadProbeDriver})
    tm = TargetManager(owned_client(), make_config())

    assert await tm.power_state("p") is True
    assert idents and idents[0] != threading.get_ident()


async def test_power_maps_action_then_returns_post_action_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(owned_client(), make_config())

    assert await tm.power("p", "on") is True
    assert await tm.power("p", "off") is False
    assert await tm.power("p", "cycle") is True
    assert await tm.power_state("p") is True


async def test_io_and_mux_dispatch_to_driver_methods(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}

    class FakeIoDriver:
        def __init__(self, target: Any, name: str | None = None) -> None:
            self._v = False

        def set(self, value: bool) -> None:
            calls["io_set"] = value
            self._v = value

        def get(self) -> bool:
            return self._v

    class FakeSdMux:
        def __init__(self, target: Any, name: str | None = None) -> None:
            pass

        def set_mode(self, mode: str) -> None:
            calls["sd_mode"] = mode

    class FakeUsbMux:
        def __init__(self, target: Any, name: str | None = None) -> None:
            pass

        def set_links(self, links: list[str]) -> None:
            calls["usb_links"] = links

    wire(monkeypatch, drivers={"io": FakeIoDriver, "sd_mux": FakeSdMux, "usb_mux": FakeUsbMux})
    tm = TargetManager(owned_client(), make_config())

    await tm.io_set("p", True)
    assert calls["io_set"] is True
    assert await tm.io_get("p") is True
    await tm.sd_mux("p", "dut")
    assert calls["sd_mode"] == "dut"
    await tm.usb_mux("p", ["dut-device", "host-dut"])
    assert calls["usb_links"] == ["dut-device", "host-dut"]


async def test_driver_exception_wraps_targeterror(monkeypatch: pytest.MonkeyPatch) -> None:
    class BoomDriver:
        def __init__(self, target: Any, name: str | None = None) -> None:
            pass

        def get(self) -> bool:
            raise RuntimeError("boom-cause")

    wire(monkeypatch, drivers={"power": BoomDriver})
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(TargetError, match="boom-cause"):
        await tm.power_state("p")


async def test_driver_bind_failure_wraps_targeterror(monkeypatch: pytest.MonkeyPatch) -> None:
    # §11.9 zero-match place: driver instantiation fails to bind -> TargetError,
    # surfaced (not crashed) after the Target was built.
    class NoBindDriver:
        def __init__(self, target: Any, name: str | None = None) -> None:
            raise RuntimeError("no matching resource")

    wire(monkeypatch, drivers={"power": NoBindDriver})
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(TargetError, match="no matching resource"):
        await tm.power_state("p")


# ---- per-place serialization (concurrent ops on one place) ----------------


async def test_concurrent_same_place_ops_are_serialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent ops on one place: one activation, no overlap.

    The gate inside the fake driver holds the first op mid-``get()``; the
    second op must not enter the driver until the first exits (the place lock
    is held across the WHOLE op), and the driver is instantiated+activated
    exactly once.
    """
    entered: list[int] = []
    gate = threading.Event()

    class GateDriver:
        instances = 0

        def __init__(self, target: Any, name: str | None = None) -> None:
            GateDriver.instances += 1
            self.state = False

        def on(self) -> None:
            self.state = True

        def get(self) -> bool:
            entered.append(threading.get_ident())
            assert gate.wait(timeout=5.0)
            return self.state

    w = wire(monkeypatch, drivers={"power": GateDriver})
    tm = TargetManager(owned_client(), make_config())

    t1 = asyncio.ensure_future(tm.power_state("p"))
    while not entered:  # first op is inside the driver, holding the gate
        await asyncio.sleep(0.001)
    t2 = asyncio.ensure_future(tm.power("p", "on"))
    await asyncio.sleep(0.05)
    # Second op must be blocked on the place lock, not inside the driver.
    assert len(entered) == 1
    gate.set()
    assert await t1 is False
    assert await t2 is True
    assert GateDriver.instances == 1  # exactly one instantiation...
    assert len(w.targets) == 1
    assert len(w.targets[0].activated) == 1  # ...and exactly one activation


async def test_manager_mutations_serialized_across_places(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """invalidate(A) concurrent with build(B) must not lose B's manager entry.

    ``_invalidate_sync``'s prune is a read-modify-write reassignment of the
    process-global manager's ``resources``; ``_build_target`` appends to it
    (via RemotePlace construction). These run on DIFFERENT places, so the
    per-place locks do not serialize them — only ``_MANAGER_LOCK`` does.

    The racy manager's ``resources`` setter (hit only by the prune) holds the
    write open until B's build has appended (bounded wait): without the global
    lock, B's append lands inside the prune's read->write window and the
    reassignment silently drops it (verified: removing ``_MANAGER_LOCK`` makes
    this test fail with ``resources == []``). With the lock, B's build blocks
    until the prune completes, the setter's wait just times out, and B's entry
    survives.
    """
    prune_read = threading.Event()  # prune fetched the list (inside its window)
    b_appended = threading.Event()  # B's RemotePlace registered itself

    class RacyManager:
        def __init__(self) -> None:
            self.session: Any = None
            self.loop: Any = None
            self.env: Any = "unset-sentinel"
            self._resources: list[Any] = []
            self.unmanaged_resources: list[Any] = []
            self.armed = False  # only trap the prune's getter/setter

        @property
        def resources(self) -> list[Any]:
            if self.armed:
                prune_read.set()
            return self._resources

        @resources.setter
        def resources(self, value: list[Any]) -> None:
            if self.armed:
                self.armed = False
                b_appended.wait(timeout=0.3)  # race window for B's append
            self._resources = value

    manager = RacyManager()

    class FakeTarget:
        def __init__(self, name: str) -> None:
            self.name = name

        def activate(self, drv: Any) -> None:
            pass

        def deactivate_all_drivers(self) -> None:
            pass

    class RegisteringRemotePlace:
        """Mirrors labgrid: registration appends to the manager's resources."""

        def __init__(self, target: Any, name: str | None = None) -> None:
            self.target = target
            self.name = name
            self.parent = None
            manager.resources.append(self)
            if name == "B":
                b_appended.set()

    monkeypatch.setattr(target_mod, "Target", FakeTarget)
    monkeypatch.setattr(target_mod, "RemotePlace", RegisteringRemotePlace)
    monkeypatch.setattr(target_mod, "_remote_place_manager", lambda: manager)
    monkeypatch.setattr(target_mod, "_DRIVERS", {"power": FakePowerDriver})

    client = FakeClient()
    for name in ("A", "B"):
        client._places[name] = {"name": name, "acquired": IDENTITY, "acquired_resources": []}
    tm = TargetManager(client, make_config())

    await tm.power_state("A")
    assert [r.name for r in manager._resources] == ["A"]

    manager.armed = True
    t_inv = asyncio.ensure_future(tm.invalidate("A"))
    # Only launch B once the prune is inside its read->write window, so B's
    # build must contend with it (and, with the lock, wait it out).
    while not prune_read.is_set():
        await asyncio.sleep(0.001)
    t_b = asyncio.ensure_future(tm.power_state("B"))
    await t_inv
    await t_b

    # A pruned, B's entry NOT lost to the concurrent reassignment.
    assert [r.name for r in manager._resources] == ["B"]


# ---- console driver (§11.10) ----------------------------------------------


class FakeConsoleDriver:
    def __init__(self, target: Any, name: str | None = None) -> None:
        self.target = target


async def test_console_driver_activates_and_returns_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"console": FakeConsoleDriver})
    tm = TargetManager(owned_client(), make_config())

    drv = await tm.console_driver("p")

    assert isinstance(drv, FakeConsoleDriver)
    assert w.targets[0].activated == [drv]  # activated before return
    assert tm._targets["p"].drivers["console"] is drv  # cached under "console"


async def test_console_driver_reuses_cached_target_and_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"console": FakeConsoleDriver})
    tm = TargetManager(owned_client(), make_config())

    first = await tm.console_driver("p")
    second = await tm.console_driver("p")

    assert first is second
    assert len(w.targets) == 1
    assert len(w.targets[0].activated) == 1  # activated exactly once


async def test_console_driver_runs_off_the_loop_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    idents: list[int] = []

    class ThreadProbeConsole:
        def __init__(self, target: Any, name: str | None = None) -> None:
            idents.append(threading.get_ident())

    wire(monkeypatch, drivers={"console": ThreadProbeConsole})
    tm = TargetManager(owned_client(), make_config())

    await tm.console_driver("p")
    assert idents and idents[0] != threading.get_ident()


async def test_console_driver_unowned_raises_before_build(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"console": FakeConsoleDriver})
    client = FakeClient()
    client._places["p"] = {"name": "p", "acquired": "other/user", "acquired_resources": []}
    tm = TargetManager(client, make_config())

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.console_driver("p")
    assert w.targets == []  # never touched labgrid


async def test_console_driver_bind_failure_wraps_targeterror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoBindConsole:
        def __init__(self, target: Any, name: str | None = None) -> None:
            raise RuntimeError("no matching NetworkSerialPort")

    wire(monkeypatch, drivers={"console": NoBindConsole})
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(TargetError, match="no matching NetworkSerialPort"):
        await tm.console_driver("p")


async def test_release_console_driver_deactivates_and_pops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"console": FakeConsoleDriver})
    tm = TargetManager(owned_client(), make_config())

    drv = await tm.console_driver("p")
    await tm.release_console_driver("p")

    assert w.targets[0].deactivated_drivers == [drv]
    assert "console" not in tm._targets["p"].drivers

    # Idempotent: a second release (nothing bound) is a no-op.
    await tm.release_console_driver("p")
    assert w.targets[0].deactivated_drivers == [drv]


async def test_release_console_driver_unknown_place_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wire(monkeypatch, drivers={"console": FakeConsoleDriver})
    tm = TargetManager(owned_client(), make_config())
    await tm.release_console_driver("never-built")  # no cached Target -> no-op


# ---- ssh driver (§11.13) ---------------------------------------------------


class FakeSSHDriver:
    def __init__(self, target: Any, name: str | None = None, keyfile: str | None = None) -> None:
        self.target = target
        self.keyfile = keyfile


async def test_ssh_driver_activates_and_returns_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"ssh": FakeSSHDriver})
    tm = TargetManager(owned_client(), make_config(ssh_keyfile="/keys/id_ed25519"))

    drv = await tm.ssh_driver("p")

    assert isinstance(drv, FakeSSHDriver)
    assert w.targets[0].activated == [drv]  # activated before return
    assert tm._targets["p"].drivers["ssh"] is drv  # cached under "ssh"


async def test_ssh_driver_injects_configured_keyfile_before_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"ssh": FakeSSHDriver})
    tm = TargetManager(owned_client(), make_config(ssh_keyfile="/keys/id_ed25519"))

    drv = await tm.ssh_driver("p")

    assert isinstance(drv, FakeSSHDriver)
    # keyfile must already be set on the driver by the time `activate` runs --
    # SSHDriver.on_activate reads it (§11.13 auth-model citation).
    assert drv.keyfile == "/keys/id_ed25519"
    assert w.targets[0].activated == [drv]


async def test_ssh_driver_reuses_cached_target_and_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"ssh": FakeSSHDriver})
    tm = TargetManager(owned_client(), make_config(ssh_keyfile="/keys/id_ed25519"))

    first = await tm.ssh_driver("p")
    second = await tm.ssh_driver("p")

    assert first is second
    assert len(w.targets) == 1
    assert len(w.targets[0].activated) == 1  # activated exactly once


async def test_ssh_driver_runs_off_the_loop_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    idents: list[int] = []

    class ThreadProbeSSH:
        def __init__(
            self, target: Any, name: str | None = None, keyfile: str | None = None
        ) -> None:
            idents.append(threading.get_ident())

    wire(monkeypatch, drivers={"ssh": ThreadProbeSSH})
    tm = TargetManager(owned_client(), make_config(ssh_keyfile="/keys/id_ed25519"))

    await tm.ssh_driver("p")
    assert idents and idents[0] != threading.get_ident()


async def test_ssh_driver_missing_keyfile_raises_before_ownership_or_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"ssh": FakeSSHDriver})
    # No place at all: if the keyfile check ran after ownership, this would
    # instead raise "unknown place" -- assert the keyfile message wins.
    tm = TargetManager(FakeClient(), make_config(ssh_keyfile=None))

    with pytest.raises(
        TargetError, match="LABGRID_MCP_SSH_KEYFILE is not set; SSH tools need a private key"
    ):
        await tm.ssh_driver("p")
    assert w.targets == []  # never touched labgrid


async def test_ssh_driver_unowned_raises_before_build(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"ssh": FakeSSHDriver})
    client = FakeClient()
    client._places["p"] = {"name": "p", "acquired": "other/user", "acquired_resources": []}
    tm = TargetManager(client, make_config(ssh_keyfile="/keys/id_ed25519"))

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.ssh_driver("p")
    assert w.targets == []  # never touched labgrid


async def test_ssh_driver_bind_failure_wraps_targeterror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoBindSSH:
        def __init__(
            self, target: Any, name: str | None = None, keyfile: str | None = None
        ) -> None:
            raise RuntimeError("no matching NetworkService")

    wire(monkeypatch, drivers={"ssh": NoBindSSH})
    tm = TargetManager(owned_client(), make_config(ssh_keyfile="/keys/id_ed25519"))

    with pytest.raises(TargetError, match="no matching NetworkService"):
        await tm.ssh_driver("p")


# ---- ownership / existence gate (before any build) ------------------------


async def test_unknown_place_raises_before_build(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(FakeClient(), make_config())  # no places at all

    with pytest.raises(TargetError, match="unknown place 'p'"):
        await tm.power_state("p")
    assert w.targets == []  # never touched labgrid


async def test_unknown_place_while_disconnected_reports_reconnect_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A coordinator RPC failure clears our local snapshot cache -- an empty
    snapshot mid-reconnect must not read as "unknown place" (misleads the
    caller into re-acquiring); it should name the real, transient cause."""
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = FakeClient()  # no places at all, exactly like the "unknown" case
    client.connected = False

    with pytest.raises(TargetError, match="coordinator disconnected \\(reconnecting\\)"):
        await TargetManager(client, make_config()).power_state("p")
    assert w.targets == []


async def test_known_then_cleared_place_while_disconnected_reports_reconnect_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A place that WAS owned (and cached) but whose snapshot entry has since
    emptied out during a reconnect gets the same reconnect-window message, not
    "unknown place"."""
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = owned_client("p")
    client._places.pop("p")  # snapshot cleared mid-reconnect
    client.connected = False
    tm = TargetManager(client, make_config())

    with pytest.raises(TargetError, match="coordinator disconnected \\(reconnecting\\)"):
        await tm.power_state("p")
    assert w.targets == []


async def test_unknown_place_while_connected_still_reports_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connected + genuinely unknown place keeps the original message (not
    just a default-True regression check: this is the discriminating case)."""
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = FakeClient()
    client.connected = True

    with pytest.raises(TargetError, match="unknown place 'p'"):
        await TargetManager(client, make_config()).power_state("p")
    assert w.targets == []


async def test_unacquired_place_while_disconnected_still_reports_not_acquired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disconnected does not change the "found but not ours" branch -- only
    an EMPTY snapshot triggers the reconnect-window message."""
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = FakeClient()
    client._places["p"] = {"name": "p", "acquired": "other/user", "acquired_resources": []}
    client.connected = False
    tm = TargetManager(client, make_config())

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.power_state("p")
    assert w.targets == []


async def test_unacquired_place_raises_before_build(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = FakeClient()
    client._places["p"] = {"name": "p", "acquired": "other/user", "acquired_resources": []}
    tm = TargetManager(client, make_config())

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.power_state("p")
    assert w.targets == []


async def test_ownership_rechecked_on_every_op(monkeypatch: pytest.MonkeyPatch) -> None:
    # A stale cached Target must not drive a place we no longer own, even if
    # invalidate was missed (review fix round 1).
    wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = owned_client()
    tm = TargetManager(client, make_config())

    assert await tm.power("p", "on") is True  # builds + caches
    client._places["p"]["acquired"] = "other/user"  # ownership lost, no invalidate

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.power_state("p")


# ---- shutdown -------------------------------------------------------------


async def test_shutdown_invalidates_all_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    client = FakeClient()
    for name in ("p", "q"):
        client._places[name] = {"name": name, "acquired": IDENTITY, "acquired_resources": []}
    tm = TargetManager(client, make_config())

    await tm.power_state("p")
    await tm.power_state("q")
    assert len(w.targets) == 2

    await tm.shutdown()
    assert tm._targets == {}
    assert all(t.deactivated == 1 for t in w.targets)

    await tm.shutdown()  # idempotent: no error, nothing left to do
    assert tm._targets == {}


# ---- flash drivers (§11.11) -----------------------------------------------


class FakeFlashDriver:
    def __init__(self, target: Any, name: str | None = None) -> None:
        self.target = target


@pytest.mark.parametrize("kind", ["dfu", "fastboot", "script", "bootstrap", "write_image"])
async def test_flash_driver_binds_activates_and_caches_per_kind(
    monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    # "bootstrap" is selected via _BOOTSTRAP_LOADERS (default loader "imx"),
    # not _DRIVERS, and caches under "bootstrap:<loader>" (§11.11 loader
    # selector, P5 review follow-up) -- the other four kinds are unaffected.
    if kind == "bootstrap":
        monkeypatch.setattr(target_mod, "_BOOTSTRAP_LOADERS", {"imx": FakeFlashDriver})
        w = wire(monkeypatch, drivers={})
        cache_key = "bootstrap:imx"
    else:
        w = wire(monkeypatch, drivers={kind: FakeFlashDriver})
        cache_key = kind
    tm = TargetManager(owned_client(), make_config())

    drv = await tm.flash_driver("p", kind)

    assert isinstance(drv, FakeFlashDriver)
    assert w.targets[0].activated == [drv]  # activated before return
    assert tm._targets["p"].drivers[cache_key] is drv  # cached under the kind key
    # Re-bind reuses the cached driver + Target (activated exactly once).
    assert await tm.flash_driver("p", kind) is drv
    assert len(w.targets[0].activated) == 1


async def test_flash_driver_unknown_kind_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"dfu": FakeFlashDriver})
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(TargetError, match="unknown flash kind 'nope'"):
        await tm.flash_driver("p", "nope")
    assert w.targets == []  # rejected before any build


async def test_flash_driver_runs_off_the_loop_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    idents: list[int] = []

    class ThreadProbeFlash:
        def __init__(self, target: Any, name: str | None = None) -> None:
            idents.append(threading.get_ident())

    wire(monkeypatch, drivers={"dfu": ThreadProbeFlash})
    tm = TargetManager(owned_client(), make_config())

    await tm.flash_driver("p", "dfu")
    assert idents and idents[0] != threading.get_ident()


async def test_flash_driver_unowned_raises_before_build(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"dfu": FakeFlashDriver})
    client = FakeClient()
    client._places["p"] = {"name": "p", "acquired": "other/user", "acquired_resources": []}
    tm = TargetManager(client, make_config())

    with pytest.raises(TargetError, match="not acquired by this server"):
        await tm.flash_driver("p", "dfu")
    assert w.targets == []


async def test_flash_driver_bind_failure_wraps_targeterror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoBindFlash:
        def __init__(self, target: Any, name: str | None = None) -> None:
            raise RuntimeError("no matching NetworkDFUDevice")

    wire(monkeypatch, drivers={"dfu": NoBindFlash})
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(TargetError, match="no matching NetworkDFUDevice"):
        await tm.flash_driver("p", "dfu")


# ---- bootstrap loader selector (§11.11 driver inventory, P5 review follow-up) ---


class FakeImxLoader:
    def __init__(self, target: Any, name: str | None = None) -> None:
        self.target = target


class FakeMxsLoader:
    def __init__(self, target: Any, name: str | None = None) -> None:
        self.target = target


def _wire_bootstrap_loaders(monkeypatch: pytest.MonkeyPatch) -> _Wiring:
    """``wire()`` for the loader table instead of ``_DRIVERS`` (bootstrap only)."""
    monkeypatch.setattr(
        target_mod, "_BOOTSTRAP_LOADERS", {"imx": FakeImxLoader, "mxs": FakeMxsLoader}
    )
    return wire(monkeypatch, drivers={})


async def test_flash_driver_bootstrap_defaults_to_imx_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_bootstrap_loaders(monkeypatch)
    tm = TargetManager(owned_client(), make_config())

    drv = await tm.flash_driver("p", "bootstrap")

    assert isinstance(drv, FakeImxLoader)
    assert tm._targets["p"].drivers["bootstrap:imx"] is drv


async def test_flash_driver_bootstrap_selects_named_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_bootstrap_loaders(monkeypatch)
    tm = TargetManager(owned_client(), make_config())

    drv = await tm.flash_driver("p", "bootstrap:mxs")

    assert isinstance(drv, FakeMxsLoader)
    assert tm._targets["p"].drivers["bootstrap:mxs"] is drv


async def test_flash_driver_bootstrap_switching_loader_rebinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later bootstrap job on the same place with a DIFFERENT loader must
    rebind rather than reuse the first loader's stale driver instance."""
    w = _wire_bootstrap_loaders(monkeypatch)
    tm = TargetManager(owned_client(), make_config())

    imx_drv = await tm.flash_driver("p", "bootstrap")
    mxs_drv = await tm.flash_driver("p", "bootstrap:mxs")

    # Compared before the isinstance narrowing below: once narrowed to their
    # (deliberately unrelated) concrete fake classes, mypy's strict-equality
    # check flags an `is not` between them as a non-overlapping comparison.
    assert imx_drv is not mxs_drv
    assert isinstance(imx_drv, FakeImxLoader)
    assert isinstance(mxs_drv, FakeMxsLoader)
    # Same cached Target reused for both (one place -> one Target).
    assert len(w.targets) == 1
    assert w.targets[0].activated == [imx_drv, mxs_drv]


async def test_flash_driver_unknown_bootstrap_loader_raises_before_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = _wire_bootstrap_loaders(monkeypatch)
    tm = TargetManager(owned_client(), make_config())

    with pytest.raises(
        TargetError, match=r"unknown bootstrap loader 'bogus'; valid options: imx, mxs"
    ):
        await tm.flash_driver("p", "bootstrap:bogus")
    assert w.targets == []  # rejected before any build, like an unknown kind


# ---- pin / unpin: invalidate refusal (§11.11) -----------------------------


async def test_invalidate_refuses_while_place_pinned_naming_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(owned_client(), make_config())
    await tm.power_state("p")  # build + cache a Target

    tm.pin("p", "job-abc123")
    with pytest.raises(TargetError, match="flash job 'job-abc123' is running"):
        await tm.invalidate("p")
    assert "p" in tm._targets  # not torn down
    assert w.targets[0].deactivated == 0

    tm.unpin("p", "job-abc123")
    await tm.invalidate("p")  # now allowed
    assert "p" not in tm._targets


async def test_unpin_is_identity_gated_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    wire(monkeypatch, drivers={"power": FakePowerDriver})
    tm = TargetManager(owned_client(), make_config())
    tm.unpin("never-pinned", "j0")  # no-op, no error
    tm.pin("p", "j1")
    tm.unpin("p", "j2")  # different job -> must NOT clear j1's pin
    assert tm._pins.get("p") == "j1"
    tm.unpin("p", "j1")
    tm.unpin("p", "j1")  # idempotent
    assert "p" not in tm._pins


# ---- thread-safe @step stack (§11.11) -------------------------------------


class _SteppedDriver:
    """Power driver whose public ops are REAL labgrid ``@step`` calls.

    The tiny in-step sleep yields the GIL between the step's push and pop so two
    concurrent calls on different threads interleave — exactly the process-global
    ``_stack`` corruption (``pop`` asserts ``_stack[-1] is step``) that
    :func:`install_thread_safe_steps` retires (§11.11).
    """

    def __init__(self, target: Any, name: str | None = None) -> None:
        self.state = False

    @lg_step()  # type: ignore[untyped-decorator]  # labgrid.step.step is untyped
    def get(self) -> bool:
        time.sleep(0.0005)
        return self.state

    @lg_step()  # type: ignore[untyped-decorator]  # labgrid.step.step is untyped
    def on(self) -> None:
        time.sleep(0.0005)
        self.state = True


async def test_concurrent_stepped_ops_across_places_do_not_corrupt_step_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent public ``@step`` power ops on two places, many iterations.

    Passes ONLY with the thread-local ``steps`` rebind installed (TargetManager
    init installs it). Reverting the fix (restoring the stock shared-stack
    ``Steps``) makes this fail with a ``TargetError`` wrapping the bare
    ``AssertionError`` from ``step.py`` — verified once while developing.
    """
    wire(monkeypatch, drivers={"power": _SteppedDriver})
    client = FakeClient()
    for name in ("A", "B"):
        client._places[name] = {"name": name, "acquired": IDENTITY, "acquired_resources": []}
    tm = TargetManager(client, make_config())

    # Build both Targets first so the hammer only exercises the concurrent ops.
    await asyncio.gather(tm.power_state("A"), tm.power_state("B"))

    for _ in range(150):
        # Two DIFFERENT places -> different per-place locks -> both run in the
        # thread pool at once -> their @step push/pop interleave.
        results = await asyncio.gather(
            tm.power("A", "on"), tm.power_state("B"), tm.power_state("A")
        )
        assert all(r in (True, False) for r in results)  # no AssertionError bubbled up


# ---- per-resource name selection (§11.14) ---------------------------------


class NamedPowerDriver:
    """Power driver that records its bound resource ``name`` (§11.14)."""

    bindings = {"port": "NetworkPowerPort"}

    def __init__(self, target: Any, name: str | None = None) -> None:
        self.target = target
        self.name = name
        self.state: bool | None = None
        self.delay = 2.0

    def on(self) -> None:
        self.state = True

    def off(self) -> None:
        self.state = False

    def cycle(self) -> None:
        self.state = True

    def get(self) -> bool:
        return bool(self.state)


class NamedIoDriver:
    bindings = {"http": "HttpDigitalOutput"}

    def __init__(self, target: Any, name: str | None = None) -> None:
        self.name = name
        self._v = False

    def set(self, value: bool) -> None:
        self._v = value

    def get(self) -> bool:
        return self._v


def _power_client(names: list[str], place: str = "p") -> FakeClient:
    """An owned place exporting one ``NetworkPowerPort`` per name in ``names``."""
    client = FakeClient()
    client._places[place] = {
        "name": place,
        "acquired": IDENTITY,
        "acquired_resources": [["exp", "grp", "NetworkPowerPort", n] for n in names],
    }
    return client


async def test_named_resource_binds_and_caches_each_name_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"power": NamedPowerDriver})
    tm = TargetManager(_power_client(["port_a", "port_b"]), make_config())

    assert await tm.power("p", "on", resource_name="port_a") is True
    assert await tm.power("p", "off", resource_name="port_b") is False

    drivers = tm._targets["p"].drivers
    assert set(drivers) == {"power:name:port_a", "power:name:port_b"}
    assert drivers["power:name:port_a"].name == "port_a"
    assert drivers["power:name:port_b"].name == "port_b"
    # set_binding_map seeded once per named bind, in call order (§11.14).
    assert w.targets[0].binding_maps == [{"port": "port_a"}, {"port": "port_b"}]
    # Independent state: each name drives only its own resource.
    assert drivers["power:name:port_a"].get() is True
    assert drivers["power:name:port_b"].get() is False


async def test_named_resource_reuses_cached_driver_per_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w = wire(monkeypatch, drivers={"power": NamedPowerDriver})
    tm = TargetManager(_power_client(["port_a", "port_b"]), make_config())

    first = await tm.power_state("p", resource_name="port_a")
    second = await tm.power_state("p", resource_name="port_a")
    assert first == second
    # One Target, the named driver activated + binding-mapped exactly once.
    assert len(w.targets) == 1
    assert len(w.targets[0].activated) == 1
    assert w.targets[0].binding_maps == [{"port": "port_a"}]


async def test_no_name_with_multiple_resources_lists_available_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wire(monkeypatch, drivers={"power": NamedPowerDriver})
    tm = TargetManager(_power_client(["port_a", "port_b"]), make_config())

    with pytest.raises(
        TargetError,
        match=r"place 'p' has multiple power resources \(port_a, port_b\); pass resource_name",
    ):
        await tm.power_state("p")


async def test_unknown_name_lists_available_names(monkeypatch: pytest.MonkeyPatch) -> None:
    wire(monkeypatch, drivers={"power": NamedPowerDriver})
    tm = TargetManager(_power_client(["port_a", "port_b"]), make_config())

    with pytest.raises(
        TargetError,
        match=r"no power resource named 'nope' on place 'p'; available: port_a, port_b",
    ):
        await tm.power_state("p", resource_name="nope")


async def test_single_resource_no_name_is_byte_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One resource + ``name=None`` keeps the legacy path: plain cache key,
    ``name=None`` construct, no ``set_binding_map`` (§11.14)."""
    w = wire(monkeypatch, drivers={"power": NamedPowerDriver})
    tm = TargetManager(_power_client(["only"]), make_config())

    assert await tm.power("p", "on") is True
    drivers = tm._targets["p"].drivers
    assert set(drivers) == {"power"}  # not "power:name:only"
    assert drivers["power"].name is None
    assert w.targets[0].binding_maps == []  # never seeded for the legacy path


async def test_named_io_resource_binds_by_http_key(monkeypatch: pytest.MonkeyPatch) -> None:
    w = wire(monkeypatch, drivers={"io": NamedIoDriver})
    client = FakeClient()
    client._places["p"] = {
        "name": "p",
        "acquired": IDENTITY,
        "acquired_resources": [
            ["exp", "grp", "HttpDigitalOutput", "io_a"],
            ["exp", "grp", "HttpDigitalOutput", "io_b"],
        ],
    }
    tm = TargetManager(client, make_config())

    await tm.io_set("p", True, resource_name="io_b")
    assert await tm.io_get("p", resource_name="io_b") is True
    # io binds on its own single key "http", not "port" (§11.14).
    assert w.targets[0].binding_maps == [{"http": "io_b"}]
    assert tm._targets["p"].drivers["io:name:io_b"].name == "io_b"


# ---- power cycle delay (§11.14) -------------------------------------------


class DelayRecordingPower:
    """Records the ``delay`` attr value observed AT ``cycle()`` time (§11.14)."""

    def __init__(self, target: Any, name: str | None = None) -> None:
        self.delay = 2.0
        self.delay_at_cycle: float | None = None
        self.state: bool | None = None

    def cycle(self) -> None:
        self.delay_at_cycle = self.delay
        self.state = True

    def get(self) -> bool:
        return bool(self.state)


async def test_cycle_delay_set_on_driver_before_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    wire(monkeypatch, drivers={"power": DelayRecordingPower})
    tm = TargetManager(owned_client(), make_config())

    assert await tm.power("p", "cycle", delay=0.3) is True
    drv = tm._targets["p"].drivers["power"]
    assert drv.delay_at_cycle == 0.3  # set BEFORE cycle() ran, not passed as an arg


async def test_cycle_delay_coerces_int_to_float(monkeypatch: pytest.MonkeyPatch) -> None:
    # NetworkPowerDriver.delay's validator is instance_of(float): int must coerce.
    wire(monkeypatch, drivers={"power": DelayRecordingPower})
    tm = TargetManager(owned_client(), make_config())

    await tm.power("p", "cycle", delay=1)  # int
    drv = tm._targets["p"].drivers["power"]
    assert drv.delay == 1.0
    assert isinstance(drv.delay, float)


async def test_cycle_delay_resets_to_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """The cached driver's ``delay`` is sticky: a later cycle with no delay must
    reset to labgrid's default, not reuse the earlier explicit delay (§11.14)."""
    wire(monkeypatch, drivers={"power": DelayRecordingPower})
    tm = TargetManager(owned_client(), make_config())

    await tm.power("p", "cycle", delay=10)  # sets 10 on the cached driver
    drv = tm._targets["p"].drivers["power"]
    assert drv.delay_at_cycle == 10.0

    await tm.power("p", "cycle")  # same cached driver, no delay
    assert drv.delay_at_cycle == 2.0  # reset to default, not the sticky 10
    assert drv.delay == 2.0


# ---- sd-mux get_mode (§11.14) ---------------------------------------------


async def test_sd_mux_mode_returns_get_mode_string(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSdMuxGet:
        def __init__(self, target: Any, name: str | None = None) -> None:
            pass

        def get_mode(self) -> str:
            return "dut"

    wire(monkeypatch, drivers={"sd_mux": FakeSdMuxGet})
    tm = TargetManager(owned_client(), make_config())

    assert await tm.sd_mux_mode("p") == "dut"


# ---- write_image option mapping (§11.14) ----------------------------------


def test_write_image_kwargs_defaults_to_dd() -> None:
    from labgrid.driver.usbstoragedriver import Mode

    from labgrid_mcp.target import write_image_kwargs

    assert write_image_kwargs() == {"mode": Mode.DD, "partition": None, "skip": 0, "seek": 0}


def test_write_image_kwargs_maps_mode_and_passes_options_through() -> None:
    from labgrid.driver.usbstoragedriver import Mode

    from labgrid_mcp.target import write_image_kwargs

    assert write_image_kwargs(mode="bmaptool", partition=2, skip=1, seek=3) == {
        "mode": Mode.BMAPTOOL,
        "partition": 2,
        "skip": 1,
        "seek": 3,
    }


def test_write_image_kwargs_bad_mode_raises_before_any_call() -> None:
    from labgrid_mcp.target import write_image_kwargs

    with pytest.raises(
        TargetError, match=r"unknown write_image mode 'nope'; valid options: dd, bmaptool"
    ):
        write_image_kwargs(mode="nope")


def test_write_image_kwargs_forwarded_to_driver_verbatim() -> None:
    from labgrid.driver.usbstoragedriver import Mode

    from labgrid_mcp.target import write_image_kwargs

    received: dict[str, Any] = {}

    class FakeStorage:
        def write_image(
            self, filename: str, *, mode: Any, partition: int | None, skip: int, seek: int
        ) -> None:
            received.update(
                filename=filename, mode=mode, partition=partition, skip=skip, seek=seek
            )

    kwargs = write_image_kwargs(mode="dd", partition=1, skip=2, seek=4)
    FakeStorage().write_image("img.bin", **kwargs)
    assert received == {
        "filename": "img.bin",
        "mode": Mode.DD,
        "partition": 1,
        "skip": 2,
        "seek": 4,
    }
