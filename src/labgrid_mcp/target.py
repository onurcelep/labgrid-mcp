"""ClientSession-free Target & driver stack for the power/io/mux tools.

This drives labgrid's own client-side driver stack (``NetworkPowerDriver``,
``HttpDigitalOutputDriver``, ``USBSDMuxDriver``, ``LXAUSBMuxDriver``) **without
ever constructing a ``labgrid.remote.client.ClientSession``** — a second gRPC
session in-process shares a subchannel with our live ``CoordinatorClient`` and
the coordinator aborts it (the shared-subchannel trap, DESIGN §11.8/§11.9).

The whole approach is verified and documented in DESIGN §11.9; the load-bearing
sequences are cited inline below. In one sentence: pre-seat a 3-member adapter
(:class:`_CoordinatorAdapter`, backed by the ``CoordinatorClient`` snapshot) on
the process-global :class:`RemotePlaceManager` singleton *before* constructing
any ``RemotePlace``, cache **one** ``Target`` per place (the manager only ever
appends — rebuilding leaks), and run every synchronous, blocking driver call
through :func:`asyncio.to_thread`.

Concurrency invariant: **all RemotePlaceManager singleton mutations are
globally serialized via** :data:`_MANAGER_LOCK`. The per-place asyncio locks
serialize ops on ONE place, but ``_build_target`` (seats ``session/loop/env``
and appends via ``RemotePlace`` construction) and ``_invalidate_sync`` (prunes
with a read-modify-write reassignment of ``mgr.resources``) mutate the
process-global singleton from worker threads — concurrent ops on DIFFERENT
places (or different ``TargetManager`` instances) would otherwise race and the
prune could silently drop a concurrent build's appends. Phase 4 console Targets
rely on this invariant.

Verified against labgrid 26.0. Re-verify when the pin moves.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol

from labgrid import Target as _Target
from labgrid.driver.dfudriver import DFUDriver
from labgrid.driver.fastbootdriver import AndroidFastbootDriver
from labgrid.driver.flashscriptdriver import FlashScriptDriver
from labgrid.driver.httpdigitaloutput import HttpDigitalOutputDriver
from labgrid.driver.lxausbmuxdriver import LXAUSBMuxDriver
from labgrid.driver.powerdriver import NetworkPowerDriver
from labgrid.driver.serialdriver import SerialDriver
from labgrid.driver.sshdriver import SSHDriver
from labgrid.driver.usbloader import (
    BDIMXUSBDriver,
    IMXUSBDriver,
    MXSUSBDriver,
    RKUSBDriver,
    UUUDriver,
)
from labgrid.driver.usbsdmuxdriver import USBSDMuxDriver
from labgrid.driver.usbstoragedriver import Mode, USBStorageDriver
from labgrid.resource.remote import RemotePlace as _RemotePlace
from labgrid.resource.remote import RemotePlaceManager
from labgrid.step import Steps as _Steps

if TYPE_CHECKING:
    from collections.abc import Callable

    from labgrid_mcp.config import Config


class _CoordinatorInfoLike(Protocol):
    """The single field :meth:`_CoordinatorClientLike.info` returns consulted here.

    Declared as a read-only property (not a bare attribute) so the real
    ``CoordinatorInfo`` -- a FROZEN dataclass, whose fields are read-only --
    still structurally satisfies this Protocol.
    """

    @property
    def connected(self) -> bool: ...


class _CoordinatorClientLike(Protocol):
    """Structural surface ``TargetManager``/``_CoordinatorAdapter`` consume.

    A ``Protocol`` (not the concrete ``CoordinatorClient``) so unit tests can
    hand in a plain duck-typed fake (no real gRPC/coordinator) and still
    satisfy strict mypy -- only ``place``/``resources``/``info`` are ever
    called on the client from this module.
    """

    def place(self, name: str) -> dict[str, Any] | None: ...
    def resources(self) -> list[dict[str, Any]]: ...
    def info(self) -> _CoordinatorInfoLike: ...


# ---- patchable labgrid seams (§11.9) --------------------------------------
# These are module attributes rather than direct imports at each call site so
# the unit tests can replace them with fakes — that is what keeps the units
# free of any real gRPC / coordinator / exporter. labgrid is untyped, so these
# are ``Any`` at the seam (mirrors coordinator.py's style).

Target: Any = _Target
RemotePlace: Any = _RemotePlace

# Driver class per capability. NetworkPowerDriver.on/off/cycle/get,
# HttpDigitalOutputDriver.set/get, USBSDMuxDriver.set_mode,
# LXAUSBMuxDriver.set_links (§11.9). "console" binds a raw-protocol SerialDriver
# on a NetworkSerialPort (§11.10); it is instantiated+activated through the same
# _activated_driver path, but its UNDECORATED _read/_write are then driven
# outside the place lock by console.py's session reader (see console_driver).
#
# The flash kinds (§11.11) bind on the same cached Target through the same
# _activated_driver path. ``bootstrap`` is special: labgrid exposes FIVE
# BootstrapProtocol driver classes, all ``load(filename)`` (§11.11's driver
# inventory table) -- selected via a loader table (below), not this dict.
# The flash driver call itself runs on a dedicated JobRegistry thread, never here.
#
# "ssh" binds an SSHDriver on a NetworkService (§11.13); like "console" it is
# NOT driven through the generic _activated_driver path, because it needs an
# extra constructor kwarg (keyfile) -- see ssh_driver/_activate_ssh below. It
# is still listed here (rather than imported directly at the call site) so
# the same patchable-seam discipline applies: unit tests replace _DRIVERS
# wholesale with a fake SSH driver class, never touching real labgrid/sshd.
_DRIVERS: dict[str, Any] = {
    "power": NetworkPowerDriver,
    "io": HttpDigitalOutputDriver,
    "sd_mux": USBSDMuxDriver,
    "usb_mux": LXAUSBMuxDriver,
    "console": SerialDriver,
    "dfu": DFUDriver,
    "fastboot": AndroidFastbootDriver,
    "script": FlashScriptDriver,
    "write_image": USBStorageDriver,
    "ssh": SSHDriver,
}

# Flash kinds accepted by :meth:`TargetManager.flash_driver` (§11.11).
_FLASH_KINDS = frozenset({"dfu", "fastboot", "script", "bootstrap", "write_image"})

# Driver kinds that accept an optional per-resource NAME (§11.14). Scoped to
# power + io this phase: they are the two hardware-free driver families, and a
# 2-same-class-resource place is currently *unusable* for them (labgrid's
# ``get_resource`` raises "multiple resources matching" with ``name=None``).
# console/mux/flash naming is an additive follow-up on the SAME plumbing.
_NAMED_KINDS = frozenset({"power", "io"})

# BootstrapProtocol loader selector (§11.11's driver inventory table, P5
# review follow-up): all five expose ``load(filename)``; "imx" (IMXUSBDriver)
# is the common i.MX case and stays the default so existing ``bootstrap()``
# callers are unaffected. A patchable seam like ``_DRIVERS`` -- tests replace
# it wholesale rather than needing real loader hardware.
# NetworkPowerDriver.delay's default (`powerdriver.py:154`). The cached driver's
# ``delay`` attr is sticky, so the cycle path sets it deterministically EVERY
# call — an explicit delay, else this default — so a prior ``delay=`` never
# leaks into a later unqualified cycle (§11.14).
_DEFAULT_CYCLE_DELAY = 2.0

_BOOTSTRAP_LOADERS: dict[str, Any] = {
    "imx": IMXUSBDriver,
    "mxs": MXSUSBDriver,
    "rk": RKUSBDriver,
    "uuu": UUUDriver,
    "bdimx": BDIMXUSBDriver,
}
_DEFAULT_BOOTSTRAP_LOADER = "imx"


def _split_flash_kind(kind: str) -> tuple[str, str]:
    """Split a job ``kind`` into its base flash kind and bootstrap loader.

    Plain kinds (``dfu``/``fastboot``/``script``/``bootstrap``/``write_image``)
    pass through unchanged, loader defaulted (irrelevant except for
    ``bootstrap``). ``bootstrap:<loader>`` -- produced ONLY by the
    ``bootstrap`` tool (server.py) for a non-default loader, since
    ``JobRegistry.submit_flash`` forwards ``kind`` opaquely and has no
    separate loader parameter -- splits into ``("bootstrap", loader)``.
    """
    base, sep, loader = kind.partition(":")
    if not sep:
        return kind, _DEFAULT_BOOTSTRAP_LOADER
    return base, loader


def _binding_spec(driver_cls: Any) -> tuple[str | None, frozenset[str]]:
    """Return ``(binding_key, acceptable resource-class names)`` for ``driver_cls``.

    §11.14: a per-resource name binds via ``set_binding_map({binding_key: name})``
    on the driver's single binding key, and the candidate resources are those
    whose exported ``cls`` is one of the binding's requirements. The binding
    value may be a class, a class-name string, or a set of either. Returns
    ``(None, frozenset())`` when the class is not single-binding (so callers
    treat it as not-nameable rather than crash on an unpacking error) — real
    power/io drivers are always single-binding; this only shields odd fakes.
    """
    bindings: Any = getattr(driver_cls, "bindings", None) or {}
    if len(bindings) != 1:
        return None, frozenset()
    ((binding_key, req),) = bindings.items()
    reqs = req if isinstance(req, (set, frozenset, list, tuple)) else (req,)
    names = frozenset(r if isinstance(r, str) else getattr(r, "__name__", "") for r in reqs)
    return binding_key, names


def write_image_kwargs(
    *, partition: int | None = None, mode: str | None = None, skip: int = 0, seek: int = 0
) -> dict[str, Any]:
    """Validate + map ``write_image`` options to ``USBStorageDriver`` kwargs (§11.14).

    ``mode`` is the ``Mode`` enum NAME (case-insensitive ``"dd"``/``"bmaptool"``);
    ``None`` defaults to ``Mode.DD``. A bad name raises :class:`TargetError`
    BEFORE the driver is ever called (server.py folds these into the ``write_image``
    job closure). ``partition`` is ``int | None`` (``None`` = whole root device);
    ``skip``/``seek`` are 512-byte-block counts. Coercion mirrors the driver's own
    signature so the kwargs pass straight through.
    """
    resolved = Mode.DD if mode is None else _resolve_mode(mode)
    return {
        "mode": resolved,
        "partition": None if partition is None else int(partition),
        "skip": int(skip),
        "seek": int(seek),
    }


def _resolve_mode(mode: str) -> Any:
    try:
        return Mode[mode.upper()]
    except (KeyError, AttributeError):
        valid = ", ".join(m.name.lower() for m in Mode)
        raise TargetError(f"unknown write_image mode {mode!r}; valid options: {valid}") from None


# ---- thread-safe @step machinery (§11.11) ---------------------------------


class _ThreadLocalSteps(_Steps):  # type: ignore[misc]  # labgrid Steps is untyped
    """``labgrid.step.Steps`` with a per-thread ``_stack`` (DESIGN §11.11).

    labgrid's ``@step`` decorator shares one process-global stack whose ``pop``
    asserts ``_stack[-1] is step`` — not thread-safe (reproduced ``AssertionError``
    with two concurrent stepped calls). Giving each thread its own LIFO stack makes
    every push/pop locally consistent, so concurrent ``@step`` driver calls across
    job / console / power threads can never corrupt one another — with NO global
    lock, so a long flash never blocks a power op. Subscribers stay shared, so any
    future step-event subscriber still sees every thread's events.
    """

    def __init__(self) -> None:
        # Set _local BEFORE super().__init__ — it assigns self._stack = [], which
        # hits our setter and needs self._local to already exist.
        self._local = threading.local()
        super().__init__()

    @property
    def _stack(self) -> list[Any]:
        stack: list[Any] | None = getattr(self._local, "stack", None)
        if stack is None:
            stack = []
            self._local.stack = stack
        return stack

    @_stack.setter
    def _stack(self, value: list[Any]) -> None:
        self._local.stack = value


_STEPS_LOCK = threading.Lock()
_steps_installed = False


def install_thread_safe_steps() -> None:
    """Rebind ``labgrid.step.steps`` to a thread-local-stack ``Steps`` (idempotent).

    Called from BOTH :class:`TargetManager` and ``JobRegistry`` init so any entry
    path installs it. ``@step`` resolves ``steps`` as a module global at call time,
    so this one rebind redirects every driver in the process (§11.11). Uses
    :func:`importlib.import_module` because ``labgrid.step`` the attribute is
    shadowed by the ``step`` function in ``labgrid/__init__``, so ``import
    labgrid.step as m`` would bind the function, not the module.
    """
    global _steps_installed
    with _STEPS_LOCK:
        if _steps_installed:
            return
        step_mod = importlib.import_module("labgrid.step")
        step_mod.steps = _ThreadLocalSteps()  # type: ignore[attr-defined]
        _steps_installed = True


def _remote_place_manager() -> Any:
    """Return the process-global ``RemotePlaceManager`` singleton (§11.9).

    A function (not a module constant) so tests can patch it without importing
    or touching the real labgrid singleton.
    """
    return RemotePlaceManager.get()


# Guards every mutation of the process-global RemotePlaceManager singleton
# (see the module-docstring invariant). Held inside asyncio.to_thread workers,
# so it must be a threading.Lock, not an asyncio.Lock. Module-level so it is
# shared across all TargetManager instances.
_MANAGER_LOCK = threading.Lock()


class TargetError(Exception):
    """A driver/Target operation failed (wraps labgrid driver errors)."""


class _StubLoop:
    """Stand-in for ``RemotePlaceManager.loop`` (§11.9).

    ``poll()`` only reads ``self.loop.is_running()`` and, when it returns True,
    skips its ``loop.run_until_complete(...)`` branch (which would crash on a
    running loop). We never run labgrid's own poll, so this is purely defensive.
    """

    @staticmethod
    def is_running() -> bool:
        return True


class _Entry:
    """Resource entry as ``on_resource_added`` expects it (§11.9 member 3)."""

    def __init__(self, cls: str, args: dict[str, Any], avail: bool, extra: dict[str, Any]) -> None:
        self.cls = cls
        self.args = args
        self.avail = avail
        self.extra = extra


class _PlaceView:
    """Place view as ``on_resource_added`` expects it (§11.9 member 2)."""

    def __init__(
        self, name: str, tags: dict[str, Any], acquired_resources: list[Any]
    ) -> None:
        self.name = name
        self.tags = tags
        self.acquired_resources = acquired_resources


class _CoordinatorAdapter:
    """The 3 members ``RemotePlaceManager`` touches, backed by our snapshot.

    §11.9: ``on_resource_added`` calls ``session.get_place`` and
    ``session.get_target_resources``; pre-seating this as ``mgr.session`` blocks
    ``_start()`` so no second ``ClientSession`` is ever opened. ``.loop`` is
    provided defensively for parity with labgrid's own session object.
    """

    def __init__(self, client: _CoordinatorClientLike) -> None:
        self._client = client
        self.loop = _StubLoop()

    def get_place(self, name: str) -> _PlaceView:
        # Snapshot dicts are dynamic (labgrid seam) -> narrow Any, per §11.9.
        snap: Any = self._client.place(name) or {}
        tags = snap.get("tags") or {}
        acquired = snap.get("acquired_resources") or []
        return _PlaceView(name, dict(tags), list(acquired))

    def get_target_resources(self, place: _PlaceView) -> dict[tuple[str, str], _Entry]:
        # Index our resource snapshot by "exporter/group/name" (§11.9).
        index: dict[str, Any] = {
            f"{r.get('exporter')}/{r.get('group')}/{r.get('name')}": r
            for r in self._client.resources()
        }
        out: dict[tuple[str, str], _Entry] = {}
        for exporter, group, cls, rname in place.acquired_resources:
            rdata: Any = index.get(f"{exporter}/{group}/{rname}", {})
            args = dict(rdata.get("params") or {})
            # §11.9: the snapshot nests ``extra`` INSIDE ``params``.
            extra = dict(args.pop("extra", {}) or {})
            avail = bool(rdata.get("avail", True))
            out[(rname, cls)] = _Entry(cls, args, avail, extra)
        return out


@dataclass
class _Cached:
    """One built Target per place (§11.9: never rebuild — the manager leaks)."""

    target: Any
    remote_place: Any
    drivers: dict[str, Any] = field(default_factory=dict)


class TargetManager:
    """Owns cached ``Target``s and drives their power/io/mux drivers.

    All labgrid work is ClientSession-free per DESIGN §11.9. Every driver call
    runs in a worker thread (labgrid drivers are synchronous and blocking) and
    every labgrid failure surfaces as :class:`TargetError`.
    """

    def __init__(self, client: _CoordinatorClientLike, config: Config) -> None:
        self._client = client
        self._config = config
        self._adapter = _CoordinatorAdapter(client)
        self._targets: dict[str, _Cached] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # place -> running flash job id; a pinned place refuses invalidate (§11.11).
        self._pins: dict[str, str] = {}
        # Install the thread-safe @step stack from every entry path (§11.11).
        install_thread_safe_steps()

    # ---- public driver API ---------------------------------------------

    async def power(
        self,
        place: str,
        action: Literal["on", "off", "cycle"],
        delay: float | None = None,
        resource_name: str | None = None,
    ) -> bool:
        """Run ``on``/``off``/``cycle`` then return the post-action state (§11.9).

        ``delay`` (§11.14) applies only to ``cycle``: ``NetworkPowerDriver.cycle``
        takes no argument and instead reads its ``delay`` attr (``off(); sleep(
        delay); on()``), so it is set on the driver instance BEFORE the call and
        coerced to ``float`` (the attr's validator is ``instance_of(float)``).
        ``resource_name`` picks one of several same-class power resources (§11.14).
        """

        def op(drv: Any) -> bool:
            if action == "cycle":
                # §11.14: the cached driver's ``delay`` attr is sticky, so set it
                # deterministically every cycle — the explicit delay, else the
                # labgrid default — so a prior ``delay=`` never leaks forward.
                drv.delay = float(delay) if delay is not None else _DEFAULT_CYCLE_DELAY
            getattr(drv, action)()  # NetworkPowerDriver.on/off/cycle
            return bool(drv.get())

        return bool(await self._run(place, "power", op, resource_name=resource_name))

    async def power_state(self, place: str, resource_name: str | None = None) -> bool:
        return bool(await self._run(place, "power", _get, resource_name=resource_name))

    async def io_get(self, place: str, resource_name: str | None = None) -> bool:
        return bool(await self._run(place, "io", _get, resource_name=resource_name))

    async def io_set(self, place: str, value: bool, resource_name: str | None = None) -> None:
        def op(drv: Any) -> None:
            drv.set(value)  # HttpDigitalOutputDriver.set

        await self._run(place, "io", op, resource_name=resource_name)

    async def sd_mux(self, place: str, mode: str) -> None:
        def op(drv: Any) -> None:
            drv.set_mode(mode)  # USBSDMuxDriver.set_mode

        await self._run(place, "sd_mux", op)

    async def sd_mux_mode(self, place: str) -> str:
        """Return the SD-mux's current mode (§11.14: ``USBSDMuxDriver.get_mode``).

        SD-mux only — ``LXAUSBMuxDriver`` has no read method, so there is no
        ``usb_mux`` equivalent.
        """

        def op(drv: Any) -> str:
            return str(drv.get_mode())  # USBSDMuxDriver.get_mode -> str

        return str(await self._run(place, "sd_mux", op))

    async def usb_mux(self, place: str, links: list[str]) -> None:
        def op(drv: Any) -> None:
            drv.set_links(links)  # LXAUSBMuxDriver.set_links

        await self._run(place, "usb_mux", op)

    # ---- console (§11.10) ----------------------------------------------

    async def console_driver(self, place: str) -> Any:
        """Bind + activate a raw ``SerialDriver`` console and RETURN the driver.

        Follows the same discipline as :meth:`_run`: ownership is re-checked, the
        Target is built/reused under the per-place lock (serialized with power
        ops), and the driver is instantiated + activated in a worker thread
        (labgrid blocks). It is cached under the ``"console"`` key so
        :meth:`release_console_driver` and ``invalidate``'s
        ``deactivate_all_drivers`` both tear it down.

        **Sanctioned exception to the §11.9 place-lock invariant (DESIGN
        §11.10):** the returned driver's UNDECORATED ``_read``/``_write`` are then
        driven by console.py's session reader OUTSIDE this per-place lock. The
        public ``read``/``write`` are ``@step``-decorated, and labgrid's ``@step``
        stack is process-global and thread-unsafe — a long-lived reader on it
        corrupts the stack for every other driver call in the process; holding
        the place lock for the reader's whole lifetime would instead block all
        power/io ops on the place. Bind/activate stay under the lock here; only
        the raw byte I/O (proven safe: one concurrent reader + one writer on a
        pyserial ``socket://``) escapes it.
        """
        async with self._lock_for(place):
            cached = await self._get_or_build(place)
            return await asyncio.to_thread(self._activate_console, cached, place)

    async def release_console_driver(self, place: str) -> None:
        """Deactivate the place's console ``SerialDriver`` (no-op if none).

        The caller (console.py) MUST have already stopped + joined the reader
        thread — deactivate closes the socket, and a reader mid-``_read`` on a
        closed socket raises (§11.10 close ordering). Runs under the place lock,
        off the loop.
        """
        async with self._lock_for(place):
            cached = self._targets.get(place)
            if cached is None:
                return
            drv = cached.drivers.pop("console", None)
            if drv is None:
                return
            await asyncio.to_thread(self._deactivate_console, cached, drv)

    def _activate_console(self, cached: _Cached, place: str) -> Any:
        try:
            return self._activated_driver(cached, place, "console")
        except TargetError:
            raise
        except Exception as exc:
            raise TargetError(f"console on place {place!r} failed: {exc}") from exc

    def _deactivate_console(self, cached: _Cached, drv: Any) -> None:
        """Worker-thread body for :meth:`release_console_driver` (best-effort)."""
        with contextlib.suppress(Exception):
            cached.target.deactivate(drv)

    # ---- ssh (§11.13) ----------------------------------------------------

    async def ssh_driver(self, place: str) -> Any:
        """Bind + activate an ``SSHDriver`` against ``place`` and RETURN it.

        Requires :attr:`Config.ssh_keyfile` to be set -- ``NetworkService``
        (the resource an ``SSHDriver`` binds against) carries no key field of
        its own (§11.13), so a configured keyfile is the ONLY way to offer a
        non-default identity, and without one every connection attempt would
        silently fall back to whatever default key(s) the local ``ssh``
        binary happens to try. Checked BEFORE the place lock/ownership check
        (like :meth:`flash_driver`'s unknown-kind check) -- a missing keyfile
        is a static config problem, not a place-specific one, so it should
        fail the same way for every place without ever touching labgrid.

        Otherwise follows :meth:`console_driver`'s discipline exactly:
        ownership re-checked, Target built/reused under the per-place lock,
        driver instantiated + activated in a worker thread (labgrid blocks).
        The keyfile is passed to the ``SSHDriver`` constructor -- an
        ``attr.ib`` read by ``on_activate`` (§11.13's auth-model citation) --
        so it is set BEFORE ``target.activate()`` ever runs. Cached under the
        ``"ssh"`` key so a repeat call reuses the same driver/ControlMaster
        and ``invalidate``'s ``deactivate_all_drivers`` tears it down.
        """
        if not self._config.ssh_keyfile:
            raise TargetError(
                "LABGRID_MCP_SSH_KEYFILE is not set; SSH tools need a private key"
            )
        async with self._lock_for(place):
            cached = await self._get_or_build(place)
            return await asyncio.to_thread(self._activate_ssh, cached, place)

    def _activate_ssh(self, cached: _Cached, place: str) -> Any:
        try:
            drv = cached.drivers.get("ssh")
            if drv is None:
                driver_cls = _DRIVERS["ssh"]
                drv = driver_cls(cached.target, name=None, keyfile=self._config.ssh_keyfile)
                cached.target.activate(drv)
                cached.drivers["ssh"] = drv
            return drv
        except TargetError:
            raise
        except Exception as exc:
            raise TargetError(f"ssh on place {place!r} failed: {exc}") from exc

    # ---- flash (§11.11) -------------------------------------------------

    async def flash_driver(self, place: str, kind: str) -> Any:
        """Bind + activate the §11.11 flash driver for ``kind`` and RETURN it.

        ``kind`` is one of :data:`_FLASH_KINDS`, or -- for ``bootstrap`` with a
        non-default loader -- ``"bootstrap:<loader>"`` (P5 review follow-up:
        the ``bootstrap`` tool's ``loader`` param, DESIGN §11.11's driver
        inventory table). It is encoded into ``kind`` because
        ``JobRegistry.submit_flash`` forwards ``kind`` opaquely with no
        separate loader parameter (jobs.py is out of scope for this change).
        An unknown flash kind OR unknown bootstrap loader raises
        :class:`TargetError` (naming the valid loaders) BEFORE the place lock
        is even acquired -- no Target is built and no driver is ever
        instantiated. Same discipline otherwise as :meth:`_run`: ownership
        re-checked, Target built/reused under the per-place lock, driver
        instantiated + activated in a worker thread (labgrid blocks). The
        driver is cached under ``"bootstrap:<loader>"`` (or the plain kind for
        the other four) so ``invalidate``'s ``deactivate_all_drivers`` tears it
        down, and so a LATER bootstrap job on the same place with a DIFFERENT
        loader rebinds instead of reusing a stale driver of the wrong class.
        Unlike power ops, the long ``download``/``flash``/``load``/
        ``write_image`` call is NOT made here — the caller (JobRegistry) runs it on
        a dedicated job thread so a multi-minute flash never occupies a shared
        executor slot (§11.11). The place should be pinned (:meth:`pin`) for the
        job's lifetime so a concurrent invalidate can't deactivate mid-write.
        """
        base_kind, loader = _split_flash_kind(kind)
        if base_kind not in _FLASH_KINDS:
            raise TargetError(f"unknown flash kind {kind!r}")
        if base_kind == "bootstrap" and loader not in _BOOTSTRAP_LOADERS:
            valid = ", ".join(sorted(_BOOTSTRAP_LOADERS))
            raise TargetError(f"unknown bootstrap loader {loader!r}; valid options: {valid}")
        async with self._lock_for(place):
            cached = await self._get_or_build(place)
            return await asyncio.to_thread(self._activate_flash, cached, place, base_kind, loader)

    def _activate_flash(self, cached: _Cached, place: str, kind: str, loader: str) -> Any:
        try:
            return self._activated_driver(cached, place, kind, loader=loader)
        except TargetError:
            raise
        except Exception as exc:
            raise TargetError(f"flash {kind} on place {place!r} failed: {exc}") from exc

    def pin(self, place: str, job: str) -> None:
        """Mark ``place`` as owned by running flash job ``job`` (§11.11).

        A pinned place refuses :meth:`invalidate` (and therefore ``release_place``)
        so its cached Target — and the active flash driver — cannot be torn down
        mid-write. Plain dict writes; called on the loop thread by the registry.
        """
        self._pins[place] = job

    def unpin(self, place: str, job: str) -> None:
        """Clear ``place``'s pin IFF it still belongs to ``job`` (idempotent).

        Identity-gated (mirrors the registry's ``_by_place`` cleanup): a job ending
        must never clear a *newer* job's pin — a completing job A and a freshly
        submitted job B can briefly overlap on a place during teardown, and an
        unconditional pop would unpin B mid-write.
        """
        if self._pins.get(place) == job:
            del self._pins[place]

    # ---- lifecycle ------------------------------------------------------

    async def invalidate(self, place: str) -> None:
        """Drop the cached Target for ``place`` (deactivate + prune best-effort).

        Holds the place lock so it cannot interleave an in-flight driver op;
        the deactivation itself runs in a worker thread (drivers block, §11.9).
        **Refuses while a flash job pins the place (§11.11)** — deactivating the
        driver mid-write can brick hardware; the caller must cancel the job first.
        The pin is re-checked UNDER the place lock (like ``_check_owned``) so a job
        pinned while this coroutine waited to acquire the lock is still honored.
        """
        async with self._lock_for(place):
            job = self._pins.get(place)
            if job is not None:
                raise TargetError(
                    f"cannot invalidate place {place!r}: flash job {job!r} is running; "
                    "cancel it first"
                )
            cached = self._targets.pop(place, None)
            if cached is None:
                return
            await asyncio.to_thread(self._invalidate_sync, cached)

    async def shutdown(self) -> None:
        """Invalidate every cached Target. Idempotent, leaves nothing behind."""
        for place in list(self._targets):
            await self.invalidate(place)

    # ---- internals ------------------------------------------------------

    async def _run(
        self,
        place: str,
        driver_key: str,
        fn: Callable[[Any], object],
        resource_name: str | None = None,
    ) -> object:
        """Serialized per-place op: check ownership, build/reuse Target, drive.

        The place lock is held across the WHOLE op — build AND driver call —
        because labgrid ``Target``/drivers are not thread-safe: without it two
        concurrent same-place calls could both instantiate+activate a driver in
        the thread pool. Ownership is re-checked on EVERY op (cheap snapshot
        read), so a stale cached Target can never drive a place we no longer
        own even if an invalidate was missed. ``resource_name`` (§11.14) selects
        a named same-class resource for the nameable kinds (power/io).
        """
        async with self._lock_for(place):
            cached = await self._get_or_build(place)
            return await asyncio.to_thread(
                self._driver_op, cached, place, driver_key, fn, resource_name
            )

    async def _get_or_build(self, place: str) -> _Cached:
        """Ownership-check, then build (once) or reuse the cached Target.

        MUST be called with the place lock held. §11.9 / plan: ownership +
        existence are checked BEFORE any Target is built, so an unknown/
        unacquired place never touches labgrid.
        """
        self._check_owned(place)
        cached = self._targets.get(place)
        if cached is None:
            try:
                cached = await asyncio.to_thread(self._build_target, place)
            except Exception as exc:
                raise TargetError(f"building target for place {place!r} failed: {exc}") from exc
            self._targets[place] = cached
        return cached

    def _invalidate_sync(self, cached: _Cached) -> None:
        """Worker-thread body for :meth:`invalidate` (best-effort, never raises)."""
        with contextlib.suppress(Exception):
            cached.target.deactivate_all_drivers()
        # §11.9 manager-leak trap: the singleton only ever appends. Prune the
        # RemotePlace itself AND its materialized children: unmanaged children
        # go to ``unmanaged_resources``, but managed ``Network*`` children
        # (``manager_cls = RemotePlaceManager``, resource/remote.py) register
        # into ``resources`` too — match both by ``parent``.
        # _MANAGER_LOCK: the reassignments are read-modify-writes on the
        # singleton; unguarded, they would drop a concurrent build's appends
        # for a DIFFERENT place (module invariant).
        with contextlib.suppress(Exception), _MANAGER_LOCK:
            mgr = _remote_place_manager()
            rp = cached.remote_place
            mgr.resources = [
                r for r in mgr.resources if r is not rp and getattr(r, "parent", None) is not rp
            ]
            mgr.unmanaged_resources = [
                r for r in mgr.unmanaged_resources if getattr(r, "parent", None) is not rp
            ]

    def _check_owned(self, place: str) -> None:
        snap = self._client.place(place)
        if snap is None:
            if not self._client.info().connected:
                # Mid-reconnect the coordinator snapshot is empty for EVERY
                # place, so a place we genuinely own can transiently look
                # "unknown" here -- name the real cause instead of misleading
                # the caller into re-acquiring (§Task 1 Step 3).
                raise TargetError("coordinator disconnected (reconnecting); retry shortly")
            raise TargetError(f"unknown place {place!r}")
        if snap.get("acquired") != self._config.identity:
            raise TargetError(
                f"place {place!r} is not acquired by this server; call acquire_place first"
            )

    def _build_target(self, place: str) -> _Cached:
        # §11.9 (PROVEN sequence): pre-seat the process-global manager BEFORE
        # constructing the RemotePlace, so ``on_resource_added`` uses our adapter
        # and skips ``_start()`` (no second ClientSession / subchannel abort).
        # _MANAGER_LOCK: seating + the RemotePlace construction (which appends
        # to ``mgr.resources``) mutate the singleton — module invariant.
        with _MANAGER_LOCK:
            mgr = _remote_place_manager()
            mgr.session = self._adapter
            mgr.loop = _StubLoop()
            mgr.env = None
            target = Target(place)
            remote_place = RemotePlace(target, name=place)  # eagerly fires on_resource_added
        return _Cached(target=target, remote_place=remote_place)

    def _driver_op(
        self,
        cached: _Cached,
        place: str,
        driver_key: str,
        fn: Callable[[Any], object],
        resource_name: str | None = None,
    ) -> object:
        """Worker-thread body: activate the driver (once) and run ``fn``.

        §11.9: labgrid drivers block, so this only ever runs under
        :func:`asyncio.to_thread`; every labgrid/driver failure wraps into
        :class:`TargetError`.
        """
        try:
            drv = self._activated_driver(cached, place, driver_key, resource_name=resource_name)
            return fn(drv)
        except TargetError:
            raise
        except Exception as exc:
            raise TargetError(f"{driver_key} on place {place!r} failed: {exc}") from exc

    def _activated_driver(
        self,
        cached: _Cached,
        place: str,
        driver_key: str,
        *,
        resource_name: str | None = None,
        loader: str = _DEFAULT_BOOTSTRAP_LOADER,
    ) -> Any:
        """Instantiate + activate (once) the driver for ``driver_key`` (§11.9/§11.11).

        ``loader`` only matters for ``driver_key == "bootstrap"``: it selects
        which of :data:`_BOOTSTRAP_LOADERS`' five BootstrapProtocol classes
        binds, and is folded into the cache key so a later bootstrap job on
        the same place with a DIFFERENT loader rebinds instead of reusing a
        stale driver of the wrong class.

        ``resource_name`` (§11.14) picks one of several same-class resources for
        the nameable kinds (:data:`_NAMED_KINDS` — power/io). It is folded into
        the cache key (``power`` → ``power:name:<name>``, mirroring the loader
        key) so two named drivers cache independently; ``name=None`` keeps the
        byte-identical single-resource path (same cache key, no
        ``set_binding_map``). Ambiguous (``name=None`` + >1 candidate) or unknown
        names raise :class:`TargetError` listing the available names, so
        labgrid's raw "multiple resources matching" never escapes.
        """
        if driver_key == "bootstrap":
            cache_key = f"bootstrap:{loader}"
        elif resource_name is not None:
            cache_key = f"{driver_key}:name:{resource_name}"
        else:
            cache_key = driver_key
        drv = cached.drivers.get(cache_key)
        if drv is None:
            driver_cls = (
                _BOOTSTRAP_LOADERS[loader] if driver_key == "bootstrap" else _DRIVERS[driver_key]
            )
            name = self._resolve_resource_name(cached, place, driver_cls, driver_key, resource_name)
            # §11.9 (PROVEN sequence): ``get_driver`` only FINDS an existing
            # driver, so instantiate ``cls(target, name=…)`` first (binds against
            # the resources materialized at RemotePlace construction), then
            # ``activate`` before any @Driver.check_active-guarded call. §11.14:
            # a named bind seeds ``set_binding_map`` immediately before, since it
            # is consumed by the very next driver's ``__init__``/``bind_driver``.
            drv = driver_cls(cached.target, name=name)
            cached.target.activate(drv)
            cached.drivers[cache_key] = drv
        return drv

    def _resolve_resource_name(
        self,
        cached: _Cached,
        place: str,
        driver_cls: Any,
        driver_key: str,
        resource_name: str | None,
    ) -> str | None:
        """Validate ``resource_name`` and seat the binding map (§11.14).

        Returns the ``name=`` to construct the driver with (``None`` for the
        legacy single-resource path). Only the nameable kinds enumerate/gate;
        every other kind returns ``None`` unchanged (byte-identical). Reads the
        candidate names from the place snapshot's ``acquired_resources`` filtered
        by the driver's binding class(es), so labgrid's raw ambiguity error is
        pre-empted with one that names the available resources.
        """
        if driver_key not in _NAMED_KINDS:
            return None
        binding_key, names = self._resource_names(place, driver_cls)
        if resource_name is None:
            if len(names) > 1:
                raise TargetError(
                    f"place {place!r} has multiple {driver_key} resources "
                    f"({', '.join(names)}); pass resource_name to pick one"
                )
            return None
        if binding_key is None or resource_name not in names:
            avail = ", ".join(names) if names else "none"
            raise TargetError(
                f"no {driver_key} resource named {resource_name!r} on place "
                f"{place!r}; available: {avail}"
            )
        cached.target.set_binding_map({binding_key: resource_name})
        return resource_name

    def _resource_names(self, place: str, driver_cls: Any) -> tuple[str | None, list[str]]:
        """Sorted candidate resource names for ``driver_cls`` on ``place`` (§11.14).

        Reads the coordinator snapshot's ``acquired_resources``
        (``[exporter, group, cls, name]``) and keeps names whose exported ``cls``
        is one of the driver's binding requirements — so a place with one power
        port and one serial port still resolves ``name=None`` for power.
        """
        binding_key, cls_names = _binding_spec(driver_cls)
        snap: Any = self._client.place(place) or {}
        names = sorted(
            {
                entry[3]
                for entry in (snap.get("acquired_resources") or [])
                if len(entry) >= 4 and entry[2] in cls_names
            }
        )
        return binding_key, names

    def _lock_for(self, place: str) -> asyncio.Lock:
        lock = self._locks.get(place)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[place] = lock
        return lock


def _get(drv: Any) -> bool:
    """Read a driver's current state (NetworkPowerDriver/HttpDigitalOutputDriver)."""
    return bool(drv.get())
