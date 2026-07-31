"""Tests for JobRegistry (DESIGN §11.11).

The per-job output capture is the one place mocks are insufficient: those tests
run a REAL subprocess through the REAL process-global ``processwrapper`` path with
a scripted fake CLI that streams ``\\r``-separated progress chunks — exactly how
dfu-util / ``dd status=progress`` behave (§11.11 proven pattern). The lifecycle /
retention / duplicate-place tests use plain Python callables and patched clocks.

``TargetManager`` is faked to a tiny pin/unpin recorder — the registry never
touches real labgrid here.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from labgrid.util.helper import processwrapper  # SAME singleton jobs.py captures from

from labgrid_mcp import jobs as jobs_mod
from labgrid_mcp.jobs import JobError, JobRegistry
from labgrid_mcp.target import TargetError

# Fake CLI: stream tag'd '\r' progress chunks, then a final '\n' line (§11.11).
_PROGRESS_CLI = """\
import sys, time
tag = sys.argv[1]
for i in range(0, 96, 12):
    sys.stdout.write(f"{tag} [ {i}% ]\\r")
    sys.stdout.flush()
    time.sleep(0.005)
sys.stdout.write(f"{tag} done\\n")
sys.stdout.flush()
"""

# Fake CLI: emit one line (so the pid is captured), then block "forever".
_SLEEPER_CLI = """\
import sys, time
sys.stdout.write("SLEEPER started\\r")
sys.stdout.flush()
time.sleep(60)
"""


class FakeTargets:
    """Stands in for TargetManager: records pin/unpin + flash_driver binds (§11.11).

    ``bind_order`` records, in call order, whether the place was already pinned
    AT THE MOMENT ``flash_driver`` was entered -- the atomicity assertion for
    :meth:`JobRegistry.submit_flash` (requirement 1: pin-before-bind).

    ``bind_gates``: optional ``place -> asyncio.Event`` map. When present for a
    place, ``flash_driver`` awaits that event before returning -- lets a test
    hold one place's bind open (a real suspension point, mirroring the real
    per-place-locked activation) while driving assertions about a CONCURRENT
    bind on a DIFFERENT place (requirement 1's cross-place-concurrency fix).
    """

    def __init__(
        self,
        *,
        flash_driver_error: TargetError | None = None,
        bind_gates: dict[str, asyncio.Event] | None = None,
    ) -> None:
        self.pins: dict[str, str] = {}
        self.flash_driver_calls: list[tuple[str, str]] = []
        self.bind_order: list[bool] = []
        self.bind_entered: list[str] = []  # order binds STARTED
        self.bind_exited: list[str] = []  # order binds COMPLETED (success or error)
        self._flash_driver_error = flash_driver_error
        self._bind_gates = bind_gates or {}

    def pin(self, place: str, job: str) -> None:
        self.pins[place] = job

    def unpin(self, place: str, job: str) -> None:
        # Identity-gated, mirroring TargetManager.unpin (§11.11).
        if self.pins.get(place) == job:
            del self.pins[place]

    async def flash_driver(self, place: str, kind: str) -> object:
        self.flash_driver_calls.append((place, kind))
        self.bind_order.append(place in self.pins)
        self.bind_entered.append(place)
        gate = self._bind_gates.get(place)
        if gate is not None:
            await gate.wait()  # a real suspension point, mirroring the real bind
        try:
            if self._flash_driver_error is not None:
                raise self._flash_driver_error
            return object()  # the "driver"; build_fn stubs never introspect it
        finally:
            self.bind_exited.append(place)


def make_registry(targets: FakeTargets | None = None) -> JobRegistry:
    # client/config are unused by the registry; pass inert placeholders.
    return JobRegistry(targets or FakeTargets(), object(), object())  # type: ignore[arg-type]


async def wait_true(pred: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.005)
    return pred()


def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


def _run_cli(script: Path, *args: str) -> Callable[[], object]:
    """A submit ``fn`` that runs the fake CLI through the REAL processwrapper."""

    def fn() -> object:
        return processwrapper.check_output([sys.executable, str(script), *args])

    return fn


# ---- submit / lifecycle ---------------------------------------------------


async def test_submit_returns_id_immediately_while_fn_blocks() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)
    entered = threading.Event()
    gate = threading.Event()

    def fn() -> None:
        entered.set()
        assert gate.wait(5.0)

    handle = await reg.submit("p", "dfu", fn)
    assert set(handle) == {"job", "place", "kind"}
    assert handle["place"] == "p" and handle["kind"] == "dfu"
    job = handle["job"]
    assert isinstance(job, str) and len(job) == 12

    assert entered.wait(5.0)  # fn is already running on its own thread...
    assert reg.status(job)["state"] == "running"  # ...and submit returned meanwhile
    assert targets.pins.get("p") == job  # place pinned for the job's lifetime

    gate.set()
    assert await wait_true(lambda: reg.status(job)["state"] == "completed")
    st = reg.status(job)
    assert st["finished"] is not None and st["error"] is None
    # Additive wall-clock fields: captured at the same instants as monotonic.
    assert isinstance(st["created_at"], float) and isinstance(st["finished_at"], float)
    assert await wait_true(lambda: "p" not in targets.pins)  # unpinned when done

    await reg.shutdown()


async def test_failing_fn_marks_failed_with_error() -> None:
    reg = make_registry()

    def fn() -> None:
        raise RuntimeError("boom-flash")

    job = (await reg.submit("p", "dfu", fn))["job"]
    assert isinstance(job, str)
    assert await wait_true(lambda: reg.status(job)["state"] == "failed")
    st = reg.status(job)
    assert st["error"] is not None and "boom-flash" in str(st["error"])
    assert st["finished"] is not None

    await reg.shutdown()


async def test_duplicate_submit_on_same_place_rejected() -> None:
    reg = make_registry()
    gate = threading.Event()

    job = (await reg.submit("p", "dfu", lambda: gate.wait(5.0)))["job"]
    with pytest.raises(JobError, match="already has a running flash job"):
        await reg.submit("p", "fastboot", lambda: None)

    gate.set()
    assert isinstance(job, str)
    assert await wait_true(lambda: reg.status(job)["state"] == "completed")
    # Once finished, the place is free again.
    job2 = (await reg.submit("p", "dfu", lambda: None))["job"]
    assert isinstance(job2, str) and job2 != job

    await reg.shutdown()


async def test_unknown_job_raises_joberror() -> None:
    reg = make_registry()
    with pytest.raises(JobError, match="unknown flash job 'nope'"):
        reg.status("nope")
    with pytest.raises(JobError, match="unknown flash job 'nope'"):
        reg.logs("nope")
    await reg.shutdown()


async def test_jobs_payload_shape() -> None:
    reg = make_registry()
    gate = threading.Event()
    job = (await reg.submit("p", "write_image", lambda: gate.wait(5.0)))["job"]
    assert isinstance(job, str)

    payload = reg.jobs()
    assert len(payload) == 1
    entry = payload[0]
    assert entry["job"] == job
    assert entry["place"] == "p"
    assert entry["kind"] == "write_image"
    assert entry["state"] == "running"
    assert set(entry) == {
        "job",
        "place",
        "kind",
        "state",
        "created",
        "created_at",
        "finished",
        "finished_at",
        "buffered_bytes",
    }

    gate.set()
    await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


# ---- submit_flash: atomic pin + bind (requirement 1) -----------------------


async def test_submit_flash_pins_before_binding_driver() -> None:
    """The place is already pinned by the time ``flash_driver`` is entered."""
    targets = FakeTargets()
    reg = make_registry(targets)
    gate = threading.Event()

    def build(driver: object) -> Callable[[], object]:
        assert driver is not None
        return lambda: gate.wait(5.0)

    handle = await reg.submit_flash("p", "dfu", build)
    assert set(handle) == {"job", "place", "kind"}
    job = handle["job"]
    assert isinstance(job, str)

    assert targets.flash_driver_calls == [("p", "dfu")]
    assert targets.bind_order == [True]  # pinned BEFORE flash_driver ran
    assert targets.pins.get("p") == job

    gate.set()
    assert await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


async def test_submit_flash_rolls_back_pin_on_driver_bind_failure() -> None:
    """A failing bind (e.g. unknown kind, ownership lost) never leaves a stuck pin."""
    targets = FakeTargets(flash_driver_error=TargetError("place 'p' not owned"))
    reg = make_registry(targets)

    with pytest.raises(TargetError, match="not owned"):
        await reg.submit_flash("p", "dfu", lambda driver: (lambda: None))

    assert "p" not in targets.pins  # rolled back, not left pinned
    assert reg.jobs() == []  # no job was ever created
    # The place is free again -- a resubmit is not blocked by the failed attempt
    # (a fresh bind that succeeds this time).
    targets._flash_driver_error = None
    job = (await reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))["job"]
    assert isinstance(job, str)
    await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


async def test_submit_flash_rolls_back_pin_when_build_fn_raises() -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    def bad_build(driver: object) -> Callable[[], object]:
        raise ValueError("bad closure")

    with pytest.raises(ValueError, match="bad closure"):
        await reg.submit_flash("p", "dfu", bad_build)

    assert "p" not in targets.pins
    assert reg.jobs() == []
    await reg.shutdown()


async def test_submit_flash_duplicate_place_rejected_like_submit() -> None:
    reg = make_registry()
    gate = threading.Event()

    job = (await reg.submit_flash("p", "dfu", lambda driver: (lambda: gate.wait(5.0))))["job"]
    with pytest.raises(JobError, match="already has a running flash job"):
        await reg.submit_flash("p", "fastboot", lambda driver: (lambda: None))

    gate.set()
    assert isinstance(job, str)
    await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


async def test_submit_flash_rejects_resubmit_during_bind_window() -> None:
    """The _binding guard blocks a same-place resubmit WHILE a bind is still in
    flight -- before any real _Job exists for _reserve()'s state check to see."""
    gate = asyncio.Event()
    targets = FakeTargets(bind_gates={"p": gate})
    reg = make_registry(targets)

    task = asyncio.create_task(reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))
    await wait_true(lambda: "p" in targets.bind_entered)
    assert reg.jobs() == []  # bind in flight -- no _Job registered yet

    with pytest.raises(JobError, match="already has a running flash job"):
        await reg.submit_flash("p", "fastboot", lambda driver: (lambda: None))

    gate.set()
    handle = await asyncio.wait_for(task, timeout=5.0)
    assert isinstance(handle["job"], str)
    await wait_true(lambda: reg.status(handle["job"])["state"] == "completed")  # type: ignore[arg-type]
    await reg.shutdown()


async def test_submit_flash_does_not_serialize_binds_on_different_places() -> None:
    """Requirement 1 must not regress §11.11's 'concurrent flashes on different
    places are fine': place b's bind must complete without waiting for place
    a's still-gated bind (a global lock held across the bind would block it)."""
    gate_a = asyncio.Event()
    targets = FakeTargets(bind_gates={"a": gate_a})
    reg = make_registry(targets)

    task_a = asyncio.create_task(reg.submit_flash("a", "dfu", lambda driver: (lambda: None)))
    await wait_true(lambda: "a" in targets.bind_entered)
    assert "a" not in targets.bind_exited  # a's bind is still gated open

    handle_b = await asyncio.wait_for(
        reg.submit_flash("b", "dfu", lambda driver: (lambda: None)), timeout=2.0
    )
    assert handle_b["place"] == "b"
    assert "b" in targets.bind_exited  # b finished WITHOUT waiting for a's gate

    gate_a.set()
    handle_a = await asyncio.wait_for(task_a, timeout=5.0)
    assert handle_a["place"] == "a"

    def _completed(j: str) -> Callable[[], bool]:
        # A factory (not a loop-body lambda default arg, which mypy can't
        # infer the type of here) so each iteration's ``job`` binds correctly.
        return lambda: reg.status(j)["state"] == "completed"

    for h in (handle_a, handle_b):
        job = h["job"]
        assert isinstance(job, str)
        await wait_true(_completed(job))
    await reg.shutdown()


async def test_submit_flash_rolls_back_pin_when_cancelled_mid_bind() -> None:
    """A CancelledError mid-bind (client cancel/timeout) must still roll back --
    this is a BaseException, not an Exception, so the rollback must catch it."""
    gate = asyncio.Event()
    targets = FakeTargets(bind_gates={"p": gate})
    reg = make_registry(targets)

    task = asyncio.create_task(reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))
    await wait_true(lambda: "p" in targets.bind_entered)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert "p" not in targets.pins  # rolled back, not left pinned
    assert reg.jobs() == []

    # The place is free again -- cancellation must not leak the reservation.
    # (Re-set the shared gate: it's still wired to "p" in targets._bind_gates.)
    gate.set()
    job = (await reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))["job"]
    assert isinstance(job, str)
    await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


async def test_submit_flash_rolls_back_when_cancelled_at_post_bind_lock() -> None:
    """Fix round 2: cancellation delivered AFTER a successful bind, while
    suspended re-acquiring the registry lock, must still roll back pin +
    reservation + _binding -- previously that await sat outside the rollback
    guard and the place was left permanently unreleasable."""
    gate = asyncio.Event()
    targets = FakeTargets(bind_gates={"p": gate})
    reg = make_registry(targets)

    # Let phase 1 (reserve+pin) run and the task suspend INSIDE the gated bind
    # BEFORE we grab the lock -- grabbing it earlier would block phase 1 itself
    # and the cancellation would land at the wrong await.
    task = asyncio.create_task(reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))
    await wait_true(lambda: "p" in targets.bind_entered)
    assert targets.pins.get("p") is not None  # phase 1 committed: pinned

    async with reg._lock:  # now hold the lock so the POST-BIND acquisition suspends
        gate.set()  # bind completes; the task moves on to wait on reg._lock
        await wait_true(lambda: "p" in targets.bind_exited)
        for _ in range(3):
            await asyncio.sleep(0)  # let the task reach the lock acquisition
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task  # rollback is synchronous -- must not need the held lock

    assert "p" not in targets.pins  # pin rolled back
    assert reg.running_job_for("p") is None  # reservation + _binding rolled back
    assert reg.jobs() == []  # no half-created job

    # The place is fully usable again.
    job = (await reg.submit_flash("p", "dfu", lambda driver: (lambda: None)))["job"]
    assert isinstance(job, str)
    await wait_true(lambda: reg.status(job)["state"] == "completed")
    await reg.shutdown()


async def test_running_job_for_tracks_running_bind_window_and_finished() -> None:
    """running_job_for: None for unknown places, the id while running AND while
    mid-bind (pin already in force), None again once the job finishes."""
    gate = asyncio.Event()
    targets = FakeTargets(bind_gates={"p": gate})
    reg = make_registry(targets)

    assert reg.running_job_for("p") is None  # nothing yet

    run_gate = threading.Event()
    task = asyncio.create_task(
        reg.submit_flash("p", "dfu", lambda driver: (lambda: run_gate.wait(5.0)))
    )
    await wait_true(lambda: "p" in targets.bind_entered)
    assert reg.running_job_for("p") is not None  # mid-bind counts as running

    gate.set()
    handle = await asyncio.wait_for(task, timeout=5.0)
    job = handle["job"]
    assert isinstance(job, str)
    assert reg.running_job_for("p") == job  # running proper

    run_gate.set()
    assert await wait_true(lambda: reg.running_job_for("p") is None)  # finished
    assert reg.running_job_for("other") is None
    await reg.shutdown()


async def test_submit_rolls_back_pin_when_thread_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    def boom(self: threading.Thread) -> None:
        raise RuntimeError("cannot start thread")

    monkeypatch.setattr(threading.Thread, "start", boom)

    with pytest.raises(RuntimeError, match="cannot start thread"):
        await reg.submit("p", "dfu", lambda: None)

    assert "p" not in targets.pins
    assert reg.jobs() == []


async def test_submit_flash_rolls_back_pin_when_thread_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = FakeTargets()
    reg = make_registry(targets)

    def boom(self: threading.Thread) -> None:
        raise RuntimeError("cannot start thread")

    monkeypatch.setattr(threading.Thread, "start", boom)

    with pytest.raises(RuntimeError, match="cannot start thread"):
        await reg.submit_flash("p", "dfu", lambda driver: (lambda: None))

    assert "p" not in targets.pins
    assert reg.jobs() == []


# ---- REAL subprocess capture (§11.11 proven pattern) ----------------------


async def test_two_concurrent_jobs_capture_with_zero_crosstalk(tmp_path: Path) -> None:
    """Two real subprocesses through the real processwrapper: no buffer bleed."""
    reg = make_registry()
    script = _write(tmp_path, "flash.py", _PROGRESS_CLI)

    a = (await reg.submit("a", "dfu", _run_cli(script, "AAA")))["job"]
    b = (await reg.submit("b", "dfu", _run_cli(script, "BBB")))["job"]
    assert isinstance(a, str) and isinstance(b, str)

    assert await wait_true(
        lambda: reg.status(a)["state"] == "completed" and reg.status(b)["state"] == "completed"
    )

    la = reg.logs(a)["data"]
    lb = reg.logs(b)["data"]
    assert isinstance(la, str) and isinstance(lb, str)
    # Each job saw ONLY its own tag — the whole point of keying by thread ident.
    assert "AAA" in la and "BBB" not in la
    assert "BBB" in lb and "AAA" not in lb
    # Incremental '\r' progress chunks were captured live (multiple, not just the
    # final line).
    assert la.count("AAA") >= 3 and "done" in la
    # Draining consumed the buffer.
    assert reg.logs(a)["bytes"] == 0

    await reg.shutdown()


async def test_logs_bound_drops_oldest_and_flags_truncated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(jobs_mod, "JOB_LOG_BYTES", 24)  # tiny ring -> overflow
    reg = make_registry()
    script = _write(tmp_path, "flash.py", _PROGRESS_CLI)

    job = (await reg.submit("p", "dfu", _run_cli(script, "TAGTAGTAG")))["job"]
    assert isinstance(job, str)
    assert await wait_true(lambda: reg.status(job)["state"] == "completed")

    assert reg.status(job)["truncated"] is True  # overflow flagged
    first = reg.logs(job)
    assert first["truncated"] is True
    assert isinstance(first["bytes"], int) and first["bytes"] <= 24  # bounded
    # Truncated flag reset after a drain (drain semantics like console_read).
    assert reg.logs(job)["truncated"] is False

    await reg.shutdown()


# ---- retention sweep ------------------------------------------------------


async def test_sweep_drops_finished_past_ttl_keeps_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(jobs_mod, "_monotonic", lambda: clock[0])
    reg = make_registry()
    gate = threading.Event()

    done = (await reg.submit("done", "dfu", lambda: None))["job"]
    running = (await reg.submit("run", "dfu", lambda: gate.wait(5.0)))["job"]
    assert isinstance(done, str) and isinstance(running, str)
    assert await wait_true(lambda: reg.status(done)["state"] == "completed")

    clock[0] = jobs_mod.JOB_RETENTION_S + 1.0  # age the finished job past the TTL
    await reg._sweep_once()

    with pytest.raises(JobError):  # finished + aged -> swept away
        reg.status(done)
    assert reg.status(running)["state"] == "running"  # still-running job survives

    gate.set()
    await wait_true(lambda: reg.status(running)["state"] == "completed")
    await reg.shutdown()


# ---- shutdown: cancel running via SIGTERM ---------------------------------


async def test_shutdown_sigterms_running_job_and_is_idempotent(tmp_path: Path) -> None:
    reg = make_registry()
    script = _write(tmp_path, "sleeper.py", _SLEEPER_CLI)

    job = (await reg.submit("p", "dfu", _run_cli(script)))["job"]
    assert isinstance(job, str)
    # Wait until the pid is captured (the sleeper emits one line first).
    assert await wait_true(lambda: reg._jobs[job].pid is not None)
    pid = reg._jobs[job].pid
    assert pid is not None

    await reg.shutdown()  # SIGTERMs the child -> check_output raises -> cancelled

    assert reg.status(job)["state"] == "cancelled"
    thread = reg._jobs[job].thread
    assert thread is not None and not thread.is_alive()  # joined
    # The subprocess is gone.
    assert not _pid_alive(pid)

    await reg.shutdown()  # idempotent: no error, nothing running


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
