"""Background flash jobs (DESIGN §11.11).

A :class:`JobRegistry` runs each flash driver call — minutes-long and blocking —
on a dedicated ``threading.Thread`` and returns a job id immediately. Per-job
output is captured through ONE process-global ``processwrapper`` callback,
demultiplexed by ``threading.get_ident()`` into bounded per-job byte rings (the
proven §11.11 mechanism; mocks are insufficient there, so its test drives a real
subprocess). Finished jobs are retained with a TTL and swept, mirroring
:mod:`console`'s crash-guarded sweeper.

**Why a dedicated thread, not ``asyncio.to_thread`` (§11.11):** a multi-minute
flash would pin a slot in the shared default ``ThreadPoolExecutor`` that every
power/io/target op draws from, starving unrelated places. **Why the thread-local
``@step`` rebind (§11.11):** every flash public method AND ``processwrapper.
check_output`` are ``@step``-decorated on the process-global, thread-unsafe step
stack; :func:`install_thread_safe_steps` makes it per-thread so concurrent flash /
console / power calls never corrupt it — with no global lock.

**Cancel is best-effort and brick-risky (§11.11):** a worker thread can't be
force-killed and ``check_output`` has no cancel hook, so the only mechanism is
``os.kill(pid, SIGTERM)`` on the subprocess captured from the callback. Killing a
flash mid-write can brick hardware — this phase only cancels from :meth:`shutdown`
(the flash family is gated behind ``LABGRID_MCP_ALLOW=flash``).

Verified against labgrid 26.0. Re-verify when the pin moves.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from labgrid.util.helper import processwrapper  # process-global ProcessWrapper (§11.11)

from labgrid_mcp.ringlog import ByteRing, Sweeper
from labgrid_mcp.target import install_thread_safe_steps

if TYPE_CHECKING:
    from collections.abc import Callable

    from labgrid_mcp.config import Config
    from labgrid_mcp.coordinator import CoordinatorClient
    from labgrid_mcp.target import TargetManager

# Bounded per-job output ring; overflow drops OLDEST and flags truncation (§3.4).
JOB_LOG_BYTES = 262144
# Finished jobs older than this are swept; the sweeper runs on this cadence.
JOB_RETENTION_S = 3600.0
JOB_SWEEP_INTERVAL_S = 60.0

# Bounded join so a wedged flash thread can never hang shutdown.
_JOIN_TIMEOUT_S = 10.0

# Clock seams (patched in tests to drive TTL deterministically), mirroring
# console.py's convention.
_sleep = asyncio.sleep
_monotonic = time.monotonic

_logger = logging.getLogger(__name__)


class JobError(Exception):
    """A flash-job operation failed (unknown job, duplicate place, etc.)."""


@dataclass
class _Job:
    """One flash job: worker thread + bounded byte ring + lifecycle state.

    ``lock`` guards every field the worker thread, the capture callback, and the
    API touch concurrently (``buffer``, ``state``, ``error``, ``finished``/
    ``finished_at``, ``pid``). ``created``/``finished`` are monotonic (interval
    math); ``created_at``/``finished_at`` are the wall-clock (epoch seconds)
    counterparts captured at the same instants, for display/logging.
    """

    id: str
    place: str
    kind: str
    created: float
    created_at: float
    state: str = "running"  # "running" | "completed" | "failed" | "cancelled"
    finished: float | None = None
    finished_at: float | None = None
    error: str | None = None
    pid: int | None = None
    cancelled: bool = False
    thread: threading.Thread | None = None
    buffer: ByteRing = field(default_factory=lambda: ByteRing(JOB_LOG_BYTES))
    lock: threading.Lock = field(default_factory=threading.Lock)


class JobRegistry:
    """Owns background flash jobs keyed by job id (§11.11).

    Structural mutations (submit / sweep / shutdown) are serialized by a single
    ``asyncio.Lock`` on the loop thread. :meth:`status`, :meth:`logs`, and
    :meth:`jobs` are lock-free snapshots (dict lookups are atomic; each job's own
    ``threading.Lock`` guards its contents). The capture callback and the
    thread<->place bookkeeping use a separate ``threading.Lock`` since they run on
    worker threads.
    """

    def __init__(self, targets: TargetManager, client: CoordinatorClient, config: Config) -> None:
        self._targets = targets
        self._client = client
        self._config = config
        self._jobs: dict[str, _Job] = {}
        self._by_place: dict[str, str] = {}  # place -> running job id (one per place)
        # job ids currently mid-bind in submit_flash (reserved + pinned, but not
        # yet a real _Job) -- blocks a same-place resubmit during the bind window
        # without needing self._jobs to already contain them (see submit_flash).
        self._binding: set[str] = set()
        self._lock = asyncio.Lock()
        self._sweeper: asyncio.Task[None] | None = None
        # interval_fn/sweep_fn are looked up FRESH each iteration (not captured
        # here), so monkeypatching JOB_SWEEP_INTERVAL_S or an instance's bound
        # _sweep_once after construction still takes effect (§ringlog).
        self._sweep_helper = Sweeper(
            interval_fn=lambda: JOB_SWEEP_INTERVAL_S,
            sweep_fn=lambda: self._sweep_once(),
            sleep=_sleep,
            logger=_logger,
            log_message="job retention sweep failed; retrying next interval",
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        # thread ident -> job, for the process-global capture callback (§11.11).
        self._buffers: dict[int, _Job] = {}
        self._capture_lock = threading.Lock()
        self._capture_registered = False
        # Cover the entry path even if no job is ever submitted here.
        install_thread_safe_steps()

    # ---- public API -----------------------------------------------------

    async def submit(self, place: str, kind: str, fn: Callable[[], object]) -> dict[str, object]:
        """Start ``fn`` on a dedicated thread and return its job id immediately.

        One job per place: a second submit while a job runs on ``place`` raises
        :class:`JobError`. The place is pinned on the :class:`TargetManager` for the
        job's lifetime so a concurrent ``release_place``/``invalidate`` can't
        deactivate the flash driver mid-write (§11.11). ``fn`` is the caller's
        driver-call closure (explicit file paths — the Target has ``env is None``,
        §11.11); its return value is ignored (flash ops return ``None``).

        This is the low-level primitive: ``fn`` must already be bound to an
        activated driver. Callers that need to bind the driver THEMSELVES (i.e.
        every flash tool) should use :meth:`submit_flash` instead — binding
        outside of this method reopens the pin-vs-invalidate race that method
        closes (see its docstring). If starting the worker thread itself fails
        (e.g. the OS refuses to create one), the reservation and pin are rolled
        back before the error propagates.
        """
        self._install()
        self._loop = asyncio.get_running_loop()
        async with self._lock:
            job_id = self._reserve(place)
            job = _Job(
                id=job_id, place=place, kind=kind, created=_monotonic(), created_at=time.time()
            )
            self._targets.pin(place, job_id)
            try:
                self._start(job, fn)
            except Exception:
                self._targets.unpin(place, job_id)
                self._forget_reservation(place, job_id)
                raise
        return {"job": job_id, "place": place, "kind": kind}

    async def submit_flash(
        self,
        place: str,
        kind: str,
        build_fn: Callable[[Any], Callable[[], object]],
    ) -> dict[str, object]:
        """Reserve + pin, bind the flash driver, then start the job.

        **Why this exists (review-mandated, closes a TOCTOU):** a caller that
        instead did ``driver = await targets.flash_driver(place, kind)`` and then
        separately ``await registry.submit(place, kind, fn)`` would leave a gap
        between those two awaited calls during which the driver is bound but the
        place is NOT YET pinned — a concurrent ``release_place``/``invalidate``
        could land in that gap and deactivate the driver mid-bind. This matters
        even for the drivers whose ``on_deactivate`` IS a no-op (dfu/fastboot/
        script/bootstrap, §11.9): ``write_image``'s ``USBStorageDriver.
        on_deactivate`` is NOT a no-op (it tears down a real udisks2
        ``AgentWrapper`` on the exporter), so an untimely deactivate is a genuine
        mid-write hazard, not just a theoretical one.

        The place is reserved (:meth:`_reserve`) and pinned, and the job id is
        marked :attr:`_binding`, ALL synchronously under :attr:`_lock` with no
        ``await`` in between — so the pin is always in place before this
        method's very first suspension point: the driver bind that follows
        (``TargetManager.flash_driver``, which calls ``build_fn`` with the
        activated driver to get the zero-arg job closure — explicit file/script
        paths, §11.11's ``target.env is None`` trap). That bind then runs
        WITHOUT holding :attr:`_lock`: ``TargetManager`` already serializes per
        PLACE internally
        (its own per-place lock), so concurrent binds on DIFFERENT places proceed
        independently here, matching §11.11 ("concurrent flashes on different
        places are fine") instead of needlessly serializing them behind one
        global lock. A same-place resubmit during the bind window is rejected by
        :meth:`_reserve` checking :attr:`_binding` -- it doesn't need the job to
        already exist in :attr:`_jobs` to be recognized as busy.

        On ANY failure before the worker thread is running -- unknown ``kind``,
        ownership lost, driver activation error, ``build_fn`` itself raising,
        the caller's task being CANCELLED at ANY of this method's await points
        (the bind itself OR the post-bind lock re-acquisition), or
        ``thread.start()`` failing -- the reservation, pin, and ``_binding``
        mark are ALL rolled back before the error propagates, so a
        failed/cancelled submission never leaves the place stuck pinned with no
        job to show for it. Cancellation is a :class:`BaseException`, not an
        ``Exception``, so one ``except BaseException`` guards the WHOLE
        post-reserve region; the rollback body is deliberately SYNCHRONOUS (set/
        dict mutations only, no ``await``) -- it must not acquire :attr:`_lock`,
        both because an ``await`` inside a cancellation handler can itself be
        re-cancelled (skipping the rest of the rollback) and because the very
        cancellation being handled may have been delivered while waiting on
        that lock. This is safe lock-free: every mutator of ``_binding``/
        ``_by_place``/the pins runs on the loop thread, and every lock-held
        critical section in this class is internally synchronous, so a
        synchronous rollback can never interleave into one.
        """
        self._install()
        self._loop = asyncio.get_running_loop()
        async with self._lock:
            job_id = self._reserve(place)
            self._binding.add(job_id)
            self._targets.pin(place, job_id)
        try:
            driver = await self._targets.flash_driver(place, kind)
            fn = build_fn(driver)
            async with self._lock:
                self._binding.discard(job_id)
                job = _Job(
                    id=job_id, place=place, kind=kind, created=_monotonic(), created_at=time.time()
                )
                # No await between here and return: once _start succeeds the
                # job is committed and cancellation can no longer be delivered
                # inside this method.
                self._start(job, fn)
        except BaseException:
            self._binding.discard(job_id)
            self._forget_reservation(place, job_id)
            self._targets.unpin(place, job_id)
            raise
        return {"job": job_id, "place": place, "kind": kind}

    def status(self, job: str) -> dict[str, object]:
        """Lifecycle snapshot for one job (running/completed/failed/cancelled).

        ``created``/``finished`` are monotonic seconds (interval math);
        ``created_at``/``finished_at`` are the epoch-seconds wall-clock
        counterparts captured at the same instants (additive fields).
        """
        j = self._require(job)
        with j.lock:
            return {
                "job": j.id,
                "place": j.place,
                "kind": j.kind,
                "state": j.state,
                "created": j.created,
                "created_at": j.created_at,
                "finished": j.finished,
                "finished_at": j.finished_at,
                "error": j.error,
                "truncated": j.buffer.truncated,
            }

    def logs(self, job: str, max_bytes: int | None = None) -> dict[str, object]:
        """Drain up to ``max_bytes`` (default all) captured bytes, consuming them.

        Drain semantics mirror ``console_read``: the ring stores bytes, decoding
        happens here with ``errors="replace"`` so arbitrary chunk boundaries never
        corrupt a full drain (a ``max_bytes`` partial drain can split a multibyte
        sequence into U+FFFD, as documented for the console). Resets the truncated
        flag it reports.
        """
        j = self._require(job)
        with j.lock:
            chunk, truncated = j.buffer.drain(max_bytes)
        return {
            "job": job,
            "data": chunk.decode("utf-8", errors="replace"),
            "bytes": len(chunk),
            "truncated": truncated,
        }

    def running_job_for(self, place: str) -> str | None:
        """The id of the job currently running (or mid-bind) on ``place``, else None.

        Lock-free snapshot like :meth:`status`/:meth:`logs`/:meth:`jobs` (dict
        lookups are atomic on the loop thread). A job mid-bind in
        :meth:`submit_flash` (reserved + pinned, no :class:`_Job` yet) counts as
        running — its pin is already in force, so callers gating on this (e.g.
        ``release_place``'s pre-RPC refusal) must refuse for it too. A finished
        job whose loop-thread cleanup hasn't drained yet does NOT count.
        """
        job_id = self._by_place.get(place)
        if job_id is None:
            return None
        if job_id in self._binding:
            return job_id
        j = self._jobs.get(job_id)
        if j is not None and j.state == "running":
            return job_id
        return None

    def jobs(self) -> list[dict[str, object]]:
        """``labgrid://sessions`` payload: one dict per known job.

        ``created``/``finished`` are monotonic seconds (interval math);
        ``created_at``/``finished_at`` are the epoch-seconds wall-clock
        counterparts captured at the same instants (additive fields).
        """
        out: list[dict[str, object]] = []
        for j in list(self._jobs.values()):
            with j.lock:
                out.append(
                    {
                        "job": j.id,
                        "place": j.place,
                        "kind": j.kind,
                        "state": j.state,
                        "created": j.created,
                        "created_at": j.created_at,
                        "finished": j.finished,
                        "finished_at": j.finished_at,
                        "buffered_bytes": len(j.buffer),
                    }
                )
        return out

    async def shutdown(self) -> None:
        """Cancel running jobs (SIGTERM best-effort) and stop the sweeper. Idempotent.

        Cancel is brick-risky (§11.11) but shutdown must not leave a flash thread
        pinning a Target forever: SIGTERM the captured pid, then bounded-join the
        thread and clear its place bookkeeping so the subsequent
        ``TargetManager.shutdown`` can invalidate.
        """
        task = self._sweeper
        self._sweeper = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            running = [j for j in self._jobs.values() if j.state == "running"]
        for job in running:
            job.cancelled = True
            with job.lock:
                pid = job.pid
            if pid is not None:
                # Best-effort: the child may already be gone.
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.kill(pid, signal.SIGTERM)
        for job in running:
            t = job.thread
            if t is not None:
                await asyncio.to_thread(t.join, _JOIN_TIMEOUT_S)
            # Clear pin/place now in case the worker's loop callback hasn't drained.
            self._on_job_done(job)
        with self._capture_lock:
            if self._capture_registered:
                processwrapper.unregister(self._capture_callback)
                self._capture_registered = False

    # ---- internals ------------------------------------------------------

    def _require(self, job: str) -> _Job:
        j = self._jobs.get(job)
        if j is None:
            raise JobError(f"unknown flash job {job!r}")
        return j

    def _reserve(self, place: str) -> str:
        """Claim a fresh job id for ``place`` in ``_by_place``. MUST be called
        with :attr:`_lock` held; synchronous (no ``await``) so the reservation
        is atomic with whatever the caller does next before its own next await.

        Rejects with :class:`JobError` if ``place`` already has a RUNNING job
        OR a job currently mid-bind (:attr:`_binding` -- ``submit_flash``'s bind
        window, before a real :class:`_Job` exists to check ``.state`` on). A
        finished job whose loop-thread cleanup (``_on_job_done``) has not
        drained yet is treated as stale and cleared, so an immediate resubmit
        right after ``status == completed`` never spuriously fails.
        """
        existing = self._by_place.get(place)
        if existing is not None:
            prev = self._jobs.get(existing)
            busy = existing in self._binding or (prev is not None and prev.state == "running")
            if busy:
                raise JobError(
                    f"place {place!r} already has a running flash job {existing!r}; "
                    "wait for it to finish"
                )
            self._by_place.pop(place, None)
        job_id = uuid4().hex[:12]
        self._by_place[place] = job_id
        return job_id

    def _forget_reservation(self, place: str, job_id: str) -> None:
        """Drop ``_by_place[place]`` IFF it still points at ``job_id`` (idempotent).

        Identity-gated like :meth:`TargetManager.unpin`: a rollback must never
        clear a *newer* submission's reservation on the same place.
        """
        if self._by_place.get(place) == job_id:
            del self._by_place[place]

    def _start(self, job: _Job, fn: Callable[[], object]) -> None:
        """Register ``job``, start its worker thread, and ensure the sweeper.

        MUST be called with :attr:`_lock` held. If starting the thread itself
        fails (e.g. the OS refuses to create one), ``job`` is un-registered
        again before the error propagates -- the CALLER is still responsible
        for rolling back the pin/place reservation :meth:`_start` doesn't own.
        """
        self._jobs[job.id] = job
        try:
            job.thread = threading.Thread(
                target=self._run, args=(job, fn), name=f"flash-{job.id}", daemon=True
            )
            job.thread.start()
            self._ensure_sweeper()
        except Exception:
            self._jobs.pop(job.id, None)
            raise

    def _install(self) -> None:
        """Idempotently install the steps fix and the process-global capture callback."""
        install_thread_safe_steps()
        with self._capture_lock:
            if not self._capture_registered:
                processwrapper.register(self._capture_callback)
                self._capture_registered = True

    def _run(self, job: _Job, fn: Callable[[], object]) -> None:
        """Worker thread: register for capture, run ``fn``, record terminal state.

        The capture callback (below) runs IN this thread, so keying ``_buffers`` by
        this thread's ident routes only this job's output here (§11.11). A
        SIGTERM'd flash raises ``CalledProcessError`` → ``cancelled`` (the
        ``cancelled`` flag distinguishes it from a genuine failure).
        """
        ident = threading.get_ident()
        with self._capture_lock:
            self._buffers[ident] = job
        try:
            fn()
            state, error = "completed", None
        except Exception as exc:
            if job.cancelled:
                state, error = "cancelled", None
            else:
                state, error = "failed", str(exc) or type(exc).__name__
        finally:
            with self._capture_lock:
                self._buffers.pop(ident, None)
        with job.lock:
            job.state = state
            job.error = error
            job.finished = _monotonic()
            job.finished_at = time.time()
        loop = self._loop
        if loop is not None:
            # A job finishing exactly as the loop is torn down would raise on a
            # closed loop; shutdown clears the pin directly, so this is best-effort.
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(self._on_job_done, job)

    def _on_job_done(self, job: _Job) -> None:
        """Clear a finished job's place bookkeeping + pin. Loop thread; idempotent.

        Both clears are identity-gated so they never disturb a newer job that has
        already claimed the place (a completing job and a resubmit can overlap).
        """
        if self._by_place.get(job.place) == job.id:
            del self._by_place[job.place]
        self._targets.unpin(job.place, job.id)

    def _capture_callback(self, chunk: bytes, process: Any) -> None:
        """Process-global ``processwrapper`` callback: demux by thread ident (§11.11).

        Runs in the flash job's worker thread. Chunks from any non-flash thread
        (labgrid's own ``enable_logging``/``enable_print`` register global
        callbacks too) find no entry and return early.
        """
        ident = threading.get_ident()
        with self._capture_lock:
            job = self._buffers.get(ident)
        if job is None:
            return
        # processwrapper splits on '\r'; rejoin chunks with '\n' for a readable log.
        data = chunk + b"\n"
        with job.lock:
            if job.pid is None:
                job.pid = int(process.pid)  # captured for best-effort cancel (§11.11)
            job.buffer.extend(data)

    def _ensure_sweeper(self) -> None:
        self._sweeper = self._sweep_helper.ensure_running(self._sweeper)

    async def _sweep_once(self) -> None:
        """Drop finished jobs retained past ``JOB_RETENTION_S``; keep running jobs."""
        async with self._lock:
            now = _monotonic()
            expired = [
                jid
                for jid, j in self._jobs.items()
                if j.finished is not None and now - j.finished >= JOB_RETENTION_S
            ]
            for jid in expired:
                self._jobs.pop(jid, None)
