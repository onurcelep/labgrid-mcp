"""Focused unit tests for the shared ByteRing/Sweeper helper (ringlog.py).

console.py's and jobs.py's own test suites cover these through their real
usage; these tests exercise the helper directly and in isolation.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from labgrid_mcp.ringlog import ByteRing, Sweeper

# ---- ByteRing ---------------------------------------------------------------


def test_ring_starts_empty_and_not_truncated() -> None:
    ring = ByteRing(8)
    assert len(ring) == 0
    assert ring.truncated is False


def test_extend_within_capacity_does_not_truncate() -> None:
    ring = ByteRing(8)
    ring.extend(b"hello")
    assert len(ring) == 5
    assert ring.truncated is False


def test_overflow_drops_oldest_and_sets_truncated() -> None:
    ring = ByteRing(8)
    ring.extend(b"0123456789")  # 10 bytes into an 8-byte ring

    assert len(ring) == 8
    assert ring.truncated is True
    chunk, truncated = ring.drain()
    assert chunk == b"23456789"  # oldest two dropped
    assert truncated is True


def test_drain_resets_truncated_flag() -> None:
    ring = ByteRing(4)
    ring.extend(b"01234")  # overflow by one

    ring.drain()
    assert ring.truncated is False
    # A second overflow-free extend + drain reports no truncation.
    ring.extend(b"ab")
    chunk, truncated = ring.drain()
    assert chunk == b"ab"
    assert truncated is False


def test_drain_all_empties_the_ring() -> None:
    ring = ByteRing(16)
    ring.extend(b"hello")

    chunk, truncated = ring.drain()
    assert chunk == b"hello"
    assert truncated is False
    assert len(ring) == 0

    # A second drain on an empty ring is a no-op, not an error.
    chunk2, truncated2 = ring.drain()
    assert chunk2 == b""
    assert truncated2 is False


def test_partial_drain_leaves_remainder_in_order() -> None:
    ring = ByteRing(16)
    ring.extend(b"hello")

    first, truncated = ring.drain(max_bytes=2)
    assert first == b"he"
    assert truncated is False
    assert len(ring) == 3

    second, _ = ring.drain()
    assert second == b"llo"
    assert len(ring) == 0


def test_partial_drain_max_bytes_larger_than_buffered_drains_all() -> None:
    ring = ByteRing(16)
    ring.extend(b"hi")

    chunk, _ = ring.drain(max_bytes=100)
    assert chunk == b"hi"
    assert len(ring) == 0


# ---- Sweeper ----------------------------------------------------------------


async def test_sweeper_ensure_running_starts_a_task_once() -> None:
    calls = {"n": 0}

    async def sweep() -> None:
        calls["n"] += 1

    interval = [0.001]
    sweeper = Sweeper(
        interval_fn=lambda: interval[0],
        sweep_fn=sweep,
        sleep=asyncio.sleep,
        logger=logging.getLogger("test-ringlog"),
        log_message="sweep failed",
    )

    task = sweeper.ensure_running(None)
    # A live task is returned unchanged by a second call.
    assert sweeper.ensure_running(task) is task

    deadline = asyncio.get_event_loop().time() + 2.0
    while calls["n"] < 2 and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.005)
    assert calls["n"] >= 2

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_sweeper_restarts_after_task_is_done() -> None:
    async def sweep() -> None:
        pass

    sweeper = Sweeper(
        interval_fn=lambda: 999.0,
        sweep_fn=sweep,
        sleep=asyncio.sleep,
        logger=logging.getLogger("test-ringlog"),
        log_message="sweep failed",
    )

    async def already_done() -> None:
        return None

    finished_task = asyncio.get_running_loop().create_task(already_done())
    await finished_task
    assert finished_task.done()

    new_task = sweeper.ensure_running(finished_task)
    assert new_task is not finished_task
    new_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await new_task


async def test_sweeper_survives_a_raising_sweep_fn() -> None:
    """A sweep_fn that raises must be logged and NOT kill the loop; a later,
    successful sweep still runs (mirrors console.py's/jobs.py's own coverage,
    exercised here against the shared helper directly)."""
    calls = {"n": 0}

    async def flaky_sweep() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("sweep boom")

    interval = [0.001]
    sweeper = Sweeper(
        interval_fn=lambda: interval[0],
        sweep_fn=flaky_sweep,
        sleep=asyncio.sleep,
        logger=logging.getLogger("test-ringlog"),
        log_message="sweep failed",
    )

    task = sweeper.ensure_running(None)
    deadline = asyncio.get_event_loop().time() + 2.0
    while calls["n"] < 2 and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.005)
    assert calls["n"] >= 2  # the loop survived the first sweep's exception

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_sweeper_cancellation_propagates() -> None:
    """CancelledError (a BaseException) must propagate out of the loop, not be
    swallowed by the Exception-only guard."""

    async def sweep() -> None:
        pass

    sweeper = Sweeper(
        interval_fn=lambda: 0.001,
        sweep_fn=sweep,
        sleep=asyncio.sleep,
        logger=logging.getLogger("test-ringlog"),
        log_message="sweep failed",
    )

    task = sweeper.ensure_running(None)
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def test_sweeper_reads_interval_and_sweep_fn_late(monkeypatch: pytest.MonkeyPatch) -> None:
    """interval_fn/sweep_fn are re-invoked each iteration, not captured once --
    mirrors console.py/jobs.py monkeypatching a module interval constant or an
    instance's bound _sweep_once AFTER the Sweeper object already exists."""

    class Box:
        def __init__(self) -> None:
            self.calls = 0

        async def sweep(self) -> None:
            self.calls += 1

    box = Box()
    interval = [0.001]
    sweeper = Sweeper(
        interval_fn=lambda: interval[0],
        sweep_fn=lambda: box.sweep(),
        sleep=asyncio.sleep,
        logger=logging.getLogger("test-ringlog"),
        log_message="sweep failed",
    )

    task = sweeper.ensure_running(None)
    await asyncio.sleep(0.02)

    # Swap in a new bound method after the Sweeper/task already exist.
    calls2 = {"n": 0}

    async def replacement() -> None:
        calls2["n"] += 1

    monkeypatch.setattr(box, "sweep", replacement)
    await asyncio.sleep(0.02)
    assert calls2["n"] > 0  # the late-bound lookup picked up the patched method

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
