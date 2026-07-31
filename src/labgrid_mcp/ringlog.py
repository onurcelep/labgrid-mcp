"""Shared bounded byte-ring log buffer and crash-guarded lazy sweeper.

Extracted (P5 review follow-up) from the duplicated shapes in
:mod:`labgrid_mcp.console` (the per-session console read buffer) and
:mod:`labgrid_mcp.jobs` (the per-job flash log buffer): both kept an identical
bounded ``deque[int]``, dropping the OLDEST bytes on overflow and sticking a
``truncated`` flag until the next drain, plus decode-at-drain semantics owned
by the caller. Both also ran an identical lazy, crash-guarded periodic sweep
loop (an idle-TTL / retention cleanup) with a patchable ``_sleep`` clock seam.
This module gives both a single implementation; the domain-specific pieces
(what a "sweep" actually closes/drops, and the reader mechanism feeding each
ring) stay in their respective modules.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class ByteRing:
    """Bounded byte ring: drop-oldest overflow + a sticky-until-drained flag.

    Not internally locked — callers that share a ring across threads (a reader
    thread and the API, e.g.) must hold their own lock around mutation, exactly
    as console.py/jobs.py did with the raw ``deque`` before this extraction.
    """

    def __init__(self, capacity: int) -> None:
        self._buf: deque[int] = deque(maxlen=capacity)
        self._capacity = capacity
        self.truncated = False

    def __len__(self) -> int:
        return len(self._buf)

    def extend(self, data: bytes) -> None:
        """Append ``data``; bytes past ``capacity`` drop the OLDEST first and
        set :attr:`truncated` (which stays set until the next :meth:`drain`)."""
        if len(self._buf) + len(data) > self._capacity:
            self.truncated = True
        self._buf.extend(data)

    def drain(self, max_bytes: int | None = None) -> tuple[bytes, bool]:
        """Pop up to ``max_bytes`` (default: all) buffered bytes, oldest-first.

        Returns the popped bytes and the truncated flag as it stood BEFORE this
        call, then resets the flag. A partial drain (``max_bytes`` less than
        what's buffered) can split a multibyte UTF-8 sequence — decoding is the
        caller's job (with ``errors="replace"``, per console.py/jobs.py), so a
        split character surfaces as U+FFFD on each side of the boundary.
        """
        take = len(self._buf) if max_bytes is None else min(max_bytes, len(self._buf))
        chunk = bytes(self._buf.popleft() for _ in range(take))
        truncated = self.truncated
        self.truncated = False
        return chunk, truncated


class Sweeper:
    """Crash-guarded, lazily-started periodic sweep loop.

    ``interval_fn`` and ``sweep_fn`` are called FRESH on every use rather than
    captured once at construction — this preserves both modules' existing test
    conventions: patching a module-level interval constant (e.g.
    ``CONSOLE_SWEEP_INTERVAL_S``) or monkeypatching an instance's bound
    ``_sweep_once`` method takes effect immediately, even for a ``Sweeper``
    built before the patch. A ``sweep_fn`` that raises ``Exception`` is logged
    and the loop continues (a failed sweep must not silently stop cleanup
    forever); ``asyncio.CancelledError`` is a ``BaseException`` and always
    propagates, so a caller's ``task.cancel()`` (shutdown) still works.
    """

    def __init__(
        self,
        interval_fn: Callable[[], float],
        sweep_fn: Callable[[], Awaitable[None]],
        sleep: Callable[[float], Awaitable[None]],
        logger: logging.Logger,
        log_message: str,
    ) -> None:
        self._interval_fn = interval_fn
        self._sweep_fn = sweep_fn
        self._sleep = sleep
        self._logger = logger
        self._log_message = log_message

    def ensure_running(self, current: asyncio.Task[None] | None) -> asyncio.Task[None]:
        """Return ``current`` if it's a live task, else start + return a new one.

        Mirrors console.py's/jobs.py's ``_ensure_sweeper``: the caller owns the
        task handle (so ``shutdown`` can cancel it directly) and only calls this
        to lazily start the loop on first use.
        """
        if current is not None and not current.done():
            return current
        return asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        while True:
            await self._sleep(self._interval_fn())
            try:
                await self._sweep_fn()
            except Exception:
                self._logger.exception(self._log_message)
