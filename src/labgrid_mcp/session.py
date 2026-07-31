"""Place session orchestration: reservation keepalive + acquire/release flows.

``PlaceSession`` sits on top of Task 1's :class:`CoordinatorClient` unary
wrappers and turns them into the two operations an agent actually wants:
"get me this place" (``acquire_place``) and "let it go" (``release_place``),
plus the lower-level ``reserve``/``cancel_reservation`` passthrough with a
background keepalive.

Spike verdict (Step 1 — verified against a live ``labgrid-coordinator`` on an
ephemeral port, driving it through this package's own ``CoordinatorClient``):
**cancelling the reservation immediately after a successful ``AcquirePlace`` is
SAFE — the place stays acquired.** Observed sequence (labgrid 26.0)::

    reservation created: allocated
    AFTER ACQUIRE  acquired=spikehost/spikeuser reservation=IZX23UPGA0
    cancel_reservation_rpc(token)
    AFTER CANCEL   acquired=spikehost/spikeuser reservation=None
    poll after cancel: errored (FAILED_PRECONDITION — token gone)

So ``acquire_place`` cancels the reservation right after acquiring: there is no
token to keep alive past the acquire, and nothing to leak. This matches §11.8
("after acquire no keepalive is needed") — the acquire hands ownership to the
session's ClientStream identity, which is independent of the reservation.

Keepalive design: **one ``asyncio.Task`` per tracked token** (keyed in
``_keepalive_tasks``), not a single shared loop. Per-token tasks make the
lifecycle trivial to reason about — registration is a dict insert, cancellation
cancels exactly one task and awaits it, and a task that reaches a terminal
reservation state removes only its own entry. The acquire flow does *not* spawn
a keepalive task: its bounded 1 s poll loop already refreshes the reservation
(``PollReservation`` calls ``res.refresh()`` — §11.8), which is a tighter
cadence than the 15 s keepalive. Background keepalive tasks come only from
``reserve()``, where the agent holds a pending reservation without acquiring.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING

from labgrid_mcp.coordinator import CoordinatorError

if TYPE_CHECKING:
    from collections.abc import Callable

    from labgrid_mcp.config import Config
    from labgrid_mcp.coordinator import CoordinatorClient

KEEPALIVE_INTERVAL_S = 15.0
ACQUIRE_POLL_INTERVAL_S = 1.0

# The coordinator's place update for Acquire/ReleasePlace arrives on the
# ClientStream asynchronously, independent of the mutating unary RPC's own
# completion (DESIGN.md section 11.8(e)) -- so a snapshot read immediately
# after a successful RPC can briefly still show the pre-mutation state.
# Poll our own in-memory cache (never a network round trip) for up to
# _SNAPSHOT_SYNC_TIMEOUT_S; if it never catches up (a genuine staleness or
# reconnect edge case) return whatever we have rather than block or raise --
# same "best effort, bounded" spirit as the rest of this module.
_SNAPSHOT_SYNC_TIMEOUT_S = 2.0
_SNAPSHOT_SYNC_POLL_S = 0.05

# Test seams (mirror coordinator.py): patch to skip real delays / drive a fake
# clock without wall-clock waits.
_sleep = asyncio.sleep
_monotonic = time.monotonic

# Reservation states that mean the keepalive should stop polling: ``acquired``
# auto-refreshes coordinator-side, ``expired``/``invalid`` are dead (§11.8).
_KEEPALIVE_DONE_STATES = frozenset({"acquired", "expired", "invalid"})
# States that mean an in-flight acquire's reservation died before allocation.
_DEAD_RESERVATION_STATES = frozenset({"expired", "invalid"})


class PlaceSession:
    """Owns reservation keepalive + the acquire/release orchestration."""

    def __init__(self, client: CoordinatorClient, config: Config) -> None:
        self._client = client
        self._config = config
        self._keepalive_tasks: dict[str, asyncio.Task[None]] = {}

    # ---- acquire / release ---------------------------------------------

    async def acquire_place(self, name: str) -> dict[str, object]:
        """Acquire ``name`` for this session; return the final place dict.

        - already ours → return the snapshot, no RPC.
        - free → direct ``AcquirePlace``.
        - taken or reserved by someone else → reserve ``{"name": name}``, poll
          until the reservation allocates our place (bounded by
          ``config.acquire_timeout``), acquire, then cancel the reservation
          (safe per the module-docstring spike).

        Raises ``CoordinatorError`` on RPC failure or acquire timeout; every
        error/cancel path cancels the reservation it created.
        """
        place = self._client.place(name)
        if place is not None and place.get("acquired") == self._config.identity:
            return place
        if place is None or not (place.get("acquired") or place.get("reservation")):
            await self._client.acquire_place_rpc(name)
            return await self._snapshot_synced(name, self._is_ours)
        # Taken or reserved. If the place is reserved by a token we already
        # hold (a prior ``reserve()`` we keepalive-track), reuse THAT
        # reservation. Creating a second ``{"name": name}`` reservation here
        # would deadlock: the coordinator only allocates places whose
        # ``reservation is None`` (labgrid/remote/coordinator.py:1007), and the
        # place is already bound to our first token -- so a second reservation
        # can never allocate and we would poll to timeout.
        reservation = place.get("reservation")
        if isinstance(reservation, str) and reservation in self._keepalive_tasks:
            return await self._acquire_via_reservation(name, token=reservation)
        return await self._acquire_via_reservation(name)

    async def _acquire_via_reservation(
        self, name: str, *, token: str | None = None
    ) -> dict[str, object]:
        if token is None:
            res = await self._client.create_reservation({"name": name})
            token = str(res["token"])
        else:
            # Reuse a reservation we already own (keepalive-tracked): poll it
            # to learn its current state -- it may already be allocated.
            res = await self._client.poll_reservation(token)
        try:
            deadline = _monotonic() + self._config.acquire_timeout
            while not _allocates(res, name):
                state = res.get("state")
                if state in _DEAD_RESERVATION_STATES:
                    raise CoordinatorError(
                        f"reservation for place {name!r} became {state} before allocation"
                    )
                if _monotonic() >= deadline:
                    raise CoordinatorError(
                        f"timed out acquiring place {name!r} after "
                        f"{self._config.acquire_timeout}s (reservation {state})"
                    )
                await _sleep(ACQUIRE_POLL_INTERVAL_S)
                res = await self._client.poll_reservation(token)
            await self._client.acquire_place_rpc(name)
        except BaseException:
            # Timeout, RPC failure, or cancellation: drop the reservation so it
            # does not linger. Best-effort — if the cancel await is itself
            # interrupted, the reservation expires within its ~60 s TTL since
            # polling has stopped. ``cancel_reservation`` (not the bare RPC)
            # also stops the keepalive when this was a reused, tracked token; a
            # freshly-created token is untracked, so the stop is a no-op.
            with contextlib.suppress(Exception):
                await self.cancel_reservation(token)
            raise
        # Acquired. Spike: the place stays acquired after cancel, so release the
        # reservation now rather than keep a token alive for nothing. Untrack
        # the keepalive too (no-op for a freshly-created token).
        with contextlib.suppress(CoordinatorError):
            await self.cancel_reservation(token)
        return await self._snapshot_synced(name, self._is_ours)

    async def release_place(self, name: str, *, kick: bool = False) -> dict[str, object]:
        """Release ``name``; return the resulting place snapshot.

        Default: verify from the snapshot that we hold it
        (``place["acquired"] == config.identity``) *before* the RPC, raising
        ``CoordinatorError`` naming the actual owner otherwise (no RPC sent) so
        the caller gets a friendly "held by X" error. ``kick=True`` skips the
        check.

        Defence in depth for the check→RPC race (ownership can change between
        the snapshot read and the RPC): the non-kick path passes
        ``fromuser=config.identity`` so the *coordinator* also enforces
        ownership. labgrid's ``ReleasePlace`` compares ``fromuser`` against
        ``place.acquired`` and silently returns without releasing when they
        differ (labgrid/remote/coordinator.py:905, labgrid 26.0).
        ``place.acquired`` is set to the full ``"host/user"`` client name
        (client.py:143 ``msg.startup.name``), which is exactly our
        ``config.identity`` — so a legitimate self-release always matches,
        while a wrongful release off a stale snapshot becomes a coordinator-side
        no-op instead of kicking whoever actually holds the place.

        ``kick=True`` keeps ``fromuser=""`` — an unconditional coordinator-side
        release (§11.8) that frees the place regardless of holder.
        """
        fromuser = ""
        if not kick:
            place = self._client.place(name)
            owner = place.get("acquired") if place is not None else None
            if owner != self._config.identity:
                raise CoordinatorError(
                    f"cannot release place {name!r}: held by "
                    f"{owner or 'nobody'}, not {self._config.identity}"
                )
            fromuser = self._config.identity
        await self._client.release_place_rpc(name, fromuser=fromuser)
        return await self._snapshot_synced(name, lambda p: not p.get("acquired"))

    # ---- reservation passthrough + keepalive ----------------------------

    async def reserve(self, filters: dict[str, str], prio: float = 0.0) -> dict[str, object]:
        """Create a reservation and keep it alive in the background.

        Returns the serialized reservation (including ``token``). A per-token
        keepalive task polls every ``KEEPALIVE_INTERVAL_S`` until the
        reservation is acquired, expires, is invalidated, or is cancelled.
        """
        res = await self._client.create_reservation(filters, prio)
        token = str(res["token"])
        self._start_keepalive(token)
        return res

    async def cancel_reservation(self, token: str) -> None:
        """Cancel a reservation and stop its keepalive task."""
        await self._stop_keepalive(token)
        await self._client.cancel_reservation_rpc(token)

    async def reservation_wait(self, token: str, timeout_s: float = 25.0) -> dict[str, object]:
        """Block-and-poll ``token`` until it allocates a place, or ``timeout_s``.

        §11.14: only an *acquired* reservation auto-refreshes coordinator-side
        -- a ``waiting``/``allocated``-but-not-yet-acquired one otherwise
        expires ~60s after creation (``coordinator.py:1103``). ``PollReservation``
        itself calls ``res.refresh()``, so simply polling on this bounded
        cadence *is* the keepalive; no separate background task is spawned
        (unlike :meth:`reserve`'s 15s keepalive, which exists precisely
        because THAT caller does not immediately poll at all). Implemented
        here rather than in server.py because :class:`PlaceSession` already
        owns the client and every other reservation-poll loop (mirrors
        :meth:`_acquire_via_reservation`'s bounded-poll shape almost exactly,
        down to reusing :data:`ACQUIRE_POLL_INTERVAL_S` and the dead-state
        early-out) -- folding it into server.py would duplicate that state
        machine and the ``_sleep``/``_monotonic`` seams for no benefit.

        ``timeout_s`` is clamped to at most 25.0 (mirrors ``wait_for_change``)
        to stay under typical MCP client request timeouts. Returns
        ``{"token", "state", "allocations", "changed"}`` -- ``changed`` is
        True iff the FINAL observed state is ``"allocated"`` (true whether
        that was already so on the very first poll -- e.g. a free place
        allocates immediately -- or only after several poll iterations); a
        dead reservation (``expired``/``invalid``) or a genuine timeout both
        report ``changed=False`` without raising -- an unknown/expired token
        is a legitimate "did not allocate" outcome for a *wait*, unlike
        ``acquire_place``'s hard failure. Never raises itself (only
        ``poll_reservation``'s own ``CoordinatorError`` can propagate, e.g.
        not connected).
        """
        clamped = min(timeout_s, 25.0)
        deadline = _monotonic() + clamped
        res = await self._client.poll_reservation(token)
        while res.get("state") != "allocated":
            if res.get("state") in _DEAD_RESERVATION_STATES or _monotonic() >= deadline:
                break
            await _sleep(ACQUIRE_POLL_INTERVAL_S)
            res = await self._client.poll_reservation(token)
        return {
            "token": token,
            "state": res.get("state"),
            "allocations": res.get("allocations"),
            "changed": res.get("state") == "allocated",
        }

    async def shutdown(self) -> None:
        """Cancel every keepalive task; idempotent, leaves no orphan tasks."""
        for token in list(self._keepalive_tasks):
            await self._stop_keepalive(token)

    # ---- internals ------------------------------------------------------

    def _is_ours(self, place: dict[str, object]) -> bool:
        """True if the snapshot shows this session's identity as the holder."""
        return place.get("acquired") == self._config.identity

    def _snapshot(self, name: str) -> dict[str, object]:
        place = self._client.place(name)
        return place if place is not None else {"name": name}

    async def _snapshot_synced(
        self, name: str, satisfied: Callable[[dict[str, object]], bool]
    ) -> dict[str, object]:
        """``_snapshot(name)``, waiting briefly for it to satisfy ``satisfied``.

        See the ``_SNAPSHOT_SYNC_*`` module constants for why this exists.
        """
        place = self._snapshot(name)
        deadline = _monotonic() + _SNAPSHOT_SYNC_TIMEOUT_S
        while not satisfied(place) and _monotonic() < deadline:
            await _sleep(_SNAPSHOT_SYNC_POLL_S)
            place = self._snapshot(name)
        return place

    def _start_keepalive(self, token: str) -> None:
        if token in self._keepalive_tasks:
            return
        loop = asyncio.get_running_loop()
        self._keepalive_tasks[token] = loop.create_task(self._keepalive_loop(token))

    async def _stop_keepalive(self, token: str) -> None:
        task = self._keepalive_tasks.pop(token, None)
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _keepalive_loop(self, token: str) -> None:
        try:
            while True:
                await _sleep(KEEPALIVE_INTERVAL_S)
                try:
                    res = await self._client.poll_reservation(token)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Reservation gone (cancelled/unknown token) or transient
                    # failure: stop polling. Never let the task die unretrieved.
                    return
                if res.get("state") in _KEEPALIVE_DONE_STATES:
                    return
        finally:
            # Auto-untrack, but only our own registration (cancel_reservation
            # may already have popped and replaced nothing).
            if self._keepalive_tasks.get(token) is asyncio.current_task():
                del self._keepalive_tasks[token]


def _allocates(res: dict[str, object], name: str) -> bool:
    """True if reservation ``res`` is allocated and includes place ``name``."""
    if res.get("state") != "allocated":
        return False
    allocations = res.get("allocations") or {}
    if not isinstance(allocations, dict):
        return False
    for value in allocations.values():
        if isinstance(value, str):
            if value == name:
                return True
        elif name in value:
            return True
    return False
