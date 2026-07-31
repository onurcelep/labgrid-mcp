"""Tests for PlaceSession against a fake CoordinatorClient (no gRPC).

The fake is a plain object exposing Task 1's method names; ``_sleep`` and
``_monotonic`` are patched so nothing waits on the wall clock.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from labgrid_mcp import session as session_mod
from labgrid_mcp.config import Config
from labgrid_mcp.coordinator import CoordinatorError
from labgrid_mcp.session import PlaceSession

IDENTITY = "testhost/tester"


def make_config(acquire_timeout: float = 120.0) -> Config:
    return Config(
        coordinator="127.0.0.1:20408",
        hostname="testhost",
        username="tester",
        readonly=False,
        allow=None,
        acquire_timeout=acquire_timeout,
    )


class FakeClient:
    """Minimal stand-in for CoordinatorClient's Task 1 surface."""

    def __init__(self, identity: str = IDENTITY) -> None:
        self.identity = identity
        self._places: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.token = "TOK123"
        self.create_state = "waiting"
        # Queue of reservation states returned by successive poll calls; the
        # last one sticks once exhausted.
        self.poll_states: list[str] = []
        self._poll_idx = 0
        self.poll_allocations: dict[str, object] = {}
        # Stale-snapshot mode (models the ClientStream update lag,
        # DESIGN.md 11.8(e)): while _frozen_reads > 0, place() serves the
        # snapshot captured by freeze_snapshot() even across RPC mutations.
        self.place_calls = 0
        self._frozen_places: dict[str, dict[str, object]] | None = None
        self._frozen_reads = 0

    # snapshot -----------------------------------------------------------
    def set_place(self, name: str, **fields: object) -> None:
        self._places[name] = {"name": name, "acquired": "", "reservation": "", **fields}

    def freeze_snapshot(self, reads: int) -> None:
        """Serve the *current* place state for the next ``reads`` place()
        calls, hiding later mutations until the budget is spent -- the fake's
        stand-in for the coordinator's asynchronous ClientStream update."""
        self._frozen_places = {k: dict(v) for k, v in self._places.items()}
        self._frozen_reads = reads

    def place(self, name: str) -> dict[str, object] | None:
        self.place_calls += 1
        source = self._places
        if self._frozen_reads > 0 and self._frozen_places is not None:
            self._frozen_reads -= 1
            source = self._frozen_places
        p = source.get(name)
        return dict(p) if p is not None else None

    # unary wrappers -----------------------------------------------------
    async def acquire_place_rpc(self, name: str) -> None:
        self.calls.append(("acquire", name))
        self.set_place(name, acquired=self.identity)

    async def release_place_rpc(self, name: str, fromuser: str = "") -> None:
        self.calls.append(("release", name, fromuser))
        # Model the coordinator's ReleasePlace matching semantics
        # (labgrid/remote/coordinator.py:905): an empty fromuser is an
        # unconditional release ("kick"); a non-empty fromuser frees the place
        # only when it equals the current holder, otherwise it is a silent
        # no-op. Freeing sets acquired="" so _snapshot_synced's catch-up poll
        # is satisfied on the first check (no real sleep in these tests).
        current = self._places.get(name, {}).get("acquired")
        if fromuser and current != fromuser:
            return  # coordinator-side no-op: our snapshot lied about ownership
        self.set_place(name, acquired="")

    async def create_reservation(
        self, filters: dict[str, str], prio: float = 0.0
    ) -> dict[str, object]:
        self.calls.append(("create", dict(filters), prio))
        return {
            "state": self.create_state,
            "token": self.token,
            "allocations": {},
        }

    async def cancel_reservation_rpc(self, token: str) -> None:
        self.calls.append(("cancel", token))

    async def poll_reservation(self, token: str) -> dict[str, object]:
        self.calls.append(("poll", token))
        if self._poll_idx < len(self.poll_states):
            state = self.poll_states[self._poll_idx]
            self._poll_idx += 1
        elif self.poll_states:
            state = self.poll_states[-1]
        else:
            state = "waiting"
        allocations = self.poll_allocations if state == "allocated" else {}
        return {"state": state, "token": token, "allocations": allocations}

    # helpers ------------------------------------------------------------
    def call_kinds(self) -> list[Any]:
        return [c[0] for c in self.calls]


def make_session(client: FakeClient, acquire_timeout: float = 120.0) -> PlaceSession:
    return PlaceSession(client, make_config(acquire_timeout))  # type: ignore[arg-type]


# ---- acquire: simple branches -------------------------------------------


async def test_acquire_free_place_uses_direct_rpc() -> None:
    client = FakeClient()
    client.set_place("board", acquired="", reservation="")
    session = make_session(client)

    result = await session.acquire_place("board")

    assert client.call_kinds() == ["acquire"]
    assert "create" not in client.call_kinds()
    assert result["acquired"] == IDENTITY


async def test_acquire_unknown_place_uses_direct_rpc() -> None:
    client = FakeClient()  # place() returns None
    session = make_session(client)

    await session.acquire_place("ghost")

    assert client.call_kinds() == ["acquire"]


async def test_acquire_already_ours_returns_without_rpc() -> None:
    client = FakeClient()
    client.set_place("board", acquired=IDENTITY)
    session = make_session(client)

    result = await session.acquire_place("board")

    assert client.calls == []
    assert result["acquired"] == IDENTITY


# ---- acquire: reservation flow ------------------------------------------


async def test_acquire_taken_place_reserves_then_acquires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    client.create_state = "waiting"
    client.poll_states = ["waiting", "allocated"]
    client.poll_allocations = {"main": ["board"]}
    session = make_session(client)

    result = await session.acquire_place("board")

    kinds = client.call_kinds()
    # name-targeted reservation with {"name": name} filter
    create_call = next(c for c in client.calls if c[0] == "create")
    assert create_call[1] == {"name": "board"}
    # polled to refresh until allocated, then acquired, then reservation cancelled
    assert kinds.index("acquire") > kinds.index("poll")
    assert kinds.index("cancel") > kinds.index("acquire")
    assert kinds.count("poll") == 2
    assert ("cancel", client.token) in client.calls
    assert result["acquired"] == IDENTITY


async def test_acquire_allocated_on_create_skips_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.set_place("board", reservation="someone-elses-token")
    client.create_state = "allocated"
    session = make_session(client)
    # create_reservation returns allocated but with our place in allocations
    orig_create = client.create_reservation

    async def create_allocated(filters: dict[str, str], prio: float = 0.0) -> dict[str, object]:
        await orig_create(filters, prio)
        return {"state": "allocated", "token": client.token, "allocations": {"main": ["board"]}}

    monkeypatch.setattr(client, "create_reservation", create_allocated)

    await session.acquire_place("board")

    assert client.call_kinds().count("poll") == 0
    assert "acquire" in client.call_kinds()
    assert ("cancel", client.token) in client.calls


async def test_acquire_timeout_cancels_reservation_and_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(session_mod, "_sleep", clock.sleep)
    monkeypatch.setattr(session_mod, "_monotonic", clock.now)
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    client.poll_states = ["waiting"]  # never allocates
    session = make_session(client, acquire_timeout=3.0)

    with pytest.raises(CoordinatorError) as excinfo:
        await session.acquire_place("board")

    assert "board" in str(excinfo.value)
    assert "timed out" in str(excinfo.value)
    assert ("cancel", client.token) in client.calls


async def test_acquire_reservation_expired_raises_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    client.poll_states = ["waiting", "expired"]
    session = make_session(client)

    with pytest.raises(CoordinatorError) as excinfo:
        await session.acquire_place("board")

    assert "expired" in str(excinfo.value)
    assert ("cancel", client.token) in client.calls
    assert "acquire" not in client.call_kinds()


async def test_acquire_cancelled_midflight_cleans_up_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()

    async def blocking_sleep(_delay: float) -> None:
        await gate.wait()

    monkeypatch.setattr(session_mod, "_sleep", blocking_sleep)
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    client.poll_states = ["waiting"]
    session = make_session(client)

    task = asyncio.create_task(session.acquire_place("board"))
    await asyncio.sleep(0)  # let it create the reservation and park at _sleep
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert ("create", {"name": "board"}, 0.0) in client.calls
    assert ("cancel", client.token) in client.calls
    assert "acquire" not in client.call_kinds()


# ---- acquire: reuse of our own tracked reservation ----------------------


async def test_acquire_reuses_own_tracked_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """reserve({"name": X}) then acquire_place(X): the place already carries
    our (keepalive-tracked) token, so acquire must REUSE it -- poll our own
    token, acquire, cancel -- and must NOT create a second reservation that
    could never allocate (it would poll to timeout)."""
    # Block keepalive at its first sleep so its background poll never fires and
    # never consumes the poll queue; the acquire path reuses an already-
    # allocated reservation and so never sleeps itself.
    gate = asyncio.Event()

    async def blocking_sleep(_delay: float) -> None:
        await gate.wait()

    monkeypatch.setattr(session_mod, "_sleep", blocking_sleep)
    client = FakeClient()
    # Place is reserved by OUR token and already allocated to it.
    client.set_place("board", acquired="", reservation=client.token)
    client.poll_states = ["allocated"]
    client.poll_allocations = {"main": ["board"]}
    session = make_session(client)

    res = await session.reserve({"name": "board"})
    assert res["token"] == client.token
    assert client.token in session._keepalive_tasks

    result = await session.acquire_place("board")

    kinds = client.call_kinds()
    # Exactly ONE create -- from reserve(); acquire reused, did not create.
    assert kinds.count("create") == 1
    # Reused our token: polled it, acquired, then cancelled after acquire.
    assert "poll" in kinds
    assert kinds.index("acquire") > kinds.index("poll")
    assert ("cancel", client.token) in client.calls
    assert kinds.index("cancel") > kinds.index("acquire")
    # cancel-after-acquire also untracked the keepalive.
    assert client.token not in session._keepalive_tasks
    assert result["acquired"] == IDENTITY


async def test_acquire_place_reserved_by_foreign_token_creates_new_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A place reserved by a token we do NOT track (someone else's) keeps the
    existing behavior: create our own reservation, which correctly queues
    behind theirs in the labgrid scheduler."""
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.set_place("board", acquired="", reservation="foreign-token")
    client.poll_states = ["waiting", "allocated"]
    client.poll_allocations = {"main": ["board"]}
    session = make_session(client)

    result = await session.acquire_place("board")

    kinds = client.call_kinds()
    # We created our OWN new reservation (foreign token is not ours to reuse).
    create_call = next(c for c in client.calls if c[0] == "create")
    assert create_call[1] == {"name": "board"}
    assert kinds.count("create") == 1
    assert kinds.index("acquire") > kinds.index("poll")
    assert ("cancel", client.token) in client.calls
    assert result["acquired"] == IDENTITY


# ---- snapshot catch-up (_snapshot_synced) --------------------------------


async def test_acquire_waits_for_stale_snapshot_to_catch_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.set_place("board", acquired="")
    # Budget: 1 pre-RPC free-check + 2 stale post-acquire reads, then fresh.
    client.freeze_snapshot(reads=3)
    session = make_session(client)

    result = await session.acquire_place("board")

    # The stale copy (acquired="") was never returned: the sync loop kept
    # polling until the fresh state appeared.
    assert result["acquired"] == IDENTITY
    # pre-check + initial synced read + 1 stale loop read + 1 fresh read --
    # proves the while-loop body actually iterated.
    assert client.place_calls == 4


async def test_snapshot_sync_returns_stale_after_bounded_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(session_mod, "_sleep", clock.sleep)
    monkeypatch.setattr(session_mod, "_monotonic", clock.now)
    client = FakeClient()
    client.set_place("board", acquired="")
    client.freeze_snapshot(reads=10**9)  # the snapshot never catches up
    session = make_session(client)

    result = await session.acquire_place("board")

    # Best effort: after the bounded deadline the stale dict comes back
    # as-is -- no hang, no exception.
    assert result["acquired"] == ""
    assert clock.now() >= session_mod._SNAPSHOT_SYNC_TIMEOUT_S
    expected_polls = int(
        session_mod._SNAPSHOT_SYNC_TIMEOUT_S / session_mod._SNAPSHOT_SYNC_POLL_S
    )
    # pre-check + initial synced read + exactly one read per poll tick until
    # the (fake-clock) deadline: the loop is bounded, not unbounded.
    assert client.place_calls == 2 + expected_polls


# ---- release ------------------------------------------------------------


async def test_release_own_place_sends_identity_fromuser() -> None:
    client = FakeClient()
    client.set_place("board", acquired=IDENTITY)
    session = make_session(client)

    result = await session.release_place("board")

    # Non-kick release passes our identity as fromuser so the coordinator
    # itself no-ops if the snapshot lied -- not the old unconditional "".
    assert ("release", "board", IDENTITY) in client.calls
    assert result.get("acquired") == ""


async def test_release_own_place_coordinator_noops_on_stale_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Fake clock so the (unsatisfiable) post-release snapshot sync hits its
    # bounded deadline instantly instead of waiting 2 real seconds.
    clock = _FakeClock()
    monkeypatch.setattr(session_mod, "_sleep", clock.sleep)
    monkeypatch.setattr(session_mod, "_monotonic", clock.now)
    client = FakeClient()
    # Client-side check reads a stale snapshot showing us as the holder...
    client.set_place("board", acquired=IDENTITY)
    client.freeze_snapshot(reads=1)  # first place() read serves the stale copy
    # ...but the true state is that someone else now holds it.
    client.set_place("board", acquired="other/user")
    session = make_session(client)

    await session.release_place("board")

    # We sent fromuser=IDENTITY; since the real holder is other/user the
    # coordinator no-ops and does NOT kick them.
    assert ("release", "board", IDENTITY) in client.calls
    assert client.place("board") == {"name": "board", "acquired": "other/user", "reservation": ""}


async def test_release_not_ours_without_kick_raises_and_skips_rpc() -> None:
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    session = make_session(client)

    with pytest.raises(CoordinatorError) as excinfo:
        await session.release_place("board")

    assert "other/user" in str(excinfo.value)
    assert "release" not in client.call_kinds()


async def test_release_with_kick_sends_rpc_regardless() -> None:
    client = FakeClient()
    client.set_place("board", acquired="other/user")
    session = make_session(client)

    await session.release_place("board", kick=True)

    assert ("release", "board", "") in client.calls


# ---- reserve / cancel passthrough + keepalive ---------------------------


async def test_reserve_returns_reservation_and_registers_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()

    async def blocking_sleep(_delay: float) -> None:
        await gate.wait()

    monkeypatch.setattr(session_mod, "_sleep", blocking_sleep)
    client = FakeClient()
    session = make_session(client)

    res = await session.reserve({"board": "foo"}, prio=1.0)

    assert res["token"] == client.token
    assert ("create", {"board": "foo"}, 1.0) in client.calls
    assert client.token in session._keepalive_tasks

    await session.cancel_reservation(client.token)

    assert client.token not in session._keepalive_tasks
    assert ("cancel", client.token) in client.calls


async def test_keepalive_polls_until_terminal_then_untracks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.poll_states = ["allocated", "allocated", "acquired"]
    session = make_session(client)

    await session.reserve({"name": "board"})
    task = session._keepalive_tasks[client.token]
    await task  # runs to the terminal "acquired" state

    assert client.call_kinds().count("poll") == 3
    assert client.token not in session._keepalive_tasks


async def test_keepalive_stops_when_poll_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    session = make_session(client)

    async def failing_poll(_token: str) -> dict[str, object]:
        raise CoordinatorError("token gone")

    monkeypatch.setattr(client, "poll_reservation", failing_poll)

    await session.reserve({"name": "board"})
    task = session._keepalive_tasks[client.token]
    await task  # must exit, not raise

    assert client.token not in session._keepalive_tasks


async def test_shutdown_cancels_tasks_cleanly_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()

    async def blocking_sleep(_delay: float) -> None:
        await gate.wait()

    monkeypatch.setattr(session_mod, "_sleep", blocking_sleep)
    client = FakeClient()
    session = make_session(client)

    await session.reserve({"name": "a"})
    client.token = "TOK2"
    await session.reserve({"name": "b"})
    assert len(session._keepalive_tasks) == 2

    await session.shutdown()
    assert session._keepalive_tasks == {}

    # Idempotent: a second shutdown is a no-op, no error.
    await session.shutdown()


# ---- reservation_wait ----------------------------------------------------


async def test_reservation_wait_returns_allocated_on_first_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A free place typically allocates on the very first poll -- no sleep at
    all needed; ``changed`` is True even though the loop never iterated."""
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.poll_states = ["allocated"]
    client.poll_allocations = {"main": ["board"]}
    session = make_session(client)

    result = await session.reservation_wait(client.token)

    assert result == {
        "token": client.token,
        "state": "allocated",
        "allocations": {"main": ["board"]},
        "changed": True,
    }
    assert client.call_kinds().count("poll") == 1


async def test_reservation_wait_polls_until_allocated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.poll_states = ["waiting", "waiting", "allocated"]
    client.poll_allocations = {"main": ["board"]}
    session = make_session(client)

    result = await session.reservation_wait(client.token)

    assert result["state"] == "allocated"
    assert result["changed"] is True
    assert client.call_kinds().count("poll") == 3


async def test_reservation_wait_times_out_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(session_mod, "_sleep", clock.sleep)
    monkeypatch.setattr(session_mod, "_monotonic", clock.now)
    client = FakeClient()
    client.poll_states = ["waiting"]  # never allocates
    session = make_session(client)

    result = await session.reservation_wait(client.token, timeout_s=3.0)

    assert result["state"] == "waiting"
    assert result["changed"] is False
    # Bounded: polled roughly once per ACQUIRE_POLL_INTERVAL_S tick until the
    # (fake-clock) deadline, not forever.
    assert 1 <= client.call_kinds().count("poll") <= 4


async def test_reservation_wait_stops_early_on_dead_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_mod, "_sleep", _instant_sleep)
    client = FakeClient()
    client.poll_states = ["waiting", "expired"]
    session = make_session(client)

    result = await session.reservation_wait(client.token, timeout_s=25.0)

    assert result["state"] == "expired"
    assert result["changed"] is False
    # Stopped as soon as it saw the dead state -- did not poll to the deadline.
    assert client.call_kinds().count("poll") == 2


async def test_reservation_wait_clamps_timeout_to_25s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(session_mod, "_sleep", clock.sleep)
    monkeypatch.setattr(session_mod, "_monotonic", clock.now)
    client = FakeClient()
    client.poll_states = ["waiting"]  # never allocates
    session = make_session(client)

    await session.reservation_wait(client.token, timeout_s=9999.0)

    assert clock.now() <= 25.0 + session_mod.ACQUIRE_POLL_INTERVAL_S


# ---- test seams ---------------------------------------------------------


async def _instant_sleep(_delay: float) -> None:
    """Yield control without waiting so poll loops advance deterministically."""
    await asyncio.sleep(0)


class _FakeClock:
    """Monotonic clock advanced only by (patched) sleeps."""

    def __init__(self) -> None:
        self._t = 0.0

    def now(self) -> float:
        return self._t

    async def sleep(self, delay: float) -> None:
        self._t += delay
        await asyncio.sleep(0)
