"""Tests for CoordinatorClient against a fake gRPC stub (no network)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest
from labgrid.remote.generated import labgrid_coordinator_pb2 as pb2

from labgrid_mcp import coordinator
from labgrid_mcp.config import Config
from labgrid_mcp.coordinator import CoordinatorClient, CoordinatorError


def make_config() -> Config:
    return Config(
        coordinator="127.0.0.1:20408",
        hostname="testhost",
        username="tester",
        readonly=False,
        allow=None,
        acquire_timeout=120.0,
    )


# ---- fake stream plumbing ------------------------------------------------


def _rpc_error() -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE,
        grpc.aio.Metadata(),
        grpc.aio.Metadata(),
        details="coordinator unavailable",
    )


def _place_update(name: str) -> pb2.UpdateResponse:
    place = pb2.Place(name=name, comment=f"comment for {name}")
    update = pb2.UpdateResponse()
    update.place.CopyFrom(place)
    return update


def _resource_update(
    exporter: str, group: str, name: str, *, avail: bool = True
) -> pb2.UpdateResponse:
    resource = pb2.Resource()
    resource.path.exporter_name = exporter
    resource.path.group_name = group
    resource.path.resource_name = name
    resource.cls = "NetworkSerialPort"
    resource.params["host"].string_value = "example"
    resource.avail = avail
    update = pb2.UpdateResponse()
    update.resource.CopyFrom(resource)
    return update


def _del_resource_update(exporter: str, group: str, name: str) -> pb2.UpdateResponse:
    update = pb2.UpdateResponse()
    update.del_resource.exporter_name = exporter
    update.del_resource.group_name = group
    update.del_resource.resource_name = name
    return update


class _FakeOut:
    """Duck-typed stand-in for a ClientOutMessage from the coordinator."""

    def __init__(
        self,
        *,
        updates: list[pb2.UpdateResponse] | None = None,
        sync_id: int | None = None,
        hello_version: str | None = None,
    ) -> None:
        self.updates = updates or []
        self._sync_id = sync_id
        self.sync = SimpleNamespace(id=sync_id)
        # Only present when the fake coordinator "announces" a version.
        self.hello = SimpleNamespace(version=hello_version) if hello_version else None

    def HasField(self, name: str) -> bool:
        if name == "sync":
            return self._sync_id is not None
        return False


class _FakeCall:
    """Async-iterable stand-in for a gRPC streaming call, with cancel()."""

    def __init__(self, gen: AsyncIterator[_FakeOut]) -> None:
        self._gen = gen
        self.cancelled = False

    def __aiter__(self) -> AsyncIterator[_FakeOut]:
        return self._gen

    def cancel(self) -> None:
        self.cancelled = True


# A session, given the sync id read from the client's handshake, yields the
# coordinator's outgoing messages for one ClientStream call.
Session = Callable[[int | None], AsyncIterator[_FakeOut]]


class FakeCoordinatorStub:
    """Fake CoordinatorStub: scripts one 'session' per ClientStream call."""

    def __init__(
        self,
        sessions: list[Session],
        *,
        reservations: list[pb2.Reservation] | None = None,
        reservations_error: Exception | None = None,
        acquire_error: Exception | None = None,
        release_error: Exception | None = None,
        allow_error: Exception | None = None,
        create_reservation_response: pb2.Reservation | None = None,
        create_reservation_error: Exception | None = None,
        cancel_error: Exception | None = None,
        poll_response: pb2.Reservation | None = None,
        poll_error: Exception | None = None,
        add_place_error: Exception | None = None,
        delete_place_error: Exception | None = None,
        set_tags_error: Exception | None = None,
        add_place_match_error: Exception | None = None,
        add_place_alias_error: Exception | None = None,
        delete_place_alias_error: Exception | None = None,
        set_place_comment_error: Exception | None = None,
        delete_place_match_error: Exception | None = None,
    ) -> None:
        self._sessions = list(sessions)
        self.call_count = 0
        self._reservations = reservations or []
        self._reservations_error = reservations_error
        self._acquire_error = acquire_error
        self._release_error = release_error
        self._allow_error = allow_error
        self._create_reservation_response = create_reservation_response
        self._create_reservation_error = create_reservation_error
        self._cancel_error = cancel_error
        self._poll_response = poll_response
        self._poll_error = poll_error
        self._add_place_error = add_place_error
        self._delete_place_error = delete_place_error
        self._set_tags_error = set_tags_error
        self._add_place_match_error = add_place_match_error
        self._add_place_alias_error = add_place_alias_error
        self._delete_place_alias_error = delete_place_alias_error
        self._set_place_comment_error = set_place_comment_error
        self._delete_place_match_error = delete_place_match_error
        # Last request seen per RPC, so tests can assert on what was sent.
        self.last_acquire_request: pb2.AcquirePlaceRequest | None = None
        self.last_release_request: pb2.ReleasePlaceRequest | None = None
        self.last_allow_request: pb2.AllowPlaceRequest | None = None
        self.last_create_reservation_request: pb2.CreateReservationRequest | None = None
        self.last_cancel_request: pb2.CancelReservationRequest | None = None
        self.last_poll_request: pb2.PollReservationRequest | None = None
        self.last_add_place_request: pb2.AddPlaceRequest | None = None
        self.last_delete_place_request: pb2.DeletePlaceRequest | None = None
        self.last_set_tags_request: pb2.SetPlaceTagsRequest | None = None
        self.last_add_place_match_request: pb2.AddPlaceMatchRequest | None = None
        self.last_add_place_alias_request: pb2.AddPlaceAliasRequest | None = None
        self.last_delete_place_alias_request: pb2.DeletePlaceAliasRequest | None = None
        self.last_set_place_comment_request: pb2.SetPlaceCommentRequest | None = None
        self.last_delete_place_match_request: pb2.DeletePlaceMatchRequest | None = None

    async def GetReservations(
        self, request: pb2.GetReservationsRequest
    ) -> pb2.GetReservationsResponse:
        if self._reservations_error is not None:
            raise self._reservations_error
        return pb2.GetReservationsResponse(reservations=self._reservations)

    async def AcquirePlace(self, request: pb2.AcquirePlaceRequest) -> pb2.AcquirePlaceResponse:
        self.last_acquire_request = request
        if self._acquire_error is not None:
            raise self._acquire_error
        return pb2.AcquirePlaceResponse()

    async def ReleasePlace(self, request: pb2.ReleasePlaceRequest) -> pb2.ReleasePlaceResponse:
        self.last_release_request = request
        if self._release_error is not None:
            raise self._release_error
        return pb2.ReleasePlaceResponse()

    async def AllowPlace(self, request: pb2.AllowPlaceRequest) -> pb2.AllowPlaceResponse:
        self.last_allow_request = request
        if self._allow_error is not None:
            raise self._allow_error
        return pb2.AllowPlaceResponse()

    async def CreateReservation(
        self, request: pb2.CreateReservationRequest
    ) -> pb2.CreateReservationResponse:
        self.last_create_reservation_request = request
        if self._create_reservation_error is not None:
            raise self._create_reservation_error
        reservation = self._create_reservation_response or pb2.Reservation()
        return pb2.CreateReservationResponse(reservation=reservation)

    async def CancelReservation(
        self, request: pb2.CancelReservationRequest
    ) -> pb2.CancelReservationResponse:
        self.last_cancel_request = request
        if self._cancel_error is not None:
            raise self._cancel_error
        return pb2.CancelReservationResponse()

    async def PollReservation(
        self, request: pb2.PollReservationRequest
    ) -> pb2.PollReservationResponse:
        self.last_poll_request = request
        if self._poll_error is not None:
            raise self._poll_error
        reservation = self._poll_response or pb2.Reservation()
        return pb2.PollReservationResponse(reservation=reservation)

    async def AddPlace(self, request: pb2.AddPlaceRequest) -> pb2.AddPlaceResponse:
        self.last_add_place_request = request
        if self._add_place_error is not None:
            raise self._add_place_error
        return pb2.AddPlaceResponse()

    async def DeletePlace(self, request: pb2.DeletePlaceRequest) -> pb2.DeletePlaceResponse:
        self.last_delete_place_request = request
        if self._delete_place_error is not None:
            raise self._delete_place_error
        return pb2.DeletePlaceResponse()

    async def SetPlaceTags(self, request: pb2.SetPlaceTagsRequest) -> pb2.SetPlaceTagsResponse:
        self.last_set_tags_request = request
        if self._set_tags_error is not None:
            raise self._set_tags_error
        return pb2.SetPlaceTagsResponse()

    async def AddPlaceMatch(
        self, request: pb2.AddPlaceMatchRequest
    ) -> pb2.AddPlaceMatchResponse:
        self.last_add_place_match_request = request
        if self._add_place_match_error is not None:
            raise self._add_place_match_error
        return pb2.AddPlaceMatchResponse()

    async def AddPlaceAlias(
        self, request: pb2.AddPlaceAliasRequest
    ) -> pb2.AddPlaceAliasResponse:
        self.last_add_place_alias_request = request
        if self._add_place_alias_error is not None:
            raise self._add_place_alias_error
        return pb2.AddPlaceAliasResponse()

    async def DeletePlaceAlias(
        self, request: pb2.DeletePlaceAliasRequest
    ) -> pb2.DeletePlaceAliasResponse:
        self.last_delete_place_alias_request = request
        if self._delete_place_alias_error is not None:
            raise self._delete_place_alias_error
        return pb2.DeletePlaceAliasResponse()

    async def SetPlaceComment(
        self, request: pb2.SetPlaceCommentRequest
    ) -> pb2.SetPlaceCommentResponse:
        self.last_set_place_comment_request = request
        if self._set_place_comment_error is not None:
            raise self._set_place_comment_error
        return pb2.SetPlaceCommentResponse()

    async def DeletePlaceMatch(
        self, request: pb2.DeletePlaceMatchRequest
    ) -> pb2.DeletePlaceMatchResponse:
        self.last_delete_place_match_request = request
        if self._delete_place_match_error is not None:
            raise self._delete_place_match_error
        return pb2.DeletePlaceMatchResponse()

    def ClientStream(self, request_iterator: AsyncIterator[Any]) -> _FakeCall:
        self.call_count += 1
        session = self._sessions.pop(0) if self._sessions else _eof_session
        return _FakeCall(self._drive(request_iterator, session))

    async def _drive(
        self, request_iterator: AsyncIterator[Any], session: Session
    ) -> AsyncIterator[_FakeOut]:
        # Mirror the real coordinator: read the client's handshake to learn the
        # sync id we must echo back.
        sync_id: int | None = None
        async for msg in request_iterator:
            if msg.HasField("sync"):
                sync_id = msg.sync.id
                break
        async for out in session(sync_id):
            yield out


async def _eof_session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
    # Immediately-closing stream (no handshake echo).
    for _ in ():
        yield _FakeOut()


def live_session(
    *,
    updates: list[pb2.UpdateResponse] | None = None,
    hello_version: str | None = None,
) -> Session:
    """Handshakes, emits the given updates, then stays open until cancelled."""

    async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
        yield _FakeOut(updates=updates, sync_id=sync_id, hello_version=hello_version)
        await asyncio.Event().wait()  # keep the stream open

    return _session


def unreachable_session() -> Session:
    async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
        raise _rpc_error()
        yield _FakeOut()  # pragma: no cover - makes this an async generator

    return _session


async def _wait_until(predicate: Callable[[], bool], tries: int = 5000) -> bool:
    for _ in range(tries):
        if predicate():
            return True
        await asyncio.sleep(0)
    return False


# ---- tests ---------------------------------------------------------------


async def test_start_sets_connected_info() -> None:
    stub = FakeCoordinatorStub([live_session(hello_version="26.0-fake")])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        info = client.info()
        assert info.connected is True
        assert info.version == "26.0-fake"
        assert info.address == "127.0.0.1:20408"
        assert info.identity == "testhost/tester"
    finally:
        await client.stop()


async def test_start_unreachable_raises() -> None:
    stub = FakeCoordinatorStub([unreachable_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    with pytest.raises(CoordinatorError):
        await client.start()
    assert client.info().connected is False


async def test_places_snapshot_updates() -> None:
    # Two place updates arrive over the stream, in separate messages.
    def two_updates() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_place_update("board-a")], sync_id=sync_id)
            yield _FakeOut(updates=[_place_update("board-b")])
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([two_updates()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        assert await _wait_until(lambda: len(client.places()) == 2)
        by_name = {p["name"]: p for p in client.places()}
        assert set(by_name) == {"board-a", "board-b"}
        # Serialized snapshot is plain, JSON-friendly dicts.
        assert by_name["board-a"]["comment"] == "comment for board-a"
        assert isinstance(by_name["board-a"]["aliases"], list)
    finally:
        await client.stop()


async def test_places_empty_before_sync() -> None:
    client = CoordinatorClient(make_config(), _stub_factory=lambda: FakeCoordinatorStub([]))
    assert client.places() == []
    assert client.info().connected is False


async def test_stop_idempotent() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    await client.start()
    await client.stop()
    await client.stop()  # must not raise
    assert client.info().connected is False


async def test_reconnect_after_drop(monkeypatch: pytest.MonkeyPatch) -> None:
    drop_trigger = asyncio.Event()

    def drop_after_handshake() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_place_update("board-a")], sync_id=sync_id)
            await drop_trigger.wait()
            raise _rpc_error()

        return _session

    stub = FakeCoordinatorStub([drop_after_handshake(), live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    observed_during_backoff: list[bool] = []

    async def fake_sleep(delay: float) -> None:
        observed_during_backoff.append(client.info().connected)

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        assert client.info().connected is True  # first handshake

        drop_trigger.set()  # force the stream to fail
        # Background task should reconnect (second session handshakes).
        assert await _wait_until(lambda: stub.call_count >= 2 and client.info().connected)
        assert client.info().connected is True  # reconnected

        # We were disconnected while waiting out the backoff: True -> False -> True.
        assert observed_during_backoff and observed_during_backoff[0] is False
    finally:
        await client.stop()


async def test_start_non_retryable_first_attempt_raises() -> None:
    # An *unexpected* (non-retryable) exception on the very first connection
    # attempt -- e.g. malformed data escaping the pre-handshake loop -- must
    # surface as CoordinatorError, not hang start() forever.
    def raising_session() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            raise ValueError("bad data before handshake")
            yield _FakeOut()  # pragma: no cover - makes this an async generator

        return _session

    stub = FakeCoordinatorStub([raising_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    with pytest.raises(CoordinatorError):
        await client.start()
    assert client.info().connected is False


async def test_reconnect_after_non_retryable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # An unexpected exception *after* a successful handshake must be treated
    # like any other lost connection: the loop retries instead of dying, so
    # connected goes True -> False -> True.
    drop_trigger = asyncio.Event()

    def raise_after_handshake() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(sync_id=sync_id)
            await drop_trigger.wait()
            raise ValueError("bad data after handshake")

        return _session

    stub = FakeCoordinatorStub([raise_after_handshake(), live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    observed_during_backoff: list[bool] = []

    async def fake_sleep(delay: float) -> None:
        observed_during_backoff.append(client.info().connected)

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        assert client.info().connected is True  # first handshake

        drop_trigger.set()  # force an unexpected error mid-stream
        assert await _wait_until(lambda: stub.call_count >= 2 and client.info().connected)
        assert client.info().connected is True  # reconnected

        # Disconnected while waiting out the backoff: True -> False -> True.
        assert observed_during_backoff and observed_during_backoff[0] is False
    finally:
        await client.stop()


async def test_reconnect_clears_stale_places(monkeypatch: pytest.MonkeyPatch) -> None:
    # Session 1 syncs {board-a, board-b} then drops; session 2 re-syncs only
    # {board-a} (board-b was deleted while we were disconnected). The stale
    # board-b must not linger: each session's snapshot is authoritative.
    drop_trigger = asyncio.Event()

    def session_two_boards() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(
                updates=[_place_update("board-a"), _place_update("board-b")],
                sync_id=sync_id,
            )
            await drop_trigger.wait()
            raise _rpc_error()

        return _session

    def session_one_board() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_place_update("board-a")], sync_id=sync_id)
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([session_two_boards(), session_one_board()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    async def fake_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        assert await _wait_until(lambda: len(client.places()) == 2)

        drop_trigger.set()  # force session 1 to fail; reconnect to session 2
        assert await _wait_until(
            lambda: {p["name"] for p in client.places()} == {"board-a"}
        )
    finally:
        await client.stop()


async def test_start_cancelled_while_blocked_leaves_no_pending_exception() -> None:
    # Fix-3 investigation: cancelling start() while it awaits the (never-resolved)
    # _started future propagates cancellation INTO that future, so _run's finally
    # sees it as done() and skips set_exception. No abandoned exception-bearing
    # future is left behind (which would GC-log "Future exception was never
    # retrieved"). _started.cancelled() proves the edge is safe.
    def hanging_session() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            await asyncio.Event().wait()  # never handshake -> _started never set
            yield _FakeOut()  # pragma: no cover - makes this an async generator

        return _session

    stub = FakeCoordinatorStub([hanging_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    task = asyncio.ensure_future(client.start())
    # Let start() create _started and block awaiting it.
    assert await _wait_until(lambda: client._started is not None)
    for _ in range(5):
        await asyncio.sleep(0)

    started = client._started
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await client.stop()

    assert started is not None
    assert started.cancelled()  # cancellation reached the future; not abandoned


def test_channel_created_with_labgrid_keepalive_options(monkeypatch: pytest.MonkeyPatch) -> None:
    # The real (non-test-seam) path must build its gRPC channel with labgrid's
    # keepalive options, otherwise a half-open idle connection never errors and
    # reconnect never fires.
    recorded: dict[str, Any] = {}

    def fake_channel(target: str, *, options: Any) -> Any:
        recorded["target"] = target
        recorded["options"] = options
        return MagicMock()

    monkeypatch.setattr(coordinator, "_channel_factory", fake_channel)

    client = CoordinatorClient(make_config())  # no stub factory -> real path
    client._build_stub()

    assert recorded["target"] == "127.0.0.1:20408"
    assert recorded["options"] == coordinator.CHANNEL_OPTIONS
    assert ("grpc.keepalive_time_ms", 7500) in recorded["options"]
    assert ("grpc.keepalive_timeout_ms", 10000) in recorded["options"]
    assert ("grpc.http2.max_pings_without_data", 0) in recorded["options"]


async def test_reconnect_backoff_grows_then_resets(monkeypatch: pytest.MonkeyPatch) -> None:
    # Session 1 handshakes then drops (a working session -> backoff resets to
    # 1s). Sessions 2+ fail before handshaking (coordinator down -> backoff
    # grows 1 -> 2 -> 4 -> 8, capped at 30).
    def drop_after_handshake() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(sync_id=sync_id)
            raise _rpc_error()

        return _session

    sessions: list[Session] = [drop_after_handshake()] + [unreachable_session() for _ in range(5)]
    stub = FakeCoordinatorStub(sessions)
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) >= 4:
            await asyncio.Event().wait()  # park the loop once we've seen enough

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        assert await _wait_until(lambda: len(delays) >= 4)
        assert delays[:4] == [1.0, 2.0, 4.0, 8.0]
    finally:
        await client.stop()


async def test_resources_snapshot_updates() -> None:
    stub = FakeCoordinatorStub([live_session(updates=[_resource_update("exp1", "grp", "serial")])])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        assert await _wait_until(lambda: len(client.resources()) == 1)
        (entry,) = client.resources()
        assert entry == {
            "exporter": "exp1",
            "group": "grp",
            "name": "serial",
            "cls": "NetworkSerialPort",
            "params": {"host": "example", "extra": {}},
            "acquired": None,
            "avail": True,
        }
    finally:
        await client.stop()


async def test_del_resource_removes_entry() -> None:
    def two_updates() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_resource_update("exp1", "grp", "serial")], sync_id=sync_id)
            yield _FakeOut(updates=[_del_resource_update("exp1", "grp", "serial")])
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([two_updates()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        assert await _wait_until(lambda: client.resources() == [])
    finally:
        await client.stop()


async def test_resources_cleared_on_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    # Mirror of test_reconnect_clears_stale_places, but for resources: session
    # 1 syncs {a, b} then drops; session 2 re-syncs only {a}.
    drop_trigger = asyncio.Event()

    def session_two_resources() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(
                updates=[
                    _resource_update("exp1", "grp", "a"),
                    _resource_update("exp1", "grp", "b"),
                ],
                sync_id=sync_id,
            )
            await drop_trigger.wait()
            raise _rpc_error()

        return _session

    def session_one_resource() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_resource_update("exp1", "grp", "a")], sync_id=sync_id)
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([session_two_resources(), session_one_resource()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    async def fake_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        assert await _wait_until(lambda: len(client.resources()) == 2)

        drop_trigger.set()  # force session 1 to fail; reconnect to session 2
        assert await _wait_until(
            lambda: {r["name"] for r in client.resources()} == {"a"}
        )
    finally:
        await client.stop()


async def test_resources_empty_before_sync() -> None:
    client = CoordinatorClient(make_config(), _stub_factory=lambda: FakeCoordinatorStub([]))
    assert client.resources() == []


async def test_get_reservations_not_connected_raises() -> None:
    client = CoordinatorClient(make_config(), _stub_factory=lambda: FakeCoordinatorStub([]))
    with pytest.raises(CoordinatorError):
        await client.get_reservations()


async def test_get_reservations_returns_serialized() -> None:
    res = pb2.Reservation(
        owner="host/user", token="TOK123", state=1, prio=0.0, created=1.0, timeout=2.0
    )
    stub = FakeCoordinatorStub([live_session()], reservations=[res])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        data = await client.get_reservations()
        assert data == [
            {
                "owner": "host/user",
                "state": "allocated",
                "prio": 0.0,
                "filters": {},
                "allocations": {},
                "created": 1.0,
                "timeout": 2.0,
                "token": "TOK123",
            }
        ]
    finally:
        await client.stop()


async def test_get_reservations_rpc_error_wrapped() -> None:
    stub = FakeCoordinatorStub([live_session()], reservations_error=_rpc_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.get_reservations()
    finally:
        await client.stop()


async def test_get_reservations_other_error_wrapped() -> None:
    stub = FakeCoordinatorStub([live_session()], reservations_error=RuntimeError("boom"))
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.get_reservations()
    finally:
        await client.stop()


# ---- unary wrapper tests --------------------------------------------------

# Two failure shapes every wrapper must wrap: a gRPC AioRpcError (preserving
# ``.details()``) and an arbitrary other exception (design §11's
# get_reservations pattern).
_ERROR_BUILDERS: list[Callable[[], Exception]] = [_rpc_error, lambda: RuntimeError("boom")]


async def test_acquire_place_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.acquire_place_rpc("board-a")
        assert stub.last_acquire_request is not None
        assert stub.last_acquire_request.placename == "board-a"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_acquire_place_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], acquire_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.acquire_place_rpc("board-a")
    finally:
        await client.stop()


async def test_release_place_happy_default_fromuser() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.release_place_rpc("board-a")
        assert stub.last_release_request is not None
        assert stub.last_release_request.placename == "board-a"
        assert stub.last_release_request.fromuser == ""
    finally:
        await client.stop()


async def test_release_place_happy_with_fromuser() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.release_place_rpc("board-a", fromuser="host2/bob")
        assert stub.last_release_request is not None
        assert stub.last_release_request.fromuser == "host2/bob"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_release_place_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], release_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.release_place_rpc("board-a")
    finally:
        await client.stop()


async def test_allow_place_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.allow_place_rpc("board-a", "host2/bob")
        assert stub.last_allow_request is not None
        assert stub.last_allow_request.placename == "board-a"
        assert stub.last_allow_request.user == "host2/bob"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_allow_place_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], allow_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.allow_place_rpc("board-a", "host2/bob")
    finally:
        await client.stop()


async def test_create_reservation_happy_builds_main_filter_group() -> None:
    res = pb2.Reservation(
        owner="host/user", token="TOK123", state=0, prio=5.0, created=1.0, timeout=61.0
    )
    stub = FakeCoordinatorStub([live_session()], create_reservation_response=res)
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        data = await client.create_reservation({"name": "board-a", "board": "foo"}, prio=5.0)
        assert data["token"] == "TOK123"
        assert data["owner"] == "host/user"
        req = stub.last_create_reservation_request
        assert req is not None
        assert req.prio == 5.0
        assert set(req.filters.keys()) == {"main"}
        assert dict(req.filters["main"].filter) == {"name": "board-a", "board": "foo"}
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_create_reservation_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], create_reservation_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.create_reservation({"name": "board-a"})
    finally:
        await client.stop()


async def test_cancel_reservation_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.cancel_reservation_rpc("TOK123")
        assert stub.last_cancel_request is not None
        assert stub.last_cancel_request.token == "TOK123"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_cancel_reservation_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], cancel_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.cancel_reservation_rpc("TOK123")
    finally:
        await client.stop()


async def test_poll_reservation_happy_returns_serialized_with_token() -> None:
    res = pb2.Reservation(
        owner="host/user", token="TOK123", state=2, prio=0.0, created=1.0, timeout=61.0
    )
    stub = FakeCoordinatorStub([live_session()], poll_response=res)
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        data = await client.poll_reservation("TOK123")
        assert data["token"] == "TOK123"
        assert data["state"] == "acquired"
        assert stub.last_poll_request is not None
        assert stub.last_poll_request.token == "TOK123"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_poll_reservation_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], poll_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.poll_reservation("TOK123")
    finally:
        await client.stop()


async def test_add_place_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.add_place("board-a")
        assert stub.last_add_place_request is not None
        assert stub.last_add_place_request.name == "board-a"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_add_place_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], add_place_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.add_place("board-a")
    finally:
        await client.stop()


async def test_delete_place_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.delete_place("board-a")
        assert stub.last_delete_place_request is not None
        assert stub.last_delete_place_request.name == "board-a"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_delete_place_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], delete_place_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.delete_place("board-a")
    finally:
        await client.stop()


async def test_set_place_tags_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.set_place_tags("board-a", {"board": "foo"})
        assert stub.last_set_tags_request is not None
        assert stub.last_set_tags_request.placename == "board-a"
        assert dict(stub.last_set_tags_request.tags) == {"board": "foo"}
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_set_place_tags_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], set_tags_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.set_place_tags("board-a", {"board": "foo"})
    finally:
        await client.stop()


async def test_add_place_match_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.add_place_match("board-a", "exporter/group/NetworkPowerPort")
        assert stub.last_add_place_match_request is not None
        assert stub.last_add_place_match_request.placename == "board-a"
        assert stub.last_add_place_match_request.pattern == "exporter/group/NetworkPowerPort"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_add_place_match_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], add_place_match_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.add_place_match("board-a", "exporter/group/NetworkPowerPort")
    finally:
        await client.stop()


async def test_add_place_alias_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.add_place_alias("board-a", "alias-1")
        assert stub.last_add_place_alias_request is not None
        assert stub.last_add_place_alias_request.placename == "board-a"
        assert stub.last_add_place_alias_request.alias == "alias-1"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_add_place_alias_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], add_place_alias_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.add_place_alias("board-a", "alias-1")
    finally:
        await client.stop()


async def test_delete_place_alias_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.delete_place_alias("board-a", "alias-1")
        assert stub.last_delete_place_alias_request is not None
        assert stub.last_delete_place_alias_request.placename == "board-a"
        assert stub.last_delete_place_alias_request.alias == "alias-1"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_delete_place_alias_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], delete_place_alias_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.delete_place_alias("board-a", "alias-1")
    finally:
        await client.stop()


async def test_set_place_comment_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.set_place_comment("board-a", "under repair")
        assert stub.last_set_place_comment_request is not None
        assert stub.last_set_place_comment_request.placename == "board-a"
        assert stub.last_set_place_comment_request.comment == "under repair"
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_set_place_comment_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], set_place_comment_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.set_place_comment("board-a", "under repair")
    finally:
        await client.stop()


async def test_delete_place_match_happy() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.delete_place_match("board-a", "exporter/group/NetworkPowerPort")
        assert stub.last_delete_place_match_request is not None
        assert stub.last_delete_place_match_request.placename == "board-a"
        assert (
            stub.last_delete_place_match_request.pattern
            == "exporter/group/NetworkPowerPort"
        )
    finally:
        await client.stop()


@pytest.mark.parametrize("build_error", _ERROR_BUILDERS)
async def test_delete_place_match_error_wrapped(build_error: Callable[[], Exception]) -> None:
    stub = FakeCoordinatorStub([live_session()], delete_place_match_error=build_error())
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        with pytest.raises(CoordinatorError):
            await client.delete_place_match("board-a", "exporter/group/NetworkPowerPort")
    finally:
        await client.stop()


async def test_add_place_match_forwards_rename() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.add_place_match("board-a", "exporter/group/NetworkPowerPort", rename="pp1")
        assert stub.last_add_place_match_request is not None
        assert stub.last_add_place_match_request.HasField("rename")
        assert stub.last_add_place_match_request.rename == "pp1"
    finally:
        await client.stop()


async def test_add_place_match_omits_rename_field_when_none() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.add_place_match("board-a", "exporter/group/NetworkPowerPort")
        assert stub.last_add_place_match_request is not None
        assert not stub.last_add_place_match_request.HasField("rename")
    finally:
        await client.stop()


async def test_delete_place_match_forwards_rename() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.delete_place_match(
            "board-a", "exporter/group/NetworkPowerPort", rename="pp1"
        )
        assert stub.last_delete_place_match_request is not None
        assert stub.last_delete_place_match_request.HasField("rename")
        assert stub.last_delete_place_match_request.rename == "pp1"
    finally:
        await client.stop()


async def test_delete_place_match_omits_rename_field_when_none() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        await client.delete_place_match("board-a", "exporter/group/NetworkPowerPort")
        assert stub.last_delete_place_match_request is not None
        assert not stub.last_delete_place_match_request.HasField("rename")
    finally:
        await client.stop()


async def test_place_lookup_known_and_unknown() -> None:
    stub = FakeCoordinatorStub([live_session(updates=[_place_update("board-a")])])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        assert await _wait_until(lambda: client.place("board-a") is not None)
        known = client.place("board-a")
        assert known is not None
        assert known["name"] == "board-a"
        assert client.place("unknown-board") is None
    finally:
        await client.stop()


# Every wrapper must raise CoordinatorError when there is no live session,
# regardless of the failure mode (never started, or stub has no sessions).
_NOT_CONNECTED_CALLS: list[tuple[str, tuple[object, ...]]] = [
    ("acquire_place_rpc", ("board-a",)),
    ("release_place_rpc", ("board-a",)),
    ("allow_place_rpc", ("board-a", "host2/bob")),
    ("create_reservation", ({"name": "board-a"},)),
    ("cancel_reservation_rpc", ("TOK123",)),
    ("poll_reservation", ("TOK123",)),
    ("add_place", ("board-a",)),
    ("delete_place", ("board-a",)),
    ("set_place_tags", ("board-a", {"board": "foo"})),
    ("add_place_match", ("board-a", "exporter/group/NetworkPowerPort")),
    ("add_place_alias", ("board-a", "alias-1")),
    ("delete_place_alias", ("board-a", "alias-1")),
    ("set_place_comment", ("board-a", "under repair")),
    ("delete_place_match", ("board-a", "exporter/group/NetworkPowerPort")),
]


@pytest.mark.parametrize("method_name, args", _NOT_CONNECTED_CALLS)
async def test_wrapper_not_connected_raises(
    method_name: str, args: tuple[object, ...]
) -> None:
    client = CoordinatorClient(make_config(), _stub_factory=lambda: FakeCoordinatorStub([]))
    method = getattr(client, method_name)
    with pytest.raises(CoordinatorError):
        await method(*args)


# ---- change cursor / wait_for_change --------------------------------------


async def test_change_cursor_zero_before_start() -> None:
    client = CoordinatorClient(make_config(), _stub_factory=lambda: FakeCoordinatorStub([]))
    assert client.change_cursor() == 0


async def test_change_cursor_bumps_on_session_start() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        assert client.change_cursor() == 0
        await client.start()
        # The session-start clear itself is a change, independent of any
        # place/resource updates the session happened to emit.
        assert client.change_cursor() > 0
    finally:
        await client.stop()


async def test_change_cursor_monotonic_across_updates() -> None:
    # The second update is gated behind a real await (not just a second
    # generator yield), so the first cursor read below is guaranteed to
    # observe state before it lands -- otherwise the background task can run
    # both updates to completion before control ever returns to this
    # coroutine (no genuine yield point between two bare `yield`s).
    second_update_trigger = asyncio.Event()

    def two_updates() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(updates=[_place_update("board-a")], sync_id=sync_id)
            await second_update_trigger.wait()
            yield _FakeOut(updates=[_place_update("board-b")])
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([two_updates()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        assert await _wait_until(lambda: len(client.places()) == 1)
        first = client.change_cursor()

        second_update_trigger.set()
        assert await _wait_until(lambda: len(client.places()) == 2)
        second = client.change_cursor()
        assert second > first
    finally:
        await client.stop()


async def test_reconnect_counts_as_change(monkeypatch: pytest.MonkeyPatch) -> None:
    drop_trigger = asyncio.Event()

    def drop_after_handshake() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(sync_id=sync_id)
            await drop_trigger.wait()
            raise _rpc_error()

        return _session

    stub = FakeCoordinatorStub([drop_after_handshake(), live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)

    async def fake_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(coordinator, "_sleep", fake_sleep)

    try:
        await client.start()
        cursor_before = client.change_cursor()

        drop_trigger.set()
        assert await _wait_until(lambda: stub.call_count >= 2 and client.info().connected)

        cursor_after = client.change_cursor()
        assert cursor_after > cursor_before
    finally:
        await client.stop()


async def test_wait_for_change_wakes_promptly_on_fed_update() -> None:
    feed_trigger = asyncio.Event()

    def session_with_delayed_update() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(sync_id=sync_id)
            await feed_trigger.wait()
            yield _FakeOut(updates=[_place_update("board-a")])
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([session_with_delayed_update()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        cursor = client.change_cursor()
        wait_task = asyncio.ensure_future(client.wait_for_change(cursor, timeout=5.0))
        # Let the waiter actually start blocking on the event before we feed
        # the update, so this exercises the wake path rather than a race.
        for _ in range(10):
            await asyncio.sleep(0)
        feed_trigger.set()

        result = await asyncio.wait_for(wait_task, timeout=2.0)
        assert result > cursor
    finally:
        await client.stop()


async def test_wait_for_change_timeout_returns_unchanged_cursor() -> None:
    stub = FakeCoordinatorStub([live_session()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        cursor = client.change_cursor()
        result = await asyncio.wait_for(
            client.wait_for_change(cursor, timeout=0.05), timeout=2.0
        )
        assert result == cursor
    finally:
        await client.stop()


async def test_wait_for_change_concurrent_waiters_both_wake() -> None:
    feed_trigger = asyncio.Event()

    def session_with_delayed_update() -> Session:
        async def _session(sync_id: int | None) -> AsyncIterator[_FakeOut]:
            yield _FakeOut(sync_id=sync_id)
            await feed_trigger.wait()
            yield _FakeOut(updates=[_place_update("board-a")])
            await asyncio.Event().wait()

        return _session

    stub = FakeCoordinatorStub([session_with_delayed_update()])
    client = CoordinatorClient(make_config(), _stub_factory=lambda: stub)
    try:
        await client.start()
        cursor = client.change_cursor()
        task_a = asyncio.ensure_future(client.wait_for_change(cursor, timeout=5.0))
        task_b = asyncio.ensure_future(client.wait_for_change(cursor, timeout=5.0))
        for _ in range(10):
            await asyncio.sleep(0)
        feed_trigger.set()

        result_a, result_b = await asyncio.wait_for(
            asyncio.gather(task_a, task_b), timeout=2.0
        )
        assert result_a > cursor
        assert result_b > cursor
    finally:
        await client.stop()
