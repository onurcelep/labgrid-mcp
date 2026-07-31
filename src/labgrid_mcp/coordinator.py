"""Persistent async gRPC client for the labgrid coordinator.

This talks to the labgrid coordinator over gRPC directly. It never imports or
instantiates ``labgrid.remote.client.ClientSession``; instead it reuses that
class's *connection pattern*, verified by reading its source. Only the generated
stubs (``labgrid.remote.generated``) and helpers in ``labgrid.remote.common``
are imported.

Verified against labgrid 26.0. Names read from the installed package:

* Channel / stub: ``grpc.aio.insecure_channel(target)`` +
  ``labgrid_coordinator_pb2_grpc.CoordinatorStub(channel)``.
* Subscription RPC: ``CoordinatorStub.ClientStream(request_iterator)`` — a
  bidirectional stream. The client sends ``ClientInMessage`` and receives
  ``ClientOutMessage``.
* Handshake (mirrors ``ClientSession.start``): send ``ClientInMessage`` with
  ``startup`` (``StartupDone`` carrying ``version`` and ``name``), then
  ``subscribe.all_places = True``, then ``subscribe.all_resources = True``,
  then a ``sync`` (``Sync`` carrying an ``id``). The coordinator echoes the
  sync id back in a ``ClientOutMessage`` whose ``HasField("sync")`` is true;
  seeing our id confirms the handshake completed.
* Place state: arrives in ``ClientOutMessage.updates`` (repeated
  ``UpdateResponse``). ``update.WhichOneof("kind")`` is one of ``resource``,
  ``del_resource``, ``place`` (a ``Place`` message), or ``del_place`` (a place
  name string). Places are deserialized with ``labgrid.remote.common.Place``
  (``Place.from_pb2`` + ``.asdict()``), whose output is JSON-serializable.

Coordinator version note (contract deviation, documented):
The task contract's ``CoordinatorInfo.version`` is "coordinator-reported
version, None until known". In labgrid 26.0 the client-facing stream carries no
coordinator version: ``ClientOutMessage`` has only ``sync`` and ``updates``, and
no unary RPC returns a version. The coordinator announces its version only to
*exporters* (via ``ExporterOutMessage.hello.version``), never to clients. So in
production ``version`` stays ``None``. The field is kept (Task 5 imports it) and
populated through a forward-compatible seam that reads an optional
``out_msg.hello.version`` — mirroring labgrid's own ``Hello.version`` convention
— should a future coordinator send one on the client stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import grpc
import grpc.aio
from labgrid.remote import common
from labgrid.remote.generated import labgrid_coordinator_pb2 as pb2
from labgrid.remote.generated import labgrid_coordinator_pb2_grpc as pb2_grpc
from labgrid.util import labgrid_version

from labgrid_mcp.config import Config

_log = logging.getLogger("labgrid_mcp.coordinator")

# Reconnect backoff: exponential from 1s, capped at 30s.
_BACKOFF_INITIAL_S = 1.0
_BACKOFF_MAX_S = 30.0
_BACKOFF_FACTOR = 2.0

# gRPC keepalive settings. The values deliberately match what labgrid 26.0's
# own client configures (labgrid/remote/client.py) so we get identical
# liveness behavior against the same coordinator build. Without keepalive a
# half-open idle connection never errors, so the stream never fails and our
# reconnect logic never fires; with pings enabled a dead peer surfaces an
# AioRpcError that drives the reconnect path.
CHANNEL_OPTIONS: list[tuple[str, int]] = [
    ("grpc.keepalive_time_ms", 7500),
    ("grpc.keepalive_timeout_ms", 10000),
    ("grpc.http2.ping_timeout_ms", 10000),
    ("grpc.http2.max_pings_without_data", 0),
]

# Indirection so tests can patch the reconnect delay without touching the
# asyncio module globally.
_sleep = asyncio.sleep

# Indirection so tests can observe/replace channel creation without opening a
# real socket. Mirrors the ``_sleep`` seam above.
_channel_factory = grpc.aio.insecure_channel


class CoordinatorError(Exception):
    """Raised when the coordinator is unreachable or the handshake fails."""


@dataclass(frozen=True)
class CoordinatorInfo:
    address: str
    identity: str
    connected: bool
    version: str | None


def _serialize_reservation(res_pb2: Any) -> dict[str, object]:
    """``Reservation.asdict()`` plus the token it omits (design §11.5)."""
    res = common.Reservation.from_pb2(res_pb2)
    data: dict[str, object] = dict(res.asdict())
    data["token"] = res.token
    return data


async def _queue_iter(queue: asyncio.Queue[Any]) -> AsyncIterator[Any]:
    """Yield queued outgoing messages until a ``None`` sentinel is seen."""
    while True:
        item = await queue.get()
        if item is None:
            return
        yield item


class CoordinatorClient:
    """Holds a long-lived subscription to the labgrid coordinator.

    ``start()`` connects, performs the handshake, and spawns a background task
    that keeps the place snapshot up to date and reconnects (with exponential
    backoff) if the stream drops. ``stop()`` tears everything down and is safe
    to call more than once.
    """

    def __init__(self, config: Config, *, _stub_factory: Any | None = None) -> None:
        self._config = config
        # Test seam: a zero-arg callable returning an object with the same
        # ``ClientStream`` method as the real CoordinatorStub.
        self._stub_factory = _stub_factory
        self._channel: Any | None = None
        self._stub: Any | None = None
        self._task: asyncio.Task[None] | None = None
        self._started: asyncio.Future[None] | None = None
        self._stream_call: Any | None = None
        self._out_queue: asyncio.Queue[Any] | None = None
        self._sync_counter = itertools.count(1)
        self._stopping = False
        self._connected = False
        self._version: str | None = None
        self._places: dict[str, dict[str, object]] = {}
        self._resources: dict[str, dict[str, object]] = {}
        # Change cursor (design §11.12): monotonic counter + wake event for
        # wait_for_change, bumped per applied update and per session-start
        # snapshot clear. See change_cursor()/wait_for_change().
        self._change_seq = 0
        self._change_event = asyncio.Event()

    # ---- public API -----------------------------------------------------

    async def start(self) -> None:
        """Connect and block until the first handshake succeeds.

        Raises ``CoordinatorError`` if the coordinator cannot be reached.
        """
        if self._task is not None:
            return
        self._stopping = False
        self._build_stub()
        loop = asyncio.get_running_loop()
        self._started = loop.create_future()
        self._task = loop.create_task(self._run())
        try:
            await self._started
        except CoordinatorError:
            await self._teardown()
            raise

    async def stop(self) -> None:
        """Cancel the background task and close the channel. Idempotent."""
        self._stopping = True
        if self._out_queue is not None:
            self._out_queue.put_nowait(None)
        if self._stream_call is not None:
            with contextlib.suppress(Exception):
                self._stream_call.cancel()
        await self._teardown()
        self._connected = False

    def info(self) -> CoordinatorInfo:
        return CoordinatorInfo(
            address=self._config.coordinator,
            identity=self._config.identity,
            connected=self._connected,
            version=self._version,
        )

    def places(self) -> list[dict[str, object]]:
        """JSON-serializable snapshot of known places (``[]`` until synced)."""
        return [dict(place) for place in self._places.values()]

    def resources(self) -> list[dict[str, object]]:
        """JSON-serializable snapshot of known resources (``[]`` until synced)."""
        return [dict(resource) for resource in self._resources.values()]

    async def get_reservations(self) -> list[dict[str, object]]:
        """Fetch current reservations via the session-bound unary RPC.

        Raises ``CoordinatorError`` when not connected or on RPC failure
        (unary RPCs need the live ClientStream session — design §11.2).
        """
        if self._stub is None or not self._connected:
            raise CoordinatorError("not connected to coordinator")
        try:
            response = await self._stub.GetReservations(pb2.GetReservationsRequest())
        except grpc.aio.AioRpcError as exc:
            raise CoordinatorError(f"GetReservations failed: {exc.details()}") from exc
        except Exception as exc:
            raise CoordinatorError(f"GetReservations failed: {exc}") from exc
        return [_serialize_reservation(res_pb2) for res_pb2 in response.reservations]

    async def acquire_place_rpc(self, name: str) -> None:
        """Acquire a place by name via the session-bound unary RPC.

        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary("AcquirePlace", pb2.AcquirePlaceRequest(placename=name))

    async def release_place_rpc(self, name: str, fromuser: str = "") -> None:
        """Release a place by name via the session-bound unary RPC.

        Empty ``fromuser`` is an unconditional release ("kick"); a non-empty
        value releases only if the place is currently acquired by that user
        (silent no-op otherwise — design §11.8). Raises ``CoordinatorError``
        when not connected or on RPC failure.
        """
        await self._call_unary(
            "ReleasePlace", pb2.ReleasePlaceRequest(placename=name, fromuser=fromuser)
        )

    async def allow_place_rpc(self, name: str, user: str) -> None:
        """Allow another user (``"host/user"``) to use an acquired place.

        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary("AllowPlace", pb2.AllowPlaceRequest(placename=name, user=user))

    async def create_reservation(
        self, filters: dict[str, str], prio: float = 0.0
    ) -> dict[str, object]:
        """Create a reservation with a single ``"main"`` filter group.

        ``filters`` are the caller's k/v pairs (e.g. ``{"name": "board-a"}`` to
        reserve one specific place, or tag filters like ``{"board": "foo"}`` —
        design §11.8: the scheduler matches ``filter.tags.issubset(place.tags)``
        per group, and labgrid allocations assume a single ``"main"`` group).
        Returns the serialized reservation, including its ``token``. Raises
        ``CoordinatorError`` when not connected or on RPC failure.
        """
        if self._stub is None or not self._connected:
            raise CoordinatorError("not connected to coordinator")
        req = pb2.CreateReservationRequest(prio=prio)
        for k, v in filters.items():
            req.filters["main"].filter[k] = v
        try:
            response = await self._stub.CreateReservation(req)
        except grpc.aio.AioRpcError as exc:
            raise CoordinatorError(f"CreateReservation failed: {exc.details()}") from exc
        except Exception as exc:
            raise CoordinatorError(f"CreateReservation failed: {exc}") from exc
        return _serialize_reservation(response.reservation)

    async def cancel_reservation_rpc(self, token: str) -> None:
        """Cancel a reservation by token via the session-bound unary RPC.

        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary("CancelReservation", pb2.CancelReservationRequest(token=token))

    async def poll_reservation(self, token: str) -> dict[str, object]:
        """Poll a reservation by token; also serves as its keepalive.

        ``PollReservation`` calls ``res.refresh()`` on the coordinator (design
        §11.8), so polling extends the reservation's TTL. Returns the
        serialized reservation, including its ``token``. Raises
        ``CoordinatorError`` when not connected or on RPC failure.
        """
        if self._stub is None or not self._connected:
            raise CoordinatorError("not connected to coordinator")
        try:
            response = await self._stub.PollReservation(pb2.PollReservationRequest(token=token))
        except grpc.aio.AioRpcError as exc:
            raise CoordinatorError(f"PollReservation failed: {exc.details()}") from exc
        except Exception as exc:
            raise CoordinatorError(f"PollReservation failed: {exc}") from exc
        return _serialize_reservation(response.reservation)

    async def add_place(self, name: str) -> None:
        """Create a new place. Exposed via the ``add_place`` tool (Category.METADATA).

        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary("AddPlace", pb2.AddPlaceRequest(name=name))

    async def delete_place(self, name: str) -> None:
        """Delete a place. Exposed via the ``delete_place`` tool
        (Category.PLACE_DELETE, opt-in only -- decision #13).

        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary("DeletePlace", pb2.DeletePlaceRequest(name=name))

    async def set_place_tags(self, name: str, tags: dict[str, str]) -> None:
        """Set (or clear, via an empty value) tags on a place.

        Exposed via the ``set_place_tags`` tool (Category.METADATA). Raises
        ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary(
            "SetPlaceTags", pb2.SetPlaceTagsRequest(placename=name, tags=tags)
        )

    async def add_place_alias(self, name: str, alias: str) -> None:
        """Add an alias to a place. Idempotent on the coordinator (set.add).

        Exposed via the ``add_place_alias`` tool (Category.METADATA); design
        §11.12: aliases are resolved client-side only, never by the
        coordinator. Raises ``CoordinatorError`` when not connected or on RPC
        failure.
        """
        await self._call_unary(
            "AddPlaceAlias", pb2.AddPlaceAliasRequest(placename=name, alias=alias)
        )

    async def delete_place_alias(self, name: str, alias: str) -> None:
        """Remove an alias from a place.

        Design §11.12 trap: the coordinator raises an uncaught ``KeyError``
        (surfaced as gRPC ``UNKNOWN``) when the alias doesn't exist -- callers
        should pre-check the snapshot before calling this. Exposed via the
        ``delete_place_alias`` tool (Category.METADATA). Raises
        ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary(
            "DeletePlaceAlias", pb2.DeletePlaceAliasRequest(placename=name, alias=alias)
        )

    async def set_place_comment(self, name: str, comment: str) -> None:
        """Set a place's free-form comment (unvalidated by the coordinator).

        Exposed via the ``set_place_comment`` tool (Category.METADATA).
        Raises ``CoordinatorError`` when not connected or on RPC failure.
        """
        await self._call_unary(
            "SetPlaceComment", pb2.SetPlaceCommentRequest(placename=name, comment=comment)
        )

    async def add_place_match(
        self, name: str, pattern: str, rename: str | None = None
    ) -> None:
        """Add a resource match (``"exporter/group/cls"`` or ``".../name"``) to a place.

        Makes a place match exported resources so ``acquired_resources``/
        ``get_target_resources`` (target.py, DESIGN §11.9) can find them after
        acquire. ``pattern`` is split on ``/`` by the coordinator into
        ``ResourceMatch(exporter, group, cls[, name])``
        (``remote/coordinator.py::AddPlaceMatch``); wildcards (``*``) are
        supported by labgrid's matcher. ``rename`` (proto3 optional) sets an
        alternate name for the matched resource; the field is left unset on
        the request when ``rename`` is ``None``, distinct from an empty
        string. Verified against labgrid 26.0's ``ResourceMatch``
        (``rename`` is declared ``eq=False``): ``rename`` is forwarded to the
        coordinator but is NOT part of a match's identity -- a duplicate
        ``pattern`` is rejected regardless of ``rename``, and
        ``delete_place_match`` removes a match by ``pattern`` alone,
        independent of what ``rename`` (if any) it carries. Raises
        ``CoordinatorError`` when not connected or on RPC failure (including a
        duplicate match).
        """
        request = pb2.AddPlaceMatchRequest(placename=name, pattern=pattern)
        if rename is not None:
            request.rename = rename
        await self._call_unary("AddPlaceMatch", request)

    async def delete_place_match(
        self, name: str, pattern: str, rename: str | None = None
    ) -> None:
        """Remove a resource match from a place.

        ``rename`` is accepted for API symmetry with ``add_place_match`` and
        forwarded on the request when not ``None``, but (see
        ``add_place_match``'s docstring) does not affect which match is
        removed -- the coordinator matches on ``pattern`` alone. Raises
        ``CoordinatorError`` when not connected or on RPC failure (including
        "match does not exist").
        """
        request = pb2.DeletePlaceMatchRequest(placename=name, pattern=pattern)
        if rename is not None:
            request.rename = rename
        await self._call_unary("DeletePlaceMatch", request)

    def change_cursor(self) -> int:
        """Monotonic counter of applied place/resource changes.

        Bumped once per applied update in ``_apply_update`` and once per
        session-start snapshot clear (so a reconnect -- which resyncs
        ``_places``/``_resources`` from scratch, design §11.4 -- always counts
        as a change too). Pair with ``wait_for_change`` for a change-cursor
        long-poll (design §11.12).
        """
        return self._change_seq

    async def wait_for_change(self, cursor: int, timeout: float) -> int:
        """Block until ``change_cursor()`` exceeds ``cursor``, or ``timeout`` elapses.

        Event-based (no busy-polling): waits on an ``asyncio.Event`` that is
        set on every bump, re-checking the cursor after each wake to guard
        against spurious wakes (e.g. a bump from an unrelated waiter's
        perspective, or the event having been cleared and re-set between
        checks). Always returns the *current* cursor, whether it changed or
        the wait simply timed out.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self._change_seq <= cursor:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self._change_event.wait(), timeout=remaining)
            except TimeoutError:
                break
            finally:
                self._change_event.clear()
        return self._change_seq

    def place(self, name: str) -> dict[str, object] | None:
        """Sync snapshot lookup of one place by name.

        Returns ``None`` if the place is unknown or not yet synced. Unlike the
        RPC wrappers above, this never raises -- it mirrors ``places()``.
        """
        place = self._places.get(name)
        return dict(place) if place is not None else None

    # ---- internals ------------------------------------------------------

    async def _call_unary(self, rpc_name: str, request: Any) -> Any:
        """Shared guard/dispatch/error-wrap for the void-returning unary RPCs.

        Mirrors ``get_reservations``: not-connected guard, ``AioRpcError``
        caught to preserve ``.details()``, any other exception caught broadly
        -- both re-raised as ``CoordinatorError`` with ``from exc``.
        """
        if self._stub is None or not self._connected:
            raise CoordinatorError("not connected to coordinator")
        try:
            return await getattr(self._stub, rpc_name)(request)
        except grpc.aio.AioRpcError as exc:
            raise CoordinatorError(f"{rpc_name} failed: {exc.details()}") from exc
        except Exception as exc:
            raise CoordinatorError(f"{rpc_name} failed: {exc}") from exc

    def _build_stub(self) -> None:
        if self._stub is not None:
            return
        if self._stub_factory is not None:
            self._stub = self._stub_factory()
            self._channel = None
        else:
            self._channel = _channel_factory(
                self._config.coordinator, options=CHANNEL_OPTIONS
            )
            self._stub = pb2_grpc.CoordinatorStub(self._channel)

    async def _teardown(self) -> None:
        task = self._task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._task = None
        if self._channel is not None:
            with contextlib.suppress(Exception):
                await self._channel.close()
            self._channel = None
        self._stub = None
        self._stream_call = None
        self._out_queue = None

    async def _run(self) -> None:
        """Connect/pump loop. Reconnects on failure with exponential backoff.

        Every non-cancellation exception is treated as a lost connection and
        routed through the backoff/retry path -- including *unexpected* ones
        (e.g. a malformed place update raising inside ``_apply_update``), which
        must not silently kill the loop. ``CancelledError`` is re-raised so
        ``stop()`` can tear the task down. The ``finally`` block guarantees the
        ``_started`` future is always resolved, so ``start()`` can never block
        forever.
        """
        backoff = _BACKOFF_INITIAL_S
        try:
            while not self._stopping:
                reached = False
                try:
                    await self._connect_and_pump()
                    reached = True
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # ``_connected`` is still True here iff the handshake had
                    # completed before the stream failed.
                    reached = self._connected
                    self._connected = False
                    if self._started is not None and not self._started.done():
                        # First attempt never handshook: fatal for start().
                        self._started.set_exception(
                            CoordinatorError(
                                f"cannot reach coordinator at {self._config.coordinator}: {exc}"
                            )
                        )
                        return
                    _log.warning("coordinator stream failed, reconnecting: %s", exc)
                else:
                    self._connected = False
                    _log.info("coordinator stream ended, reconnecting")
                if self._stopping:
                    break
                if reached:
                    # We had a working session; restart the backoff schedule.
                    backoff = _BACKOFF_INITIAL_S
                await _sleep(backoff)
                backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX_S)
        finally:
            self._connected = False
            # No exit path may leave ``start()`` blocked on ``_started``: if we
            # stop before the first handshake ever resolved it, fail it here so
            # ``start()`` raises instead of hanging.
            if self._started is not None and not self._started.done():
                self._started.set_exception(
                    CoordinatorError(
                        f"cannot reach coordinator at {self._config.coordinator}"
                    )
                )

    async def _connect_and_pump(self) -> None:
        """Open the stream, handshake, then consume updates until it ends.

        Returns normally when the stream closes cleanly after a successful
        handshake; raises ``CoordinatorError`` if it closes before the
        handshake completes, and propagates ``AioRpcError`` on stream failure.
        """
        assert self._stub is not None
        # Each session re-subscribes ``all_places=True``/``all_resources=True``,
        # so the coordinator resends its full snapshot. Clear first: a place or
        # resource deleted while we were disconnected is absent from the new
        # snapshot and must not linger. ``places()``/``resources()`` are briefly
        # empty during a reconnect window -- acceptable for Phase 0/1; the
        # snapshot re-syncs as soon as the handshake lands.
        self._places.clear()
        self._resources.clear()
        # A resync is itself a change (design §11.12): a cursor held across a
        # reconnect must not under-report places/resources deleted while we
        # were disconnected, even if the new snapshot happens to be identical.
        self._bump_change()
        out_queue: asyncio.Queue[Any] = asyncio.Queue()
        self._out_queue = out_queue

        startup = pb2.ClientInMessage()
        startup.startup.version = labgrid_version()
        startup.startup.name = self._config.identity
        out_queue.put_nowait(startup)

        sub_places = pb2.ClientInMessage()
        sub_places.subscribe.all_places = True
        out_queue.put_nowait(sub_places)

        sub_resources = pb2.ClientInMessage()
        sub_resources.subscribe.all_resources = True
        out_queue.put_nowait(sub_resources)

        sync_id = next(self._sync_counter)
        sync_msg = pb2.ClientInMessage()
        sync_msg.sync.id = sync_id
        out_queue.put_nowait(sync_msg)

        call = self._stub.ClientStream(_queue_iter(out_queue))
        self._stream_call = call
        handshaken = False
        try:
            async for out_msg in call:
                self._maybe_update_version(out_msg)
                for update in out_msg.updates:
                    self._apply_update(update)
                if not handshaken and out_msg.HasField("sync") and out_msg.sync.id == sync_id:
                    handshaken = True
                    self._connected = True
                    if self._started is not None and not self._started.done():
                        self._started.set_result(None)
        finally:
            # Let the sender side of the stream exit gracefully.
            out_queue.put_nowait(None)
        if not handshaken:
            raise CoordinatorError("coordinator stream closed before handshake completed")

    def _maybe_update_version(self, out_msg: Any) -> None:
        # See module docstring: labgrid 26.0 never sends this on the client
        # stream, so this is a forward-compatible no-op in production.
        hello = getattr(out_msg, "hello", None)
        if hello is None:
            return
        version = getattr(hello, "version", None)
        if version:
            self._version = version

    def _apply_update(self, update: Any) -> None:
        kind = update.WhichOneof("kind")
        if kind == "place":
            place = common.Place.from_pb2(update.place)
            data: dict[str, object] = dict(place.asdict())
            # Place.asdict() omits the name; callers need it to identify places.
            data["name"] = place.name
            self._places[place.name] = data
        elif kind == "del_place":
            self._places.pop(update.del_place, None)
        elif kind == "resource":
            res_pb2 = update.resource
            path = res_pb2.path
            key = f"{path.exporter_name}/{path.group_name}/{path.resource_name}"
            # ResourceEntry.from_pb2 has a broken isinstance assert in labgrid
            # 26.0 (checks Place); go through data_from_pb2 (design §11.5).
            entry = common.ResourceEntry(common.ResourceEntry.data_from_pb2(res_pb2))
            rdata: dict[str, object] = dict(entry.asdict())
            rdata["exporter"] = path.exporter_name
            rdata["group"] = path.group_name
            rdata["name"] = path.resource_name
            self._resources[key] = rdata
        elif kind == "del_resource":
            path = update.del_resource
            key = f"{path.exporter_name}/{path.group_name}/{path.resource_name}"
            self._resources.pop(key, None)
        else:
            return
        self._bump_change()

    def _bump_change(self) -> None:
        """Advance the change cursor and wake any ``wait_for_change`` waiters."""
        self._change_seq += 1
        self._change_event.set()
