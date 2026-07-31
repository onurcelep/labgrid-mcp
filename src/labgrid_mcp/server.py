"""FastMCP application: tools, resources, and the stdio entrypoint."""

import argparse
import asyncio
import contextlib
import json
import logging
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal, Protocol

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ResourceError, ToolError
from mcp.types import ToolAnnotations

from labgrid_mcp import coordinator, demo
from labgrid_mcp.config import Config, load_config
from labgrid_mcp.console import ConsoleError, ConsoleRegistry
from labgrid_mcp.coordinator import CoordinatorClient, CoordinatorError, CoordinatorInfo
from labgrid_mcp.forwards import ForwardError, ForwardRegistry
from labgrid_mcp.jobs import JobError, JobRegistry
from labgrid_mcp.policy import Category, annotations, is_enabled
from labgrid_mcp.session import PlaceSession
from labgrid_mcp.target import TargetError, TargetManager, write_image_kwargs

_POWER_ACTIONS: tuple[Literal["on", "off", "cycle"], ...] = ("on", "off", "cycle")

_log = logging.getLogger("labgrid_mcp.server")

# Bounded catch-up for a metadata mutator's "refreshed place" return shape
# (design §11.12): the mutating RPC's own completion is independent of the
# ClientStream update that actually lands the edit in our snapshot (mirrors
# session.py's ``_snapshot_synced`` / ``_SNAPSHOT_SYNC_*``). Poll our
# in-memory cache only (never a network round trip) for up to
# _METADATA_SYNC_TIMEOUT_S; if it never catches up, return whatever is
# cached -- best effort, bounded, never blocks longer or raises.
_METADATA_SYNC_TIMEOUT_S = 2.0
_METADATA_SYNC_POLL_S = 0.05

# Test seams (mirror coordinator.py/session.py): patch to skip real delays /
# drive a fake clock without wall-clock waits.
_sleep = asyncio.sleep
_monotonic = time.monotonic


# ---- structural collaborator types (mypy-over-tests gate, Phase 6) --------
# build_server (and the two helpers that share its ``client``) are typed
# against these Protocols rather than the concrete classes below so unit
# tests can hand in plain duck-typed fakes (no real gRPC/coordinator/labgrid)
# and still satisfy ``mypy --strict`` -- only the methods actually called
# from this module are listed. ``main()`` still constructs and passes the
# real ``CoordinatorClient``/``PlaceSession``/``TargetManager``/
# ``ConsoleRegistry``/``JobRegistry``, which all satisfy these structurally.


class _ClientLike(Protocol):
    """Surface of ``CoordinatorClient`` this module calls."""

    def info(self) -> CoordinatorInfo: ...
    def places(self) -> list[dict[str, object]]: ...
    def place(self, name: str) -> dict[str, object] | None: ...
    def resources(self) -> list[dict[str, object]]: ...
    async def get_reservations(self) -> list[dict[str, object]]: ...
    async def allow_place_rpc(self, name: str, user: str) -> None: ...
    async def release_place_rpc(self, name: str, fromuser: str = "") -> None: ...
    async def add_place(self, name: str) -> None: ...
    async def delete_place(self, name: str) -> None: ...
    async def add_place_alias(self, name: str, alias: str) -> None: ...
    async def delete_place_alias(self, name: str, alias: str) -> None: ...
    async def set_place_tags(self, name: str, tags: dict[str, str]) -> None: ...
    async def set_place_comment(self, name: str, comment: str) -> None: ...
    async def add_place_match(self, name: str, pattern: str, rename: str | None = None) -> None: ...
    async def delete_place_match(
        self, name: str, pattern: str, rename: str | None = None
    ) -> None: ...
    def change_cursor(self) -> int: ...
    async def wait_for_change(self, cursor: int, timeout: float) -> int: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class _SessionLike(Protocol):
    """Surface of ``PlaceSession`` this module calls."""

    async def reserve(self, filters: dict[str, str], prio: float = 0.0) -> dict[str, object]: ...
    async def cancel_reservation(self, token: str) -> None: ...
    async def reservation_wait(self, token: str, timeout_s: float = 25.0) -> dict[str, object]: ...
    async def release_place(self, name: str, *, kick: bool = False) -> dict[str, object]: ...
    async def acquire_place(self, name: str) -> dict[str, object]: ...
    async def shutdown(self) -> None: ...


class _TargetsLike(Protocol):
    """Surface of ``TargetManager`` this module calls."""

    async def power(
        self,
        place: str,
        action: Literal["on", "off", "cycle"],
        delay: float | None = None,
        resource_name: str | None = None,
    ) -> bool: ...
    async def power_state(self, place: str, resource_name: str | None = None) -> bool: ...
    async def io_get(self, place: str, resource_name: str | None = None) -> bool: ...
    async def io_set(
        self, place: str, value: bool, resource_name: str | None = None
    ) -> None: ...
    async def sd_mux(self, place: str, mode: str) -> None: ...
    async def sd_mux_mode(self, place: str) -> str: ...
    async def usb_mux(self, place: str, links: list[str]) -> None: ...
    async def ssh_driver(self, place: str) -> Any: ...
    async def invalidate(self, place: str) -> None: ...
    async def shutdown(self) -> None: ...


class _ConsolesLike(Protocol):
    """Surface of ``ConsoleRegistry`` this module calls."""

    async def open(self, place: str) -> dict[str, object]: ...
    def read(self, session: str, max_bytes: int | None = None) -> dict[str, object]: ...
    async def send(self, session: str, data: str, newline: bool = False) -> dict[str, object]: ...
    async def close(self, session: str) -> dict[str, object]: ...
    async def close_place(self, place: str) -> None: ...
    def sessions(self) -> list[dict[str, object]]: ...
    async def shutdown(self) -> None: ...


class _ForwardsLike(Protocol):
    """Surface of ``ForwardRegistry`` this module calls."""

    async def open(
        self, place: str, remote_port: int, local_port: int = 0
    ) -> dict[str, object]: ...
    async def open_remote(
        self, place: str, remote_port: int, local_port: int
    ) -> dict[str, object]: ...
    async def close(self, forward: str) -> dict[str, object]: ...
    async def close_place(self, place: str) -> None: ...
    def sessions(self) -> list[dict[str, object]]: ...
    async def shutdown(self) -> None: ...


class _JobsLike(Protocol):
    """Surface of ``JobRegistry`` this module calls."""

    async def submit_flash(
        self, place: str, kind: str, build_fn: Callable[[Any], Callable[[], object]]
    ) -> dict[str, object]: ...
    def status(self, job: str) -> dict[str, object]: ...
    def logs(self, job: str, max_bytes: int | None = None) -> dict[str, object]: ...
    def jobs(self) -> list[dict[str, object]]: ...
    def running_job_for(self, place: str) -> str | None: ...
    async def shutdown(self) -> None: ...


def _normalize_job_payload(payload: dict[str, object]) -> dict[str, object]:
    """Normalize a job payload's ``kind`` at the client boundary (additive).

    ``bootstrap`` with a non-default loader is encoded INTERNALLY as kind
    ``"bootstrap:<loader>"`` (server.py <-> target.py private detail --
    ``JobRegistry`` forwards ``kind`` opaquely, see the ``bootstrap`` tool).
    That encoding must never leak to clients: here ``kind`` is split back to
    its canonical base (``"bootstrap"``) and the suffix moves into an ADDITIVE
    ``"loader"`` field. ``loader`` is ALWAYS present -- the requested loader
    for a non-default bootstrap job, ``None`` otherwise (default-loader
    bootstrap included; the default is implied by the ``bootstrap`` tool's
    signature). Applied at every point a jobs payload crosses to the client:
    the flash submitters' return (``_submit``), ``flash_status``, and the
    ``labgrid://sessions`` resource's jobs list.
    """
    kind = payload.get("kind")
    loader: str | None = None
    if isinstance(kind, str) and ":" in kind:
        base, _, loader = kind.partition(":")
        payload = {**payload, "kind": base}
    return {**payload, "loader": loader}


def _exporters_of(acquired_resources: object) -> list[str]:
    """Exporter names from acquired_resources entries (tuple/list or "exp/..." strings)."""
    exporters: set[str] = set()
    if not isinstance(acquired_resources, list):
        return []
    for entry in acquired_resources:
        if isinstance(entry, str) and entry:
            exporters.add(entry.split("/")[0])
        elif isinstance(entry, (list, tuple)) and entry:
            exporters.add(str(entry[0]))
    return sorted(exporters)


def _validate_identity(user: str) -> None:
    """Raise ``ToolError`` unless ``user`` is exactly ``"host/user"``.

    Exactly one "/" separator, both halves non-empty. Called before any RPC
    so a malformed identity never reaches the coordinator.
    """
    parts = user.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ToolError(f"user must be 'host/user' format (exactly one '/'), got {user!r}")


def _validate_release_from_identity(host: str, user: str) -> None:
    """Raise ``ToolError`` unless ``host``/``user`` are non-empty and "/"-free.

    Mirrors ``_validate_identity``'s style, but ``release_from`` takes
    ``host``/``user`` as two separate params (not one pre-joined string) --
    join them into the ``"host/user"`` shape the coordinator expects only
    AFTER confirming neither half is empty or itself contains a "/" (which
    would make the joined string ambiguous/malformed). Called before any RPC.
    """
    if not host or not user or "/" in host or "/" in user:
        raise ToolError(
            "host and user must both be non-empty and contain no '/', "
            f"got host={host!r} user={user!r}"
        )


def _check_place_owned(client: _ClientLike, config: Config, name: str) -> None:
    """Raise a friendly ``ToolError`` unless ``name`` is acquired by us.

    This is the tool-level early check (unknown and unacquired places share
    the same message); ``TargetManager`` re-checks ownership per-op as the
    backstop, so a stale check here can never let an op through.
    """
    place = client.place(name)
    if place is None:
        if not client.info().connected:
            # Mid-reconnect the coordinator snapshot is empty for EVERY
            # place, so a place we genuinely own can transiently look
            # "unknown" here -- name the real cause instead of misleading
            # the caller into re-acquiring (mirrors TargetManager._check_owned).
            raise ToolError("coordinator disconnected (reconnecting); retry shortly")
        raise ToolError(f"place {name!r} is not acquired by this server; call acquire_place first")
    if place.get("acquired") != config.identity:
        raise ToolError(f"place {name!r} is not acquired by this server; call acquire_place first")


def _check_not_foreign_acquired(
    client: _ClientLike, config: Config, name: str, force: bool, *, refuse_own: bool = False
) -> None:
    """Refuse a metadata mutation on an acquired EXISTING place, unless ``force``.

    Design §11.12's headline finding: none of the eight place-metadata RPCs
    checks ownership on the coordinator -- any handshaked session can
    retag/re-alias/re-comment/re-match/delete ANY place, acquired or not, by
    anyone. This is the client-side backstop. An unknown/unacquired place is a
    no-op here -- ``add_place`` needs no check at all (new name), and every
    other mutator lets its own RPC surface "does not exist"/succeed normally.
    An acquired place refuses when the holder is a DIFFERENT identity than
    ours, naming the owner; ``refuse_own=True`` (``delete_place`` only) also
    refuses when WE hold it -- deleting your own acquired place strands the
    acquisition (verified: the coordinator allows it with no guard).
    ``force=True`` skips this check entirely, mirroring ``release_place``'s
    ``kick`` semantics.
    """
    if force:
        return
    place = client.place(name)
    if place is None:
        return
    owner = place.get("acquired")
    if not owner:
        return
    if owner == config.identity and not refuse_own:
        return
    raise ToolError(f"place {name!r} is acquired by {owner!r}; pass force=True to override")


def _validate_match_pattern(pattern: str) -> None:
    """Raise ``ToolError`` unless ``pattern`` has exactly 3 or 4 non-empty segments.

    Design §11.12 trap: ``AddPlaceMatch``/``DeletePlaceMatch`` split ``pattern``
    on ``/`` into ``ResourceMatch(exporter, group, cls[, name])`` with NO
    arity check -- 2 segments or 5+ segments both crash the coordinator with an
    uncaught ``TypeError`` (gRPC ``UNKNOWN``). Validated here, before any RPC,
    so a malformed pattern is always a clean tool error instead.
    """
    segments = pattern.split("/")
    if len(segments) not in (3, 4) or not all(segments):
        raise ToolError(
            "pattern must be 'exporter/group/cls' or 'exporter/group/cls/name' "
            f"(exactly 3 or 4 non-empty '/'-separated segments), got {pattern!r}"
        )


def _aliases_of(place: dict[str, object]) -> list[object]:
    """``place["aliases"]`` narrowed to a list (``[]`` if missing/malformed)."""
    aliases = place.get("aliases")
    return aliases if isinstance(aliases, list) else []


def _pattern_in_matches(place: dict[str, object], pattern: str) -> bool:
    """True if ``place["matches"]`` has an entry for ``pattern``'s exporter/group/cls[/name].

    Rename-independent: verified against labgrid 26.0's ``ResourceMatch``
    (``rename`` is declared ``eq=False``) that a match's identity is the
    exporter/group/cls[/name] tuple alone -- see ``coordinator.py``'s
    ``add_place_match``/``delete_place_match`` docstrings.
    """
    segments = pattern.split("/")
    exporter, group, cls = segments[0], segments[1], segments[2]
    name = segments[3] if len(segments) == 4 else None
    matches = place.get("matches")
    if not isinstance(matches, list):
        return False
    for match in matches:
        if not isinstance(match, dict):
            continue
        if (
            match.get("exporter") == exporter
            and match.get("group") == group
            and match.get("cls") == cls
            and match.get("name") == name
        ):
            return True
    return False


async def _refreshed_place(
    client: _ClientLike, name: str, satisfied: Callable[[dict[str, object]], bool]
) -> dict[str, object]:
    """``client.place(name)``, waiting briefly for it to satisfy ``satisfied``.

    See the ``_METADATA_SYNC_*`` module constants for why this exists (mirrors
    session.py's ``_snapshot_synced``). Falls back to whatever is cached if
    the wait never catches up -- best effort, bounded.
    """
    place = client.place(name) or {"name": name}
    deadline = _monotonic() + _METADATA_SYNC_TIMEOUT_S
    while not satisfied(place) and _monotonic() < deadline:
        await _sleep(_METADATA_SYNC_POLL_S)
        place = client.place(name) or {"name": name}
    return place


async def _retry_start(client: _ClientLike) -> None:
    """Keep retrying ``client.start()`` until it succeeds or we are cancelled.

    ``start()`` is safe to re-call after a failed attempt: its teardown resets
    the client's task/stub/channel state (verified in ``coordinator.py``).
    Backoff discipline and the patchable sleep seam are shared with
    ``coordinator.py`` via module attributes, not duplicated.
    """
    backoff = coordinator._BACKOFF_INITIAL_S
    while True:
        await coordinator._sleep(backoff)
        backoff = min(backoff * coordinator._BACKOFF_FACTOR, coordinator._BACKOFF_MAX_S)
        try:
            await client.start()
        except CoordinatorError as exc:
            _log.warning("coordinator still unreachable, retrying: %s", exc)
            continue
        _log.info("coordinator connection established")
        return


def build_server(
    config: Config,
    client: _ClientLike,
    session: _SessionLike,
    targets: _TargetsLike,
    consoles: _ConsolesLike,
    jobs: _JobsLike,
    forwards: _ForwardsLike,
) -> FastMCP:
    """Assemble the FastMCP app: its tools, resources, and lifespan.

    Always registers seven read tools (coordinator_info, list_places,
    show_place, who, list_resources, list_reservations, wait_for_change) and
    five resources (labgrid://places, labgrid://resources,
    labgrid://reservations, labgrid://places/{name}, labgrid://sessions).
    Gated by policy.is_enabled: reserve, cancel_reservation, and
    reservation_wait (Category.RESERVATION), acquire_place, release_place,
    allow_place, and release_from (Category.ACQUIRE), get_power_state/
    set_power (Category.POWER, both gaining an optional resource_name;
    set_power also gaining an optional cycle-only delay, §11.14), get_io/
    set_io (Category.IO, likewise gaining resource_name), get_sd_mux/
    set_sd_mux/set_usb_mux (Category.MUX; get_sd_mux is SD-only -- usb_mux has
    no read method), console_open/console_read/console_send/console_close
    (Category.CONSOLE), ssh_run/put_file/get_file/forward_open/
    forward_remote_open/forward_close (Category.SSH; forward_list is
    unconditional like the reads -- it only lists in-memory tunnel state
    (now additionally carrying each entry's "local"/"remote" direction), so
    it survives readonly),
    flash_dfu/flash_fastboot/flash_script/bootstrap/write_image/flash_status/
    flash_logs (Category.FLASH; write_image additionally takes optional
    partition/mode/skip/seek, §11.14), add_place/add_place_alias/
    delete_place_alias/set_place_tags/set_place_comment/add_place_match
    (Category.METADATA), and delete_place/delete_place_match
    (Category.PLACE_DELETE) -- readonly mode or an explicit LABGRID_MCP_ALLOW
    list can drop any of them. FLASH and PLACE_DELETE are opt-in ONLY: unlike
    every other gated category they are excluded by default even without
    readonly (DESIGN §11.11 decision #5 -- a killed flash mid-write can brick
    hardware; §11.12/decision #13 -- the coordinator has no ownership guard
    on DeletePlace/DeletePlaceMatch, so either can destroy any place lab-wide,
    acquired or not). METADATA (the other six mutators) is default-on like
    the rest. Unlike the plain read
    tools, get_power_state, get_io, and get_sd_mux talk to real hardware paths
    through ``targets``, so they stay gated by their category rather than
    always-on like the Phase 1/2 reads. ``wait_for_change`` is registered unconditionally
    like the other reads even though it is not gated by any category (design
    §11.12: the long-poll change-cursor tool, a FastMCP subscription
    substitute). The eight place-metadata mutators (six METADATA plus the two
    PLACE_DELETE deleters) are the only place any tool talks directly to the
    coordinator client's raw wrappers rather than ``session``/``targets`` --
    the coordinator has NO ownership guard on any of these RPCs (§11.12), so
    every mutator on an EXISTING place refuses a foreign acquisition
    client-side unless ``force=True``.
    ``labgrid://sessions`` (consoles + jobs) stays unconditional like the
    other resources, even under readonly.

    The client's lifecycle is bracketed by the server lifespan -- start() runs
    before the first request and stop() on shutdown. A coordinator that is
    unreachable at startup is logged, not fatal: the server still serves,
    coordinator_info reports connected=false, and a background task keeps
    retrying start() with backoff until it succeeds or the server shuts down
    (DESIGN.md section 3.2: persistent auto-reconnecting connection). On
    shutdown, background flash jobs are cancelled first (jobs.shutdown(),
    SIGTERM best-effort -- must run before the Targets they pin are torn
    down), then console sessions (consoles.shutdown()), then forward tunnels
    (forwards.shutdown()), then cached Targets (targets.shutdown()), then the
    session's keepalive tasks (session.shutdown()), then the client
    (client.stop()) -- each stage must not depend on a later one still being
    alive, and every stage runs even if an earlier one raises.
    """

    @contextlib.asynccontextmanager
    async def lifespan(_server: FastMCP) -> AsyncIterator[None]:
        retry_task: asyncio.Task[None] | None = None
        try:
            await client.start()
        except CoordinatorError:
            _log.warning(
                "coordinator unreachable at startup (%s); serving anyway and retrying",
                config.coordinator,
            )
            retry_task = asyncio.get_running_loop().create_task(_retry_start(client))
        try:
            yield
        finally:
            # client.stop() must ALWAYS run, even if the retry task ended with
            # an unexpected (non-CancelledError) exception -- in that case
            # cancel() is a no-op and ``await retry_task`` re-raises it, which
            # would otherwise skip teardown. Nested try/finally guarantees stop;
            # any non-cancel error from the retry task is logged, not swallowed.
            try:
                if retry_task is not None:
                    retry_task.cancel()
                    try:
                        await retry_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        _log.exception("coordinator retry task failed during shutdown")
            finally:
                # jobs.shutdown() must run before consoles.shutdown()/
                # forwards.shutdown()/targets.shutdown() (a running flash job
                # pins the cached Target it's driving -- SIGTERM it before we
                # tear down the Targets it pins), consoles.shutdown() and
                # forwards.shutdown() must run before targets.shutdown() (a
                # console session's driver and a forward tunnel's SSHDriver are
                # both bound on a cached Target, so neither must outlive that
                # Target), targets.shutdown() must run before session.shutdown()
                # (a cached Target must not outlive our ownership of a place),
                # session.shutdown() must run before client.stop() (keepalive
                # polls need the client), and client.stop() must ALWAYS run
                # even if an earlier stage raises.
                try:
                    await jobs.shutdown()
                finally:
                    try:
                        await consoles.shutdown()
                    finally:
                        try:
                            await forwards.shutdown()
                        finally:
                            try:
                                await targets.shutdown()
                            finally:
                                try:
                                    await session.shutdown()
                                finally:
                                    await client.stop()

    mcp = FastMCP("labgrid-mcp", lifespan=lifespan)

    # policy.annotations() is the single source of truth for the hint values;
    # the SDK wants a ToolAnnotations object, so map its dict across here rather
    # than reshaping the policy helper.
    read_only_hints = annotations(read_only=True, idempotent=True)
    info_annotations = ToolAnnotations(
        readOnlyHint=read_only_hints["readOnlyHint"],
        destructiveHint=read_only_hints["destructiveHint"],
        idempotentHint=read_only_hints["idempotentHint"],
    )
    reserve_hints = annotations(destructive=False, idempotent=False)
    reserve_annotations = ToolAnnotations(
        readOnlyHint=reserve_hints["readOnlyHint"],
        destructiveHint=reserve_hints["destructiveHint"],
        idempotentHint=reserve_hints["idempotentHint"],
    )
    destructive_hints = annotations(destructive=True, idempotent=False)
    destructive_annotations = ToolAnnotations(
        readOnlyHint=destructive_hints["readOnlyHint"],
        destructiveHint=destructive_hints["destructiveHint"],
        idempotentHint=destructive_hints["idempotentHint"],
    )
    # console_close: non-destructive but idempotent (repeated closes of an
    # already-closed session are a no-op) -- neither of the combos above fits.
    close_hints = annotations(destructive=False, idempotent=True)
    close_annotations = ToolAnnotations(
        readOnlyHint=close_hints["readOnlyHint"],
        destructiveHint=close_hints["destructiveHint"],
        idempotentHint=close_hints["idempotentHint"],
    )

    @mcp.tool(annotations=info_annotations)
    async def coordinator_info() -> dict[str, object]:
        """Report the coordinator connection.

        Returns the coordinator address, the client's claimed identity, whether
        the subscription is currently connected, and the coordinator version if
        the protocol provides one (currently null).
        """
        return asdict(client.info())

    @mcp.tool(annotations=info_annotations)
    async def list_places() -> dict[str, object]:
        """All places known to the coordinator, as labgrid place dicts."""
        return {"places": client.places()}

    @mcp.tool(annotations=info_annotations)
    async def show_place(name: str) -> dict[str, object]:
        """One place by exact name. Raises a tool error if unknown."""
        for place in client.places():
            if place.get("name") == name:
                return place
        raise ToolError(f"unknown place: {name!r}")

    @mcp.tool(annotations=info_annotations)
    async def who() -> dict[str, object]:
        """Currently acquired places by host/user (derived from the snapshot)."""
        acquisitions: list[dict[str, object]] = []
        for place in client.places():
            acquired = place.get("acquired")
            if not isinstance(acquired, str) or not acquired:
                continue
            host, _, user = acquired.partition("/")
            acquisitions.append(
                {
                    "place": place.get("name"),
                    "host": host,
                    "user": user,
                    "exporters": _exporters_of(place.get("acquired_resources")),
                }
            )
        return {"acquisitions": acquisitions}

    @mcp.tool(annotations=info_annotations)
    async def list_resources() -> dict[str, object]:
        """All exporter resources known to the coordinator."""
        return {"resources": client.resources()}

    @mcp.tool(annotations=info_annotations)
    async def list_reservations() -> dict[str, object]:
        """Current reservations (live unary RPC to the coordinator)."""
        try:
            return {"reservations": await client.get_reservations()}
        except CoordinatorError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool(annotations=info_annotations)
    async def wait_for_change(
        cursor: int | None = None, timeout_s: float = 25.0
    ) -> dict[str, object]:
        """Long-poll for the next place/resource change (design §11.12 --
        the live-monitor substitute; FastMCP has no subscription surface).

        ``cursor=None`` bootstraps: returns the coordinator's current change
        cursor immediately, with ``changed=false`` (no waiting) -- call this
        once to get a starting point, then pass the returned ``cursor`` back
        on later calls. Otherwise blocks (event-driven, no busy-polling)
        until the cursor advances past ``cursor``, or ``timeout_s`` elapses,
        whichever comes first, then returns the current cursor and whether it
        changed. ``timeout_s`` is clamped to at most 25.0 regardless of what
        is requested, to stay under typical MCP client request timeouts --
        this assumes the caller's own MCP client timeout is set higher than
        25s; a lower client timeout just means the client gives up first and
        the orphaned poll on our side finishes harmlessly on its own.
        Read-only and idempotent: it never mutates anything, and repeated
        calls with the same arguments are safe to retry. Registered
        unconditionally, like the other read tools -- available even in
        readonly mode. The change cursor is process-local and resets to 0 on
        server restart (design §11.12); a cursor value held from a previous
        process is simply stale and self-heals after at most one full
        ``timeout_s`` -- the next call either sees an already-advanced
        cursor (``changed=True``) or times out and returns the current one.
        """
        if cursor is None:
            return {"cursor": client.change_cursor(), "changed": False}
        clamped = min(timeout_s, 25.0)
        new_cursor = await client.wait_for_change(cursor, clamped)
        return {"cursor": new_cursor, "changed": new_cursor > cursor}

    if is_enabled(Category.RESERVATION, config):

        @mcp.tool(annotations=reserve_annotations)
        async def reserve(filters: dict[str, str], prio: float = 0.0) -> dict[str, object]:
            """Create a reservation (with the given filters/priority) and keep
            it alive in the background until it is cancelled, expires, or is
            invalidated. Returns the serialized reservation, including its
            token.
            """
            try:
                return {"reservation": await session.reserve(filters, prio)}
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=destructive_annotations)
        async def cancel_reservation(token: str) -> dict[str, object]:
            """Cancel a reservation by token and stop its keepalive task."""
            try:
                await session.cancel_reservation(token)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            return {"cancelled": token}

        @mcp.tool(annotations=reserve_annotations)
        async def reservation_wait(token: str, timeout_s: float = 25.0) -> dict[str, object]:
            """Block-and-poll a reservation until it allocates a place, or times out.

            ``timeout_s`` is clamped to at most 25.0 (PlaceSession.reservation_wait,
            §11.14), like ``wait_for_change``. Only an *acquired* reservation
            auto-refreshes coordinator-side -- polling here IS what keeps a
            merely-``waiting``/``allocated`` reservation's TTL alive, so the
            token still needs an ``acquire_place`` (or another ``reservation_wait``/
            ``cancel_reservation``) soon after this returns, or it expires
            ~60s after creation regardless of the outcome reported here.
            Returns ``{"token", "state", "allocations", "changed"}`` --
            ``changed`` is True iff the reservation is allocated by the time
            this returns; a dead token (expired/invalid/unknown) or a genuine
            timeout both report ``changed=False`` without raising. Non-
            destructive but not idempotent (each call keepalive-polls).
            """
            try:
                return await session.reservation_wait(token, timeout_s)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc

    if is_enabled(Category.ACQUIRE, config):

        @mcp.tool(annotations=destructive_annotations)
        async def acquire_place(name: str) -> dict[str, object]:
            """Acquire a place by name for this session.

            Free places are acquired directly; a taken or reserved place is
            acquired via a name-filtered reservation, polled until it
            allocates our place (bounded by ``config.acquire_timeout``), then
            acquired and the reservation dropped (session.py, DESIGN.md
            section 11.8). Raises a tool error on RPC failure or timeout.
            """
            try:
                return {"place": await session.acquire_place(name)}
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=destructive_annotations)
        async def release_place(name: str, kick: bool = False) -> dict[str, object]:
            """Release a place by name.

            By default this verifies we currently hold the place before
            releasing it; pass kick=True to release unconditionally
            regardless of the current holder.
            """
            # A RUNNING flash job pins this place's cached Target (§11.11) --
            # refuse BEFORE the coordinator RPC (on both the normal and kick
            # paths; kick does not override a local mid-write flash) so we
            # never release coordinator-side ownership and then fail the local
            # invalidate, leaving a half-released place. JobRegistry is the
            # source of truth for running jobs; the post-RPC invalidate below
            # stays as the backstop for the residual check-then-act window.
            running = jobs.running_job_for(name)
            if running is not None:
                raise ToolError(
                    f"cannot release place {name!r}: flash job {running!r} is running; "
                    "cancel it first"
                )
            # A console session's driver is bound on a Target we are about to
            # give up ownership of -- close it BEFORE the release RPC (on both
            # the normal and kick paths) so a stale reader/driver can never
            # linger past this call. The non-kick path gates the close on the
            # same snapshot ownership check session.release_place will make,
            # so the common failure (not-owner/race) does not destroy a live
            # console for nothing; kick releases unconditionally, so it also
            # closes unconditionally. A release that then fails anyway (the
            # residual TOCTOU race) still closed first -- the release intent
            # was valid when we checked.
            # Forward tunnels bind an SSHDriver on the same about-to-be-released
            # Target; close them alongside the console (same ownership gate).
            # Unlike a running flash job, neither a console nor a forward PINS
            # the place, so this is cleanup, not a pre-RPC refusal.
            place_snap = client.place(name)
            if kick or (place_snap is not None and place_snap.get("acquired") == config.identity):
                await consoles.close_place(name)
                await forwards.close_place(name)
            try:
                result = await session.release_place(name, kick=kick)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            # The cached Target (target.py, DESIGN §11.9) must not outlive our
            # ownership of the place -- drop it now so a stale driver binding
            # can never run against a place we no longer hold. Runs on both
            # the normal and kick release paths since both reach here only
            # after a successful release. invalidate() REFUSES while a flash
            # job pins the place (§11.11) -- surface that as a ToolError naming
            # the job so the caller knows to cancel it first; kick does NOT
            # override this (a coordinator-level "release anyway" is not
            # license to rip a driver out from under a mid-write flash).
            try:
                await targets.invalidate(name)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            return {"place": result}

        @mcp.tool(annotations=destructive_annotations)
        async def allow_place(name: str, user: str) -> dict[str, object]:
            """Allow another user to use a place this session has acquired.

            ``user`` must be in "host/user" format; raises before any RPC if
            it is not.
            """
            _validate_identity(user)
            try:
                await client.allow_place_rpc(name, user)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            return {"allowed": user, "place": name}

        @mcp.tool(annotations=destructive_annotations)
        async def release_from(place: str, host: str, user: str) -> dict[str, object]:
            """Release ``place`` from a specific ``host``/``user`` identity.

            Unlike ``release_place`` (which only ever releases OUR OWN
            acquisition), this targets an arbitrary identity -- useful for an
            operator clearing a stale/foreign hold. Design §11.14/§11.8 trap:
            the coordinator's ``ReleasePlace`` does NO format validation on
            ``fromuser``, and a non-empty ``fromuser`` that does not match the
            place's actual holder is a SILENT no-op that still reports
            success -- so simply checking "not acquired by fromuser
            afterward" is USELESS (it is already true before the call
            whenever ``fromuser`` never held the place, which is exactly the
            mismatch case). The only way to tell whether THIS call actually
            changed anything is to compare a snapshot from BEFORE the RPC
            against one from after: ``released`` is True iff ``fromuser`` was
            the holder beforehand AND is no longer the holder afterward (a
            place ``fromuser`` never held reports ``released=False`` --
            nothing was there for this call to release). This tool talks
            directly to the coordinator client (like the place-metadata
            mutators) rather than through ``session.release_place``, since
            that helper only knows how to release OUR OWN identity and would
            refuse before ever sending the RPC for anyone else's.
            ``host``/``user`` must each be non-empty and "/"-free -- validated
            (mirroring ``allow_place``'s ``_validate_identity``) BEFORE any RPC,
            since the coordinator does no validation of its own (§11.14 above).
            """
            _validate_release_from_identity(host, user)
            fromuser = f"{host}/{user}"
            before = client.place(place)
            was_held = before is not None and before.get("acquired") == fromuser
            try:
                await client.release_place_rpc(place, fromuser=fromuser)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(
                client, place, lambda p: not was_held or p.get("acquired") != fromuser
            )
            released = was_held and result.get("acquired") != fromuser
            # If we just released OUR OWN hold (fromuser == config.identity)
            # and the release actually took effect, the local state
            # release_place normally tears down (console sessions, forward
            # tunnels, cached Target) is now dangling -- run the identical
            # cleanup release_place performs after a successful release so a
            # later acquire_place can never reuse a stale Target. A foreign
            # release or a silent no-op (released=False) leaves our local
            # state untouched, which is correct -- we never held anything to
            # clean up in either case.
            if released and fromuser == config.identity:
                await consoles.close_place(place)
                await forwards.close_place(place)
                try:
                    await targets.invalidate(place)
                except TargetError as exc:
                    raise ToolError(str(exc)) from exc
            return {"place": place, "released_from": fromuser, "released": released}

    if is_enabled(Category.POWER, config):

        @mcp.tool(annotations=info_annotations)
        async def get_power_state(
            place: str, resource_name: str | None = None
        ) -> dict[str, object]:
            """Current power state for a place this server has acquired.

            Reads via ``NetworkPowerDriver.get()`` (target.py). Category.POWER
            gated even though it only reads: it talks to a real hardware path,
            unlike the always-on Phase 1/2 read tools. ``resource_name``
            (§11.14) picks one of several same-class power resources on the
            place -- omit it for a single-power-resource place (unchanged
            behavior); an unnamed pick on a multi-resource place is a tool
            error naming the available resources. Echoed back in the result
            only when given (additive).
            """
            _check_place_owned(client, config, place)
            try:
                power = await targets.power_state(place, resource_name=resource_name)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            result: dict[str, object] = {"place": place, "power": power}
            if resource_name is not None:
                result["resource_name"] = resource_name
            return result

        @mcp.tool(annotations=destructive_annotations)
        async def set_power(
            place: str,
            action: str,
            delay: float | None = None,
            resource_name: str | None = None,
        ) -> dict[str, object]:
            """Drive power on/off/cycle on an acquired place, then return the
            resulting state.

            ``action`` must be one of "on", "off", "cycle"; anything else is
            rejected with a tool error before the place is even checked or any
            driver is touched. ``delay`` (§11.14) sets the off/on gap (in
            seconds) ``NetworkPowerDriver.cycle()`` sleeps for; it is only
            meaningful for ``action="cycle"`` -- passing it with "on"/"off" is
            a tool error (clearer than silently ignoring it), raised alongside
            the action check, before ownership or any driver call.
            ``resource_name`` picks one of several same-class power resources
            on the place (§11.14); omit it for a single-power-resource place
            (unchanged behavior).
            """
            if action not in _POWER_ACTIONS:
                raise ToolError(f"action must be one of 'on', 'off', 'cycle', got {action!r}")
            if delay is not None and action != "cycle":
                raise ToolError(f"delay is only valid with action='cycle', got action={action!r}")
            # mypy narrows `action: str` to Literal["on", "off", "cycle"] from
            # the membership check above, matching targets.power()'s signature.
            _check_place_owned(client, config, place)
            try:
                power = await targets.power(
                    place, action, delay=delay, resource_name=resource_name
                )
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            result: dict[str, object] = {"place": place, "power": power}
            if resource_name is not None:
                result["resource_name"] = resource_name
            return result

    if is_enabled(Category.IO, config):

        @mcp.tool(annotations=info_annotations)
        async def get_io(place: str, resource_name: str | None = None) -> dict[str, object]:
            """Current digital IO state for an acquired place (HttpDigitalOutputDriver.get()).

            ``resource_name`` (§11.14) picks one of several same-class IO
            resources on the place; omit it for a single-IO-resource place
            (unchanged behavior). Echoed back in the result only when given.
            """
            _check_place_owned(client, config, place)
            try:
                value = await targets.io_get(place, resource_name=resource_name)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            result: dict[str, object] = {"place": place, "value": value}
            if resource_name is not None:
                result["resource_name"] = resource_name
            return result

        @mcp.tool(annotations=destructive_annotations)
        async def set_io(
            place: str, value: bool, resource_name: str | None = None
        ) -> dict[str, object]:
            """Set digital IO on an acquired place, then re-read and return
            the resulting state (HttpDigitalOutputDriver.set() then .get()).

            ``resource_name`` (§11.14) picks one of several same-class IO
            resources on the place; the SAME name is used for both the set
            and the re-read, and omitting it preserves single-resource
            behavior unchanged.
            """
            _check_place_owned(client, config, place)
            try:
                await targets.io_set(place, value, resource_name=resource_name)
                result_value = await targets.io_get(place, resource_name=resource_name)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            result: dict[str, object] = {"place": place, "value": result_value}
            if resource_name is not None:
                result["resource_name"] = resource_name
            return result

    if is_enabled(Category.MUX, config):

        @mcp.tool(annotations=info_annotations)
        async def get_sd_mux(place: str) -> dict[str, object]:
            """Current SD-mux mode for an acquired place (``USBSDMuxDriver.get_mode()``).

            SD-mux only (§11.14): ``LXAUSBMuxDriver`` (the ``usb_mux`` kind)
            has no read method, so there is no ``usb_mux`` equivalent.
            """
            _check_place_owned(client, config, place)
            try:
                mode = await targets.sd_mux_mode(place)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            return {"place": place, "mode": mode}

        @mcp.tool(annotations=destructive_annotations)
        async def set_sd_mux(place: str, mode: str) -> dict[str, object]:
            """Set the SD mux mode on an acquired place (USBSDMuxDriver.set_mode())."""
            _check_place_owned(client, config, place)
            try:
                await targets.sd_mux(place, mode)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            return {"place": place, "mode": mode}

        @mcp.tool(annotations=destructive_annotations)
        async def set_usb_mux(place: str, links: list[str]) -> dict[str, object]:
            """Set USB mux links on an acquired place (LXAUSBMuxDriver.set_links())."""
            _check_place_owned(client, config, place)
            try:
                await targets.usb_mux(place, links)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            return {"place": place, "links": links}

    if is_enabled(Category.CONSOLE, config):

        @mcp.tool(annotations=reserve_annotations)
        async def console_open(place: str) -> dict[str, object]:
            """Open an interactive console session on an acquired place.

            Binds a raw-protocol ``SerialDriver`` and starts its background
            reader (console.py, DESIGN §11.10); one session per place -- a
            second open on a place that already has one is rejected by the
            registry. The ownership check runs first, before any registry
            call, so an unacquired/unknown place never touches the registry.
            """
            _check_place_owned(client, config, place)
            try:
                return await consoles.open(place)
            except (ConsoleError, TargetError) as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=info_annotations)
        async def console_read(session: str, max_bytes: int | None = None) -> dict[str, object]:
            """Drain up to ``max_bytes`` (default all) buffered console bytes.

            Consumes the drained bytes and resets the truncated flag. An
            unknown session is a tool error; an errored reader still surfaces
            any buffered data alongside the failure (registry.read()).
            """
            try:
                return consoles.read(session, max_bytes)
            except ConsoleError as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=destructive_annotations)
        async def console_send(session: str, data: str, newline: bool = False) -> dict[str, object]:
            """Write ``data`` to an open console session.

            Appends ``"\\n"`` when ``newline=True``. An unknown, closed, or
            errored session is a tool error.
            """
            try:
                return await consoles.send(session, data, newline)
            except ConsoleError as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=close_annotations)
        async def console_close(session: str) -> dict[str, object]:
            """Close a console session: stop and join its reader, then
            deactivate the driver. Idempotent; an unknown session is a tool
            error.
            """
            try:
                return await consoles.close(session)
            except ConsoleError as exc:
                raise ToolError(str(exc)) from exc

    # forward_list only lists in-memory tunnel state (no hardware, no place
    # ownership), so it is registered UNCONDITIONALLY like the plain reads and
    # the sessions resource -- it survives readonly, unlike the five gated SSH
    # tools below (plan: "READONLY excludes all but forward_list").
    @mcp.tool(annotations=info_annotations)
    async def forward_list() -> dict[str, object]:
        """List all live forward tunnels across every place (forwards.sessions())."""
        return {"forwards": forwards.sessions()}

    if is_enabled(Category.SSH, config):

        @mcp.tool(annotations=destructive_annotations)
        async def ssh_run(place: str, command: str, timeout_s: float = 30.0) -> dict[str, object]:
            """Run ``command`` over SSH on an acquired place and return its result.

            Binds an ``SSHDriver`` (target.py, §11.13) and shells out via
            ``SSHDriver.run`` off the loop. Returns
            ``{"place", "stdout", "stderr", "exit_code"}`` -- labgrid returns
            stdout/stderr as line LISTS, joined here with ``"\\n"``. The
            driver's own ``timeout=timeout_s`` is the sole enforcer of
            ``timeout_s``; the outer ``asyncio.wait_for`` only backstops a
            wedged thread with a small grace (``timeout_s + 5.0``) so it
            doesn't co-fire with the inner timeout. CAVEAT: a timeout does NOT
            kill the remote command -- ``subprocess`` does not reap on
            ``communicate(timeout=...)``, so the underlying ``ssh`` may linger
            until it finishes or the ControlMaster tears down. CAVEAT: labgrid
            whitespace-splits ``command`` (``cmd.split(" ")``) with no shell
            quoting, so multi-space or quoted arguments will split wrong.
            """
            _check_place_owned(client, config, place)
            try:
                driver = await targets.ssh_driver(place)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            try:
                stdout, stderr, exit_code = await asyncio.wait_for(
                    asyncio.to_thread(driver.run, command, timeout=timeout_s),
                    timeout_s + 5.0,
                )
            except (TimeoutError, subprocess.TimeoutExpired) as exc:
                raise ToolError(
                    f"ssh_run on {place!r} timed out after {timeout_s}s "
                    "(the underlying ssh command may still be running)"
                ) from exc
            except Exception as exc:
                raise ToolError(f"ssh_run on {place!r} failed: {exc}") from exc
            return {
                "place": place,
                "stdout": "\n".join(stdout),
                "stderr": "\n".join(stderr),
                "exit_code": int(exit_code),
            }

        @mcp.tool(annotations=destructive_annotations)
        async def put_file(place: str, local_path: str, remote_path: str) -> dict[str, object]:
            """Copy a local file to ``remote_path`` on an acquired place (scp).

            The local file must exist (a missing one is a pinned ToolError,
            checked before the driver is bound). Returns
            ``{"place", "put": remote_path, "bytes": <local size>}``.
            """
            _check_place_owned(client, config, place)
            src = Path(local_path)
            if not src.is_file():
                raise ToolError(f"local file {local_path!r} does not exist")
            size = src.stat().st_size
            try:
                driver = await targets.ssh_driver(place)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            try:
                await asyncio.to_thread(driver.put, local_path, remote_path)
            except Exception as exc:
                raise ToolError(f"put_file to {place!r} failed: {exc}") from exc
            return {"place": place, "put": remote_path, "bytes": size}

        @mcp.tool(annotations=reserve_annotations)
        async def get_file(
            place: str, remote_path: str, local_path: str, overwrite: bool = False
        ) -> dict[str, object]:
            """Copy ``remote_path`` from an acquired place to ``local_path`` (scp).

            An existing local target is refused unless ``overwrite=True`` (pinned
            ToolError); the parent directory must already exist. Returns
            ``{"place", "got": local_path, "bytes": <resulting size>}``. NOT
            read-only (it writes a local file), but non-destructive to the DUT.
            """
            _check_place_owned(client, config, place)
            dest = Path(local_path)
            if dest.exists() and not overwrite:
                raise ToolError(
                    f"local path {local_path!r} already exists; pass overwrite=True to replace"
                )
            if not dest.parent.is_dir():
                raise ToolError(f"parent directory {str(dest.parent)!r} does not exist")
            try:
                driver = await targets.ssh_driver(place)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            try:
                await asyncio.to_thread(driver.get, remote_path, local_path)
            except Exception as exc:
                raise ToolError(f"get_file from {place!r} failed: {exc}") from exc
            size = dest.stat().st_size if dest.exists() else 0
            return {"place": place, "got": local_path, "bytes": size}

        @mcp.tool(annotations=reserve_annotations)
        async def forward_open(
            place: str, remote_port: int, local_port: int = 0
        ) -> dict[str, object]:
            """Open a local port-forward tunnel to ``remote_port`` on a place.

            ``local_port=0`` (default) auto-assigns a free local port. Returns
            ``{"forward", "place", "local_port", "remote_port"}``. Multiple
            tunnels per place are allowed (forwards.py, §11.13). The ownership
            check runs first, before any registry call.
            """
            _check_place_owned(client, config, place)
            try:
                return await forwards.open(place, remote_port, local_port)
            except (ForwardError, TargetError) as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=reserve_annotations)
        async def forward_remote_open(
            place: str, remote_port: int, local_port: int
        ) -> dict[str, object]:
            """Open a REMOTE (``-R``) port-forward tunnel on a place.

            Unlike ``forward_open``'s local (``-L``) forward, BOTH ports are
            required -- labgrid's ``SSHDriver.forward_remote_port`` has no
            auto-assign for the local side (§11.14): a connection to
            ``remote_port`` on the DUT is forwarded to
            ``localhost:local_port`` on THIS host, so a local service must
            already be listening there. Returns ``{"forward", "place",
            "direction": "remote", "remote_port", "local_port"}``.
            ``forward_list``/``forward_close`` (and the ``labgrid://sessions``
            forwards payload) work identically regardless of direction. The
            ownership check runs first, before any registry call.
            """
            _check_place_owned(client, config, place)
            try:
                return await forwards.open_remote(place, remote_port, local_port)
            except (ForwardError, TargetError) as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=close_annotations)
        async def forward_close(forward: str) -> dict[str, object]:
            """Close a forward tunnel by id (``ssh -O cancel``, best-effort).

            Idempotent shape; an unknown tunnel id is a tool error.
            """
            try:
                return await forwards.close(forward)
            except ForwardError as exc:
                raise ToolError(str(exc)) from exc

    if is_enabled(Category.FLASH, config):

        def _require_local_file(path: str) -> None:
            """Raise a ``ToolError`` before any submission if ``path`` is missing.

            §11.11's ``target.env is None`` trap: the Target has no config to
            fall back to, so every flash tool passes an EXPLICIT local path
            straight through to the driver (labgrid's own ``ManagedFile`` then
            copies it to the exporter). Checked here, synchronously, before
            ``jobs.submit_flash`` ever touches the registry or pins the place --
            a missing file must never start a job.
            """
            if not Path(path).is_file():
                raise ToolError(f"file {path!r} does not exist")

        async def _submit(
            place: str, kind: str, build: Callable[[Any], Callable[[], object]]
        ) -> dict[str, object]:
            """Shared submit path for all 5 flash submitters (§11.11 argv table).

            Ownership is re-checked by the tool before this (friendly), and
            again inside ``TargetManager`` (backstop) -- see ``_check_place_owned``
            call sites below. ``jobs.submit_flash`` does the pin-before-bind
            critical section (requirement 1; jobs.py). The returned payload is
            normalized (``_normalize_job_payload``) so the internal
            ``"bootstrap:<loader>"`` kind encoding never reaches the client --
            every submitter's result carries the canonical ``kind`` plus the
            additive ``loader`` field (``None`` except non-default bootstrap).
            """
            try:
                return _normalize_job_payload(await jobs.submit_flash(place, kind, build))
            except (JobError, TargetError) as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=destructive_annotations)
        async def flash_dfu(place: str, alt: int, file: str) -> dict[str, object]:
            """Flash ``file`` over DFU on an acquired place's USB export.

            Runs ``DFUDriver.download(altsetting, filename)`` (§11.11 argv
            table) as a background job; returns immediately with a job id --
            poll ``flash_status``/``flash_logs``. ``file`` is validated to exist
            locally before anything is submitted.
            """
            _check_place_owned(client, config, place)
            _require_local_file(file)

            def build(driver: Any) -> Callable[[], object]:
                return lambda: driver.download(alt, file)

            return await _submit(place, "dfu", build)

        @mcp.tool(annotations=destructive_annotations)
        async def flash_fastboot(place: str, partition: str, file: str) -> dict[str, object]:
            """Flash ``file`` to ``partition`` over fastboot on an acquired place.

            Runs ``AndroidFastbootDriver.flash(partition, filename)`` (§11.11
            argv table) as a background job. ``file`` is validated to exist
            locally before anything is submitted.
            """
            _check_place_owned(client, config, place)
            _require_local_file(file)

            def build(driver: Any) -> Callable[[], object]:
                return lambda: driver.flash(partition, file)

            return await _submit(place, "fastboot", build)

        @mcp.tool(annotations=destructive_annotations)
        async def flash_script(
            place: str, script: str, args: list[str] | None = None
        ) -> dict[str, object]:
            """Run a local flash ``script`` (with ``args``) on an acquired place.

            Runs ``FlashScriptDriver.flash(script, args)`` (§11.11 argv table)
            as a background job. ``script`` is validated to exist locally
            before anything is submitted; ``args`` defaults to an empty list.
            """
            _check_place_owned(client, config, place)
            _require_local_file(script)
            argv = list(args) if args else []

            def build(driver: Any) -> Callable[[], object]:
                return lambda: driver.flash(script, argv)

            return await _submit(place, "script", build)

        @mcp.tool(annotations=destructive_annotations)
        async def bootstrap(place: str, file: str, loader: str = "imx") -> dict[str, object]:
            """Bootstrap ``file`` onto an acquired place over its USB loader.

            Runs ``<Loader>Driver.load(filename)`` (§11.11 argv table) as a
            background job. ``loader`` selects which of the 5
            ``BootstrapProtocol`` implementations binds: "imx" (default,
            ``IMXUSBDriver``), "mxs" (``MXSUSBDriver``), "rk"
            (``RKUSBDriver``), "uuu" (``UUUDriver``), "bdimx"
            (``BDIMXUSBDriver``). This tool only forwards ``loader`` --
            ``TargetManager`` (target.py) does the mapping and rejects an
            unknown one (naming the valid options) before any Target/driver
            bind is attempted. ``file`` is validated to exist locally before
            anything is submitted. The returned payload's ``kind`` is always
            the canonical ``"bootstrap"``; a non-default ``loader`` is
            reported in the additive ``loader`` field, ``None`` for the
            default (``_normalize_job_payload`` -- the same shape
            ``flash_status`` and ``labgrid://sessions`` report).
            """
            _check_place_owned(client, config, place)
            _require_local_file(file)
            # jobs.submit_flash forwards "kind" opaquely with no separate
            # loader parameter, so a non-default loader is encoded into it
            # (target.py's _split_flash_kind decodes it back); the default
            # stays the plain "bootstrap" string -- byte-identical to every
            # existing caller/job payload (target.py §11.11 docstring). The
            # encoding is INTERNAL: _submit normalizes it back out before the
            # payload reaches the client (_normalize_job_payload).
            kind = "bootstrap" if loader == "imx" else f"bootstrap:{loader}"

            def build(driver: Any) -> Callable[[], object]:
                return lambda: driver.load(file)

            return await _submit(place, kind, build)

        @mcp.tool(annotations=destructive_annotations)
        async def write_image(
            place: str,
            file: str,
            partition: int | None = None,
            mode: str | None = None,
            skip: int = 0,
            seek: int = 0,
        ) -> dict[str, object]:
            """Write ``file`` to an acquired place's USB mass storage / SD mux.

            Runs ``USBStorageDriver.write_image(filename, mode=..., partition=...,
            skip=..., seek=...)`` (§11.14; ``dd if=<remote> of=<path> ...`` on
            the exporter) as a background job. ``mode`` is the ``Mode`` enum's
            NAME, case-insensitive ("dd"/"bmaptool"); omitted defaults to
            "dd". A bad name is a tool error naming the valid options, raised
            BEFORE ownership is even checked or anything submitted (like
            ``set_power``'s action validation) -- ``target.write_image_kwargs``
            does the mapping/validation once here, and its ``dict`` is
            forwarded verbatim to the driver call. ``partition`` is ``None``
            for the whole root device; ``skip``/``seek`` are counts of
            512-byte blocks at the start of input/output (``BMAPTOOL`` rejects
            a nonzero skip/seek -- a driver-side ``ExecutionError`` surfaced as
            a job failure, not validated here). ``file`` is validated to exist
            locally before anything is submitted.
            """
            try:
                kwargs = write_image_kwargs(partition=partition, mode=mode, skip=skip, seek=seek)
            except TargetError as exc:
                raise ToolError(str(exc)) from exc
            _check_place_owned(client, config, place)
            _require_local_file(file)

            def build(driver: Any) -> Callable[[], object]:
                return lambda: driver.write_image(file, **kwargs)

            return await _submit(place, "write_image", build)

        @mcp.tool(annotations=info_annotations)
        async def flash_status(job: str) -> dict[str, object]:
            """Lifecycle snapshot for a flash job (running/completed/failed/cancelled).

            ``kind`` is always the canonical base kind (e.g. ``"bootstrap"``,
            never the internal ``"bootstrap:<loader>"`` encoding); the additive
            ``loader`` field carries the requested bootstrap loader, ``None``
            for every other case (default-loader bootstrap included).
            """
            try:
                return _normalize_job_payload(jobs.status(job))
            except JobError as exc:
                raise ToolError(str(exc)) from exc

        @mcp.tool(annotations=info_annotations)
        async def flash_logs(job: str, max_bytes: int | None = None) -> dict[str, object]:
            """Drain up to ``max_bytes`` (default all) captured flash job output.

            Consumes the drained bytes and resets the truncated flag (drain
            semantics like ``console_read``).
            """
            try:
                return jobs.logs(job, max_bytes)
            except JobError as exc:
                raise ToolError(str(exc)) from exc

    if is_enabled(Category.METADATA, config):
        # Client-side safety layer for these six (design §11.12 headline
        # finding): the coordinator has NO ownership guard on any
        # place-metadata RPC -- any handshaked session can retag/re-alias/
        # re-comment/re-match ANY place, acquired by anyone or not.
        # ``_check_not_foreign_acquired`` is the only protection; every
        # mutator below except ``add_place`` (a brand-new name needs no
        # check) calls it before its RPC. The two DELETE mutators
        # (delete_place, delete_place_match) are registered separately below,
        # under Category.PLACE_DELETE -- opt-in only (decision #13), since
        # they are irreversible in the same way FLASH is.

        @mcp.tool(annotations=destructive_annotations)
        async def add_place(name: str) -> dict[str, object]:
            """Create a new place by name.

            No ownership check -- a brand-new name can never collide with an
            existing acquisition.
            """
            try:
                await client.add_place(name)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            return {"place": name, "added": True}

        @mcp.tool(annotations=destructive_annotations)
        async def add_place_alias(place: str, alias: str, force: bool = False) -> dict[str, object]:
            """Add an alias to an existing place.

            Idempotent on the coordinator (adding a duplicate alias is a
            no-op, OK). Refuses if the place is acquired by a DIFFERENT
            identity unless ``force=True``. Returns the refreshed place dict
            (bounded snapshot catch-up -- the mutating RPC's completion is
            independent of the ClientStream update that lands the edit).
            """
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.add_place_alias(place, alias)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(client, place, lambda p: alias in _aliases_of(p))
            return {"place": result}

        @mcp.tool(annotations=destructive_annotations)
        async def delete_place_alias(
            place: str, alias: str, force: bool = False
        ) -> dict[str, object]:
            """Remove an alias from an existing place.

            Pre-validates that ``alias`` is present in our snapshot BEFORE
            any RPC: design §11.12 trap -- the coordinator raises an
            uncaught ``KeyError`` (surfaced as gRPC ``UNKNOWN``) for a
            nonexistent alias, so this is checked here instead, giving a
            clean tool error and sending zero RPCs for a typo'd alias.
            Refuses if the place is acquired by a DIFFERENT identity unless
            ``force=True``.
            """
            current = client.place(place)
            aliases = current.get("aliases") if current is not None else None
            if not isinstance(aliases, list) or alias not in aliases:
                raise ToolError(f"place {place!r} has no alias {alias!r}")
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.delete_place_alias(place, alias)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(client, place, lambda p: alias not in _aliases_of(p))
            return {"place": result}

        @mcp.tool(annotations=destructive_annotations)
        async def set_place_tags(
            place: str, tags: dict[str, str], force: bool = False
        ) -> dict[str, object]:
            """Set tags on an existing place.

            An empty string value for a key DELETES that key -- intentional
            labgrid semantics (design §11.12): the coordinator's own value
            validation is a no-op that lets an empty string straight
            through, so this is surfaced honestly here rather than hidden.
            Refuses if the place is acquired by a DIFFERENT identity unless
            ``force=True``.
            """
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.set_place_tags(place, tags)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc

            def _tags_applied(p: dict[str, object]) -> bool:
                current_tags = p.get("tags")
                current_tags = current_tags if isinstance(current_tags, dict) else {}
                for key, value in tags.items():
                    if value == "":
                        if key in current_tags:
                            return False
                    elif current_tags.get(key) != value:
                        return False
                return True

            result = await _refreshed_place(client, place, _tags_applied)
            return {"place": result}

        @mcp.tool(annotations=destructive_annotations)
        async def set_place_comment(
            place: str, comment: str, force: bool = False
        ) -> dict[str, object]:
            """Set an existing place's free-form comment (unvalidated by the coordinator).

            Refuses if the place is acquired by a DIFFERENT identity unless
            ``force=True``.
            """
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.set_place_comment(place, comment)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(client, place, lambda p: p.get("comment") == comment)
            return {"place": result}

        @mcp.tool(annotations=destructive_annotations)
        async def add_place_match(
            place: str, pattern: str, rename: str | None = None, force: bool = False
        ) -> dict[str, object]:
            """Add a resource match to an existing place.

            ``pattern`` is ``"exporter/group/cls"`` or
            ``"exporter/group/cls/name"`` (exactly 3 or 4 non-empty
            ``/``-separated segments) -- validated here before any RPC,
            since a different arity crashes the coordinator uncaught
            (design §11.12 trap). ``rename`` sets an alternate resource
            name; verified against labgrid 26.0 it is NOT part of a match's
            identity (a duplicate ``pattern`` is rejected regardless of
            ``rename``, and ``delete_place_match`` removes by ``pattern``
            alone -- see ``coordinator.py``). Refuses if the place is
            acquired by a DIFFERENT identity unless ``force=True``.
            """
            _validate_match_pattern(pattern)
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.add_place_match(place, pattern, rename)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(
                client, place, lambda p: _pattern_in_matches(p, pattern)
            )
            return {"place": result}

    if is_enabled(Category.PLACE_DELETE, config):
        # Opt-in only (decision #13, mirrors FLASH/decision #5): these two
        # RPCs are the irreversible cross-user destroyers of the place-
        # metadata surface -- the coordinator has NO ownership guard on
        # either (§11.12), so an unattended delete_place/delete_place_match
        # can destroy any place lab-wide, acquired by anyone or not, unlike
        # the other six METADATA mutators which only retag/re-alias/
        # re-comment/re-match in place. Excluded from the default env even
        # without readonly; enable explicitly with
        # ``LABGRID_MCP_ALLOW=place_delete``.

        @mcp.tool(annotations=destructive_annotations)
        async def delete_place(name: str, force: bool = False) -> dict[str, object]:
            """Delete a place by name.

            Refuses if the place is currently acquired -- by ANYONE,
            including this server itself -- unless ``force=True``: design
            §11.12 verified the coordinator lets you delete an acquired
            place, stranding the acquisition, so deleting your OWN acquired
            place needs ``force=True`` too (unlike every other metadata
            mutator, which only refuses a DIFFERENT holder). The
            coordinator misreports "delete a nonexistent place" as
            ``ALREADY_EXISTS`` ("Place x does not exist", labgrid 26.0 bug)
            -- mapped here to a clean not-found error.
            """
            _check_not_foreign_acquired(client, config, name, force, refuse_own=True)
            try:
                await client.delete_place(name)
            except CoordinatorError as exc:
                if "does not exist" in str(exc):
                    raise ToolError(f"place {name!r} does not exist") from exc
                raise ToolError(str(exc)) from exc
            return {"place": name, "deleted": True}

        @mcp.tool(annotations=destructive_annotations)
        async def delete_place_match(
            place: str, pattern: str, rename: str | None = None, force: bool = False
        ) -> dict[str, object]:
            """Remove a resource match from an existing place.

            Same pattern-arity validation as ``add_place_match``. ``rename``
            is accepted for API symmetry but does not affect which match is
            removed (see ``add_place_match``'s docstring) -- the coordinator
            matches on ``pattern`` alone. Refuses if the place is acquired
            by a DIFFERENT identity unless ``force=True``.
            """
            _validate_match_pattern(pattern)
            _check_not_foreign_acquired(client, config, place, force)
            try:
                await client.delete_place_match(place, pattern, rename)
            except CoordinatorError as exc:
                raise ToolError(str(exc)) from exc
            result = await _refreshed_place(
                client, place, lambda p: not _pattern_in_matches(p, pattern)
            )
            return {"place": result}

    @mcp.resource("labgrid://places")
    async def places() -> str:
        """JSON array of the places known to the coordinator (empty until synced)."""
        return json.dumps(client.places())

    @mcp.resource("labgrid://resources")
    async def resources_resource() -> str:
        """JSON array of the resources known to the coordinator."""
        return json.dumps(client.resources())

    @mcp.resource("labgrid://reservations")
    async def reservations_resource() -> str:
        """JSON array of current reservations."""
        try:
            return json.dumps(await client.get_reservations())
        except CoordinatorError as exc:
            raise ResourceError(str(exc)) from exc

    @mcp.resource("labgrid://places/{name}")
    async def place_resource(name: str) -> str:
        """JSON of a single place by name."""
        for place in client.places():
            if place.get("name") == name:
                return json.dumps(place)
        raise ResourceError(f"unknown place: {name!r}")

    @mcp.resource("labgrid://sessions")
    async def sessions_resource() -> str:
        """JSON of live console sessions, forward tunnels, AND background flash jobs.

        ``{"consoles": [...], "forwards": [...], "jobs": [...]}``
        (consoles.sessions(), forwards.sessions(), jobs.jobs()). Unconditional
        like the other resources -- registered even when Category.CONSOLE/
        Category.SSH/Category.FLASH tools are gated off (readonly mode
        included). ``created``/``finished`` inside each job entry are
        MONOTONIC-clock values (process-relative, not wall-clock timestamps) --
        passed through as-is, never rendered or converted here. Each job entry
        is normalized like ``flash_status``: canonical ``kind`` plus the
        additive ``loader`` field (``None`` except non-default bootstrap).
        """
        job_entries = [_normalize_job_payload(j) for j in jobs.jobs()]
        return json.dumps(
            {
                "consoles": consoles.sessions(),
                "forwards": forwards.sessions(),
                "jobs": job_entries,
            }
        )

    return mcp


def main(argv: Sequence[str] | None = None) -> None:
    """Console entrypoint.

    No subcommand (``argv`` empty, the shape every MCP client spawns) serves
    the stdio server exactly as before this dispatch was added -- zero
    behavior change on that path. ``demo`` instead boots a hardware-free local
    lab (``labgrid_mcp.demo``). ``argv`` defaults to ``sys.argv[1:]`` via
    ``argparse``; the parameter only exists so tests can drive routing without
    touching real process argv.
    """
    parser = argparse.ArgumentParser(prog="labgrid-mcp")
    subparsers = parser.add_subparsers(dest="command")
    demo_parser = subparsers.add_parser(
        "demo", help="Boot a hardware-free local lab (coordinator + exporter + fakes)."
    )
    demo_parser.add_argument(
        "--port",
        type=int,
        default=demo.DEMO_DEFAULT_PORT,
        help=f"coordinator port to listen on (default: {demo.DEMO_DEFAULT_PORT})",
    )
    args = parser.parse_args(argv)

    if args.command == "demo":
        try:
            demo.run_demo(args.port)
        except demo.DemoError as exc:
            print(f"labgrid-mcp demo: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        return

    _serve_stdio()


def _serve_stdio() -> None:
    """Load config, connect, and serve over stdio -- the real MCP entrypoint."""
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    client = CoordinatorClient(config)
    session = PlaceSession(client, config)
    targets = TargetManager(client, config)
    consoles = ConsoleRegistry(targets, client, config)
    jobs = JobRegistry(targets, client, config)
    forwards = ForwardRegistry(targets, client, config)
    server = build_server(config, client, session, targets, consoles, jobs, forwards)
    server.run()
