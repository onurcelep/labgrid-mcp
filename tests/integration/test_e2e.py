"""End-to-end verification against a real, locally-running labgrid-coordinator.

This is Phase 0's acceptance gate (DESIGN.md): the server is exercised over its
real stdio transport, talking to a real ``labgrid-coordinator`` subprocess over
gRPC. Nothing here is mocked. Excluded from the default run via the
``integration`` marker; run with ``uv run pytest -m integration``.
"""

from __future__ import annotations

import asyncio
import contextlib
import getpass
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Awaitable, Callable, Iterator
from contextlib import closing
from pathlib import Path
from typing import IO

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import TextContent, TextResourceContents
from pydantic import AnyUrl

from labgrid_mcp.config import Config
from labgrid_mcp.coordinator import CoordinatorClient

pytestmark = pytest.mark.integration

# Bounds kept generous but finite so a hung dependency fails loudly, never hangs.
_PORT_TIMEOUT_S = 15.0
_CONNECT_TIMEOUT_S = 20.0
_TERM_TIMEOUT_S = 10.0
# Per-await bound on each MCP protocol call. Without it, a spawned server that
# starts but never answers on stdio would hang the test coroutine forever --
# and a hung test never returns, so neither the coordinator fixture's finally
# nor stdio_client's __aexit__ would run: two orphans and a wedged pytest.
_RPC_TIMEOUT_S = 15.0

# The 7 FLASH-gated tools (DESIGN.md section 11.11 decision #5): opt-in only,
# excluded from the default env even without readonly. Shared between the
# default-env absence assertion and the allowlisted-presence assertion below.
_FLASH_TOOL_NAMES = {
    "flash_dfu",
    "flash_fastboot",
    "flash_script",
    "bootstrap",
    "write_image",
    "flash_status",
    "flash_logs",
}


def _free_port() -> int:
    """Reserve an ephemeral port and return it (closed before the caller binds)."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    """Block until ``host:port`` accepts a TCP connection, or fail after timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(1.0)
            try:
                sock.connect((host, port))
            except OSError:
                time.sleep(0.2)
                continue
            return
    raise AssertionError(f"coordinator never accepted connections on {host}:{port}")


async def _wait_connected(session: ClientSession, port: int) -> None:
    """Poll ``coordinator_info`` until the server reports ``connected: true``.

    The server serves even while its coordinator link is still coming up and
    reconnects in the background, so the first tool call after
    ``initialize()`` is not guaranteed to already be connected. Each poll is
    individually bounded by ``asyncio.wait_for`` so a hung call cannot turn
    the outer ``_CONNECT_TIMEOUT_S`` deadline into an unbounded wait.
    """
    deadline = time.monotonic() + _CONNECT_TIMEOUT_S
    info: dict[str, object] = {}
    while time.monotonic() < deadline:
        result = await asyncio.wait_for(
            session.call_tool("coordinator_info", {}), timeout=_RPC_TIMEOUT_S
        )
        assert result.isError is False
        assert result.structuredContent is not None
        info = result.structuredContent
        if info.get("connected") is True:
            break
        await asyncio.sleep(0.3)

    assert info.get("connected") is True, f"never connected: {info}"
    address = info.get("address")
    assert isinstance(address, str)
    assert address.endswith(f":{port}")


async def _reservation_tokens(session: ClientSession) -> set[str]:
    """Return the set of reservation tokens the coordinator currently holds."""
    result = await asyncio.wait_for(
        session.call_tool("list_reservations", {}), timeout=_RPC_TIMEOUT_S
    )
    assert result.isError is False
    assert result.structuredContent is not None
    reservations = result.structuredContent["reservations"]
    assert isinstance(reservations, list)
    return {
        r["token"]
        for r in reservations
        if isinstance(r, dict) and isinstance(r.get("token"), str)
    }


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]], *, timeout: float, desc: str
) -> None:
    """Poll ``predicate`` (each call individually bounded upstream) until true.

    Same "poll for an observable state, every await bounded" shape as
    ``_wait_connected`` -- no fixed sleeps standing in for real sequencing.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {desc}")


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Terminate a subprocess, escalating to kill; never leaves an orphan."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=_TERM_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_TERM_TIMEOUT_S)


@pytest.fixture
def coordinator(tmp_path: Path) -> Iterator[int]:
    """Start a real ``labgrid-coordinator`` on an ephemeral port; yield the port.

    Runs with ``cwd=tmp_path``: the coordinator persists places/resources to
    ``places.yaml``/``resources.yaml`` in its CWD (DESIGN.md section 11.8),
    and this fixture now exercises ``add_place``, so a throwaway directory
    keeps that file out of the repo working tree.
    """
    port = _free_port()
    proc = subprocess.Popen(
        ["labgrid-coordinator", "-l", f"127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=tmp_path,
    )
    try:
        _wait_for_port("127.0.0.1", port, _PORT_TIMEOUT_S)
        yield port
    finally:
        _terminate(proc)


async def test_end_to_end_against_real_coordinator(coordinator: int) -> None:
    """Drive the shipped server over stdio against a live coordinator.

    Proves the connected path: tool discovery, a ``coordinator_info`` call that
    reports ``connected: true`` for the real address, and a ``labgrid://places``
    read that yields a valid (empty, no exporter) JSON list. Then exercises the
    full Phase 1 read surface against the same bare (place-less) coordinator:
    every list tool/resource reports empty rather than erroring, an unknown
    ``show_place`` name surfaces as an error result, and ``list_reservations``
    -- a session-bound unary RPC -- succeeds, proving it rides the same
    handshaken channel as the streaming subscription.
    """
    port = coordinator
    params = StdioServerParameters(
        command="uv",
        args=["run", "labgrid-mcp"],
        env={"LG_COORDINATOR": f"127.0.0.1:{port}"},
    )

    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        # Every protocol await is bounded by asyncio.wait_for: a timeout raises
        # here, the test fails, and both context managers + the coordinator
        # fixture tear their subprocesses down normally.
        await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)

        tools = await asyncio.wait_for(session.list_tools(), timeout=_RPC_TIMEOUT_S)
        tool_names = {tool.name for tool in tools.tools}
        assert {
            "coordinator_info",
            "list_places",
            "show_place",
            "who",
            "list_resources",
            "list_reservations",
        } <= tool_names
        # FLASH is opt-in only (DESIGN.md section 11.11 decision #5): absent
        # from the default env even though every other gated category is on
        # -- this also covers write_image's additive partition/mode/skip/seek
        # options (§11.14), which are unit-tested only (SSHes to a real block
        # device; no hardware-free e2e chain exists, per Phase 5's own
        # boundary).
        assert tool_names.isdisjoint(_FLASH_TOOL_NAMES)
        # get_sd_mux (§11.14) is MUX, default-on -- registered but never
        # DRIVEN here (SD-only, needs a real usbsdmux device on the exporter
        # host); the get/set round trip against the coordinator client is
        # already covered by unit tests (test_server.py, test_target.py).
        assert "get_sd_mux" in tool_names

        await _wait_connected(session, port)

        resource = await asyncio.wait_for(
            session.read_resource(AnyUrl("labgrid://places")),
            timeout=_RPC_TIMEOUT_S,
        )
        assert len(resource.contents) == 1
        content = resource.contents[0]
        assert isinstance(content, TextResourceContents)
        places = json.loads(content.text)
        assert isinstance(places, list)

        # A bare coordinator (no exporters, no places file loaded) has an empty
        # lab: every read tool should report empty rather than erroring.
        list_places_result = await asyncio.wait_for(
            session.call_tool("list_places", {}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert list_places_result.isError is False
        assert list_places_result.structuredContent == {"places": []}

        who_result = await asyncio.wait_for(
            session.call_tool("who", {}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert who_result.isError is False
        assert who_result.structuredContent == {"acquisitions": []}

        list_resources_result = await asyncio.wait_for(
            session.call_tool("list_resources", {}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert list_resources_result.isError is False
        assert list_resources_result.structuredContent == {"resources": []}

        # list_reservations is a session-bound unary RPC (DESIGN.md section
        # 11.2): it only succeeds if the unary call rides the same handshaken
        # channel as the streaming subscription, so this is the real proof
        # that path works end-to-end against a live coordinator.
        list_reservations_result = await asyncio.wait_for(
            session.call_tool("list_reservations", {}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert list_reservations_result.isError is False
        assert list_reservations_result.structuredContent == {"reservations": []}

        show_place_error = await asyncio.wait_for(
            session.call_tool("show_place", {"name": "nope"}),
            timeout=_RPC_TIMEOUT_S,
        )
        # Exceptions do not propagate across the stdio transport: a tool error
        # surfaces as a CallToolResult with isError set, not a raised
        # exception, so assert on the structured result instead of pytest.raises.
        assert show_place_error.isError is True
        assert len(show_place_error.content) == 1
        error_content = show_place_error.content[0]
        assert isinstance(error_content, TextContent)
        assert "unknown place" in error_content.text

        resources_resource = await asyncio.wait_for(
            session.read_resource(AnyUrl("labgrid://resources")),
            timeout=_RPC_TIMEOUT_S,
        )
        assert len(resources_resource.contents) == 1
        resources_content = resources_resource.contents[0]
        assert isinstance(resources_content, TextResourceContents)
        assert json.loads(resources_content.text) == []

        reservations_resource = await asyncio.wait_for(
            session.read_resource(AnyUrl("labgrid://reservations")),
            timeout=_RPC_TIMEOUT_S,
        )
        assert len(reservations_resource.contents) == 1
        reservations_content = reservations_resource.contents[0]
        assert isinstance(reservations_content, TextResourceContents)
        assert json.loads(reservations_content.text) == []

        # labgrid://sessions is unconditional (registered even with FLASH/SSH
        # gated off): a bare coordinator with no console sessions, forward
        # tunnels, or flash jobs reports all three lists empty, in the
        # {"consoles", "forwards", "jobs"} shape (Phase 8 added "forwards"
        # additively).
        sessions_resource = await asyncio.wait_for(
            session.read_resource(AnyUrl("labgrid://sessions")),
            timeout=_RPC_TIMEOUT_S,
        )
        assert len(sessions_resource.contents) == 1
        sessions_content = sessions_resource.contents[0]
        assert isinstance(sessions_content, TextResourceContents)
        assert json.loads(sessions_content.text) == {
            "consoles": [],
            "forwards": [],
            "jobs": [],
        }


# Task 4: acquire_place + the rest of the Phase 2 surface, still hardware-free
# (DESIGN.md section 11.8(a): a bare place with zero resource matches can be
# acquired, so no exporter is needed).

_PLACE_NAME = "e2e-place"
_FIXTURE_HOSTNAME = "e2e-fixture"
_FIXTURE_USERNAME = "seeder"
_SERVER_HOSTNAME = "e2e-server"
_SERVER_USERNAME = "agent"


async def test_acquire_release_reserve_hardware_free(coordinator: int) -> None:
    """Exercise acquire/release/reserve/cancel and the foreign-owner release
    guard against a real coordinator, with no exporter/hardware involved.

    Fixture-seeding path: this test drives a *second* ``CoordinatorClient``
    directly from the test process (importing ``labgrid_mcp.coordinator``),
    connected to the same coordinator the MCP server subprocess uses, to
    ``add_place`` and -- for the foreign-owner scenario -- to acquire the
    place under an identity distinct from the server's. DESIGN.md section
    11.8 warns that two client *channels* to the same target sharing one
    process's gRPC subchannel pool trip the coordinator's
    ``assert peer not in self.clients`` with ``UNKNOWN``. That trap needs
    *two or more* channels inside the *same* process; here the test process
    opens exactly one extra channel (this fixture client), while the MCP
    server's own coordinator channel lives in its own subprocess with its own
    gRPC runtime -- so the two never share a subchannel pool. Verified
    empirically (this test passes as written): neither the
    ``grpc.use_local_subchannel_pool`` channel-option fix nor a
    ``labgrid-client`` subprocess fallback was needed. Distinct
    hostname/username for the fixture client vs. the server (set via
    ``LG_HOSTNAME``/``LG_USERNAME`` in the server's env below) is what makes
    the fixture-acquired place "foreign" from the server's point of view.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    fixture_identity = f"{_FIXTURE_HOSTNAME}/{_FIXTURE_USERNAME}"
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        await asyncio.wait_for(
            fixture_client.add_place(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            # Free place: acquire_place acquires it directly under the
            # server's own identity.
            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            show_result = await asyncio.wait_for(
                session.call_tool("show_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert show_result.isError is False
            assert show_result.structuredContent is not None
            assert show_result.structuredContent.get("acquired") == server_identity

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
            assert release_result.structuredContent is not None
            released_place = release_result.structuredContent["place"]
            assert isinstance(released_place, dict)
            assert released_place.get("acquired") is None

            # reserve -> token visible in list_reservations -> cancel removes it.
            reserve_result = await asyncio.wait_for(
                session.call_tool("reserve", {"filters": {"name": _PLACE_NAME}}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert reserve_result.isError is False
            assert reserve_result.structuredContent is not None
            reservation = reserve_result.structuredContent["reservation"]
            assert isinstance(reservation, dict)
            token = reservation.get("token")
            assert isinstance(token, str) and token

            list_reservations_result = await asyncio.wait_for(
                session.call_tool("list_reservations", {}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert list_reservations_result.isError is False
            assert list_reservations_result.structuredContent is not None
            reservations = list_reservations_result.structuredContent["reservations"]
            assert isinstance(reservations, list)
            tokens = {r.get("token") for r in reservations if isinstance(r, dict)}
            assert token in tokens

            cancel_result = await asyncio.wait_for(
                session.call_tool("cancel_reservation", {"token": token}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert cancel_result.isError is False

            list_reservations_after = await asyncio.wait_for(
                session.call_tool("list_reservations", {}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert list_reservations_after.isError is False
            assert list_reservations_after.structuredContent is not None
            reservations_after = list_reservations_after.structuredContent["reservations"]
            assert isinstance(reservations_after, list)
            tokens_after = {r.get("token") for r in reservations_after if isinstance(r, dict)}
            assert token not in tokens_after

            # Foreign-owned place: the fixture client (a distinct identity)
            # acquires it directly, bypassing the MCP session entirely.
            await asyncio.wait_for(
                fixture_client.acquire_place_rpc(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
            )

            # Deterministic sequencing: the fixture's acquire lands at the
            # coordinator, but the server's ownership guard reads its own
            # streamed snapshot -- wait until the hold is visible there, or
            # the refusal below races and reports "held by nobody".
            async def _fixture_hold_visible() -> bool:
                shown = await asyncio.wait_for(
                    session.call_tool("show_place", {"name": _PLACE_NAME}),
                    timeout=_RPC_TIMEOUT_S,
                )
                return (shown.structuredContent or {}).get("acquired") == fixture_identity

            await _wait_until(
                _fixture_hold_visible,
                timeout=_CONNECT_TIMEOUT_S,
                desc="fixture's hold to appear in the server snapshot",
            )

            release_foreign_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_foreign_result.isError is True
            assert len(release_foreign_result.content) == 1
            foreign_error_content = release_foreign_result.content[0]
            assert isinstance(foreign_error_content, TextContent)
            assert fixture_identity in foreign_error_content.text

            release_kick_result = await asyncio.wait_for(
                session.call_tool(
                    "release_place", {"name": _PLACE_NAME, "kick": True}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_kick_result.isError is False
            assert release_kick_result.structuredContent is not None
            kicked_place = release_kick_result.structuredContent["place"]
            assert isinstance(kicked_place, dict)
            assert kicked_place.get("acquired") is None
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


async def test_acquire_waits_for_reservation_then_allocates(coordinator: int) -> None:
    """Exercise the real reservation->allocate->acquire orchestration.

    The direct-acquire path is covered above; this drives the contended path
    end-to-end: the fixture client (a distinct identity) holds the place, so
    the server's ``acquire_place`` cannot acquire directly -- it must create a
    name-reservation and poll. Run as a background task, it stays pending until
    the fixture releases; the coordinator then allocates the place to the
    server's reservation, the poll observes it, and the server acquires. We
    assert the final owner is the SERVER's identity and that the server's
    cancel-after-acquire leaves no reservation lingering.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        await asyncio.wait_for(
            fixture_client.add_place(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        # Fixture holds the place under its own identity, so the server's
        # acquire must go through reserve->poll rather than a direct acquire.
        await asyncio.wait_for(
            fixture_client.acquire_place_rpc(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
                # Generous timeout so the pending acquire comfortably outlives
                # the fixture's brief hold below and never times out first.
                "LABGRID_MCP_ACQUIRE_TIMEOUT": "30",
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            # Kick off acquire_place in the background: it blocks in its
            # reserve->poll loop while the fixture still holds the place. The
            # MCP session multiplexes request ids, so list_reservations calls
            # below are serviced concurrently by the same server event loop.
            acquire_task = asyncio.create_task(
                session.call_tool("acquire_place", {"name": _PLACE_NAME})
            )
            try:
                # Deterministic sequencing: wait until the server's reservation
                # is observable at the coordinator -- proof the acquire really
                # entered the reserve->poll path -- before releasing.
                async def _server_reservation_present() -> bool:
                    return len(await _reservation_tokens(session)) >= 1

                await _wait_until(
                    _server_reservation_present,
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="server's name-reservation to appear",
                )
                # Still held by the fixture: the acquire must not have resolved.
                assert not acquire_task.done()

                # Release from the fixture; the coordinator now allocates the
                # place to the server's pending reservation.
                await asyncio.wait_for(
                    fixture_client.release_place_rpc(_PLACE_NAME),
                    timeout=_RPC_TIMEOUT_S,
                )

                acquire_result = await asyncio.wait_for(
                    acquire_task, timeout=_RPC_TIMEOUT_S
                )
            finally:
                acquire_task.cancel()
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await acquire_task

            # The pending acquire allocated + acquired under the SERVER's
            # identity -- not the fixture's.
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            # cancel-after-acquire: the server's reservation must be gone.
            async def _reservations_empty() -> bool:
                return len(await _reservation_tokens(session)) == 0

            await _wait_until(
                _reservations_empty,
                timeout=_CONNECT_TIMEOUT_S,
                desc="server's reservation to be cancelled after acquire",
            )

            # Clean teardown: release the place we now hold.
            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


# Task 3 (Phase 3): power driver e2e against a fake HTTP power switch, still
# hardware-free (DESIGN.md section 11.9's appendix: exporter YAML + fake
# switch, reused verbatim below). Proves the full driver chain -- real
# exporter export, TargetManager's ClientSession-free Target build, real
# NetworkPowerDriver bind+activate -- end to end. Task 2 (Phase 6) extends it
# with the io half (HttpDigitalOutputDriver), deferred since Phase 3.

_POWER_PLACE_NAME = "e2e-power-place"
_EXPORTER_NAME = "e2e-exporter"
_EXPORTER_GROUP = "powerplace"
_POWER_MATCH_PATTERN = f"{_EXPORTER_NAME}/{_EXPORTER_GROUP}/NetworkPowerPort"
# Task 2 (Phase 6): the io e2e reuses this same place/exporter/group -- a
# static HttpDigitalOutput lives right next to the NetworkPowerPort (DESIGN
# §11.9's io+mux paragraph, now PROVEN).
_IO_MATCH_PATTERN = f"{_EXPORTER_NAME}/{_EXPORTER_GROUP}/HttpDigitalOutput"

# Extends DESIGN.md section 11.9's appendix ("fake `rest` switch") with a
# SECOND endpoint for the io e2e (Task 2, Phase 6): `_idx` now keys state by
# the WHOLE stripped request path (e.g. "relay/0/value" / "io/0/value")
# instead of just the second segment, so the power path (`/relay/...`) and
# the io path (`/io/...`) never collide in the shared STATE dict. Same
# GET->"0"/"1", PUT-sets-body behaviour as the appendix for both.
_FAKE_SWITCH_SRC = '''\
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
STATE = {}
class H(BaseHTTPRequestHandler):
    def _idx(self):
        return self.path.strip("/")
    def do_GET(self):
        v = b"1" if STATE.get(self._idx(), False) else b"0"
        self.send_response(200); self.send_header("Content-Length", str(len(v)))
        self.end_headers(); self.wfile.write(v)
    def do_PUT(self):
        n = int(self.headers.get("Content-Length", 0))
        STATE[self._idx()] = self.rfile.read(n).strip() == b"1"
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
'''


def _spawn_logged(
    args: list[str], *, cwd: Path, log_path: Path
) -> tuple[subprocess.Popen[bytes], IO[bytes]]:
    """Start a subprocess with combined stdout/stderr captured to ``log_path``.

    The child inherits the fd directly, so the file on disk is readable at
    any time (not just after the handle is closed) -- useful for diagnosing
    an exporter/switch that never comes up (escalation path: dump both logs).
    The returned handle must be closed by the caller once the process is torn
    down.
    """
    log = log_path.open("wb")
    proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd=cwd)
    return proc, log


def _read_log(path: Path) -> str:
    return path.read_bytes().decode(errors="replace") if path.exists() else "<no log>"


def _switch_value(port: int, path: str = "relay/0/value") -> str:
    """GET the fake switch's own recorded state directly (not via MCP).

    This is the independent check that the driver chain really flipped a
    real (fake) relay/output, not just that the tool call returned a
    plausible dict. ``path`` defaults to the power relay's endpoint; the io
    e2e passes ``"io/0/value"`` (Task 2, Phase 6).
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/{path}", timeout=5.0) as resp:
        body: bytes = resp.read()
        return body.decode().strip()


async def test_power_and_io_via_fake_switch(coordinator: int, tmp_path: Path) -> None:
    """Full power AND io driver chains, zero hardware (DESIGN.md section 11.9
    appendix; the io half closes Task 2 (Phase 6)'s deferred-since-P3 follow-up).

    Real coordinator + a real ``uv run labgrid-exporter`` exporting a
    ``NetworkPowerPort`` (model ``rest``) AND a static ``HttpDigitalOutput``
    (§11.9's SECOND no-hardware-capable path) side by side in the same place,
    against a real threaded-HTTP fake switch with two independent endpoints
    (the exact power YAML/server code proven live in section 11.9's appendix,
    now extended with the io endpoint), driven end to end through the MCP
    server's gated tools:

    - Power: acquire -> ``set_power("on")`` -> ``{"power": true}`` AND the
      switch's own recorded relay state is on -> ``set_power("off")`` ->
      ``false``, switch off too -> ``get_power_state`` consistent.
    - Io: ``get_io`` -> ``false`` (switch's io endpoint starts de-asserted) ->
      ``set_io(True)`` -> ``{"value": true}`` AND the switch's own recorded io
      state is on -> ``set_io(False)`` -> ``false``, switch off too -> a
      second ``get_io`` consistent.
    - release. Also proves the ownership gate: calling a driver tool on a
      place this server no longer holds surfaces "not acquired by this
      server", not a crash.

    Mux tools are asserted *registered* (default env enables all three driver
    categories) but never *driven* here: sd-mux/usb-mux are hardware-only
    (DESIGN section 11.9 -- their resources SSH to the exporter host to run a
    CLI there).
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    switch_port = _free_port()
    switch_script = tmp_path / "fake_switch.py"
    switch_script.write_text(_FAKE_SWITCH_SRC)
    switch_log = tmp_path / "switch.log"
    switch_proc, switch_handle = _spawn_logged(
        [sys.executable, str(switch_script), str(switch_port)],
        cwd=tmp_path,
        log_path=switch_log,
    )

    # DESIGN.md section 11.9 appendix YAML verbatim (NetworkPowerPort), only
    # the switch's ephemeral port is substituted -- ``{index}`` must survive
    # as a literal (the ``rest`` backend does ``host.format(index=...)``
    # itself), hence the doubled braces in this f-string. The HttpDigitalOutput
    # block is the Task 2 (Phase 6) addition -- a genuinely different YAML
    # shape (url/body_asserted/body_deasserted vs model/host/index), added to
    # the DESIGN §11.9 appendix. Both are plain ``Resource`` subclasses (never
    # ``NetworkResource``), so the exporter exports each statically -- no
    # udev, no ser2net, ``avail=True`` immediately, the same trick that makes
    # the power path hardware-free.
    exporter_yaml = tmp_path / "exporter.yaml"
    exporter_yaml.write_text(
        f"""\
{_EXPORTER_GROUP}:
  NetworkPowerPort:
    model: rest
    host: 'http://127.0.0.1:{switch_port}/relay/{{index}}/value'
    index: 0
  HttpDigitalOutput:
    url: 'http://127.0.0.1:{switch_port}/io/0/value'
    body_asserted: '1'
    body_deasserted: '0'
"""
    )
    exporter_log = tmp_path / "exporter.log"
    exporter_proc, exporter_handle = _spawn_logged(
        [
            "uv",
            "run",
            "labgrid-exporter",
            "-n",
            _EXPORTER_NAME,
            "-c",
            f"127.0.0.1:{port}",
            str(exporter_yaml),
        ],
        cwd=tmp_path,
        log_path=exporter_log,
    )

    try:
        _wait_for_port("127.0.0.1", switch_port, _PORT_TIMEOUT_S)

        await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
        await asyncio.wait_for(
            fixture_client.add_place(_POWER_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_POWER_PLACE_NAME, _POWER_MATCH_PATTERN),
            timeout=_RPC_TIMEOUT_S,
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_POWER_PLACE_NAME, _IO_MATCH_PATTERN),
            timeout=_RPC_TIMEOUT_S,
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            # Io tools are driven below; mux tools are registered under the
            # default env but never driven (hardware-only, see docstring).
            tools = await asyncio.wait_for(session.list_tools(), timeout=_RPC_TIMEOUT_S)
            tool_names = {tool.name for tool in tools.tools}
            assert {
                "get_power_state",
                "set_power",
                "get_io",
                "set_io",
                "set_sd_mux",
                "set_usb_mux",
            } <= tool_names

            async def _resource_registered(cls: str) -> bool:
                result = await asyncio.wait_for(
                    session.call_tool("list_resources", {}), timeout=_RPC_TIMEOUT_S
                )
                assert result.isError is False
                assert result.structuredContent is not None
                resources = result.structuredContent["resources"]
                assert isinstance(resources, list)
                return any(
                    isinstance(r, dict)
                    and r.get("exporter") == _EXPORTER_NAME
                    and r.get("group") == _EXPORTER_GROUP
                    and r.get("cls") == cls
                    for r in resources
                )

            try:
                await _wait_until(
                    lambda: _resource_registered("NetworkPowerPort"),
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register NetworkPowerPort",
                )
                await _wait_until(
                    lambda: _resource_registered("HttpDigitalOutput"),
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register HttpDigitalOutput",
                )
            except AssertionError as exc:
                raise AssertionError(
                    f"{exc}\n--- exporter log ---\n{_read_log(exporter_log)}\n"
                    f"--- fake switch log ---\n{_read_log(switch_log)}"
                ) from exc

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            set_on_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power", {"place": _POWER_PLACE_NAME, "action": "on"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_on_result.isError is False
            assert set_on_result.structuredContent is not None
            assert set_on_result.structuredContent.get("power") is True
            # Independent proof the real (fake) relay flipped, not just that
            # the tool call returned a plausible dict.
            assert _switch_value(switch_port) == "1"

            set_off_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power", {"place": _POWER_PLACE_NAME, "action": "off"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_off_result.isError is False
            assert set_off_result.structuredContent is not None
            assert set_off_result.structuredContent.get("power") is False
            assert _switch_value(switch_port) == "0"

            get_state_result = await asyncio.wait_for(
                session.call_tool("get_power_state", {"place": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert get_state_result.isError is False
            assert get_state_result.structuredContent is not None
            assert get_state_result.structuredContent.get("power") is False

            # Io half (Task 2, Phase 6): the fake switch's "io/0/value"
            # endpoint is the independent oracle, entirely separate from the
            # power relay's own state above.
            get_io_result = await asyncio.wait_for(
                session.call_tool("get_io", {"place": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert get_io_result.isError is False
            assert get_io_result.structuredContent is not None
            assert get_io_result.structuredContent.get("value") is False
            assert _switch_value(switch_port, "io/0/value") == "0"

            set_io_on_result = await asyncio.wait_for(
                session.call_tool(
                    "set_io", {"place": _POWER_PLACE_NAME, "value": True}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_io_on_result.isError is False
            assert set_io_on_result.structuredContent is not None
            assert set_io_on_result.structuredContent.get("value") is True
            # Independent proof the real (fake) output flipped, not just that
            # the tool call returned a plausible dict.
            assert _switch_value(switch_port, "io/0/value") == "1"

            set_io_off_result = await asyncio.wait_for(
                session.call_tool(
                    "set_io", {"place": _POWER_PLACE_NAME, "value": False}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_io_off_result.isError is False
            assert set_io_off_result.structuredContent is not None
            assert set_io_off_result.structuredContent.get("value") is False
            assert _switch_value(switch_port, "io/0/value") == "0"

            get_io_after_result = await asyncio.wait_for(
                session.call_tool("get_io", {"place": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert get_io_after_result.isError is False
            assert get_io_after_result.structuredContent is not None
            assert get_io_after_result.structuredContent.get("value") is False

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
            assert release_result.structuredContent is not None
            released_place = release_result.structuredContent["place"]
            assert isinstance(released_place, dict)
            assert released_place.get("acquired") is None

            # Ownership gate: a driver tool called on a place this server no
            # longer holds must be rejected with a clear message, not crash
            # or silently reach the (now stale) cached driver.
            not_acquired_result = await asyncio.wait_for(
                session.call_tool("get_power_state", {"place": _POWER_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert not_acquired_result.isError is True
            assert len(not_acquired_result.content) == 1
            not_acquired_content = not_acquired_result.content[0]
            assert isinstance(not_acquired_content, TextContent)
            assert "not acquired by this server" in not_acquired_content.text
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)
        _terminate(exporter_proc)
        exporter_handle.close()
        _terminate(switch_proc)
        switch_handle.close()


# Task 3 (Phase 9): per-resource NAME selection e2e (DESIGN.md section 11.14 +
# its appendix's two-same-class exporter recipe, reused verbatim). A place
# with TWO NetworkPowerPort resources is otherwise unusable (labgrid's raw
# get_resource raises "multiple resources matching"); resource_name is the
# only way to drive it. Each named resource points at a DISTINCT endpoint on
# the fake switch above (already keyed by the whole stripped request path, so
# relay/0/value and relay/1/value never collide) -- an independent oracle per
# name, not just a plausible per-call dict.

_NAME_PLACE_NAME = "e2e-name-place"
_NAME_EXPORTER_NAME = "e2e-name-exporter"
_NAME_GROUP = "e2e-name-group"
_NAME_A = "power_a"
_NAME_B = "power_b"
_NAME_MATCH_A = f"{_NAME_EXPORTER_NAME}/{_NAME_GROUP}/NetworkPowerPort/{_NAME_A}"
_NAME_MATCH_B = f"{_NAME_EXPORTER_NAME}/{_NAME_GROUP}/NetworkPowerPort/{_NAME_B}"


async def test_power_resource_name_selects_independent_endpoints(
    coordinator: int, tmp_path: Path
) -> None:
    """Two named ``NetworkPowerPort``s on one place, driven via ``resource_name``.

    Real coordinator + a real exporter statically exporting TWO
    ``NetworkPowerPort``s in one group via the explicit ``cls:`` / two-entry
    recipe (DESIGN.md section 11.14 appendix) against the fake switch's two
    independent relay endpoints:

    - No ``resource_name`` on this 2-resource place -> ``set_power`` is a tool
      error naming BOTH candidates (never labgrid's raw "multiple resources
      matching").
    - ``resource_name="power_a"`` flips ONLY the ``relay/0/value`` endpoint;
      ``relay/1/value`` (b) is untouched -- proven via the switch's own
      recorded state, not just the tool's return value.
    - ``resource_name="power_b"`` flips ONLY ``relay/1/value``; a's state
      (still on from the step above) is untouched.
    - ``get_power_state``/``set_power`` off with a name round-trip correctly
      without disturbing the other resource.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    switch_port = _free_port()
    switch_script = tmp_path / "fake_switch.py"
    switch_script.write_text(_FAKE_SWITCH_SRC)
    switch_log = tmp_path / "switch.log"
    switch_proc, switch_handle = _spawn_logged(
        [sys.executable, str(switch_script), str(switch_port)],
        cwd=tmp_path,
        log_path=switch_log,
    )

    # DESIGN.md section 11.14 appendix's two-same-class recipe, verbatim
    # shape: an explicit ``cls:`` under an arbitrary key makes that key the
    # resource's NAME (exporter.py:865), unlike the bare-class-key form used
    # by the single-port place above (which yields an unnamed/"default"
    # resource).
    exporter_yaml = tmp_path / "exporter.yaml"
    exporter_yaml.write_text(
        f"""\
{_NAME_GROUP}:
  {_NAME_A}:
    cls: NetworkPowerPort
    model: rest
    host: 'http://127.0.0.1:{switch_port}/relay/{{index}}/value'
    index: 0
  {_NAME_B}:
    cls: NetworkPowerPort
    model: rest
    host: 'http://127.0.0.1:{switch_port}/relay/{{index}}/value'
    index: 1
"""
    )
    exporter_log = tmp_path / "exporter.log"
    exporter_proc, exporter_handle = _spawn_logged(
        [
            "uv",
            "run",
            "labgrid-exporter",
            "-n",
            _NAME_EXPORTER_NAME,
            "-c",
            f"127.0.0.1:{port}",
            str(exporter_yaml),
        ],
        cwd=tmp_path,
        log_path=exporter_log,
    )

    try:
        _wait_for_port("127.0.0.1", switch_port, _PORT_TIMEOUT_S)

        await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
        await asyncio.wait_for(
            fixture_client.add_place(_NAME_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_NAME_PLACE_NAME, _NAME_MATCH_A),
            timeout=_RPC_TIMEOUT_S,
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_NAME_PLACE_NAME, _NAME_MATCH_B),
            timeout=_RPC_TIMEOUT_S,
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            async def _both_named_ports_registered() -> bool:
                result = await asyncio.wait_for(
                    session.call_tool("list_resources", {}), timeout=_RPC_TIMEOUT_S
                )
                assert result.isError is False
                assert result.structuredContent is not None
                resources = result.structuredContent["resources"]
                assert isinstance(resources, list)
                names = {
                    r.get("name")
                    for r in resources
                    if isinstance(r, dict)
                    and r.get("exporter") == _NAME_EXPORTER_NAME
                    and r.get("cls") == "NetworkPowerPort"
                }
                return {_NAME_A, _NAME_B} <= names

            try:
                await _wait_until(
                    _both_named_ports_registered,
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register both named NetworkPowerPorts",
                )
            except AssertionError as exc:
                raise AssertionError(
                    f"{exc}\n--- exporter log ---\n{_read_log(exporter_log)}\n"
                    f"--- fake switch log ---\n{_read_log(switch_log)}"
                ) from exc

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _NAME_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            # No resource_name on a place with two candidates: a tool error
            # naming BOTH, never labgrid's raw "multiple resources matching".
            ambiguous_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power", {"place": _NAME_PLACE_NAME, "action": "on"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert ambiguous_result.isError is True
            assert len(ambiguous_result.content) == 1
            ambiguous_content = ambiguous_result.content[0]
            assert isinstance(ambiguous_content, TextContent)
            assert "multiple power resources" in ambiguous_content.text
            assert _NAME_A in ambiguous_content.text
            assert _NAME_B in ambiguous_content.text
            assert "pass resource_name" in ambiguous_content.text

            # resource_name="power_a" flips ONLY endpoint a (relay/0/value).
            set_a_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power",
                    {"place": _NAME_PLACE_NAME, "action": "on", "resource_name": _NAME_A},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_a_result.isError is False
            assert set_a_result.structuredContent is not None
            assert set_a_result.structuredContent.get("power") is True
            assert set_a_result.structuredContent.get("resource_name") == _NAME_A
            assert _switch_value(switch_port, "relay/0/value") == "1"
            assert _switch_value(switch_port, "relay/1/value") == "0"

            # resource_name="power_b" flips ONLY endpoint b (relay/1/value); a
            # (still on from above) is untouched.
            set_b_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power",
                    {"place": _NAME_PLACE_NAME, "action": "on", "resource_name": _NAME_B},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_b_result.isError is False
            assert set_b_result.structuredContent is not None
            assert set_b_result.structuredContent.get("power") is True
            assert set_b_result.structuredContent.get("resource_name") == _NAME_B
            assert _switch_value(switch_port, "relay/0/value") == "1"
            assert _switch_value(switch_port, "relay/1/value") == "1"

            get_a_result = await asyncio.wait_for(
                session.call_tool(
                    "get_power_state",
                    {"place": _NAME_PLACE_NAME, "resource_name": _NAME_A},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert get_a_result.isError is False
            assert get_a_result.structuredContent is not None
            assert get_a_result.structuredContent.get("power") is True

            # Turning a off must not disturb b.
            set_a_off_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power",
                    {"place": _NAME_PLACE_NAME, "action": "off", "resource_name": _NAME_A},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert set_a_off_result.isError is False
            assert set_a_off_result.structuredContent is not None
            assert set_a_off_result.structuredContent.get("power") is False
            assert _switch_value(switch_port, "relay/0/value") == "0"
            assert _switch_value(switch_port, "relay/1/value") == "1"

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _NAME_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)
        _terminate(exporter_proc)
        exporter_handle.close()
        _terminate(switch_proc)
        switch_handle.close()


# Task 3 (Phase 9): power cycle delay e2e (DESIGN.md section 11.14):
# ``NetworkPowerDriver.cycle()`` takes no argument -- ``delay`` is set as an
# instance attr on the driver BEFORE calling cycle() -- so this proves the
# attr assignment actually took effect (an ignored delay would cycle near-
# instantly) rather than just that the tool call returned a plausible dict.

_CYCLE_PLACE_NAME = "e2e-cycle-place"
_CYCLE_EXPORTER_NAME = "e2e-cycle-exporter"
_CYCLE_GROUP = "e2e-cycle-group"
_CYCLE_MATCH_PATTERN = f"{_CYCLE_EXPORTER_NAME}/{_CYCLE_GROUP}/NetworkPowerPort"

# Generous lower bound on the observed off->on gap: absorbs scheduler/process
# jitter without weakening the assertion that delay=1.0 was actually honored
# (an ignored/zeroed delay would complete in a few milliseconds, nowhere close
# to this bound).
_CYCLE_MIN_ELAPSED_S = 0.9


async def test_power_cycle_delay_e2e(coordinator: int, tmp_path: Path) -> None:
    """``set_power(place, "cycle", delay=1.0)`` on a single-port place.

    Real coordinator + a real exporter statically exporting a single
    ``NetworkPowerPort`` (the appendix's fake ``rest`` switch, reused
    verbatim). Also proves ``delay``'s validation: passing it with a
    non-cycle action is a pinned tool error, raised before any driver call.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)

    switch_port = _free_port()
    switch_script = tmp_path / "fake_switch.py"
    switch_script.write_text(_FAKE_SWITCH_SRC)
    switch_log = tmp_path / "switch.log"
    switch_proc, switch_handle = _spawn_logged(
        [sys.executable, str(switch_script), str(switch_port)],
        cwd=tmp_path,
        log_path=switch_log,
    )

    exporter_yaml = tmp_path / "exporter.yaml"
    exporter_yaml.write_text(
        f"""\
{_CYCLE_GROUP}:
  NetworkPowerPort:
    model: rest
    host: 'http://127.0.0.1:{switch_port}/relay/{{index}}/value'
    index: 0
"""
    )
    exporter_log = tmp_path / "exporter.log"
    exporter_proc, exporter_handle = _spawn_logged(
        [
            "uv",
            "run",
            "labgrid-exporter",
            "-n",
            _CYCLE_EXPORTER_NAME,
            "-c",
            f"127.0.0.1:{port}",
            str(exporter_yaml),
        ],
        cwd=tmp_path,
        log_path=exporter_log,
    )

    try:
        _wait_for_port("127.0.0.1", switch_port, _PORT_TIMEOUT_S)

        await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
        await asyncio.wait_for(
            fixture_client.add_place(_CYCLE_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_CYCLE_PLACE_NAME, _CYCLE_MATCH_PATTERN),
            timeout=_RPC_TIMEOUT_S,
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            async def _power_port_registered() -> bool:
                result = await asyncio.wait_for(
                    session.call_tool("list_resources", {}), timeout=_RPC_TIMEOUT_S
                )
                assert result.isError is False
                assert result.structuredContent is not None
                resources = result.structuredContent["resources"]
                assert isinstance(resources, list)
                return any(
                    isinstance(r, dict)
                    and r.get("exporter") == _CYCLE_EXPORTER_NAME
                    and r.get("cls") == "NetworkPowerPort"
                    for r in resources
                )

            try:
                await _wait_until(
                    _power_port_registered,
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register NetworkPowerPort",
                )
            except AssertionError as exc:
                raise AssertionError(
                    f"{exc}\n--- exporter log ---\n{_read_log(exporter_log)}\n"
                    f"--- fake switch log ---\n{_read_log(switch_log)}"
                ) from exc

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _CYCLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False

            # delay only means anything for action="cycle" -- pinned ToolError
            # before ownership/any driver call.
            bad_delay_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power",
                    {"place": _CYCLE_PLACE_NAME, "action": "on", "delay": 1.0},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert bad_delay_result.isError is True
            assert len(bad_delay_result.content) == 1
            bad_delay_content = bad_delay_result.content[0]
            assert isinstance(bad_delay_content, TextContent)
            assert "delay is only valid with action='cycle'" in bad_delay_content.text

            start = time.monotonic()
            cycle_result = await asyncio.wait_for(
                session.call_tool(
                    "set_power",
                    {"place": _CYCLE_PLACE_NAME, "action": "cycle", "delay": 1.0},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            elapsed = time.monotonic() - start
            assert cycle_result.isError is False
            assert cycle_result.structuredContent is not None
            assert cycle_result.structuredContent.get("power") is True
            # Independent oracle: the real (fake) relay ends up on.
            assert _switch_value(switch_port) == "1"
            assert elapsed >= _CYCLE_MIN_ELAPSED_S, (
                f"cycle with delay=1.0 completed suspiciously fast ({elapsed:.3f}s) "
                "-- the driver's delay attr may not have been honored"
            )

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _CYCLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)
        _terminate(exporter_proc)
        exporter_handle.close()
        _terminate(switch_proc)
        switch_handle.close()


# Task 3 (Phase 9): release_from + reservation_wait e2e (DESIGN.md section
# 11.14), both hardware-free -- no exporter needed (a bare place with zero
# resource matches can be acquired/reserved, §11.8(a)).


async def test_release_from_hardware_free(coordinator: int) -> None:
    """``release_from`` against a real coordinator: matching vs. mismatched
    identity.

    The coordinator's ``ReleasePlace`` does NO format validation on
    ``fromuser`` and a mismatch is a SILENT no-op that still reports success
    (§11.8(b)/§11.14) -- so the wrong-identity case is only provable by
    reading the place back afterward (``show_place``), not by trusting the
    tool's own return value alone.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    fixture_identity = f"{_FIXTURE_HOSTNAME}/{_FIXTURE_USERNAME}"

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        await asyncio.wait_for(
            fixture_client.add_place(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            # The fixture client acquires DIRECTLY under its own identity,
            # bypassing the MCP session entirely (mirrors the foreign-owner
            # pattern used by the acquire/release e2e above).
            await asyncio.wait_for(
                fixture_client.acquire_place_rpc(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
            )

            # Deterministic sequencing: release_from's was_held readback and
            # the show_place assertions below read the server's streamed
            # snapshot -- wait until the fixture's acquire is visible there.
            async def _hold_visible() -> bool:
                shown = await asyncio.wait_for(
                    session.call_tool("show_place", {"name": _PLACE_NAME}),
                    timeout=_RPC_TIMEOUT_S,
                )
                return (shown.structuredContent or {}).get("acquired") == fixture_identity

            await _wait_until(
                _hold_visible,
                timeout=_CONNECT_TIMEOUT_S,
                desc="fixture's hold to appear in the server snapshot",
            )

            # Wrong identity: a silent coordinator-side no-op -- released
            # False, the place stays held by its REAL owner.
            wrong_result = await asyncio.wait_for(
                session.call_tool(
                    "release_from",
                    {"place": _PLACE_NAME, "host": "wronghost", "user": "wronguser"},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert wrong_result.isError is False
            assert wrong_result.structuredContent == {
                "place": _PLACE_NAME,
                "released_from": "wronghost/wronguser",
                "released": False,
            }
            show_still_held = await asyncio.wait_for(
                session.call_tool("show_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert show_still_held.isError is False
            assert show_still_held.structuredContent is not None
            assert show_still_held.structuredContent.get("acquired") == fixture_identity

            # Matching identity: actually releases.
            correct_result = await asyncio.wait_for(
                session.call_tool(
                    "release_from",
                    {
                        "place": _PLACE_NAME,
                        "host": _FIXTURE_HOSTNAME,
                        "user": _FIXTURE_USERNAME,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert correct_result.isError is False
            assert correct_result.structuredContent == {
                "place": _PLACE_NAME,
                "released_from": fixture_identity,
                "released": True,
            }
            show_free = await asyncio.wait_for(
                session.call_tool("show_place", {"name": _PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert show_free.isError is False
            assert show_free.structuredContent is not None
            assert show_free.structuredContent.get("acquired") is None
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


async def test_reservation_wait_allocates_free_place(coordinator: int) -> None:
    """``reserve`` + ``reservation_wait`` on a free place -> allocated, changed.

    A free place allocates its reservation immediately (§11.8(a)/session.py's
    ``reservation_wait`` docstring), so this resolves on the FIRST poll --
    bounded well within ``_RPC_TIMEOUT_S``. Cleans up via
    ``cancel_reservation`` since an allocated-but-never-acquired reservation
    is not auto-refreshed and would otherwise just expire ~60s later
    (§11.14) -- an explicit cancel keeps the test's teardown crisp instead of
    relying on that timer.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        await asyncio.wait_for(
            fixture_client.add_place(_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            reserve_result = await asyncio.wait_for(
                session.call_tool("reserve", {"filters": {"name": _PLACE_NAME}}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert reserve_result.isError is False
            assert reserve_result.structuredContent is not None
            reservation = reserve_result.structuredContent["reservation"]
            assert isinstance(reservation, dict)
            token = reservation.get("token")
            assert isinstance(token, str) and token

            wait_result = await asyncio.wait_for(
                session.call_tool("reservation_wait", {"token": token}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert wait_result.isError is False
            assert wait_result.structuredContent is not None
            assert wait_result.structuredContent.get("state") == "allocated"
            assert wait_result.structuredContent.get("changed") is True

            cancel_result = await asyncio.wait_for(
                session.call_tool("cancel_reservation", {"token": token}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert cancel_result.isError is False
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


# Task 3 (Phase 4): console e2e against a fake TCP serial bridge, still
# hardware-free (DESIGN.md section 11.10's appendix: exporter YAML + bridge,
# reused faithfully). Proves the full console chain -- real exporter export of
# a static NetworkSerialPort (protocol: raw, no ser2net/pty needed), the
# session registry's reader thread pumping the driver's undecorated _read,
# console_send writing via the undecorated _write, the labgrid://sessions
# resource, and the auto-close-on-release path -- end to end.

_CONSOLE_PLACE_NAME = "e2e-console-place"
_CONSOLE_EXPORTER_NAME = "e2e-console-exporter"
_CONSOLE_GROUP = "consolegrp"
_CONSOLE_RESOURCE_NAME = "serial0"
_CONSOLE_MATCH_PATTERN = (
    f"{_CONSOLE_EXPORTER_NAME}/{_CONSOLE_GROUP}/NetworkSerialPort/{_CONSOLE_RESOURCE_NAME}"
)

# DESIGN.md section 11.10 appendix's fake TCP console bridge, extended with an
# independent oracle: every chunk it receives is ALSO appended to a log file
# on disk (flushed immediately), so the test can confirm the bridge actually
# received the bytes without relying on the echo/console_read round trip at
# all. The echo/banner behaviour itself is verbatim from the appendix.
_BRIDGE_SRC = '''\
import socket, sys, threading

def _serve_conn(conn, recv_log_path):
    conn.sendall(b"BOOT-BANNER labgrid-mcp phase4\\r\\n")   # unsolicited output
    buf = b""
    while True:
        data = conn.recv(4096)
        if not data:
            return
        with open(recv_log_path, "ab") as f:
            f.write(data)
            f.flush()
        buf += data
        while b"\\n" in buf:
            line, buf = buf.split(b"\\n", 1)
            conn.sendall(b"echo:" + line.rstrip(b"\\r") + b"\\r\\n")

def main(port, recv_log_path):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port)); srv.listen(8)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=_serve_conn, args=(conn, recv_log_path), daemon=True).start()

if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])
'''


async def _drain_console_until(
    session: ClientSession, session_id: str, needle: str, *, timeout: float
) -> tuple[str, bool]:
    """Poll ``console_read`` (bounded), accumulating drained text.

    ``console_read`` consumes the ring buffer, so a chunk containing
    ``needle`` may arrive split across polls (banner + echo are separate
    writes) -- accumulate across calls rather than asserting on any single
    read's ``data``. Returns the accumulated text and whether any poll ever
    reported ``truncated``.
    """
    deadline = time.monotonic() + timeout
    buf = ""
    truncated_any = False
    while time.monotonic() < deadline:
        result = await asyncio.wait_for(
            session.call_tool("console_read", {"session": session_id}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert result.isError is False
        assert result.structuredContent is not None
        data = result.structuredContent.get("data")
        assert isinstance(data, str)
        buf += data
        if result.structuredContent.get("truncated"):
            truncated_any = True
        if needle in buf:
            return buf, truncated_any
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {needle!r} in console output; got {buf!r}")


async def _sessions_snapshot(session: ClientSession) -> dict[str, object]:
    """Read ``labgrid://sessions``: ``{"consoles": [...], "forwards": [...], "jobs": [...]}``."""
    resource = await asyncio.wait_for(
        session.read_resource(AnyUrl("labgrid://sessions")),
        timeout=_RPC_TIMEOUT_S,
    )
    assert len(resource.contents) == 1
    content = resource.contents[0]
    assert isinstance(content, TextResourceContents)
    sessions = json.loads(content.text)
    assert isinstance(sessions, dict)
    return sessions


async def test_console_via_tcp_bridge(coordinator: int, tmp_path: Path) -> None:
    """Full console-session chain, zero hardware (DESIGN.md section 11.10 appendix).

    Real coordinator + a real ``uv run labgrid-exporter`` exporting a static
    ``NetworkSerialPort`` (``protocol: raw`` -- the default ``rfc2217`` has no
    server here) against a real fake TCP bridge (the appendix's ~30-line
    server, extended with a side-channel recv log for an independent oracle).
    Driven end to end through the MCP server's gated console tools:
    acquire -> ``console_open`` -> handle; ``console_send("hello\\n")`` ->
    the bridge's recv log independently proves the bytes arrived (no
    dependency on the echo path) -> poll ``console_read`` until the echoed
    line surfaces, ``truncated`` false -> ``labgrid://sessions`` lists the
    session (place + ``state: open``) -> ``console_close`` -> sessions empty
    -> re-``console_open`` -> new handle -> ``release_place`` with the
    console OPEN succeeds AND auto-closes it (sessions empty, no explicit
    close). Also proves the ownership gate: ``console_open`` on a place this
    server no longer holds surfaces "not acquired by this server".
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    bridge_port = _free_port()
    bridge_script = tmp_path / "bridge.py"
    bridge_script.write_text(_BRIDGE_SRC)
    bridge_recv_log = tmp_path / "bridge_recv.log"
    bridge_log = tmp_path / "bridge.log"
    bridge_proc, bridge_handle = _spawn_logged(
        [sys.executable, str(bridge_script), str(bridge_port), str(bridge_recv_log)],
        cwd=tmp_path,
        log_path=bridge_log,
    )

    # DESIGN.md section 11.10 appendix YAML, port substituted directly (same
    # convention as the power e2e above rather than the appendix's
    # illustrative jinja env-var indirection). ``protocol: raw`` is mandatory
    # -- the default ``rfc2217`` needs an rfc2217-compliant server (ser2net)
    # this bridge is not.
    exporter_yaml = tmp_path / "exporter.yaml"
    exporter_yaml.write_text(
        f"""\
{_CONSOLE_GROUP}:
  {_CONSOLE_RESOURCE_NAME}:
    cls: NetworkSerialPort
    host: 127.0.0.1
    port: {bridge_port}
    protocol: raw
    speed: 115200
"""
    )
    exporter_log = tmp_path / "exporter.log"
    exporter_proc, exporter_handle = _spawn_logged(
        [
            "uv",
            "run",
            "labgrid-exporter",
            "-n",
            _CONSOLE_EXPORTER_NAME,
            "-c",
            f"127.0.0.1:{port}",
            str(exporter_yaml),
        ],
        cwd=tmp_path,
        log_path=exporter_log,
    )

    try:
        _wait_for_port("127.0.0.1", bridge_port, _PORT_TIMEOUT_S)

        await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
        await asyncio.wait_for(
            fixture_client.add_place(_CONSOLE_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_CONSOLE_PLACE_NAME, _CONSOLE_MATCH_PATTERN),
            timeout=_RPC_TIMEOUT_S,
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            tools = await asyncio.wait_for(session.list_tools(), timeout=_RPC_TIMEOUT_S)
            tool_names = {tool.name for tool in tools.tools}
            assert {
                "console_open",
                "console_read",
                "console_send",
                "console_close",
            } <= tool_names

            async def _console_resource_registered() -> bool:
                result = await asyncio.wait_for(
                    session.call_tool("list_resources", {}), timeout=_RPC_TIMEOUT_S
                )
                assert result.isError is False
                assert result.structuredContent is not None
                resources = result.structuredContent["resources"]
                assert isinstance(resources, list)
                return any(
                    isinstance(r, dict)
                    and r.get("exporter") == _CONSOLE_EXPORTER_NAME
                    and r.get("group") == _CONSOLE_GROUP
                    and r.get("cls") == "NetworkSerialPort"
                    for r in resources
                )

            try:
                await _wait_until(
                    _console_resource_registered,
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register NetworkSerialPort",
                )
            except AssertionError as exc:
                raise AssertionError(
                    f"{exc}\n--- exporter log ---\n{_read_log(exporter_log)}\n"
                    f"--- bridge log ---\n{_read_log(bridge_log)}"
                ) from exc

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _CONSOLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            open_result = await asyncio.wait_for(
                session.call_tool("console_open", {"place": _CONSOLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert open_result.isError is False
            assert open_result.structuredContent is not None
            first_session_id = open_result.structuredContent.get("session")
            assert isinstance(first_session_id, str) and first_session_id

            send_result = await asyncio.wait_for(
                session.call_tool(
                    "console_send", {"session": first_session_id, "data": "hello\n"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert send_result.isError is False
            assert send_result.structuredContent is not None
            assert send_result.structuredContent.get("bytes_written") == len(b"hello\n")

            # Independent oracle: the bridge's own recv log proves the bytes
            # actually arrived there -- entirely separate from the
            # console_read/echo path asserted next.
            async def _bridge_received_hello() -> bool:
                if not bridge_recv_log.exists():
                    return False
                return b"hello" in bridge_recv_log.read_bytes()

            await _wait_until(
                _bridge_received_hello,
                timeout=_CONNECT_TIMEOUT_S,
                desc="bridge to record the received 'hello' bytes",
            )

            # Echo path: poll console_read until the bridge's echoed line
            # surfaces in the drained text.
            drained, truncated_any = await _drain_console_until(
                session, first_session_id, "hello", timeout=_CONNECT_TIMEOUT_S
            )
            assert "hello" in drained
            assert truncated_any is False

            sessions_after_open = await _sessions_snapshot(session)
            consoles_after_open = sessions_after_open["consoles"]
            assert isinstance(consoles_after_open, list)
            assert any(
                isinstance(s, dict)
                and s.get("session") == first_session_id
                and s.get("place") == _CONSOLE_PLACE_NAME
                and s.get("state") == "open"
                for s in consoles_after_open
            )
            # No flash jobs in this test -- jobs list stays empty throughout.
            assert sessions_after_open["jobs"] == []

            close_result = await asyncio.wait_for(
                session.call_tool("console_close", {"session": first_session_id}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert close_result.isError is False

            sessions_after_close = await _sessions_snapshot(session)
            assert sessions_after_close == {"consoles": [], "forwards": [], "jobs": []}

            reopen_result = await asyncio.wait_for(
                session.call_tool("console_open", {"place": _CONSOLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert reopen_result.isError is False
            assert reopen_result.structuredContent is not None
            second_session_id = reopen_result.structuredContent.get("session")
            assert isinstance(second_session_id, str) and second_session_id
            assert second_session_id != first_session_id

            # release_place with the console still OPEN: succeeds AND
            # auto-closes the console (no explicit console_close first).
            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _CONSOLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
            assert release_result.structuredContent is not None
            released_place = release_result.structuredContent["place"]
            assert isinstance(released_place, dict)
            assert released_place.get("acquired") is None

            sessions_after_release = await _sessions_snapshot(session)
            assert sessions_after_release == {"consoles": [], "forwards": [], "jobs": []}

            # Ownership gate: console_open on a place this server no longer
            # holds must be rejected with a clear message, not crash.
            not_acquired_result = await asyncio.wait_for(
                session.call_tool("console_open", {"place": _CONSOLE_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert not_acquired_result.isError is True
            assert len(not_acquired_result.content) == 1
            not_acquired_content = not_acquired_result.content[0]
            assert isinstance(not_acquired_content, TextContent)
            assert "not acquired by this server" in not_acquired_content.text
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)
        _terminate(exporter_proc)
        exporter_handle.close()
        _terminate(bridge_proc)
        bridge_handle.close()


# Task 3 (Phase 5): flash gating e2e, still hardware-free (DESIGN.md section
# 11.11's hardware-boundary note: there is NO hardware-free real flash chain
# -- every flash driver op SSHes to the exporter over udev-managed USB
# exports. So this test drives only what IS hardware-free end to end: the
# opt-in gate itself (LABGRID_MCP_ALLOW=flash,acquire makes all 7 flash tools
# appear alongside the plain reads) and the local-file validation path, which
# runs and rejects BEFORE jobs.submit_flash ever binds a driver or touches the
# job registry -- so nothing is submitted and no hardware/exporter is needed.

_FLASH_PLACE_NAME = "e2e-flash-place"


async def test_flash_gating_and_local_file_validation(
    coordinator: int, tmp_path: Path
) -> None:
    """Exercise the FLASH opt-in gate and the pinned local-file validation.

    FLASH is excluded from the default env even without readonly (DESIGN.md
    section 11.11 decision #5 -- a killed flash mid-write can brick hardware;
    the default-env e2e above asserts it absent). Here the server is started
    with an explicit ``LABGRID_MCP_ALLOW=flash,acquire`` allowlist: all 7
    flash-family tools must be present, alongside the plain read tools.

    ``flash_dfu``'s ownership check runs BEFORE its local-file validation
    (server.py: ``_check_place_owned`` then ``_require_local_file``), so a
    missing-file call against an UNACQUIRED place would surface the ownership
    error instead -- not the one this test pins. A bare place (no resource
    matches, no exporter needed) is seeded via the fixture-client pattern and
    acquired through the MCP session first, so ``flash_dfu``'s call with a
    nonexistent file path reaches and pins the file-validation message
    specifically. Nothing is submitted to the job registry and no hardware
    path is touched.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        await asyncio.wait_for(
            fixture_client.add_place(_FLASH_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
                "LABGRID_MCP_ALLOW": "flash,acquire",
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            tools = await asyncio.wait_for(session.list_tools(), timeout=_RPC_TIMEOUT_S)
            tool_names = {tool.name for tool in tools.tools}
            assert tool_names >= _FLASH_TOOL_NAMES
            assert {
                "coordinator_info",
                "list_places",
                "show_place",
                "who",
                "list_resources",
                "list_reservations",
                "acquire_place",
                "release_place",
            } <= tool_names

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _FLASH_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            # Never created -- the point is that flash_dfu must reject this
            # before submitting any job (§11.11's target.env-is-None trap:
            # the local path is validated synchronously up front).
            missing_file = str(tmp_path / "no-such-image.bin")
            flash_result = await asyncio.wait_for(
                session.call_tool(
                    "flash_dfu",
                    {"place": _FLASH_PLACE_NAME, "alt": 0, "file": missing_file},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert flash_result.isError is True
            assert len(flash_result.content) == 1
            flash_error_content = flash_result.content[0]
            assert isinstance(flash_error_content, TextContent)
            assert f"{missing_file!r} does not exist" in flash_error_content.text

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _FLASH_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
            assert release_result.structuredContent is not None
            released_place = release_result.structuredContent["place"]
            assert isinstance(released_place, dict)
            assert released_place.get("acquired") is None
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


# Task 3 (Phase 7): place-metadata + wait_for_change e2e, still hardware-free
# (DESIGN.md section 11.12: none of the eight metadata RPCs needs an exporter
# or a resource to exist -- AddPlaceMatch/DeletePlaceMatch only check the
# PLACE's existence, never the pattern's, so a bare coordinator with no
# exporter covers the full round trip). Proves: the full metadata surface over
# MCP (add/alias/tags incl. empty-value delete/comment/match add+delete), the
# client-side foreign-acquired safety layer (the coordinator itself has NO
# ownership guard on any of these RPCs -- our policy layer is the only
# protection, per section 11.12's headline finding) including its force
# override, and the wait_for_change long-poll bootstrap/advance cycle.
# delete_place/delete_place_match are Category.PLACE_DELETE (opt-in only,
# decision #13) since Phase 7's whole-branch review -- the server below is
# started with an explicit LABGRID_MCP_ALLOW=metadata,place_delete.

_METADATA_PLACE_NAME = "e2e-metadata-place"
_METADATA_PLACE_NAME_2 = "e2e-metadata-place-2"
_METADATA_ALIAS = "metadata-alias"
_METADATA_MATCH_PATTERN = "fake-exporter/fake-group/FakeResourceCls"


async def test_metadata_round_trip_and_wait_for_change(coordinator: int) -> None:
    """Drive all eight metadata tools plus wait_for_change over MCP, no exporter.

    Fixture-seeding pattern (as in the acquire/release e2e above): a second
    ``CoordinatorClient`` under a distinct identity is used both to force the
    "foreign acquired" scenario and to seed the second place the
    wait_for_change assertion observes.
    """
    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    fixture_identity = f"{_FIXTURE_HOSTNAME}/{_FIXTURE_USERNAME}"

    await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
    try:
        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
                # place_delete is opt-in only since decision #13 (mirrors
                # flash/decision #5) -- this test's delete_place/
                # delete_place_match calls below need it explicitly listed
                # alongside metadata (the other six mutators default-on).
                "LABGRID_MCP_ALLOW": "metadata,place_delete",
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            # add_place -> alias add -> show_place reflects the alias.
            add_result = await asyncio.wait_for(
                session.call_tool("add_place", {"name": _METADATA_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert add_result.isError is False
            assert add_result.structuredContent == {
                "place": _METADATA_PLACE_NAME,
                "added": True,
            }

            alias_result = await asyncio.wait_for(
                session.call_tool(
                    "add_place_alias",
                    {"place": _METADATA_PLACE_NAME, "alias": _METADATA_ALIAS},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert alias_result.isError is False
            assert alias_result.structuredContent is not None
            aliased_place = alias_result.structuredContent["place"]
            assert isinstance(aliased_place, dict)
            assert _METADATA_ALIAS in aliased_place.get("aliases", [])

            show_result = await asyncio.wait_for(
                session.call_tool("show_place", {"name": _METADATA_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert show_result.isError is False
            assert show_result.structuredContent is not None
            assert _METADATA_ALIAS in show_result.structuredContent.get("aliases", [])

            # tags set, then a second set with an empty value DELETING one key
            # (design section 11.12: intentional labgrid semantics).
            tags_result = await asyncio.wait_for(
                session.call_tool(
                    "set_place_tags",
                    {
                        "place": _METADATA_PLACE_NAME,
                        "tags": {"env": "ci", "temp": "hot"},
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert tags_result.isError is False
            assert tags_result.structuredContent is not None
            tagged_place = tags_result.structuredContent["place"]
            assert isinstance(tagged_place, dict)
            assert tagged_place.get("tags") == {"env": "ci", "temp": "hot"}

            tags_delete_result = await asyncio.wait_for(
                session.call_tool(
                    "set_place_tags",
                    {"place": _METADATA_PLACE_NAME, "tags": {"temp": ""}},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert tags_delete_result.isError is False
            assert tags_delete_result.structuredContent is not None
            retagged_place = tags_delete_result.structuredContent["place"]
            assert isinstance(retagged_place, dict)
            assert retagged_place.get("tags") == {"env": "ci"}

            # comment.
            comment_result = await asyncio.wait_for(
                session.call_tool(
                    "set_place_comment",
                    {"place": _METADATA_PLACE_NAME, "comment": "seeded by e2e"},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert comment_result.isError is False
            assert comment_result.structuredContent is not None
            commented_place = comment_result.structuredContent["place"]
            assert isinstance(commented_place, dict)
            assert commented_place.get("comment") == "seeded by e2e"

            # match add (3-segment pattern -- no exporter/resource needed,
            # design section 11.12: AddPlaceMatch only checks the place
            # exists) -> match delete.
            match_add_result = await asyncio.wait_for(
                session.call_tool(
                    "add_place_match",
                    {
                        "place": _METADATA_PLACE_NAME,
                        "pattern": _METADATA_MATCH_PATTERN,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert match_add_result.isError is False
            assert match_add_result.structuredContent is not None
            matched_place = match_add_result.structuredContent["place"]
            assert isinstance(matched_place, dict)
            matches = matched_place.get("matches")
            assert isinstance(matches, list)
            assert any(
                isinstance(m, dict)
                and m.get("exporter") == "fake-exporter"
                and m.get("group") == "fake-group"
                and m.get("cls") == "FakeResourceCls"
                for m in matches
            )

            match_delete_result = await asyncio.wait_for(
                session.call_tool(
                    "delete_place_match",
                    {
                        "place": _METADATA_PLACE_NAME,
                        "pattern": _METADATA_MATCH_PATTERN,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert match_delete_result.isError is False
            assert match_delete_result.structuredContent is not None
            unmatched_place = match_delete_result.structuredContent["place"]
            assert isinstance(unmatched_place, dict)
            remaining_matches = unmatched_place.get("matches")
            assert isinstance(remaining_matches, list)
            assert not any(
                isinstance(m, dict) and m.get("exporter") == "fake-exporter"
                for m in remaining_matches
            )

            # Foreign-acquired scenario: the fixture client (a distinct
            # identity) acquires the place directly, bypassing the MCP
            # session entirely -- design section 11.12's headline finding is
            # that the coordinator itself enforces NO ownership guard on any
            # metadata RPC, so this refusal is entirely our own policy layer.
            await asyncio.wait_for(
                fixture_client.acquire_place_rpc(_METADATA_PLACE_NAME),
                timeout=_RPC_TIMEOUT_S,
            )

            # Deterministic sequencing: the refusal below comes from OUR
            # policy layer reading the server's streamed snapshot -- wait
            # until the fixture's acquire is visible there, or the un-forced
            # mutation races through before the guard can see the hold.
            async def _metadata_hold_visible() -> bool:
                shown = await asyncio.wait_for(
                    session.call_tool("show_place", {"name": _METADATA_PLACE_NAME}),
                    timeout=_RPC_TIMEOUT_S,
                )
                return (shown.structuredContent or {}).get("acquired") == fixture_identity

            await _wait_until(
                _metadata_hold_visible,
                timeout=_CONNECT_TIMEOUT_S,
                desc="fixture's hold to appear in the server snapshot",
            )

            foreign_comment_result = await asyncio.wait_for(
                session.call_tool(
                    "set_place_comment",
                    {
                        "place": _METADATA_PLACE_NAME,
                        "comment": "should be refused",
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert foreign_comment_result.isError is True
            assert len(foreign_comment_result.content) == 1
            foreign_comment_content = foreign_comment_result.content[0]
            assert isinstance(foreign_comment_content, TextContent)
            assert fixture_identity in foreign_comment_content.text

            forced_comment_result = await asyncio.wait_for(
                session.call_tool(
                    "set_place_comment",
                    {
                        "place": _METADATA_PLACE_NAME,
                        "comment": "forced through",
                        "force": True,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert forced_comment_result.isError is False
            assert forced_comment_result.structuredContent is not None
            forced_place = forced_comment_result.structuredContent["place"]
            assert isinstance(forced_place, dict)
            assert forced_place.get("comment") == "forced through"

            await asyncio.wait_for(
                fixture_client.release_place_rpc(_METADATA_PLACE_NAME),
                timeout=_RPC_TIMEOUT_S,
            )

            # Deterministic sequencing: delete_place's foreign-acquired guard
            # reads the server's streamed snapshot -- wait until the fixture's
            # release is visible there, or the un-forced delete below races
            # and is refused as still-acquired.
            async def _release_visible() -> bool:
                shown = await asyncio.wait_for(
                    session.call_tool("show_place", {"name": _METADATA_PLACE_NAME}),
                    timeout=_RPC_TIMEOUT_S,
                )
                content = shown.structuredContent or {}
                return bool(content) and content.get("acquired") is None

            await _wait_until(
                _release_visible,
                timeout=_CONNECT_TIMEOUT_S,
                desc="fixture's release to appear in the server snapshot",
            )

            delete_result = await asyncio.wait_for(
                session.call_tool("delete_place", {"name": _METADATA_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert delete_result.isError is False
            assert delete_result.structuredContent == {
                "place": _METADATA_PLACE_NAME,
                "deleted": True,
            }

            # wait_for_change: bootstrap via the tool (cursor omitted), then
            # the fixture adds a second place -- by the time we call
            # wait_for_change again the change has already happened (the
            # deterministic shape per the task brief); if the broadcast
            # hasn't yet landed in the server's own snapshot the call simply
            # blocks the bounded timeout_s until it does, so this can never
            # race regardless of propagation latency.
            bootstrap_result = await asyncio.wait_for(
                session.call_tool("wait_for_change", {}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert bootstrap_result.isError is False
            assert bootstrap_result.structuredContent is not None
            assert bootstrap_result.structuredContent.get("changed") is False
            cursor = bootstrap_result.structuredContent.get("cursor")
            assert isinstance(cursor, int)

            await asyncio.wait_for(
                fixture_client.add_place(_METADATA_PLACE_NAME_2), timeout=_RPC_TIMEOUT_S
            )

            changed_result = await asyncio.wait_for(
                session.call_tool(
                    "wait_for_change", {"cursor": cursor, "timeout_s": 10.0}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert changed_result.isError is False
            assert changed_result.structuredContent is not None
            assert changed_result.structuredContent.get("changed") is True
            new_cursor = changed_result.structuredContent.get("cursor")
            assert isinstance(new_cursor, int)
            assert new_cursor > cursor
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)


# Task 3 (Phase 8): SSH e2e against a user-mode sshd (DESIGN.md section 11.13
# and its appendix, reused verbatim: sshd_config incl. the load-bearing
# `Subsystem sftp` line, and the static NetworkService exporter YAML). Unlike
# every other SSH-shaped tool, `labgrid-client`'s SSH surface targets a plain
# `NetworkService` resource and SSHes DIRECTLY to `address:port` -- no
# exporter-host tunnel, no udev -- so a user-mode sshd confined to `tmp_path`
# is the entire vehicle: no VM, no container, no root, no system changes, and
# (per the appendix) unchanged on ubuntu CI.
#
# PROBES first with a raw `ssh` invocation before touching the coordinator at
# all (section 11.13 trap #2, "login shell must exist"): sshd's
# `allowed_user` check rejects at the `none` auth stage if the connecting
# user's registered login shell doesn't resolve to an existing binary --
# evaluated BEFORE pubkey, so it looks exactly like an auth failure. That is a
# machine-local problem (this Mac's directory-services login shell points at
# a homebrew fish that has since been uninstalled), not a labgrid/sshd config
# bug, so a probe failure skips cleanly with the captured stderr instead of
# failing outright. CI runners have a normal bash login shell and are
# unaffected -- the PR's ubuntu CI run, with this test EXECUTED (not
# skipped), is the required proof (phase plan Global Constraints).

_SSH_PLACE_NAME = "e2e-ssh-place"
_SSH_EXPORTER_NAME = "e2e-ssh-exporter"
_SSH_GROUP = "sshgrp"
_SSH_RESOURCE_NAME = "dut-ssh"
_SSH_MATCH_PATTERN = f"{_SSH_EXPORTER_NAME}/{_SSH_GROUP}/NetworkService/{_SSH_RESOURCE_NAME}"

# Section 11.13 appendix's two known sftp-server install locations. The
# `Subsystem sftp` config line is load-bearing (trap #1): OpenSSH >=9's
# scp/put/get speak SFTP by default and silently fail without it, even though
# plain command `run` works fine regardless -- so this is probed and wired
# for real, not just carried as a comment.
_SFTP_SERVER_PATHS = ("/usr/libexec/sftp-server", "/usr/lib/openssh/sftp-server")

_SSH_TOOL_NAMES = {
    "ssh_run",
    "put_file",
    "get_file",
    "forward_open",
    "forward_remote_open",
    "forward_list",
    "forward_close",
}


def _find_sftp_server() -> str | None:
    return next((p for p in _SFTP_SERVER_PATHS if Path(p).is_file()), None)


def _probe_ssh(*, port: int, keyfile: Path, username: str) -> tuple[bool, str]:
    """Raw `ssh` round trip (no labgrid involved yet): run `echo probe-ok`
    over the freshly-started sshd. Returns ``(ok, stderr)`` so a failure can
    be skipped with the exact captured reason.
    """
    result = subprocess.run(
        [
            "ssh",
            "-i",
            str(keyfile),
            "-p",
            str(port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            f"{username}@127.0.0.1",
            "echo probe-ok",
        ],
        capture_output=True,
        text=True,
        timeout=_PORT_TIMEOUT_S,
    )
    return (result.returncode == 0 and "probe-ok" in result.stdout), result.stderr


class _EchoServer:
    """Minimal in-process threaded TCP echo server on 127.0.0.1 (ephemeral port).

    Used as the `forward_open` scratch target: since the user-mode sshd's
    host IS localhost (the whole point of the recipe), the forward's remote
    side and this echo server are physically the same machine as the test
    process -- but the byte round trip below still goes through the actual
    `ssh -L` tunnel (labgrid's ControlMaster-driven `forward_local_port`), not
    a direct connect, so it genuinely proves the tunnel path end to end.
    """

    def __init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.5)
        self.port: int = self._sock.getsockname()[1]
        self._stop = False
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                continue
            with contextlib.suppress(OSError), conn:
                conn.settimeout(5.0)
                data = conn.recv(4096)
                if data:
                    conn.sendall(data)

    def close(self) -> None:
        self._stop = True
        with contextlib.suppress(OSError):
            self._sock.close()
        self._thread.join(timeout=_TERM_TIMEOUT_S)


def _tcp_roundtrip(port: int, payload: bytes, *, timeout: float) -> bytes:
    """Connect to 127.0.0.1:port, send payload, return whatever comes back."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        sock.connect(("127.0.0.1", port))
        sock.sendall(payload)
        return sock.recv(4096)


async def test_ssh_via_user_mode_sshd(coordinator: int, tmp_path: Path) -> None:
    """Full ssh_run/put_file/get_file/forward_* chain against a real user-mode
    sshd (DESIGN.md section 11.13 appendix), zero VM/container/root.

    Real coordinator + a real ``uv run labgrid-exporter`` statically exporting
    a ``NetworkService`` (address 127.0.0.1, the sshd's ephemeral port,
    current user) + a real `/usr/sbin/sshd -D` confined to ``tmp_path`` --
    PROBED first with a raw `ssh` call (skips cleanly on this host's known
    dangling-login-shell trap, section 11.13 trap #2). Then, driven end to
    end through the MCP server's SSH-gated tools:

    - acquire -> ``ssh_run("echo hello-from-dut")`` -> stdout pinned,
      exit_code 0 -> ``ssh_run("false")`` -> exit_code 1 (the nonzero-rc gap).
    - ``put_file`` a generated local file -> ``ssh_run("cat <remote>")``
      independently proves the remote content (not just a plausible dict) ->
      ``get_file`` back -> content equality -> re-``get_file`` onto the same
      local target without ``overwrite`` -> refused -> with
      ``overwrite=True`` -> succeeds, content still equal.
    - ``forward_open`` to a scratch TCP echo server listening on 127.0.0.1 ->
      connect through the FORWARD's local port, round-trip bytes (proves the
      real `ssh -L` tunnel path, even though remote/local physically coincide
      here) -> ``labgrid://sessions`` shows the forward -> ``forward_close``
      -> sessions forwards empty.
    - ``forward_remote_open`` (``-R``, §11.14): a SECOND, distinct scratch echo
      server acts as the required local listener; connecting to the tunnel's
      ``remote_port`` (otherwise unused -- nothing else could answer on it)
      round-trips through the real ``ssh -R`` tunnel back down to that
      listener, proving the remote->local direction end to end ->
      ``labgrid://sessions`` shows ``direction: "remote"`` -> ``forward_close``.
    - A second ``forward_open`` left OPEN, then ``release_place``: auto-closed
      (sessions forwards empty with no explicit ``forward_close``), mirroring
      the console session's auto-close-on-release behaviour.
    """
    sftp_server = _find_sftp_server()
    if sftp_server is None:
        pytest.skip(f"no sftp-server binary found in any of {_SFTP_SERVER_PATHS}")
    if not Path("/usr/sbin/sshd").is_file():
        pytest.skip("no /usr/sbin/sshd binary on this host")

    port = coordinator
    fixture_config = Config(
        coordinator=f"127.0.0.1:{port}",
        hostname=_FIXTURE_HOSTNAME,
        username=_FIXTURE_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    fixture_client = CoordinatorClient(fixture_config)
    server_identity = f"{_SERVER_HOSTNAME}/{_SERVER_USERNAME}"
    current_username = getpass.getuser()

    sshd_dir = tmp_path / "sshd"
    sshd_dir.mkdir()
    hostkey = sshd_dir / "hostkey"
    clientkey = sshd_dir / "clientkey"
    authorized_keys = sshd_dir / "authorized_keys"
    sshd_config_path = sshd_dir / "sshd_config"
    sshd_pid_file = sshd_dir / "sshd.pid"
    sshd_log = tmp_path / "sshd.log"

    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-f", str(hostkey), "-N", ""],
        check=True,
        timeout=_PORT_TIMEOUT_S,
    )
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-f", str(clientkey), "-N", ""],
        check=True,
        timeout=_PORT_TIMEOUT_S,
    )
    hostkey.chmod(0o600)
    clientkey.chmod(0o600)
    authorized_keys.write_text(Path(f"{clientkey}.pub").read_text())

    sshd_port = _free_port()
    # DESIGN.md section 11.13 appendix, verbatim (only the sftp-server path is
    # probed per-OS and substituted, per the phase plan Global Constraints).
    sshd_config_path.write_text(
        f"""\
ListenAddress 127.0.0.1
HostKey {hostkey}
PidFile {sshd_pid_file}
AuthorizedKeysFile {authorized_keys}
StrictModes no
UsePAM no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
PrintMotd no
Subsystem sftp {sftp_server}
"""
    )

    # -D: stay in the foreground so the Popen-tracked pid IS the running
    # sshd (no daemonizing away to a pid this fixture can't reach); -e: log to
    # stderr (captured below) instead of syslog, so a probe failure has a real
    # sshd-side log to show alongside the raw ssh client's own stderr.
    sshd_proc, sshd_handle = _spawn_logged(
        ["/usr/sbin/sshd", "-D", "-e", "-f", str(sshd_config_path), "-p", str(sshd_port)],
        cwd=sshd_dir,
        log_path=sshd_log,
    )

    echo_server: _EchoServer | None = None
    # Distinct listener for the -R (remote-forward) section below: forward_open
    # tunnels TO echo_server above (a "remote" scratch target reached via a
    # local ssh -L tunnel); forward_remote_open needs a listener on THIS side
    # instead (ssh -R forwards a connection made on the DUT back to it) -- a
    # second server keeps the two roles from being conflated on the same port.
    echo_server_for_remote: _EchoServer | None = None
    exporter_proc: subprocess.Popen[bytes] | None = None
    exporter_handle: IO[bytes] | None = None
    try:
        _wait_for_port("127.0.0.1", sshd_port, _PORT_TIMEOUT_S)

        probe_ok, probe_stderr = _probe_ssh(
            port=sshd_port, keyfile=clientkey, username=current_username
        )
        if not probe_ok:
            pytest.skip(
                "user-mode sshd probe failed (raw `ssh ... echo probe-ok`); most "
                "likely this host's registered login shell doesn't resolve to an "
                "existing binary (DESIGN.md section 11.13 trap #2: sshd's "
                "allowed_user check rejects at the `none` auth stage BEFORE "
                "pubkey is ever evaluated, so it looks like an auth failure) -- "
                "a machine-local problem, not a labgrid/sshd config regression. "
                "The PR's ubuntu CI run is the required, executed proof.\n"
                f"--- ssh client stderr ---\n{probe_stderr.strip()}\n"
                f"--- sshd log (tail) ---\n{_read_log(sshd_log)[-2000:]}"
            )

        echo_server = _EchoServer()

        await asyncio.wait_for(fixture_client.start(), timeout=_RPC_TIMEOUT_S)
        await asyncio.wait_for(
            fixture_client.add_place(_SSH_PLACE_NAME), timeout=_RPC_TIMEOUT_S
        )
        await asyncio.wait_for(
            fixture_client.add_place_match(_SSH_PLACE_NAME, _SSH_MATCH_PATTERN),
            timeout=_RPC_TIMEOUT_S,
        )

        # Static NetworkService export (section 11.13 appendix's YAML,
        # verbatim): a plain Resource (not NetworkResource), so it
        # static-exports like the power e2e's NetworkPowerPort -- avail=True
        # immediately, no udev, no hardware.
        exporter_yaml = tmp_path / "exporter.yaml"
        exporter_yaml.write_text(
            f"""\
{_SSH_GROUP}:
  {_SSH_RESOURCE_NAME}:
    cls: NetworkService
    address: 127.0.0.1
    username: {current_username}
    port: {sshd_port}
"""
        )
        exporter_log = tmp_path / "exporter.log"
        exporter_proc, exporter_handle = _spawn_logged(
            [
                "uv",
                "run",
                "labgrid-exporter",
                "-n",
                _SSH_EXPORTER_NAME,
                "-c",
                f"127.0.0.1:{port}",
                str(exporter_yaml),
            ],
            cwd=tmp_path,
            log_path=exporter_log,
        )

        params = StdioServerParameters(
            command="uv",
            args=["run", "labgrid-mcp"],
            env={
                "LG_COORDINATOR": f"127.0.0.1:{port}",
                "LG_HOSTNAME": _SERVER_HOSTNAME,
                "LG_USERNAME": _SERVER_USERNAME,
                "LABGRID_MCP_SSH_KEYFILE": str(clientkey),
                "LABGRID_MCP_ALLOW": "ssh,acquire",
            },
        )

        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)
            await _wait_connected(session, port)

            tools = await asyncio.wait_for(session.list_tools(), timeout=_RPC_TIMEOUT_S)
            tool_names = {tool.name for tool in tools.tools}
            assert tool_names >= _SSH_TOOL_NAMES

            async def _ssh_resource_registered() -> bool:
                result = await asyncio.wait_for(
                    session.call_tool("list_resources", {}), timeout=_RPC_TIMEOUT_S
                )
                assert result.isError is False
                assert result.structuredContent is not None
                resources = result.structuredContent["resources"]
                assert isinstance(resources, list)
                return any(
                    isinstance(r, dict)
                    and r.get("exporter") == _SSH_EXPORTER_NAME
                    and r.get("group") == _SSH_GROUP
                    and r.get("cls") == "NetworkService"
                    for r in resources
                )

            try:
                await _wait_until(
                    _ssh_resource_registered,
                    timeout=_CONNECT_TIMEOUT_S,
                    desc="exporter to register NetworkService",
                )
            except AssertionError as exc:
                raise AssertionError(
                    f"{exc}\n--- exporter log ---\n{_read_log(exporter_log)}"
                ) from exc

            acquire_result = await asyncio.wait_for(
                session.call_tool("acquire_place", {"name": _SSH_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert acquire_result.isError is False
            assert acquire_result.structuredContent is not None
            acquired_place = acquire_result.structuredContent["place"]
            assert isinstance(acquired_place, dict)
            assert acquired_place.get("acquired") == server_identity

            hello_result = await asyncio.wait_for(
                session.call_tool(
                    "ssh_run", {"place": _SSH_PLACE_NAME, "command": "echo hello-from-dut"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert hello_result.isError is False
            assert hello_result.structuredContent is not None
            assert hello_result.structuredContent.get("stdout") == "hello-from-dut"
            assert hello_result.structuredContent.get("exit_code") == 0

            # The nonzero-rc gap: a command that fails must surface its real
            # exit code, not be silently treated as success.
            false_result = await asyncio.wait_for(
                session.call_tool(
                    "ssh_run", {"place": _SSH_PLACE_NAME, "command": "false"}
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert false_result.isError is False
            assert false_result.structuredContent is not None
            assert false_result.structuredContent.get("exit_code") == 1

            # put_file / get_file (scp): the "remote" side is, physically,
            # this same machine (the whole point of the recipe), but the
            # bytes travel through a real scp invocation over the real sshd
            # -- a separate directory keeps the local/remote roles distinct
            # on disk even though the filesystem is shared.
            local_dir = tmp_path / "local_side"
            remote_dir = tmp_path / "remote_side"
            local_dir.mkdir()
            remote_dir.mkdir()
            payload = "phase8-ssh-e2e-payload"
            local_src = local_dir / "upload.txt"
            local_src.write_text(payload)
            remote_path = str(remote_dir / "uploaded.txt")

            put_result = await asyncio.wait_for(
                session.call_tool(
                    "put_file",
                    {
                        "place": _SSH_PLACE_NAME,
                        "local_path": str(local_src),
                        "remote_path": remote_path,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert put_result.isError is False
            assert put_result.structuredContent == {
                "place": _SSH_PLACE_NAME,
                "put": remote_path,
                "bytes": len(payload.encode()),
            }

            # Independent oracle: prove the remote content directly via
            # ssh_run's `cat`, not just that put_file returned a plausible
            # dict.
            cat_result = await asyncio.wait_for(
                session.call_tool(
                    "ssh_run",
                    {"place": _SSH_PLACE_NAME, "command": f"cat {remote_path}"},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert cat_result.isError is False
            assert cat_result.structuredContent is not None
            assert cat_result.structuredContent.get("stdout") == payload
            assert cat_result.structuredContent.get("exit_code") == 0

            local_back = local_dir / "downloaded.txt"
            get_result = await asyncio.wait_for(
                session.call_tool(
                    "get_file",
                    {
                        "place": _SSH_PLACE_NAME,
                        "remote_path": remote_path,
                        "local_path": str(local_back),
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert get_result.isError is False
            assert get_result.structuredContent == {
                "place": _SSH_PLACE_NAME,
                "got": str(local_back),
                "bytes": len(payload.encode()),
            }
            assert local_back.read_text() == payload

            # Overwrite-refusal case: the local target now exists.
            refused_result = await asyncio.wait_for(
                session.call_tool(
                    "get_file",
                    {
                        "place": _SSH_PLACE_NAME,
                        "remote_path": remote_path,
                        "local_path": str(local_back),
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert refused_result.isError is True
            assert len(refused_result.content) == 1
            refused_content = refused_result.content[0]
            assert isinstance(refused_content, TextContent)
            assert "already exists" in refused_content.text

            overwrite_result = await asyncio.wait_for(
                session.call_tool(
                    "get_file",
                    {
                        "place": _SSH_PLACE_NAME,
                        "remote_path": remote_path,
                        "local_path": str(local_back),
                        "overwrite": True,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert overwrite_result.isError is False
            assert overwrite_result.structuredContent == {
                "place": _SSH_PLACE_NAME,
                "got": str(local_back),
                "bytes": len(payload.encode()),
            }
            assert local_back.read_text() == payload

            # forward_open: tunnel to the scratch echo server. remote_port
            # resolves on the sshd HOST's localhost (section 11.13) -- which
            # here happens to be this same test process -- but the bytes
            # below travel through the real ssh -L tunnel (ControlMaster +
            # kernel port forwarding), proving the tunnel machinery itself,
            # not a direct connect.
            assert echo_server is not None
            forward_open_result = await asyncio.wait_for(
                session.call_tool(
                    "forward_open",
                    {"place": _SSH_PLACE_NAME, "remote_port": echo_server.port},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert forward_open_result.isError is False
            assert forward_open_result.structuredContent is not None
            forward_id = forward_open_result.structuredContent.get("forward")
            assert isinstance(forward_id, str) and forward_id
            forward_local_port = forward_open_result.structuredContent.get("local_port")
            assert isinstance(forward_local_port, int) and forward_local_port > 0
            assert forward_open_result.structuredContent.get("remote_port") == echo_server.port

            echoed = _tcp_roundtrip(
                forward_local_port, b"ping-through-tunnel", timeout=_CONNECT_TIMEOUT_S
            )
            assert echoed == b"ping-through-tunnel"

            sessions_with_forward = await _sessions_snapshot(session)
            forwards_with_open = sessions_with_forward["forwards"]
            assert isinstance(forwards_with_open, list)
            assert any(
                isinstance(f, dict)
                and f.get("forward") == forward_id
                and f.get("place") == _SSH_PLACE_NAME
                and f.get("local_port") == forward_local_port
                and f.get("remote_port") == echo_server.port
                for f in forwards_with_open
            )

            forward_close_result = await asyncio.wait_for(
                session.call_tool("forward_close", {"forward": forward_id}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert forward_close_result.isError is False
            assert forward_close_result.structuredContent == {"closed": forward_id}

            sessions_after_close = await _sessions_snapshot(session)
            assert sessions_after_close["forwards"] == []

            # forward_remote_open (-R, §11.14): unlike forward_open's -L, BOTH
            # ports are required -- a local listener must already exist, since
            # labgrid has no auto-assign for the local side. echo_server_for_remote
            # plays that role; a connection made to remote_port on the sshd
            # HOST (this same machine, per the recipe) is forwarded back down
            # through the real ssh -R tunnel to echo_server_for_remote's port,
            # proving the remote->local direction end to end, not a direct
            # connect (remote_port is otherwise unused, so nothing but the
            # tunnel could answer on it).
            echo_server_for_remote = _EchoServer()
            remote_port = _free_port()
            forward_remote_result = await asyncio.wait_for(
                session.call_tool(
                    "forward_remote_open",
                    {
                        "place": _SSH_PLACE_NAME,
                        "remote_port": remote_port,
                        "local_port": echo_server_for_remote.port,
                    },
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert forward_remote_result.isError is False
            assert forward_remote_result.structuredContent is not None
            remote_forward_id = forward_remote_result.structuredContent.get("forward")
            assert isinstance(remote_forward_id, str) and remote_forward_id
            assert forward_remote_result.structuredContent.get("direction") == "remote"
            assert forward_remote_result.structuredContent.get("remote_port") == remote_port
            assert (
                forward_remote_result.structuredContent.get("local_port")
                == echo_server_for_remote.port
            )

            echoed_remote = _tcp_roundtrip(
                remote_port, b"ping-through-remote-tunnel", timeout=_CONNECT_TIMEOUT_S
            )
            assert echoed_remote == b"ping-through-remote-tunnel"

            sessions_with_remote = await _sessions_snapshot(session)
            forwards_with_remote = sessions_with_remote["forwards"]
            assert isinstance(forwards_with_remote, list)
            assert any(
                isinstance(f, dict)
                and f.get("forward") == remote_forward_id
                and f.get("place") == _SSH_PLACE_NAME
                and f.get("direction") == "remote"
                and f.get("remote_port") == remote_port
                and f.get("local_port") == echo_server_for_remote.port
                for f in forwards_with_remote
            )

            forward_remote_close_result = await asyncio.wait_for(
                session.call_tool("forward_close", {"forward": remote_forward_id}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert forward_remote_close_result.isError is False
            assert forward_remote_close_result.structuredContent == {"closed": remote_forward_id}

            sessions_after_remote_close = await _sessions_snapshot(session)
            assert sessions_after_remote_close["forwards"] == []

            # Auto-close on release: open a SECOND forward and leave it open;
            # release_place must close it automatically (forwards.close_place,
            # ownership-gated), mirroring the console session's auto-close.
            second_forward_result = await asyncio.wait_for(
                session.call_tool(
                    "forward_open",
                    {"place": _SSH_PLACE_NAME, "remote_port": echo_server.port},
                ),
                timeout=_RPC_TIMEOUT_S,
            )
            assert second_forward_result.isError is False
            assert second_forward_result.structuredContent is not None
            second_forward_id = second_forward_result.structuredContent.get("forward")
            assert isinstance(second_forward_id, str) and second_forward_id

            release_result = await asyncio.wait_for(
                session.call_tool("release_place", {"name": _SSH_PLACE_NAME}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert release_result.isError is False
            assert release_result.structuredContent is not None
            released_place = release_result.structuredContent["place"]
            assert isinstance(released_place, dict)
            assert released_place.get("acquired") is None

            sessions_after_release = await _sessions_snapshot(session)
            assert sessions_after_release["forwards"] == []
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(fixture_client.stop(), timeout=_RPC_TIMEOUT_S)
        if exporter_proc is not None:
            _terminate(exporter_proc)
        if exporter_handle is not None:
            exporter_handle.close()
        if echo_server is not None:
            echo_server.close()
        if echo_server_for_remote is not None:
            echo_server_for_remote.close()
        _terminate(sshd_proc)
        sshd_handle.close()
