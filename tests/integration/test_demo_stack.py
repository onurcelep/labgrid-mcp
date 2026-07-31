"""Integration proof for the ``labgrid-mcp demo`` stack (``labgrid_mcp.demo``).

Companion to test_e2e.py, but drives ``demo.py``'s own internals directly
(``start_processes`` / ``seed_place`` / ``DemoStack.stop``) rather than the
CLI's banner/signal-handling path -- that path has no MCP surface to assert
against, it just prints and blocks. What's proven here: the productized
hardware-free stack behaves exactly like the e2e fixtures it was modeled on --
``demo-place`` is immediately usable (acquire/power/console) the moment
``seed_place`` returns, and ``DemoStack.stop()`` leaves no orphaned
subprocess. Same conventions as test_e2e.py: excluded from the default run
via the ``integration`` marker, every await individually bounded.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from labgrid_mcp import demo

pytestmark = pytest.mark.integration

_RPC_TIMEOUT_S = 15.0
_CONNECT_TIMEOUT_S = 20.0

_TEST_HOSTNAME = "demo-stack-test"
_TEST_USERNAME = "tester"
_TEST_IDENTITY = f"{_TEST_HOSTNAME}/{_TEST_USERNAME}"


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]], *, timeout: float, desc: str
) -> None:
    """Poll ``predicate`` (each call individually bounded upstream) until true.

    Same shape as test_e2e.py's helper of the same name -- no fixed sleep
    standing in for real sequencing.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {desc}")


@pytest.fixture
def demo_stack() -> Iterator[demo.DemoStack]:
    """Start the real coordinator + exporter + fake switch/bridge, seeded.

    Uses a port well clear of both the labgrid coordinator default (20408)
    and the demo's own default (``demo.DEMO_DEFAULT_PORT``, 20499), so this
    never collides with a coordinator a developer happens to have running
    locally.
    """
    stack = demo.start_processes(20699)
    try:
        asyncio.run(asyncio.wait_for(demo.seed_place(stack), timeout=_CONNECT_TIMEOUT_S))
        yield stack
    finally:
        stack.stop()


async def test_demo_stack_end_to_end(demo_stack: demo.DemoStack) -> None:
    """Drive the shipped server over stdio against the demo stack: list, acquire,
    flip the fake power relay (checked via the switch's own recorded state,
    not just the tool's return value), see the fake console's banner, release
    -- then, once the fixture's ``finally`` runs ``DemoStack.stop()``, confirm
    neither the coordinator nor the exporter subprocess is left running.
    """
    params = StdioServerParameters(
        command="uv",
        args=["run", "labgrid-mcp"],
        env={
            "LG_COORDINATOR": f"127.0.0.1:{demo_stack.coordinator_port}",
            "LG_HOSTNAME": _TEST_HOSTNAME,
            "LG_USERNAME": _TEST_USERNAME,
        },
    )

    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await asyncio.wait_for(session.initialize(), timeout=_RPC_TIMEOUT_S)

        # Bounded poll until the server's own coordinator link is up --
        # mirrors test_e2e.py's _wait_connected, inlined here since it also
        # asserts the exact address, which this test doesn't need.
        async def _connected() -> bool:
            result = await asyncio.wait_for(
                session.call_tool("coordinator_info", {}), timeout=_RPC_TIMEOUT_S
            )
            assert result.isError is False
            assert result.structuredContent is not None
            return result.structuredContent.get("connected") is True

        await _wait_until(_connected, timeout=_CONNECT_TIMEOUT_S, desc="server to connect")

        # demo-place is immediately usable: seed_place already waited for the
        # exporter to register both resources before this fixture yielded.
        list_places_result = await asyncio.wait_for(
            session.call_tool("list_places", {}), timeout=_RPC_TIMEOUT_S
        )
        assert list_places_result.isError is False
        assert list_places_result.structuredContent is not None
        places = list_places_result.structuredContent["places"]
        assert isinstance(places, list)
        assert any(p.get("name") == demo.PLACE_NAME for p in places if isinstance(p, dict))

        acquire_result = await asyncio.wait_for(
            session.call_tool("acquire_place", {"name": demo.PLACE_NAME}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert acquire_result.isError is False
        assert acquire_result.structuredContent is not None
        acquired_place = acquire_result.structuredContent["place"]
        assert isinstance(acquired_place, dict)
        assert acquired_place.get("acquired") == _TEST_IDENTITY

        # Power: the fake switch starts off (demo.py never asserts it).
        assert demo_stack.switch.value() is False

        set_on_result = await asyncio.wait_for(
            session.call_tool(
                "set_power", {"place": demo.PLACE_NAME, "action": "on"}
            ),
            timeout=_RPC_TIMEOUT_S,
        )
        assert set_on_result.isError is False
        assert set_on_result.structuredContent is not None
        assert set_on_result.structuredContent.get("power") is True
        # Independent oracle: the fake switch's OWN recorded state flipped,
        # not just a plausible tool return value (mirrors test_e2e.py's
        # _switch_value check).
        assert demo_stack.switch.value() is True

        set_off_result = await asyncio.wait_for(
            session.call_tool(
                "set_power", {"place": demo.PLACE_NAME, "action": "off"}
            ),
            timeout=_RPC_TIMEOUT_S,
        )
        assert set_off_result.isError is False
        assert set_off_result.structuredContent is not None
        assert set_off_result.structuredContent.get("power") is False
        assert demo_stack.switch.value() is False

        # Console: banner arrives unsolicited on connect (demo.py's
        # _FakeBridge). console_read drains the ring buffer, so poll rather
        # than asserting on a single read.
        open_result = await asyncio.wait_for(
            session.call_tool("console_open", {"place": demo.PLACE_NAME}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert open_result.isError is False
        assert open_result.structuredContent is not None
        session_id = open_result.structuredContent.get("session")
        assert isinstance(session_id, str) and session_id

        async def _banner_seen() -> tuple[bool, str]:
            result = await asyncio.wait_for(
                session.call_tool("console_read", {"session": session_id}),
                timeout=_RPC_TIMEOUT_S,
            )
            assert result.isError is False
            assert result.structuredContent is not None
            data = result.structuredContent.get("data")
            assert isinstance(data, str)
            return "labgrid-mcp demo device" in data, data

        deadline = time.monotonic() + _CONNECT_TIMEOUT_S
        drained = ""
        while time.monotonic() < deadline:
            seen, chunk = await _banner_seen()
            drained += chunk
            if "labgrid-mcp demo device" in drained:
                break
            await asyncio.sleep(0.2)
        assert "labgrid-mcp demo device" in drained, f"banner never arrived: {drained!r}"

        # Release with the console still open: proves auto-close-on-release
        # (test_e2e.py's console test proves the same path) and leaves the
        # place clean for the next acquirer.
        release_result = await asyncio.wait_for(
            session.call_tool("release_place", {"name": demo.PLACE_NAME}),
            timeout=_RPC_TIMEOUT_S,
        )
        assert release_result.isError is False
        assert release_result.structuredContent is not None
        released_place = release_result.structuredContent["place"]
        assert isinstance(released_place, dict)
        assert released_place.get("acquired") is None

    # The MCP server subprocess has exited its stdio context above; now
    # confirm the demo stack's OWN children (coordinator, exporter) are still
    # alive here (the fixture's `finally` hasn't run stop() yet) ...
    assert demo_stack.coordinator_proc.poll() is None
    assert demo_stack.exporter_proc.poll() is None


def test_demo_stack_teardown_leaves_no_children() -> None:
    """``DemoStack.stop()`` must leave neither the coordinator nor the
    exporter subprocess running -- poll-checked directly on the ``Popen``
    objects, the same no-orphan proof test_e2e.py's own ``_terminate``
    relies on, just asserted explicitly here instead of only in a fixture
    ``finally``.
    """
    stack = demo.start_processes(20698)
    asyncio.run(asyncio.wait_for(demo.seed_place(stack), timeout=_CONNECT_TIMEOUT_S))

    assert stack.coordinator_proc.poll() is None
    assert stack.exporter_proc.poll() is None

    stack.stop()

    assert stack.coordinator_proc.poll() is not None
    assert stack.exporter_proc.poll() is not None

    # Idempotent: calling stop() again must not raise or hang.
    stack.stop()
