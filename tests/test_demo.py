"""Unit tests for the ``labgrid-mcp demo`` subcommand.

No real coordinator/exporter here -- that is
tests/integration/test_demo_stack.py's job. This covers pure/cheap surfaces:
argparse routing (``server.main``), exporter YAML rendering, banner/snippet
rendering, and the two clean-failure paths (port already bound, a missing
labgrid console script).
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import threading
from contextlib import closing
from unittest.mock import MagicMock

import pytest

from labgrid_mcp import demo, server


def test_main_no_args_serves_stdio(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero args (the shape every MCP client spawns) must reach ``_serve_stdio``,
    never ``demo.run_demo`` -- this is the zero-behavior-change guarantee."""
    serve = MagicMock()
    run_demo = MagicMock()
    monkeypatch.setattr(server, "_serve_stdio", serve)
    monkeypatch.setattr(demo, "run_demo", run_demo)

    server.main([])

    serve.assert_called_once_with()
    run_demo.assert_not_called()


def test_main_demo_routes_to_run_demo_with_default_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve = MagicMock()
    run_demo = MagicMock()
    monkeypatch.setattr(server, "_serve_stdio", serve)
    monkeypatch.setattr(demo, "run_demo", run_demo)

    server.main(["demo"])

    run_demo.assert_called_once_with(demo.DEMO_DEFAULT_PORT)
    serve.assert_not_called()


def test_main_demo_port_flag_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    run_demo = MagicMock()
    monkeypatch.setattr(demo, "run_demo", run_demo)
    monkeypatch.setattr(server, "_serve_stdio", MagicMock())

    server.main(["demo", "--port", "12345"])

    run_demo.assert_called_once_with(12345)


def test_main_demo_error_exits_one_with_clean_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A ``DemoError`` from ``run_demo`` (port busy, missing script, ...) must
    surface as exit code 1 with a one-line stderr message, never a traceback."""
    monkeypatch.setattr(
        demo,
        "run_demo",
        MagicMock(side_effect=demo.DemoError("port 20499 is already in use")),
    )

    with pytest.raises(SystemExit) as exc_info:
        server.main(["demo"])

    assert exc_info.value.code == 1
    assert "port 20499 is already in use" in capsys.readouterr().err


def test_exporter_config_embeds_both_ports() -> None:
    yaml_text = demo._exporter_config(switch_port=18001, bridge_port=18002)

    assert "demogroup:" in yaml_text
    assert "NetworkPowerPort:" in yaml_text
    assert "http://127.0.0.1:18001/relay/{index}/value" in yaml_text
    assert "NetworkSerialPort:" in yaml_text
    assert "port: 18002" in yaml_text
    assert "protocol: raw" in yaml_text


def test_render_banner_contains_coordinator_address_and_prompts() -> None:
    banner = demo.render_banner(20499)

    assert "127.0.0.1:20499" in banner
    assert '"LG_COORDINATOR": "127.0.0.1:20499"' in banner
    assert '"mcpServers"' in banner
    # The snippet must be the install-agnostic uvx form -- a
    # `uv run --directory <cwd>` snippet only works from a source checkout
    # and leaks the invoking directory.
    assert '"command": "uvx"' in banner
    assert "--directory" not in banner
    assert demo.PLACE_NAME in banner
    assert "acquire demo-place" in banner
    assert "Ctrl-C" in banner


def test_check_port_available_raises_clean_error_when_bound() -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as bound:
        bound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bound.bind(("127.0.0.1", 0))
        bound.listen(1)
        port = bound.getsockname()[1]

        with pytest.raises(demo.DemoError, match=r"--port"):
            demo.check_port_available(port)


def test_check_port_available_passes_on_free_port() -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        free_port = sock.getsockname()[1]
    # Socket closed above frees the port again before the assertion below.
    demo.check_port_available(free_port)


def test_find_script_missing_raises_clean_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patches the real shutil module -- demo.py's own ``shutil.which(...)`` call
    # resolves through the same module object, so this reaches it without
    # needing demo.py to re-export ``shutil`` (mypy --strict flags that).
    monkeypatch.setattr(shutil, "which", lambda name: None)

    with pytest.raises(demo.DemoError, match="labgrid-coordinator"):
        demo._find_script("labgrid-coordinator")


def test_find_script_found_returns_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")

    assert demo._find_script("labgrid-exporter") == "/usr/bin/labgrid-exporter"


def test_parse_acquired() -> None:
    free = "Place 'demo-place':\n  matches:\n    x/y/z\n  acquired: None\n"
    held = "Place 'demo-place':\n  acquired: claude/agent\n  acquired resources:\n"
    assert demo._parse_acquired(free) is None
    assert demo._parse_acquired(held) == "claude/agent"
    assert demo._parse_acquired("") is None
    assert demo._parse_acquired("garbage\nwithout the field\n") is None


def test_watch_ownership_narrates_transitions(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """One line per transition: free->held prints acquired, held->free prints
    released, and steady state prints nothing."""
    stop = threading.Event()
    outputs = iter(
        [
            "  acquired: None\n",
            "  acquired: claude/agent\n",
            "  acquired: claude/agent\n",
            "  acquired: None\n",
        ]
    )

    class _Proc:
        def __init__(self, out: str) -> None:
            self.stdout = out

    def fake_run(*args: object, **kwargs: object) -> _Proc:
        try:
            return _Proc(next(outputs))
        except StopIteration:
            stop.set()
            return _Proc("  acquired: None\n")

    # Patch the stdlib modules directly (docs/memory/
    # mypy-strict-monkeypatch-reexport-trap.md): module singletons mean
    # demo's own subprocess.run/shutil.which resolve through these objects.
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/labgrid-client")

    class _FastEvent(threading.Event):
        """wait() returns immediately so each loop tick is instant."""

        def wait(self, timeout: float | None = None) -> bool:
            return self.is_set()

    fast_stop = _FastEvent()

    def _sync_stop() -> None:
        while not stop.wait(0.01):
            pass
        fast_stop.set()

    t = threading.Thread(target=_sync_stop)
    t.start()
    demo._watch_ownership(20499, fast_stop)
    t.join()

    out = capsys.readouterr().out
    assert out.count("acquired by claude/agent") == 1
    assert out.count("released (free)") == 1
