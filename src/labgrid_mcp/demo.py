"""``labgrid-mcp demo``: boot a hardware-free local lab.

Productizes the exact technique tests/integration/test_e2e.py proves against
a real coordinator (DESIGN.md sections 11.9/11.10): a real
``labgrid-coordinator`` + a real ``labgrid-exporter`` statically exporting a
``NetworkPowerPort`` (fake ``rest`` HTTP switch) and a ``NetworkSerialPort``
(``protocol: raw``, fake TCP bridge) into one place, so a user can try every
MCP tool against a live-but-fake lab with zero real hardware. Self-contained:
only stdlib plus ``labgrid_mcp.config``/``labgrid_mcp.coordinator`` (the same
way the e2e fixtures use them), never ``tests/``.

The fake switch and fake bridge run as background threads *inside* this
process (not spawned helper subprocesses like the e2e fixtures do) -- one
fewer moving part; only the coordinator and exporter are real subprocesses,
since those are the pieces actually under test.
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import signal
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import FrameType

from labgrid_mcp.config import Config
from labgrid_mcp.coordinator import CoordinatorClient

DEMO_DEFAULT_PORT = 20499
PLACE_NAME = "demo-place"

_EXPORTER_NAME = "demo-exporter"
_GROUP = "demogroup"
_POWER_MATCH = f"{_EXPORTER_NAME}/{_GROUP}/NetworkPowerPort"
_CONSOLE_MATCH = f"{_EXPORTER_NAME}/{_GROUP}/NetworkSerialPort"

# The throwaway identity used to seed demo-place; distinct from whatever
# LG_HOSTNAME/LG_USERNAME the user's own MCP server session runs under.
_HOSTNAME = "labgrid-mcp-demo"
_USERNAME = "demo"

# Bounds kept generous but finite -- same rationale as the e2e suite's: a
# hung dependency fails loudly with a clear message, never hangs forever.
_PORT_TIMEOUT_S = 15.0
_CONNECT_TIMEOUT_S = 20.0
_TERM_TIMEOUT_S = 10.0

_BANNER_LINE = "labgrid-mcp demo device — type and lines echo back\r\n".encode()


class DemoError(Exception):
    """A clean, user-facing demo failure (never a raw traceback from main())."""


def check_port_available(port: int, host: str = "127.0.0.1") -> None:
    """Raise ``DemoError`` with a ``--port`` hint if ``host:port`` is already bound.

    Checked eagerly, before any subprocess is spawned, so a busy port fails
    fast with one clear line instead of surfacing as a coordinator crash
    buried in a log file.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError as exc:
            raise DemoError(
                f"port {port} is already in use -- pick another with --port"
            ) from exc


def _find_script(name: str) -> str:
    """Locate a labgrid console script on PATH, or raise a clear ``DemoError``.

    ``labgrid-coordinator``/``labgrid-exporter`` ship as console-script entry
    points on the ``labgrid`` package (a hard dependency of this project) --
    if either is missing, the environment itself is broken, so this fails
    before spawning anything rather than letting ``subprocess.Popen`` raise a
    bare ``FileNotFoundError``.
    """
    path = shutil.which(name)
    if path is None:
        raise DemoError(
            f"{name!r} not found on PATH -- it ships with the labgrid package; "
            "reinstall/`uv sync` this project's environment"
        )
    return path


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    """Block until ``host:port`` accepts a TCP connection, or raise ``DemoError``."""
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
    raise DemoError(f"{host}:{port} never accepted connections")


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


def _spawn_logged(args: list[str], *, cwd: Path, log_path: Path) -> subprocess.Popen[bytes]:
    """Start a subprocess with combined stdout/stderr captured to ``log_path``.

    The child inherits the fd directly, so the file on disk is readable at any
    time, not just after the process exits -- the escalation path when the
    coordinator or exporter never comes up (see ``seed_place``'s error).
    """
    log = log_path.open("wb")
    return subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd=cwd)


class _FakeSwitch:
    """In-process HTTP server backing the demo's fake power relay.

    Mirrors the e2e fixture's fake ``rest`` switch (tests/integration/
    test_e2e.py, DESIGN.md section 11.9 appendix) -- same GET->"0"/"1",
    PUT-sets-body behaviour -- but runs as a background thread inside this
    process rather than a spawned subprocess. State is keyed by the stripped
    request path (e.g. ``"relay/0/value"``), readable either directly
    (``value()``) or via a real HTTP GET against ``port``: either is an
    independent oracle, never the MCP layer.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, bool] = {}
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                body = b"1" if outer.value(self.path.strip("/")) else b"0"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                outer._set(self.path.strip("/"), body.strip() == b"1")
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                pass  # silence stdlib's default per-request stderr logging

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.port = int(self.server.server_address[1])
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def value(self, path: str = "relay/0/value") -> bool:
        with self._lock:
            return self._values.get(path, False)

    def _set(self, path: str, value: bool) -> None:
        with self._lock:
            self._values[path] = value

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class _FakeBridge:
    """In-process TCP server backing the demo's fake serial console.

    Mirrors the e2e fixture's fake TCP bridge (tests/integration/
    test_e2e.py, DESIGN.md section 11.10 appendix): ``NetworkSerialPort``'s
    ``protocol: raw`` (docs/memory/labgrid-serial-protocol-raw.md) needs
    nothing more than a plain TCP peer. On connect it sends a banner line;
    each newline-terminated line it receives is echoed back prefixed
    ``"demo> "``. Runs as a background thread, same rationale as
    ``_FakeSwitch``.
    """

    def __init__(self) -> None:
        class _Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                sock: socket.socket = self.request
                with contextlib.suppress(OSError):
                    sock.sendall(_BANNER_LINE)
                    buf = b""
                    while True:
                        data = sock.recv(4096)
                        if not data:
                            return
                        buf += data
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            sock.sendall(b"demo> " + line.rstrip(b"\r") + b"\r\n")

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Handler)
        self.server.daemon_threads = True
        self.port = int(self.server.server_address[1])
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _exporter_config(switch_port: int, bridge_port: int) -> str:
    """Render the exporter YAML for demo-place's power + console resources.

    Bare class keys (no explicit ``cls:``) inside one group: each class
    appears once, so both resources are the unnamed "default" match -- no
    ``resource_name`` disambiguation needed by the caller (mirrors the e2e
    power+io fixture's two-bare-classes-in-one-group shape, DESIGN.md section
    11.9 appendix). ``{{index}}`` must survive as a literal since the ``rest``
    backend formats it itself, hence the doubled braces.
    """
    return (
        f"{_GROUP}:\n"
        "  NetworkPowerPort:\n"
        "    model: rest\n"
        f"    host: 'http://127.0.0.1:{switch_port}/relay/{{index}}/value'\n"
        "    index: 0\n"
        "  NetworkSerialPort:\n"
        "    host: 127.0.0.1\n"
        f"    port: {bridge_port}\n"
        "    protocol: raw\n"
        "    speed: 115200\n"
    )


class DemoStack:
    """Everything the demo lab needs, already running.

    Built by ``start_processes`` (spawns the coordinator + exporter, starts
    the fake switch/bridge threads) and seeded by ``seed_place`` (creates
    ``demo-place`` and wires its two resource matches). ``stop()`` tears every
    piece down and is idempotent -- safe to call from a signal handler or a
    test's ``finally``.
    """

    def __init__(
        self,
        *,
        coordinator_port: int,
        coordinator_proc: subprocess.Popen[bytes],
        exporter_proc: subprocess.Popen[bytes],
        switch: _FakeSwitch,
        bridge: _FakeBridge,
        tmp_dir: str,
        exporter_log: Path,
    ) -> None:
        self.coordinator_port = coordinator_port
        self.coordinator_proc = coordinator_proc
        self.exporter_proc = exporter_proc
        self.switch = switch
        self.bridge = bridge
        self.tmp_dir = tmp_dir
        self.exporter_log = exporter_log
        self._stopped = False

    def stop(self) -> None:
        """Tear down the exporter, coordinator, fake servers, and temp dir. Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        _terminate(self.exporter_proc)
        _terminate(self.coordinator_proc)
        self.switch.stop()
        self.bridge.stop()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)


def start_processes(port: int) -> DemoStack:
    """Spawn the coordinator + exporter and start the fake switch/bridge threads.

    Does NOT create the place or its resource matches -- that is
    ``seed_place``'s job, kept separate so a caller can exercise process
    wiring alone. Every wait here is bounded (``_PORT_TIMEOUT_S``); on failure
    everything already started is torn down before ``DemoError`` propagates,
    so a failed startup never leaks a subprocess or a temp directory.
    """
    coordinator_script = _find_script("labgrid-coordinator")
    exporter_script = _find_script("labgrid-exporter")

    switch = _FakeSwitch()
    bridge = _FakeBridge()
    switch.start()
    bridge.start()

    tmp_dir = tempfile.mkdtemp(prefix="labgrid-mcp-demo-")
    tmp_path = Path(tmp_dir)
    coordinator_log = tmp_path / "coordinator.log"
    exporter_log = tmp_path / "exporter.log"

    coordinator_proc = _spawn_logged(
        [coordinator_script, "-l", f"127.0.0.1:{port}"],
        cwd=tmp_path,
        log_path=coordinator_log,
    )
    try:
        _wait_for_port("127.0.0.1", port, _PORT_TIMEOUT_S)
    except DemoError:
        _terminate(coordinator_proc)
        switch.stop()
        bridge.stop()
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    exporter_yaml = tmp_path / "exporter.yaml"
    exporter_yaml.write_text(_exporter_config(switch.port, bridge.port))
    exporter_proc = _spawn_logged(
        [exporter_script, "-n", _EXPORTER_NAME, "-c", f"127.0.0.1:{port}", str(exporter_yaml)],
        cwd=tmp_path,
        log_path=exporter_log,
    )

    return DemoStack(
        coordinator_port=port,
        coordinator_proc=coordinator_proc,
        exporter_proc=exporter_proc,
        switch=switch,
        bridge=bridge,
        tmp_dir=tmp_dir,
        exporter_log=exporter_log,
    )


async def seed_place(stack: DemoStack, *, timeout: float = _CONNECT_TIMEOUT_S) -> None:
    """Create ``demo-place`` and wait until the exporter's resources are live.

    Uses its own throwaway ``CoordinatorClient`` under a distinct identity,
    connected and disconnected within this call -- separate from whatever
    session the user's own MCP server later opens against the same
    coordinator. The final wait polls the client's own cached resource
    snapshot (never a network round trip per poll -- mirrors
    tests/integration/test_e2e.py's ``_wait_until``/``_resource_registered``),
    so ``demo-place`` really is immediately usable by the time this returns.
    """
    config = Config(
        coordinator=f"127.0.0.1:{stack.coordinator_port}",
        hostname=_HOSTNAME,
        username=_USERNAME,
        readonly=False,
        allow=None,
        acquire_timeout=30.0,
    )
    client = CoordinatorClient(config)
    await client.start()
    try:
        await client.add_place(PLACE_NAME)
        await client.add_place_match(PLACE_NAME, _POWER_MATCH)
        await client.add_place_match(PLACE_NAME, _CONSOLE_MATCH)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            classes = {
                r.get("cls") for r in client.resources() if r.get("exporter") == _EXPORTER_NAME
            }
            if {"NetworkPowerPort", "NetworkSerialPort"} <= classes:
                return
            await asyncio.sleep(0.2)
        raise DemoError(
            f"exporter never registered demo-place's resources -- see {stack.exporter_log}"
        )
    finally:
        await client.stop()


def _mcp_snippet(port: int) -> str:
    """Ready-to-paste ``.mcp.json``, same shape as the README's Configure section.

    Uses the ``uvx`` form: it is correct however labgrid-mcp was installed,
    whereas a ``uv run --directory <cwd>`` snippet is only valid from a
    source checkout (and leaks whatever directory the demo was run from).
    """
    return (
        "{\n"
        '  "mcpServers": {\n'
        '    "labgrid": {\n'
        '      "command": "uvx",\n'
        '      "args": ["labgrid-mcp"],\n'
        '      "env": {\n'
        f'        "LG_COORDINATOR": "127.0.0.1:{port}"\n'
        "      }\n"
        "    }\n"
        "  }\n"
        "}"
    )


def render_banner(port: int) -> str:
    """The full post-startup banner: status line, paste-ready snippet, example prompts."""
    return (
        "\n"
        "labgrid-mcp demo lab is ready.\n"
        "\n"
        f"  coordinator  127.0.0.1:{port}\n"
        f"  exporter     {_EXPORTER_NAME}  (place {PLACE_NAME!r})\n"
        "  power        fake HTTP switch  -- independent fake\n"
        "  console      fake TCP bridge   -- independent fake, banner + line echo\n"
        "\n"
        "Paste into .mcp.json (or Claude Desktop's config):\n"
        "\n"
        f"{_mcp_snippet(port)}\n"
        "\n"
        "Try asking Claude:\n"
        f'  - "List places, then acquire {PLACE_NAME}"\n'
        f'  - "Power {PLACE_NAME} on and read its power state"\n'
        f'  - "Open the console on {PLACE_NAME} and read its output"\n'
        f'  - "Power {PLACE_NAME} off, then release it"\n'
        "\n"
        "Power state and the console are independent fakes -- toggling one\n"
        "has no effect on the other. Press Ctrl-C to stop.\n"
    )


def run_demo(port: int) -> None:
    """Entry point for ``labgrid-mcp demo``.

    Boots the stack, seeds ``demo-place``, prints the banner, then blocks
    until SIGINT/SIGTERM, tearing everything down cleanly either way. Raises
    ``DemoError`` for a clean, user-facing failure (port busy, a missing
    labgrid console script, or the exporter never coming up) -- ``server.main``
    turns that into a one-line stderr message and exit code 1, never a
    traceback.
    """
    check_port_available(port)
    stack = start_processes(port)
    try:
        asyncio.run(seed_place(stack))
        print(render_banner(port), flush=True)

        stop_event = threading.Event()

        def _handle_signal(signum: int, frame: FrameType | None) -> None:
            stop_event.set()

        previous_int = signal.signal(signal.SIGINT, _handle_signal)
        previous_term = signal.signal(signal.SIGTERM, _handle_signal)
        try:
            stop_event.wait()
        finally:
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)
    finally:
        stack.stop()
