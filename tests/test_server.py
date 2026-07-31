"""Tests for the FastMCP server assembly (in-process, no transport)."""

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ResourceError, ToolError
from mcp.types import ContentBlock

from labgrid_mcp import coordinator
from labgrid_mcp import server as server_module
from labgrid_mcp.config import Config, load_config
from labgrid_mcp.console import ConsoleError
from labgrid_mcp.coordinator import CoordinatorError, CoordinatorInfo
from labgrid_mcp.forwards import ForwardError
from labgrid_mcp.jobs import JobError
from labgrid_mcp.server import build_server
from labgrid_mcp.target import TargetError, write_image_kwargs


async def _call_tool(
    mcp: FastMCP, name: str, arguments: dict[str, Any]
) -> tuple[list[ContentBlock], dict[str, Any] | None]:
    """Call a tool and return its ``(content, structured)`` pair.

    ``FastMCP.call_tool``'s declared return type (``Sequence[ContentBlock] |
    dict[str, Any]``) does not reflect what it actually returns with
    ``convert_result=True`` (always a content/structured pair, per
    ``ToolManager.call_tool`` -> ``Tool.run``) -- an upstream mcp-sdk typing
    gap. Narrowing the cast to this ONE helper (used at every ``call_tool``
    site below) keeps the untyped third-party boundary in a single,
    documented place instead of a ``type: ignore`` at each call site.
    """
    content, structured = cast(
        "tuple[list[ContentBlock], dict[str, Any] | None]",
        await mcp.call_tool(name, arguments),
    )
    return content, structured


def _lifespan(mcp: FastMCP) -> AbstractAsyncContextManager[AsyncIterator[None]]:
    """``mcp.settings.lifespan(mcp)`` used directly at each test's call site.

    ``Settings.lifespan`` is typed ``Callable[...] | None`` (mcp-sdk) even
    though ``build_server`` always passes a real one -- an ``assert`` narrows
    it here ONCE instead of a ``type: ignore`` at each of this file's four
    ``async with`` sites.
    """
    assert mcp.settings.lifespan is not None
    return mcp.settings.lifespan(mcp)


def _as_list(value: object) -> list[object]:
    """Narrow a place dict field (``object``) to a ``list`` (``[]`` otherwise)."""
    return value if isinstance(value, list) else []


@dataclass
class _StubClient:
    """Duck-typed stand-in for CoordinatorClient."""

    info_value: CoordinatorInfo
    places_value: list[dict[str, object]]
    resources_value: list[dict[str, object]]
    reservations_value: list[dict[str, object]] | CoordinatorError | None
    allow_error: CoordinatorError | None = None
    allow_calls: list[tuple[str, str]] = field(default_factory=list)
    release_place_rpc_error: CoordinatorError | None = None
    release_place_rpc_calls: list[tuple[str, str]] = field(default_factory=list)
    add_place_error: CoordinatorError | None = None
    delete_place_error: CoordinatorError | None = None
    add_place_alias_error: CoordinatorError | None = None
    delete_place_alias_error: CoordinatorError | None = None
    set_place_tags_error: CoordinatorError | None = None
    set_place_comment_error: CoordinatorError | None = None
    add_place_match_error: CoordinatorError | None = None
    delete_place_match_error: CoordinatorError | None = None
    change_cursor_value: int = 0
    wait_for_change_value: int | None = None
    add_place_calls: list[str] = field(default_factory=list)
    delete_place_calls: list[str] = field(default_factory=list)
    add_place_alias_calls: list[tuple[str, str]] = field(default_factory=list)
    delete_place_alias_calls: list[tuple[str, str]] = field(default_factory=list)
    set_place_tags_calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    set_place_comment_calls: list[tuple[str, str]] = field(default_factory=list)
    add_place_match_calls: list[tuple[str, str, str | None]] = field(default_factory=list)
    delete_place_match_calls: list[tuple[str, str, str | None]] = field(default_factory=list)
    wait_for_change_calls: list[tuple[int, float]] = field(default_factory=list)

    def info(self) -> CoordinatorInfo:
        return self.info_value

    def places(self) -> list[dict[str, object]]:
        return self.places_value

    def place(self, name: str) -> dict[str, object] | None:
        for place in self.places_value:
            if place.get("name") == name:
                return place
        return None

    def resources(self) -> list[dict[str, object]]:
        return self.resources_value

    async def get_reservations(self) -> list[dict[str, object]]:
        if isinstance(self.reservations_value, CoordinatorError):
            raise self.reservations_value
        return self.reservations_value or []

    async def allow_place_rpc(self, name: str, user: str) -> None:
        self.allow_calls.append((name, user))
        if self.allow_error is not None:
            raise self.allow_error

    async def release_place_rpc(self, name: str, fromuser: str = "") -> None:
        self.release_place_rpc_calls.append((name, fromuser))
        if self.release_place_rpc_error is not None:
            raise self.release_place_rpc_error
        place = self.place(name)
        if place is None:
            return
        # Mirrors the real coordinator's ReleasePlace matching semantics
        # (§11.8/§11.14): empty fromuser is an unconditional kick; a
        # non-empty one releases only when it equals the current holder,
        # otherwise it is a SILENT no-op (release_from's readback trap).
        current = place.get("acquired")
        if fromuser and current != fromuser:
            return
        place["acquired"] = None

    async def add_place(self, name: str) -> None:
        self.add_place_calls.append(name)
        if self.add_place_error is not None:
            raise self.add_place_error
        self.places_value.append({"name": name})

    async def delete_place(self, name: str) -> None:
        self.delete_place_calls.append(name)
        if self.delete_place_error is not None:
            raise self.delete_place_error
        self.places_value[:] = [p for p in self.places_value if p.get("name") != name]

    async def add_place_alias(self, name: str, alias: str) -> None:
        self.add_place_alias_calls.append((name, alias))
        if self.add_place_alias_error is not None:
            raise self.add_place_alias_error
        place = self.place(name)
        if place is not None:
            aliases = [a for a in _as_list(place.get("aliases")) if a != alias]
            aliases.append(alias)
            place["aliases"] = aliases

    async def delete_place_alias(self, name: str, alias: str) -> None:
        self.delete_place_alias_calls.append((name, alias))
        if self.delete_place_alias_error is not None:
            raise self.delete_place_alias_error
        place = self.place(name)
        if place is not None:
            place["aliases"] = [a for a in _as_list(place.get("aliases")) if a != alias]

    async def set_place_tags(self, name: str, tags: dict[str, str]) -> None:
        self.set_place_tags_calls.append((name, tags))
        if self.set_place_tags_error is not None:
            raise self.set_place_tags_error
        place = self.place(name)
        if place is not None:
            existing = place.get("tags")
            current = dict(existing) if isinstance(existing, dict) else {}
            for key, value in tags.items():
                if value == "":
                    current.pop(key, None)
                else:
                    current[key] = value
            place["tags"] = current

    async def set_place_comment(self, name: str, comment: str) -> None:
        self.set_place_comment_calls.append((name, comment))
        if self.set_place_comment_error is not None:
            raise self.set_place_comment_error
        place = self.place(name)
        if place is not None:
            place["comment"] = comment

    async def add_place_match(self, name: str, pattern: str, rename: str | None = None) -> None:
        self.add_place_match_calls.append((name, pattern, rename))
        if self.add_place_match_error is not None:
            raise self.add_place_match_error
        place = self.place(name)
        if place is not None:
            segments = pattern.split("/")
            entry: dict[str, object] = {
                "exporter": segments[0],
                "group": segments[1],
                "cls": segments[2],
                "name": segments[3] if len(segments) == 4 else None,
                "rename": rename,
            }
            matches = list(_as_list(place.get("matches")))
            matches.append(entry)
            place["matches"] = matches

    async def delete_place_match(self, name: str, pattern: str, rename: str | None = None) -> None:
        self.delete_place_match_calls.append((name, pattern, rename))
        if self.delete_place_match_error is not None:
            raise self.delete_place_match_error
        place = self.place(name)
        if place is not None:
            segments = pattern.split("/")
            exporter, group, cls = segments[0], segments[1], segments[2]
            match_name = segments[3] if len(segments) == 4 else None
            place["matches"] = [
                m
                for m in _as_list(place.get("matches"))
                if not (
                    isinstance(m, dict)
                    and m.get("exporter") == exporter
                    and m.get("group") == group
                    and m.get("cls") == cls
                    and m.get("name") == match_name
                )
            ]

    def change_cursor(self) -> int:
        return self.change_cursor_value

    async def wait_for_change(self, cursor: int, timeout: float) -> int:
        self.wait_for_change_calls.append((cursor, timeout))
        if self.wait_for_change_value is not None:
            return self.wait_for_change_value
        return cursor

    async def start(self) -> None:
        """No-op: these tests call tools directly, never the lifespan."""

    async def stop(self) -> None:
        """No-op: these tests call tools directly, never the lifespan."""


@dataclass
class _StubSession:
    """Duck-typed stand-in for PlaceSession."""

    reserve_value: dict[str, object] | CoordinatorError | None = None
    cancel_error: CoordinatorError | None = None
    release_value: dict[str, object] | CoordinatorError | None = None
    acquire_value: dict[str, object] | CoordinatorError | None = None
    reservation_wait_value: dict[str, object] | CoordinatorError | None = None
    reserve_calls: list[tuple[dict[str, str], float]] = field(default_factory=list)
    cancel_calls: list[str] = field(default_factory=list)
    release_calls: list[tuple[str, bool]] = field(default_factory=list)
    acquire_calls: list[str] = field(default_factory=list)
    reservation_wait_calls: list[tuple[str, float]] = field(default_factory=list)

    async def reserve(self, filters: dict[str, str], prio: float = 0.0) -> dict[str, object]:
        self.reserve_calls.append((filters, prio))
        if isinstance(self.reserve_value, CoordinatorError):
            raise self.reserve_value
        return self.reserve_value or {}

    async def cancel_reservation(self, token: str) -> None:
        self.cancel_calls.append(token)
        if self.cancel_error is not None:
            raise self.cancel_error

    async def reservation_wait(self, token: str, timeout_s: float = 25.0) -> dict[str, object]:
        self.reservation_wait_calls.append((token, timeout_s))
        if isinstance(self.reservation_wait_value, CoordinatorError):
            raise self.reservation_wait_value
        return self.reservation_wait_value or {
            "token": token,
            "state": "waiting",
            "allocations": {},
            "changed": False,
        }

    async def release_place(self, name: str, *, kick: bool = False) -> dict[str, object]:
        self.release_calls.append((name, kick))
        if isinstance(self.release_value, CoordinatorError):
            raise self.release_value
        return self.release_value or {}

    async def acquire_place(self, name: str) -> dict[str, object]:
        self.acquire_calls.append(name)
        if isinstance(self.acquire_value, CoordinatorError):
            raise self.acquire_value
        return self.acquire_value or {}

    async def shutdown(self) -> None:
        pass


@dataclass
class _StubTargets:
    """Duck-typed stand-in for TargetManager."""

    power_value: bool | TargetError = False
    power_state_value: bool | TargetError = False
    io_get_value: bool | TargetError = False
    io_set_error: TargetError | None = None
    sd_mux_error: TargetError | None = None
    sd_mux_mode_value: str | TargetError = "dut"
    usb_mux_error: TargetError | None = None
    ssh_driver_value: object | TargetError | None = None
    power_calls: list[tuple[str, str, float | None, str | None]] = field(default_factory=list)
    power_state_calls: list[tuple[str, str | None]] = field(default_factory=list)
    io_get_calls: list[tuple[str, str | None]] = field(default_factory=list)
    io_set_calls: list[tuple[str, bool, str | None]] = field(default_factory=list)
    sd_mux_calls: list[tuple[str, str]] = field(default_factory=list)
    sd_mux_mode_calls: list[str] = field(default_factory=list)
    usb_mux_calls: list[tuple[str, list[str]]] = field(default_factory=list)
    ssh_driver_calls: list[str] = field(default_factory=list)
    invalidate_calls: list[str] = field(default_factory=list)
    shutdown_calls: int = 0

    async def power(
        self,
        place: str,
        action: str,
        delay: float | None = None,
        resource_name: str | None = None,
    ) -> bool:
        self.power_calls.append((place, action, delay, resource_name))
        if isinstance(self.power_value, TargetError):
            raise self.power_value
        return self.power_value

    async def power_state(self, place: str, resource_name: str | None = None) -> bool:
        self.power_state_calls.append((place, resource_name))
        if isinstance(self.power_state_value, TargetError):
            raise self.power_state_value
        return self.power_state_value

    async def io_get(self, place: str, resource_name: str | None = None) -> bool:
        self.io_get_calls.append((place, resource_name))
        if isinstance(self.io_get_value, TargetError):
            raise self.io_get_value
        return self.io_get_value

    async def io_set(self, place: str, value: bool, resource_name: str | None = None) -> None:
        self.io_set_calls.append((place, value, resource_name))
        if self.io_set_error is not None:
            raise self.io_set_error

    async def sd_mux(self, place: str, mode: str) -> None:
        self.sd_mux_calls.append((place, mode))
        if self.sd_mux_error is not None:
            raise self.sd_mux_error

    async def sd_mux_mode(self, place: str) -> str:
        self.sd_mux_mode_calls.append(place)
        if isinstance(self.sd_mux_mode_value, TargetError):
            raise self.sd_mux_mode_value
        return self.sd_mux_mode_value

    async def usb_mux(self, place: str, links: list[str]) -> None:
        self.usb_mux_calls.append((place, links))
        if self.usb_mux_error is not None:
            raise self.usb_mux_error

    async def ssh_driver(self, place: str) -> object:
        self.ssh_driver_calls.append(place)
        if isinstance(self.ssh_driver_value, TargetError):
            raise self.ssh_driver_value
        return self.ssh_driver_value

    async def invalidate(self, place: str) -> None:
        self.invalidate_calls.append(place)

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def _config() -> Config:
    return Config(
        coordinator="10.0.0.1:20408",
        hostname="host",
        username="user",
        readonly=False,
        allow=None,
        acquire_timeout=120.0,
    )


def _owned_place(name: str = "p1") -> dict[str, object]:
    """A place snapshot acquired by ``_config()``'s identity ("host/user")."""
    return {"name": name, "acquired": "host/user"}


def _stub(
    places: list[dict[str, object]] | None = None,
    resources: list[dict[str, object]] | None = None,
    reservations: list[dict[str, object]] | CoordinatorError | None = None,
    connected: bool = True,
) -> _StubClient:
    info = CoordinatorInfo(
        address="10.0.0.1:20408",
        identity="host/user",
        connected=connected,
        version=None,
    )
    return _StubClient(
        info_value=info,
        places_value=places or [],
        resources_value=resources or [],
        reservations_value=reservations,
    )


def _session(
    reserve_value: dict[str, object] | CoordinatorError | None = None,
    cancel_error: CoordinatorError | None = None,
    release_value: dict[str, object] | CoordinatorError | None = None,
    acquire_value: dict[str, object] | CoordinatorError | None = None,
    reservation_wait_value: dict[str, object] | CoordinatorError | None = None,
) -> _StubSession:
    return _StubSession(
        reserve_value=reserve_value,
        cancel_error=cancel_error,
        release_value=release_value,
        acquire_value=acquire_value,
        reservation_wait_value=reservation_wait_value,
    )


def _targets(
    power_value: bool | TargetError = False,
    power_state_value: bool | TargetError = False,
    io_get_value: bool | TargetError = False,
    io_set_error: TargetError | None = None,
    sd_mux_error: TargetError | None = None,
    sd_mux_mode_value: str | TargetError = "dut",
    usb_mux_error: TargetError | None = None,
    ssh_driver_value: object | TargetError | None = None,
) -> _StubTargets:
    return _StubTargets(
        power_value=power_value,
        power_state_value=power_state_value,
        io_get_value=io_get_value,
        io_set_error=io_set_error,
        sd_mux_error=sd_mux_error,
        sd_mux_mode_value=sd_mux_mode_value,
        usb_mux_error=usb_mux_error,
        ssh_driver_value=ssh_driver_value,
    )


@dataclass
class _StubConsoles:
    """Duck-typed stand-in for ConsoleRegistry."""

    open_value: dict[str, object] | ConsoleError | TargetError | None = None
    read_value: dict[str, object] | ConsoleError | None = None
    send_value: dict[str, object] | ConsoleError | None = None
    close_value: dict[str, object] | ConsoleError | None = None
    sessions_value: list[dict[str, object]] = field(default_factory=list)
    open_calls: list[str] = field(default_factory=list)
    read_calls: list[tuple[str, int | None]] = field(default_factory=list)
    send_calls: list[tuple[str, str, bool]] = field(default_factory=list)
    close_calls: list[str] = field(default_factory=list)
    close_place_calls: list[str] = field(default_factory=list)
    shutdown_calls: int = 0

    async def open(self, place: str) -> dict[str, object]:
        self.open_calls.append(place)
        if isinstance(self.open_value, (ConsoleError, TargetError)):
            raise self.open_value
        return self.open_value or {"session": "s1", "place": place}

    def read(self, session: str, max_bytes: int | None = None) -> dict[str, object]:
        self.read_calls.append((session, max_bytes))
        if isinstance(self.read_value, ConsoleError):
            raise self.read_value
        return self.read_value or {
            "session": session,
            "data": "",
            "bytes": 0,
            "truncated": False,
        }

    async def send(self, session: str, data: str, newline: bool = False) -> dict[str, object]:
        self.send_calls.append((session, data, newline))
        if isinstance(self.send_value, ConsoleError):
            raise self.send_value
        return self.send_value or {"session": session, "bytes_written": len(data)}

    async def close(self, session: str) -> dict[str, object]:
        self.close_calls.append(session)
        if isinstance(self.close_value, ConsoleError):
            raise self.close_value
        return self.close_value or {"closed": session}

    async def close_place(self, place: str) -> None:
        self.close_place_calls.append(place)

    def sessions(self) -> list[dict[str, object]]:
        return self.sessions_value

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def _stub_consoles(
    open_value: dict[str, object] | ConsoleError | TargetError | None = None,
    read_value: dict[str, object] | ConsoleError | None = None,
    send_value: dict[str, object] | ConsoleError | None = None,
    close_value: dict[str, object] | ConsoleError | None = None,
    sessions_value: list[dict[str, object]] | None = None,
) -> _StubConsoles:
    return _StubConsoles(
        open_value=open_value,
        read_value=read_value,
        send_value=send_value,
        close_value=close_value,
        sessions_value=sessions_value or [],
    )


@dataclass
class _StubForwards:
    """Duck-typed stand-in for ForwardRegistry."""

    open_value: dict[str, object] | ForwardError | TargetError | None = None
    open_remote_value: dict[str, object] | ForwardError | TargetError | None = None
    close_value: dict[str, object] | ForwardError | None = None
    sessions_value: list[dict[str, object]] = field(default_factory=list)
    open_calls: list[tuple[str, int, int]] = field(default_factory=list)
    open_remote_calls: list[tuple[str, int, int]] = field(default_factory=list)
    close_calls: list[str] = field(default_factory=list)
    close_place_calls: list[str] = field(default_factory=list)
    shutdown_calls: int = 0

    async def open(self, place: str, remote_port: int, local_port: int = 0) -> dict[str, object]:
        self.open_calls.append((place, remote_port, local_port))
        if isinstance(self.open_value, (ForwardError, TargetError)):
            raise self.open_value
        return self.open_value or {
            "forward": "f1",
            "place": place,
            "local_port": local_port or 40000,
            "remote_port": remote_port,
        }

    async def open_remote(
        self, place: str, remote_port: int, local_port: int
    ) -> dict[str, object]:
        self.open_remote_calls.append((place, remote_port, local_port))
        if isinstance(self.open_remote_value, (ForwardError, TargetError)):
            raise self.open_remote_value
        return self.open_remote_value or {
            "forward": "f1",
            "place": place,
            "direction": "remote",
            "remote_port": remote_port,
            "local_port": local_port,
        }

    async def close(self, forward: str) -> dict[str, object]:
        self.close_calls.append(forward)
        if isinstance(self.close_value, ForwardError):
            raise self.close_value
        return self.close_value or {"closed": forward}

    async def close_place(self, place: str) -> None:
        self.close_place_calls.append(place)

    def sessions(self) -> list[dict[str, object]]:
        return self.sessions_value

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def _stub_forwards(
    open_value: dict[str, object] | ForwardError | TargetError | None = None,
    open_remote_value: dict[str, object] | ForwardError | TargetError | None = None,
    close_value: dict[str, object] | ForwardError | None = None,
    sessions_value: list[dict[str, object]] | None = None,
) -> _StubForwards:
    return _StubForwards(
        open_value=open_value,
        open_remote_value=open_remote_value,
        close_value=close_value,
        sessions_value=sessions_value or [],
    )


@dataclass
class _RecordingDriver:
    """Fake flash driver: records every call the tool's built closure makes."""

    calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    # write_image is the only flash method that also takes kwargs (§11.14's
    # partition/mode/skip/seek); tracked separately so ``calls`` above keeps
    # its existing 2-tuple shape (and every existing assertion using it)
    # byte-identical.
    write_image_kwargs_calls: list[dict[str, object]] = field(default_factory=list)

    def download(self, *args: object) -> None:
        self.calls.append(("download", args))

    def flash(self, *args: object) -> None:
        self.calls.append(("flash", args))

    def load(self, *args: object) -> None:
        self.calls.append(("load", args))

    def write_image(self, *args: object, **kwargs: object) -> None:
        self.calls.append(("write_image", args))
        self.write_image_kwargs_calls.append(kwargs)


@dataclass
class _StubJobs:
    """Duck-typed stand-in for JobRegistry."""

    submit_value: dict[str, object] | JobError | TargetError | None = None
    status_value: dict[str, object] | JobError | None = None
    logs_value: dict[str, object] | JobError | None = None
    jobs_value: list[dict[str, object]] = field(default_factory=list)
    running_job: str | None = None  # running_job_for() result for any place
    driver: _RecordingDriver = field(default_factory=_RecordingDriver)
    submit_calls: list[tuple[str, str]] = field(default_factory=list)
    built_fns: list[Callable[[], object]] = field(default_factory=list)
    status_calls: list[str] = field(default_factory=list)
    logs_calls: list[tuple[str, int | None]] = field(default_factory=list)
    shutdown_calls: int = 0

    async def submit_flash(
        self, place: str, kind: str, build_fn: Callable[[object], Callable[[], object]]
    ) -> dict[str, object]:
        self.submit_calls.append((place, kind))
        if isinstance(self.submit_value, (JobError, TargetError)):
            raise self.submit_value
        # Exercise the tool's closure against a recording fake driver so
        # argv-table wiring (alt/partition/script/args/file) is verifiable
        # without ever touching a real one.
        self.built_fns.append(build_fn(self.driver))
        return self.submit_value or {"job": "j1", "place": place, "kind": kind}

    def status(self, job: str) -> dict[str, object]:
        self.status_calls.append(job)
        if isinstance(self.status_value, JobError):
            raise self.status_value
        return self.status_value or {
            "job": job,
            "place": "p1",
            "kind": "dfu",
            "state": "running",
            "created": 1.0,
            "finished": None,
            "error": None,
            "truncated": False,
        }

    def logs(self, job: str, max_bytes: int | None = None) -> dict[str, object]:
        self.logs_calls.append((job, max_bytes))
        if isinstance(self.logs_value, JobError):
            raise self.logs_value
        return self.logs_value or {"job": job, "data": "", "bytes": 0, "truncated": False}

    def jobs(self) -> list[dict[str, object]]:
        return self.jobs_value

    def running_job_for(self, place: str) -> str | None:
        return self.running_job

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def _stub_jobs(
    submit_value: dict[str, object] | JobError | TargetError | None = None,
    status_value: dict[str, object] | JobError | None = None,
    logs_value: dict[str, object] | JobError | None = None,
    jobs_value: list[dict[str, object]] | None = None,
) -> _StubJobs:
    return _StubJobs(
        submit_value=submit_value,
        status_value=status_value,
        logs_value=logs_value,
        jobs_value=jobs_value or [],
    )


async def test_coordinator_info_tool() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "coordinator_info", {})

    assert structured == {
        "address": "10.0.0.1:20408",
        "identity": "host/user",
        "connected": True,
        "version": None,
    }


async def test_coordinator_info_is_readonly_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "coordinator_info")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.destructiveHint is False


async def test_places_resource() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    contents = list(await mcp.read_resource("labgrid://places"))

    assert len(contents) == 1
    assert json.loads(contents[0].content) == [{"name": "p1"}]


def test_server_name() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    assert mcp.name == "labgrid-mcp"


class _RetryClient(_StubClient):
    """Stub whose start() fails ``failures`` times before succeeding."""

    def __init__(self, failures: int) -> None:
        base = _stub()
        super().__init__(
            info_value=base.info_value,
            places_value=base.places_value,
            resources_value=base.resources_value,
            reservations_value=base.reservations_value,
        )
        self.failures = failures
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_calls <= self.failures:
            raise CoordinatorError("coordinator down")

    async def stop(self) -> None:
        self.stop_calls += 1


async def _spin(condition: object, attempts: int = 100) -> None:
    """Yield to the event loop until ``condition()`` holds (bounded)."""
    for _ in range(attempts):
        if condition():  # type: ignore[operator]
            return
        await asyncio.sleep(0)


async def test_lifespan_retries_start_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instant_sleep(_delay: float) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(coordinator, "_sleep", instant_sleep)
    client = _RetryClient(failures=2)
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    async with _lifespan(mcp):
        await _spin(lambda: client.start_calls >= 3)
        assert client.start_calls == 3  # initial failure + failed retry + success

    assert client.stop_calls == 1
    # No further retries after success: nothing left to bump the counter.
    for _ in range(10):
        await asyncio.sleep(0)
    assert client.start_calls == 3


async def test_lifespan_shutdown_cancels_retry_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instant_sleep(_delay: float) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(coordinator, "_sleep", instant_sleep)
    client = _RetryClient(failures=10**9)  # start() raises forever
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    async with _lifespan(mcp):
        await _spin(lambda: client.start_calls >= 2)
        assert client.start_calls >= 2  # retry loop is live mid-failure

    # Exiting the lifespan cancelled the retry task and ran stop().
    assert client.stop_calls == 1
    calls_at_exit = client.start_calls
    for _ in range(10):
        await asyncio.sleep(0)
    assert client.start_calls == calls_at_exit  # retry loop is really dead


def _order_client() -> _StubClient:
    """A full ``_StubClient`` (satisfies ``_ClientLike`` in full) that the
    order tests below subclass, overriding only the ONE method they assert
    the sequencing of -- the rest of the surface is never exercised."""
    return _stub()


async def test_lifespan_shuts_down_jobs_consoles_targets_session_client_in_order() -> None:
    """5-stage teardown order (Task 2): jobs before consoles/targets -- a
    running flash job pins the cached Target it's driving, so it must be
    SIGTERM'd before we tear down the Targets it pins."""
    events: list[str] = []

    class _OrderClient(_StubClient):
        async def stop(self) -> None:
            events.append("client.stop")

    class _OrderSession(_StubSession):
        async def shutdown(self) -> None:
            events.append("session.shutdown")

    class _OrderTargets(_StubTargets):
        async def shutdown(self) -> None:
            events.append("targets.shutdown")

    class _OrderConsoles(_StubConsoles):
        async def shutdown(self) -> None:
            events.append("consoles.shutdown")

    class _OrderForwards(_StubForwards):
        async def shutdown(self) -> None:
            events.append("forwards.shutdown")

    class _OrderJobs(_StubJobs):
        async def shutdown(self) -> None:
            events.append("jobs.shutdown")

    client = _order_client()
    mcp = build_server(
        _config(),
        _OrderClient(
            info_value=client.info_value,
            places_value=client.places_value,
            resources_value=client.resources_value,
            reservations_value=client.reservations_value,
        ),
        _OrderSession(),
        _OrderTargets(),
        _OrderConsoles(),
        _OrderJobs(),
        _OrderForwards(),
    )

    async with _lifespan(mcp):
        pass

    assert events == [
        "jobs.shutdown",
        "consoles.shutdown",
        "forwards.shutdown",
        "targets.shutdown",
        "session.shutdown",
        "client.stop",
    ]


async def test_lifespan_shutdown_runs_later_stages_even_if_earlier_stage_raises() -> None:
    """Closes the Phase-4 carry-over: an earlier stage raising must never skip
    a later one -- each stage's finally must run regardless of an upstream
    failure, all the way down to client.stop()."""
    events: list[str] = []

    class _OrderClient(_StubClient):
        async def stop(self) -> None:
            events.append("client.stop")

    class _OrderSession(_StubSession):
        async def shutdown(self) -> None:
            events.append("session.shutdown")

    class _OrderTargets(_StubTargets):
        async def shutdown(self) -> None:
            events.append("targets.shutdown")

    class _OrderConsoles(_StubConsoles):
        async def shutdown(self) -> None:
            events.append("consoles.shutdown")

    class _OrderForwards(_StubForwards):
        async def shutdown(self) -> None:
            events.append("forwards.shutdown")

    class _RaisingJobs(_StubJobs):
        async def shutdown(self) -> None:
            events.append("jobs.shutdown")
            raise RuntimeError("jobs boom")

    client = _order_client()
    mcp = build_server(
        _config(),
        _OrderClient(
            info_value=client.info_value,
            places_value=client.places_value,
            resources_value=client.resources_value,
            reservations_value=client.reservations_value,
        ),
        _OrderSession(),
        _OrderTargets(),
        _OrderConsoles(),
        _RaisingJobs(),
        _OrderForwards(),
    )

    with pytest.raises(RuntimeError, match="jobs boom"):
        async with _lifespan(mcp):
            pass

    # Every later stage still ran despite jobs.shutdown() raising.
    assert events == [
        "jobs.shutdown",
        "consoles.shutdown",
        "forwards.shutdown",
        "targets.shutdown",
        "session.shutdown",
        "client.stop",
    ]


async def test_list_places_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1"}, {"name": "p2"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "list_places", {})

    assert structured == {"places": [{"name": "p1"}, {"name": "p2"}]}


async def test_show_place_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1", "comment": "c"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "show_place", {"name": "p1"})

    assert structured == {"name": "p1", "comment": "c"}


async def test_show_place_unknown_raises() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="unknown place: 'nope'"):
        await _call_tool(mcp, "show_place", {"name": "nope"})


async def test_who_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(
            places=[
                {
                    "name": "p1",
                    "acquired": "hostA/alice",
                    "acquired_resources": [
                        ["exp1", "grp", "cls", "res"],
                        "exp2/grp/cls/res2",
                    ],
                },
                {"name": "p2", "acquired": None},
            ]
        ),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "who", {})

    assert structured == {
        "acquisitions": [
            {
                "place": "p1",
                "host": "hostA",
                "user": "alice",
                "exporters": ["exp1", "exp2"],
            }
        ]
    }


async def test_read_tools_are_readonly_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in ["list_places", "show_place", "who"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False


# Task 3 tests


async def test_list_resources_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(resources=[{"exporter": "e", "name": "r"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "list_resources", {})

    assert structured == {"resources": [{"exporter": "e", "name": "r"}]}


async def test_list_reservations_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(reservations=[{"owner": "h/u", "token": "T"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "list_reservations", {})

    assert structured == {"reservations": [{"owner": "h/u", "token": "T"}]}


async def test_list_reservations_not_connected() -> None:
    mcp = build_server(
        _config(),
        _stub(reservations=CoordinatorError("not connected")),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "list_reservations", {})


async def test_resources_resource() -> None:
    mcp = build_server(
        _config(),
        _stub(resources=[{"exporter": "e", "name": "r"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    contents = list(await mcp.read_resource("labgrid://resources"))

    assert len(contents) == 1
    assert json.loads(contents[0].content) == [{"exporter": "e", "name": "r"}]


async def test_reservations_resource() -> None:
    mcp = build_server(
        _config(),
        _stub(reservations=[{"owner": "h/u", "token": "T"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    contents = list(await mcp.read_resource("labgrid://reservations"))

    assert len(contents) == 1
    assert json.loads(contents[0].content) == [{"owner": "h/u", "token": "T"}]


async def test_reservations_resource_not_connected() -> None:
    mcp = build_server(
        _config(),
        _stub(reservations=CoordinatorError("not connected")),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ResourceError, match="not connected"):
        await mcp.read_resource("labgrid://reservations")


async def test_place_template_resource() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    contents = list(await mcp.read_resource("labgrid://places/p1"))

    assert len(contents) == 1
    assert json.loads(contents[0].content) == {"name": "p1"}


async def test_place_template_resource_unknown() -> None:
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1"}]),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ValueError, match="unknown place: 'nope'"):
        await mcp.read_resource("labgrid://places/nope")


async def test_list_resources_and_list_reservations_are_readonly_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in ["list_resources", "list_reservations"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False


_READ_TOOL_NAMES = {
    "coordinator_info",
    "list_places",
    "show_place",
    "who",
    "list_resources",
    "list_reservations",
    "wait_for_change",
    # forward_list is unconditional (in-memory tunnel listing only) -- it
    # survives readonly, so it belongs with the always-on reads (§11.13).
    "forward_list",
}

# Category.SSH (default-on): the six gated SSH tools. forward_list is NOT
# here -- it is always-on (see _READ_TOOL_NAMES).
_SSH_TOOL_NAMES = {
    "ssh_run",
    "put_file",
    "get_file",
    "forward_open",
    "forward_remote_open",
    "forward_close",
}

_FLASH_TOOL_NAMES = {
    "flash_dfu",
    "flash_fastboot",
    "flash_script",
    "bootstrap",
    "write_image",
    "flash_status",
    "flash_logs",
}

_METADATA_TOOL_NAMES = {
    "add_place",
    "add_place_alias",
    "delete_place_alias",
    "set_place_tags",
    "set_place_comment",
    "add_place_match",
}

# Category.PLACE_DELETE (opt-in only, decision #13): the two irreversible
# cross-user destroyers split out of METADATA -- mirrors FLASH's opt-in shape.
_PLACE_DELETE_TOOL_NAMES = {
    "delete_place",
    "delete_place_match",
}


# Gating


async def test_readonly_env_registers_only_read_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    assert {t.name for t in tools} == _READ_TOOL_NAMES


async def test_allow_reservation_env_registers_reserve_but_not_release_or_allow() -> None:
    config = load_config(
        env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "reservation"}
    )
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert "reserve" in names
    assert "cancel_reservation" in names
    assert "reservation_wait" in names
    assert "acquire_place" not in names
    assert "release_place" not in names
    assert "allow_place" not in names
    assert names == _READ_TOOL_NAMES | {"reserve", "cancel_reservation", "reservation_wait"}


async def test_default_env_registers_all_non_opt_in_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert (
        names
        == _READ_TOOL_NAMES
        | {
            "reserve",
            "cancel_reservation",
            "reservation_wait",
            "acquire_place",
            "release_place",
            "allow_place",
            "release_from",
            "get_power_state",
            "set_power",
            "get_io",
            "set_io",
            "get_sd_mux",
            "set_sd_mux",
            "set_usb_mux",
            "console_open",
            "console_read",
            "console_send",
            "console_close",
        }
        | _SSH_TOOL_NAMES
        | _METADATA_TOOL_NAMES
    )
    # Flash and place_delete are opt-in ONLY (decisions #5, #13): excluded
    # even from the default env, unlike every other gated category above.
    # METADATA (the other six mutators) is default-on -- already asserted
    # via the union above.
    assert names.isdisjoint(_FLASH_TOOL_NAMES)
    assert names.isdisjoint(_PLACE_DELETE_TOOL_NAMES)


async def test_readonly_env_excludes_flash_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names.isdisjoint(_FLASH_TOOL_NAMES)


async def test_readonly_env_excludes_metadata_tools_but_not_wait_for_change() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names.isdisjoint(_METADATA_TOOL_NAMES)
    assert names.isdisjoint(_PLACE_DELETE_TOOL_NAMES)
    assert "wait_for_change" in names


async def test_allow_flash_env_registers_exactly_the_flash_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "flash"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | _FLASH_TOOL_NAMES


async def test_allow_ssh_env_registers_exactly_the_ssh_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "ssh"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    # The six SSH tools = five gated + forward_list (always-on, in _READ).
    assert names == _READ_TOOL_NAMES | _SSH_TOOL_NAMES


async def test_readonly_env_excludes_ssh_tools_but_keeps_forward_list() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names.isdisjoint(_SSH_TOOL_NAMES)
    assert "forward_list" in names  # survives readonly


async def test_allow_metadata_env_registers_exactly_the_metadata_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "metadata"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    # ALLOW=metadata excludes the two PLACE_DELETE tools too: they are a
    # separate opt-in category now (decision #13), not part of METADATA.
    assert names == _READ_TOOL_NAMES | _METADATA_TOOL_NAMES
    assert names.isdisjoint(_PLACE_DELETE_TOOL_NAMES)


async def test_allow_place_delete_env_registers_exactly_the_place_delete_tools() -> None:
    config = load_config(
        env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "place_delete"}
    )
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | _PLACE_DELETE_TOOL_NAMES


async def test_allow_metadata_and_place_delete_env_includes_both() -> None:
    config = load_config(
        env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "metadata,place_delete"}
    )
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | _METADATA_TOOL_NAMES | _PLACE_DELETE_TOOL_NAMES


# reserve / cancel_reservation


async def test_reserve_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(reserve_value={"token": "T1", "state": "waiting"}),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "reserve", {"filters": {"name": "p1"}})

    assert structured == {"reservation": {"token": "T1", "state": "waiting"}}


async def test_reserve_tool_error() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(reserve_value=CoordinatorError("not connected")),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "reserve", {"filters": {"name": "p1"}})


async def test_cancel_reservation_tool() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "cancel_reservation", {"token": "T1"})

    assert structured == {"cancelled": "T1"}


async def test_cancel_reservation_tool_error() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(cancel_error=CoordinatorError("reservation gone")),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="reservation gone"):
        await _call_tool(mcp, "cancel_reservation", {"token": "T1"})


async def test_reservation_tools_annotations() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    reserve_tool = next(t for t in tools if t.name == "reserve")
    cancel_tool = next(t for t in tools if t.name == "cancel_reservation")

    assert reserve_tool.annotations is not None
    assert reserve_tool.annotations.readOnlyHint is False
    assert reserve_tool.annotations.destructiveHint is False
    assert reserve_tool.annotations.idempotentHint is False

    assert cancel_tool.annotations is not None
    assert cancel_tool.annotations.readOnlyHint is False
    assert cancel_tool.annotations.destructiveHint is True
    assert cancel_tool.annotations.idempotentHint is False


# reservation_wait


async def test_reservation_wait_tool() -> None:
    session = _session(
        reservation_wait_value={
            "token": "T1",
            "state": "allocated",
            "allocations": {"main": ["p1"]},
            "changed": True,
        }
    )
    mcp = build_server(
        _config(), _stub(), session, _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "reservation_wait", {"token": "T1"})

    assert structured == {
        "token": "T1",
        "state": "allocated",
        "allocations": {"main": ["p1"]},
        "changed": True,
    }
    assert session.reservation_wait_calls == [("T1", 25.0)]


async def test_reservation_wait_forwards_explicit_timeout() -> None:
    session = _session()
    mcp = build_server(
        _config(), _stub(), session, _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "reservation_wait", {"token": "T1", "timeout_s": 5.0})

    assert session.reservation_wait_calls == [("T1", 5.0)]


async def test_reservation_wait_tool_error() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(reservation_wait_value=CoordinatorError("not connected")),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "reservation_wait", {"token": "T1"})


async def test_reservation_wait_is_non_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "reservation_wait")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is False


# acquire_place


async def test_acquire_place_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(acquire_value={"name": "p1", "acquired": "host/user"}),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "acquire_place", {"name": "p1"})

    assert structured == {"place": {"name": "p1", "acquired": "host/user"}}


async def test_acquire_place_tool_forwards_name() -> None:
    session = _session(acquire_value={"name": "p1"})
    mcp = build_server(
        _config(), _stub(), session, _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "acquire_place", {"name": "p1"})

    assert session.acquire_calls == ["p1"]


async def test_acquire_place_tool_error() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(
            acquire_value=CoordinatorError(
                "timed out acquiring place 'p1' after 120.0s (reservation waiting)"
            )
        ),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="timed out acquiring place 'p1'"):
        await _call_tool(mcp, "acquire_place", {"name": "p1"})


async def test_acquire_place_annotations() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "acquire_place")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is False


# release_place / allow_place


async def test_release_place_tool() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(release_value={"name": "p1", "acquired": None}),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "release_place", {"name": "p1"})

    assert structured == {"place": {"name": "p1", "acquired": None}}


async def test_release_place_tool_forwards_kick() -> None:
    session = _session(release_value={"name": "p1"})
    mcp = build_server(
        _config(), _stub(), session, _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "release_place", {"name": "p1", "kick": True})

    assert session.release_calls == [("p1", True)]


async def test_release_place_tool_defaults_kick_false() -> None:
    session = _session(release_value={"name": "p1"})
    mcp = build_server(
        _config(), _stub(), session, _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "release_place", {"name": "p1"})

    assert session.release_calls == [("p1", False)]


async def test_release_place_tool_error() -> None:
    mcp = build_server(
        _config(),
        _stub(),
        _session(release_value=CoordinatorError("cannot release place 'p1': held by other/user")),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="held by other/user"):
        await _call_tool(mcp, "release_place", {"name": "p1"})


async def test_allow_place_tool() -> None:
    client = _stub()
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "allow_place", {"name": "p1", "user": "otherhost/otheruser"}
    )

    assert structured == {"allowed": "otherhost/otheruser", "place": "p1"}
    assert client.allow_calls == [("p1", "otherhost/otheruser")]


async def test_allow_place_tool_error() -> None:
    client = _stub()
    client.allow_error = CoordinatorError("not connected")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "allow_place", {"name": "p1", "user": "otherhost/otheruser"})


@pytest.mark.parametrize("bad_user", ["nouser", "host/", "/user", "a/b/c", ""])
async def test_allow_place_rejects_malformed_user(bad_user: str) -> None:
    client = _stub()
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="user must be 'host/user' format"):
        await _call_tool(mcp, "allow_place", {"name": "p1", "user": bad_user})

    assert client.allow_calls == []


async def test_release_and_allow_tools_annotations() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in ["release_place", "allow_place"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is False
        assert tool.annotations.destructiveHint is True
        assert tool.annotations.idempotentHint is False


# release_from (§11.14: conditional release + silent-no-op readback)


async def test_release_from_tool_releases_matching_identity() -> None:
    client = _stub(places=[{"name": "p1", "acquired": "otherhost/otheruser"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "otherhost", "user": "otheruser"}
    )

    assert structured == {
        "place": "p1",
        "released_from": "otherhost/otheruser",
        "released": True,
    }
    assert client.release_place_rpc_calls == [("p1", "otherhost/otheruser")]
    assert client.place("p1") == {"name": "p1", "acquired": None}


async def test_release_from_our_own_identity_cleans_up_local_state() -> None:
    """release_from releasing OUR OWN hold (fromuser == config.identity,
    "host/user") must tear down the same local state release_place does
    after a successful release -- otherwise a cached Target/console/forward
    is left dangling and a later acquire_place would reuse a stale Target."""
    events: list[tuple[str, str]] = []

    class _OrderConsoles(_StubConsoles):
        async def close_place(self, place: str) -> None:
            events.append(("consoles.close_place", place))

    class _OrderForwards(_StubForwards):
        async def close_place(self, place: str) -> None:
            events.append(("forwards.close_place", place))

    class _OrderTargets(_StubTargets):
        async def invalidate(self, place: str) -> None:
            events.append(("invalidate", place))

    client = _stub(places=[_owned_place()])
    mcp = build_server(
        _config(),
        client,
        _session(),
        _OrderTargets(),
        _OrderConsoles(),
        _stub_jobs(),
        _OrderForwards(),
    )

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "host", "user": "user"}
    )

    assert structured == {"place": "p1", "released_from": "host/user", "released": True}
    assert events == [
        ("consoles.close_place", "p1"),
        ("forwards.close_place", "p1"),
        ("invalidate", "p1"),
    ]


async def test_release_from_foreign_identity_leaves_local_state_untouched() -> None:
    """A FOREIGN release (fromuser != config.identity) must NOT touch our
    local state even though it succeeded -- we never held this place, so
    there is nothing of ours to clean up."""
    client = _stub(places=[{"name": "p1", "acquired": "otherhost/otheruser"}])
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    targets = _targets()
    mcp = build_server(_config(), client, _session(), targets, consoles, _stub_jobs(), forwards)

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "otherhost", "user": "otheruser"}
    )

    assert structured == {
        "place": "p1",
        "released_from": "otherhost/otheruser",
        "released": True,
    }
    assert consoles.close_place_calls == []
    assert forwards.close_place_calls == []
    assert targets.invalidate_calls == []


async def test_release_from_our_own_identity_silent_no_op_skips_cleanup() -> None:
    """Our own identity but the place was never held by us (silent no-op,
    released=False) must also skip cleanup -- there is no local state to
    tear down for a hold we never had."""
    client = _stub(places=[{"name": "p1", "acquired": "realhost/realuser"}])
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    targets = _targets()
    mcp = build_server(_config(), client, _session(), targets, consoles, _stub_jobs(), forwards)

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "host", "user": "user"}
    )

    assert structured == {
        "place": "p1",
        "released_from": "host/user",
        "released": False,
    }
    assert consoles.close_place_calls == []
    assert forwards.close_place_calls == []
    assert targets.invalidate_calls == []


async def test_release_from_tool_reports_false_on_silent_no_op() -> None:
    """A mismatched identity is a SILENT coordinator-side no-op (§11.8/§11.14)
    -- the RPC still "succeeds", so only the readback can tell the caller the
    place was NOT actually released."""
    client = _stub(places=[{"name": "p1", "acquired": "realhost/realuser"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "wronghost", "user": "wronguser"}
    )

    assert structured == {
        "place": "p1",
        "released_from": "wronghost/wronguser",
        "released": False,
    }
    # The place is still held by its real owner -- the no-op did not kick them.
    assert client.place("p1") == {"name": "p1", "acquired": "realhost/realuser"}


async def test_release_from_tool_reports_false_on_race_despite_matching_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The readback must compare a BEFORE/after pair, not just "not acquired
    by fromuser afterward" -- that check is vacuously true whenever fromuser
    never held the place (see the other no-op test) and would ALSO wrongly
    report True here if it ignored the "before" state. This simulates a
    coordinator-side race: fromuser genuinely held the place when we read it,
    but by the time the RPC actually executes something else has already
    changed it (modeled by monkeypatching the RPC to a true no-op) -- the
    place is unchanged afterward, so a correct implementation must report
    ``released=False``."""
    # The unsatisfiable readback would otherwise poll for the full bounded
    # catch-up window (_METADATA_SYNC_TIMEOUT_S, real wall-clock seconds) --
    # zero it out so this test resolves immediately (mirrors
    # test_add_place_alias_falls_back_to_stale_snapshot_when_sync_never_lands).
    monkeypatch.setattr(server_module, "_METADATA_SYNC_TIMEOUT_S", 0.0)
    client = _stub(places=[{"name": "p1", "acquired": "raceduser/raceduser"}])

    async def noop_release(name: str, fromuser: str = "") -> None:
        client.release_place_rpc_calls.append((name, fromuser))
        # Deliberately does NOT mutate the snapshot -- models the RPC
        # silently not taking effect despite a matching identity at read time.

    monkeypatch.setattr(client, "release_place_rpc", noop_release)
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "release_from", {"place": "p1", "host": "raceduser", "user": "raceduser"}
    )

    assert structured == {
        "place": "p1",
        "released_from": "raceduser/raceduser",
        "released": False,
    }


async def test_release_from_tool_error() -> None:
    client = _stub(places=[_owned_place()])
    client.release_place_rpc_error = CoordinatorError("not connected")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "release_from", {"place": "p1", "host": "host", "user": "user"})


@pytest.mark.parametrize(
    ("bad_host", "bad_user"),
    [
        ("", "user"),
        ("host", ""),
        ("host/extra", "user"),
        ("host", "user/extra"),
    ],
)
async def test_release_from_rejects_malformed_identity(bad_host: str, bad_user: str) -> None:
    """§11.14 review Minor: unlike ``allow_place``'s single pre-joined string,
    ``release_from`` takes ``host``/``user`` as two params -- an empty half or
    a "/" embedded in either would join into an ambiguous/malformed
    ``fromuser`` string. Validated before any RPC, mirroring
    ``_validate_identity``'s style; zero RPCs on rejection."""
    client = _stub(places=[{"name": "p1", "acquired": "otherhost/otheruser"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="host and user must both be non-empty and contain no '/'"):
        await _call_tool(mcp, "release_from", {"place": "p1", "host": bad_host, "user": bad_user})

    assert client.release_place_rpc_calls == []


async def test_release_from_is_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "release_from")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is False


# Task 2: power / io / mux driver tools


async def test_release_place_invalidates_target() -> None:
    targets = _targets()
    session = _session(release_value={"name": "p1", "acquired": None})
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        session,
        targets,
        consoles,
        _stub_jobs(),
        forwards,
    )

    await _call_tool(mcp, "release_place", {"name": "p1"})

    assert targets.invalidate_calls == ["p1"]
    assert consoles.close_place_calls == ["p1"]
    assert forwards.close_place_calls == ["p1"]


async def test_release_place_kick_invalidates_target() -> None:
    targets = _targets()
    session = _session(release_value={"name": "p1", "acquired": None})
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    mcp = build_server(_config(), _stub(), session, targets, consoles, _stub_jobs(), forwards)

    await _call_tool(mcp, "release_place", {"name": "p1", "kick": True})

    assert targets.invalidate_calls == ["p1"]
    assert consoles.close_place_calls == ["p1"]
    assert forwards.close_place_calls == ["p1"]


async def test_release_place_error_does_not_invalidate_target() -> None:
    targets = _targets()
    session = _session(release_value=CoordinatorError("cannot release place 'p1'"))
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    mcp = build_server(_config(), _stub(), session, targets, consoles, _stub_jobs(), forwards)

    with pytest.raises(ToolError):
        await _call_tool(mcp, "release_place", {"name": "p1"})

    assert targets.invalidate_calls == []
    # The stub snapshot has no owned place, so the non-kick console/forward
    # close is gated off: a release that was never going to succeed must not
    # destroy a live console session or forward tunnel.
    assert consoles.close_place_calls == []
    assert forwards.close_place_calls == []


async def test_release_place_not_owner_leaves_console_untouched() -> None:
    """A not-owner release attempt raises and never touches console/forwards."""
    targets = _targets()
    session = _session(
        release_value=CoordinatorError("cannot release place 'p1': held by other/user")
    )
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1", "acquired": "other/user"}]),
        session,
        targets,
        consoles,
        _stub_jobs(),
        forwards,
    )

    with pytest.raises(ToolError, match="held by other/user"):
        await _call_tool(mcp, "release_place", {"name": "p1"})

    assert consoles.close_place_calls == []
    assert forwards.close_place_calls == []
    assert targets.invalidate_calls == []


async def test_release_place_closes_console_before_releasing_and_invalidating() -> None:
    """Order: consoles.close_place -> forwards.close_place -> session.release_place
    -> targets.invalidate.

    Holds on both the normal (kick=False) and kick=True paths.
    """
    events: list[tuple[str, ...]] = []

    class _OrderConsoles(_StubConsoles):
        async def close_place(self, place: str) -> None:
            events.append(("consoles.close_place", place))

    class _OrderForwards(_StubForwards):
        async def close_place(self, place: str) -> None:
            events.append(("forwards.close_place", place))

    class _OrderSession(_StubSession):
        async def release_place(self, name: str, *, kick: bool = False) -> dict[str, object]:
            events.append(("release_place", name, str(kick)))
            return {"name": name, "acquired": None}

    class _OrderTargets(_StubTargets):
        async def invalidate(self, place: str) -> None:
            events.append(("invalidate", place))

    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _OrderSession(),
        _OrderTargets(),
        _OrderConsoles(),
        _stub_jobs(),
        _OrderForwards(),
    )

    await _call_tool(mcp, "release_place", {"name": "p1"})

    assert events == [
        ("consoles.close_place", "p1"),
        ("forwards.close_place", "p1"),
        ("release_place", "p1", "False"),
        ("invalidate", "p1"),
    ]


async def test_release_place_kick_closes_console_before_releasing_and_invalidating() -> None:
    # The stub snapshot deliberately has NO owned place: kick releases
    # unconditionally, so it must also close the console + forwards unconditionally.
    events: list[tuple[str, ...]] = []

    class _OrderConsoles(_StubConsoles):
        async def close_place(self, place: str) -> None:
            events.append(("consoles.close_place", place))

    class _OrderForwards(_StubForwards):
        async def close_place(self, place: str) -> None:
            events.append(("forwards.close_place", place))

    class _OrderSession(_StubSession):
        async def release_place(self, name: str, *, kick: bool = False) -> dict[str, object]:
            events.append(("release_place", name, str(kick)))
            return {"name": name, "acquired": None}

    class _OrderTargets(_StubTargets):
        async def invalidate(self, place: str) -> None:
            events.append(("invalidate", place))

    mcp = build_server(
        _config(),
        _stub(),
        _OrderSession(),
        _OrderTargets(),
        _OrderConsoles(),
        _stub_jobs(),
        _OrderForwards(),
    )

    await _call_tool(mcp, "release_place", {"name": "p1", "kick": True})

    assert events == [
        ("consoles.close_place", "p1"),
        ("forwards.close_place", "p1"),
        ("release_place", "p1", "True"),
        ("invalidate", "p1"),
    ]


# gating: power / io / mux categories


async def test_allow_power_env_registers_only_power_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "power"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | {"get_power_state", "set_power"}


async def test_allow_io_env_registers_only_io_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "io"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | {"get_io", "set_io"}


async def test_allow_mux_env_registers_only_mux_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "mux"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | {"get_sd_mux", "set_sd_mux", "set_usb_mux"}


async def test_allow_console_env_registers_only_console_tools() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_ALLOW": "console"})
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    names = {t.name for t in await mcp.list_tools()}

    assert names == _READ_TOOL_NAMES | {
        "console_open",
        "console_read",
        "console_send",
        "console_close",
    }


# ownership: driver tools must reject a place we have not acquired


_DRIVER_TOOL_ARGS: dict[str, dict[str, object]] = {
    "get_power_state": {"place": "p1"},
    "set_power": {"place": "p1", "action": "on"},
    "get_io": {"place": "p1"},
    "set_io": {"place": "p1", "value": True},
    "set_sd_mux": {"place": "p1", "mode": "sd"},
    "set_usb_mux": {"place": "p1", "links": ["a"]},
}

_OWNERSHIP_ERROR = "place 'p1' is not acquired by this server; call acquire_place first"


def _assert_no_manager_calls(targets: _StubTargets) -> None:
    assert targets.power_calls == []
    assert targets.power_state_calls == []
    assert targets.io_get_calls == []
    assert targets.io_set_calls == []
    assert targets.sd_mux_calls == []
    assert targets.usb_mux_calls == []


@pytest.mark.parametrize("tool_name,args", _DRIVER_TOOL_ARGS.items())
async def test_driver_tools_reject_unacquired_place(
    tool_name: str, args: dict[str, object]
) -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1", "acquired": None}]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, tool_name, args)

    _assert_no_manager_calls(targets)


@pytest.mark.parametrize("tool_name,args", _DRIVER_TOOL_ARGS.items())
async def test_driver_tools_reject_unknown_place(tool_name: str, args: dict[str, object]) -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, tool_name, args)

    _assert_no_manager_calls(targets)


_RECONNECT_ERROR = r"coordinator disconnected \(reconnecting\); retry shortly"


async def test_driver_tool_reports_reconnect_window_while_disconnected() -> None:
    """A coordinator RPC failure clears our local snapshot cache -- an empty
    snapshot mid-reconnect must not read as "unknown place" (misleads the
    caller into re-acquiring); it should name the real, transient cause
    (mirrors ``TargetManager._check_owned``'s reconnect branch, target.py)."""
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[], connected=False),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_RECONNECT_ERROR):
        await _call_tool(mcp, "get_power_state", {"place": "p1"})

    _assert_no_manager_calls(targets)


async def test_driver_tool_reject_unknown_place_still_reports_unknown_while_connected() -> None:
    """Connected + genuinely unknown place keeps the original message (not
    just a default-True regression check: this is the discriminating case)."""
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[], connected=True),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, "get_power_state", {"place": "p1"})

    _assert_no_manager_calls(targets)


# get_power_state / set_power


async def test_get_power_state_tool() -> None:
    targets = _targets(power_state_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "get_power_state", {"place": "p1"})

    assert structured == {"place": "p1", "power": True}
    assert targets.power_state_calls == [("p1", None)]


async def test_get_power_state_tool_error() -> None:
    targets = _targets(power_state_value=TargetError("driver blew up"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="driver blew up"):
        await _call_tool(mcp, "get_power_state", {"place": "p1"})


async def test_set_power_tool() -> None:
    targets = _targets(power_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "set_power", {"place": "p1", "action": "on"})

    assert structured == {"place": "p1", "power": True}
    assert targets.power_calls == [("p1", "on", None, None)]


async def test_set_power_tool_error() -> None:
    targets = _targets(power_value=TargetError("cycle failed"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="cycle failed"):
        await _call_tool(mcp, "set_power", {"place": "p1", "action": "cycle"})


@pytest.mark.parametrize("bad_action", ["ON", "reboot", "", "onn"])
async def test_set_power_rejects_invalid_action(bad_action: str) -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="action must be one of"):
        await _call_tool(mcp, "set_power", {"place": "p1", "action": bad_action})

    assert targets.power_calls == []


async def test_set_power_validates_action_before_ownership_check() -> None:
    """An invalid action is rejected even for an unacquired/unknown place."""
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="action must be one of"):
        await _call_tool(mcp, "set_power", {"place": "nope", "action": "reboot"})

    assert targets.power_calls == []


# Task 2 gap-closing: set_power delay + power/io resource_name (§11.14)


async def test_set_power_forwards_cycle_delay() -> None:
    targets = _targets(power_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "set_power", {"place": "p1", "action": "cycle", "delay": 1.5}
    )

    assert structured == {"place": "p1", "power": True}
    assert targets.power_calls == [("p1", "cycle", 1.5, None)]


async def test_set_power_rejects_delay_on_non_cycle_action() -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="delay is only valid with action='cycle'"):
        await _call_tool(mcp, "set_power", {"place": "p1", "action": "on", "delay": 1.0})

    assert targets.power_calls == []


async def test_set_power_rejects_delay_before_ownership_check() -> None:
    """A delay-on-non-cycle rejection happens even for an unacquired place."""
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="delay is only valid with action='cycle'"):
        await _call_tool(mcp, "set_power", {"place": "nope", "action": "off", "delay": 2.0})

    assert targets.power_calls == []


async def test_set_power_forwards_resource_name_and_echoes_it() -> None:
    targets = _targets(power_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "set_power", {"place": "p1", "action": "on", "resource_name": "port-a"}
    )

    assert structured == {"place": "p1", "power": True, "resource_name": "port-a"}
    assert targets.power_calls == [("p1", "on", None, "port-a")]


async def test_get_power_state_forwards_resource_name_and_echoes_it() -> None:
    targets = _targets(power_state_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "get_power_state", {"place": "p1", "resource_name": "port-b"}
    )

    assert structured == {"place": "p1", "power": True, "resource_name": "port-b"}
    assert targets.power_state_calls == [("p1", "port-b")]


async def test_get_power_state_omits_resource_name_when_not_given() -> None:
    targets = _targets(power_state_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "get_power_state", {"place": "p1"})

    assert structured == {"place": "p1", "power": True}
    assert "resource_name" not in structured


async def test_get_power_state_surfaces_ambiguous_resource_error() -> None:
    """TargetManager's ambiguous-name TargetError (§11.14) propagates as a
    ToolError naming the available resources -- the tool does no filtering of
    its own, it only forwards resource_name."""
    targets = _targets(
        power_state_value=TargetError(
            "place 'p1' has multiple power resources (port-a, port-b); "
            "pass resource_name to pick one"
        )
    )
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="pass resource_name to pick one"):
        await _call_tool(mcp, "get_power_state", {"place": "p1"})


# get_io / set_io


async def test_get_io_tool() -> None:
    targets = _targets(io_get_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "get_io", {"place": "p1"})

    assert structured == {"place": "p1", "value": True}
    assert targets.io_get_calls == [("p1", None)]


async def test_get_io_tool_error() -> None:
    targets = _targets(io_get_value=TargetError("no such driver"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="no such driver"):
        await _call_tool(mcp, "get_io", {"place": "p1"})


async def test_set_io_tool_rereads_state() -> None:
    targets = _targets(io_get_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "set_io", {"place": "p1", "value": True})

    assert structured == {"place": "p1", "value": True}
    assert targets.io_set_calls == [("p1", True, None)]
    assert targets.io_get_calls == [("p1", None)]


async def test_set_io_tool_set_error() -> None:
    targets = _targets(io_set_error=TargetError("set failed"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="set failed"):
        await _call_tool(mcp, "set_io", {"place": "p1", "value": False})

    assert targets.io_get_calls == []  # never re-reads after a failed set


async def test_set_io_tool_get_error() -> None:
    targets = _targets(io_get_value=TargetError("get failed"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="get failed"):
        await _call_tool(mcp, "set_io", {"place": "p1", "value": False})

    assert targets.io_set_calls == [("p1", False, None)]


async def test_get_io_forwards_resource_name_and_echoes_it() -> None:
    targets = _targets(io_get_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "get_io", {"place": "p1", "resource_name": "io-a"}
    )

    assert structured == {"place": "p1", "value": True, "resource_name": "io-a"}
    assert targets.io_get_calls == [("p1", "io-a")]


async def test_set_io_forwards_resource_name_to_both_set_and_reread() -> None:
    targets = _targets(io_get_value=True)
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "set_io", {"place": "p1", "value": True, "resource_name": "io-b"}
    )

    assert structured == {"place": "p1", "value": True, "resource_name": "io-b"}
    assert targets.io_set_calls == [("p1", True, "io-b")]
    assert targets.io_get_calls == [("p1", "io-b")]


# get_sd_mux / set_sd_mux / set_usb_mux


async def test_get_sd_mux_tool() -> None:
    targets = _targets(sd_mux_mode_value="dut")
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "get_sd_mux", {"place": "p1"})

    assert structured == {"place": "p1", "mode": "dut"}
    assert targets.sd_mux_mode_calls == ["p1"]


async def test_get_sd_mux_tool_error() -> None:
    targets = _targets(sd_mux_mode_value=TargetError("no sd mux driver"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="no sd mux driver"):
        await _call_tool(mcp, "get_sd_mux", {"place": "p1"})


async def test_get_sd_mux_rejects_unacquired_place() -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1", "acquired": None}]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, "get_sd_mux", {"place": "p1"})

    assert targets.sd_mux_mode_calls == []


async def test_get_sd_mux_is_readonly_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "get_sd_mux")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is True


async def test_set_sd_mux_tool() -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "set_sd_mux", {"place": "p1", "mode": "sd"})

    assert structured == {"place": "p1", "mode": "sd"}
    assert targets.sd_mux_calls == [("p1", "sd")]


async def test_set_sd_mux_tool_error() -> None:
    targets = _targets(sd_mux_error=TargetError("no sd mux driver"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="no sd mux driver"):
        await _call_tool(mcp, "set_sd_mux", {"place": "p1", "mode": "sd"})


async def test_set_usb_mux_tool() -> None:
    targets = _targets()
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "set_usb_mux", {"place": "p1", "links": ["dut", "host"]}
    )

    assert structured == {"place": "p1", "links": ["dut", "host"]}
    assert targets.usb_mux_calls == [("p1", ["dut", "host"])]


async def test_set_usb_mux_tool_error() -> None:
    targets = _targets(usb_mux_error=TargetError("no usb mux driver"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="no usb mux driver"):
        await _call_tool(mcp, "set_usb_mux", {"place": "p1", "links": ["a"]})


# annotations


async def test_get_power_state_and_get_io_are_readonly_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in ["get_power_state", "get_io"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False
        assert tool.annotations.idempotentHint is True


async def test_driver_mutation_tools_are_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in ["set_power", "set_io", "set_sd_mux", "set_usb_mux"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is False
        assert tool.annotations.destructiveHint is True
        assert tool.annotations.idempotentHint is False


# Task 2: console tools + sessions resource


async def test_console_open_rejects_unacquired_place() -> None:
    consoles = _stub_consoles()
    mcp = build_server(
        _config(),
        _stub(places=[{"name": "p1", "acquired": None}]),
        _session(),
        _targets(),
        consoles,
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, "console_open", {"place": "p1"})

    assert consoles.open_calls == []


async def test_console_open_rejects_unknown_place() -> None:
    consoles = _stub_consoles()
    mcp = build_server(
        _config(),
        _stub(places=[]),
        _session(),
        _targets(),
        consoles,
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, "console_open", {"place": "p1"})

    assert consoles.open_calls == []


async def test_console_open_tool() -> None:
    consoles = _stub_consoles(open_value={"session": "abc123", "place": "p1"})
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        consoles,
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "console_open", {"place": "p1"})

    assert structured == {"session": "abc123", "place": "p1"}
    assert consoles.open_calls == ["p1"]


async def test_console_open_tool_console_error() -> None:
    consoles = _stub_consoles(
        open_value=ConsoleError("place 'p1' already has console session 'xyz'")
    )
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        consoles,
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="already has console session"):
        await _call_tool(mcp, "console_open", {"place": "p1"})


async def test_console_open_tool_target_error() -> None:
    consoles = _stub_consoles(open_value=TargetError("console on place 'p1' failed: boom"))
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        consoles,
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="console on place 'p1' failed: boom"):
        await _call_tool(mcp, "console_open", {"place": "p1"})


async def test_console_read_tool() -> None:
    consoles = _stub_consoles(
        read_value={"session": "s1", "data": "hi", "bytes": 2, "truncated": False}
    )
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "console_read", {"session": "s1"})

    assert structured == {"session": "s1", "data": "hi", "bytes": 2, "truncated": False}
    assert consoles.read_calls == [("s1", None)]


async def test_console_read_tool_forwards_max_bytes() -> None:
    consoles = _stub_consoles()
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "console_read", {"session": "s1", "max_bytes": 4})

    assert consoles.read_calls == [("s1", 4)]


async def test_console_read_tool_error() -> None:
    consoles = _stub_consoles(read_value=ConsoleError("unknown console session 's1'"))
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="unknown console session"):
        await _call_tool(mcp, "console_read", {"session": "s1"})


async def test_console_send_tool() -> None:
    consoles = _stub_consoles(send_value={"session": "s1", "bytes_written": 5})
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "console_send", {"session": "s1", "data": "hello"})

    assert structured == {"session": "s1", "bytes_written": 5}
    assert consoles.send_calls == [("s1", "hello", False)]


async def test_console_send_tool_forwards_newline() -> None:
    consoles = _stub_consoles()
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "console_send", {"session": "s1", "data": "hello", "newline": True})

    assert consoles.send_calls == [("s1", "hello", True)]


async def test_console_send_tool_error() -> None:
    consoles = _stub_consoles(
        send_value=ConsoleError("console session 's1' is in error state: EOF")
    )
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="is in error state"):
        await _call_tool(mcp, "console_send", {"session": "s1", "data": "x"})


async def test_console_close_tool() -> None:
    consoles = _stub_consoles(close_value={"closed": "s1"})
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "console_close", {"session": "s1"})

    assert structured == {"closed": "s1"}
    assert consoles.close_calls == ["s1"]


async def test_console_close_tool_error() -> None:
    consoles = _stub_consoles(close_value=ConsoleError("unknown console session 's1'"))
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), consoles, _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="unknown console session"):
        await _call_tool(mcp, "console_close", {"session": "s1"})


async def test_console_read_is_readonly_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "console_read")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is True


async def test_console_open_is_non_readonly_non_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "console_open")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is False


async def test_console_send_is_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "console_send")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is False


async def test_console_close_is_non_readonly_non_destructive_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "console_close")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is True


# Task 2: flash tools (Category.FLASH, opt-in only) + release_place pin refusal


def _flash_config() -> Config:
    # Explicit hostname/username so identity matches _owned_place()'s "host/user"
    # (load_config would otherwise fall back to the real machine's values).
    return load_config(
        env={
            "LG_COORDINATOR": "10.0.0.1:20408",
            "LG_HOSTNAME": "host",
            "LG_USERNAME": "user",
            "LABGRID_MCP_ALLOW": "flash",
        }
    )


def _place_delete_config() -> Config:
    # PLACE_DELETE is opt-in only (decision #13, mirrors _flash_config()) --
    # _config()'s allow=None default no longer registers delete_place/
    # delete_place_match, so their tool tests need this explicit allowlist.
    # Same explicit hostname/username as _flash_config() for _owned_place()/
    # _foreign_place()'s "host/user" identity.
    return load_config(
        env={
            "LG_COORDINATOR": "10.0.0.1:20408",
            "LG_HOSTNAME": "host",
            "LG_USERNAME": "user",
            "LABGRID_MCP_ALLOW": "place_delete",
        }
    )


_FLASH_TOOL_ARGS: dict[str, dict[str, object]] = {
    "flash_dfu": {"place": "p1", "alt": 0, "file": "/nonexistent/flash-dfu.bin"},
    "flash_fastboot": {"place": "p1", "partition": "boot", "file": "/nonexistent/flash-fb.bin"},
    "flash_script": {"place": "p1", "script": "/nonexistent/flash.sh"},
    "bootstrap": {"place": "p1", "file": "/nonexistent/bootstrap.bin"},
    "write_image": {"place": "p1", "file": "/nonexistent/image.bin"},
}


@pytest.mark.parametrize("tool_name,args", _FLASH_TOOL_ARGS.items())
async def test_flash_submitters_reject_unacquired_place(
    tool_name: str, args: dict[str, object]
) -> None:
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[{"name": "p1", "acquired": None}]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, tool_name, args)

    assert jobs.submit_calls == []  # ownership fails before any submission


@pytest.mark.parametrize("tool_name,args", _FLASH_TOOL_ARGS.items())
async def test_flash_submitters_reject_unknown_place(
    tool_name: str, args: dict[str, object]
) -> None:
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match=_OWNERSHIP_ERROR):
        await _call_tool(mcp, tool_name, args)

    assert jobs.submit_calls == []


@pytest.mark.parametrize("tool_name,args", _FLASH_TOOL_ARGS.items())
async def test_flash_submitters_reject_missing_local_file(
    tool_name: str, args: dict[str, object]
) -> None:
    """A missing local file/script is a pinned ToolError -- no submission at all."""
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    path_key = "script" if tool_name == "flash_script" else "file"
    missing = str(args[path_key])
    with pytest.raises(ToolError, match=f"{missing!r} does not exist"):
        await _call_tool(mcp, tool_name, args)

    assert jobs.submit_calls == []


async def test_flash_dfu_submits_job_and_builds_explicit_driver_call(tmp_path: Path) -> None:
    file = tmp_path / "image.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs(submit_value={"job": "j1", "place": "p1", "kind": "dfu"})
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "flash_dfu", {"place": "p1", "alt": 2, "file": str(file)}
    )

    # Additive normalization (_normalize_job_payload): loader is always
    # present, None for every non-bootstrap kind.
    assert structured == {"job": "j1", "place": "p1", "kind": "dfu", "loader": None}
    assert jobs.submit_calls == [("p1", "dfu")]
    # The built closure calls DFUDriver.download(altsetting, filename) with the
    # EXPLICIT local path (§11.11 argv table / "target.env is None" trap).
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("download", (2, str(file)))]


async def test_flash_fastboot_submits_job_and_builds_explicit_driver_call(tmp_path: Path) -> None:
    file = tmp_path / "image.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(mcp, "flash_fastboot", {"place": "p1", "partition": "boot", "file": str(file)})

    assert jobs.submit_calls == [("p1", "fastboot")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("flash", ("boot", str(file)))]


async def test_flash_script_submits_job_and_forwards_args(tmp_path: Path) -> None:
    script = tmp_path / "flash.sh"
    script.write_text("#!/bin/sh\n")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(
        mcp, "flash_script", {"place": "p1", "script": str(script), "args": ["--verbose"]}
    )

    assert jobs.submit_calls == [("p1", "script")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("flash", (str(script), ["--verbose"]))]


async def test_flash_script_defaults_args_to_empty_list(tmp_path: Path) -> None:
    script = tmp_path / "flash.sh"
    script.write_text("#!/bin/sh\n")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(mcp, "flash_script", {"place": "p1", "script": str(script)})

    jobs.built_fns[0]()
    assert jobs.driver.calls == [("flash", (str(script), []))]


async def test_bootstrap_submits_job_and_builds_explicit_driver_call(tmp_path: Path) -> None:
    file = tmp_path / "boot.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(mcp, "bootstrap", {"place": "p1", "file": str(file)})

    assert jobs.submit_calls == [("p1", "bootstrap")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("load", (str(file),))]


async def test_bootstrap_default_loader_matches_explicit_imx(tmp_path: Path) -> None:
    """``loader`` defaults to "imx" -- an explicit "imx" must submit the exact
    same "bootstrap" kind as omitting it (byte-identical to every existing
    caller/job payload, DESIGN §11.11 P5 follow-up)."""
    file = tmp_path / "boot.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "bootstrap", {"place": "p1", "file": str(file), "loader": "imx"}
    )

    assert jobs.submit_calls == [("p1", "bootstrap")]
    # Client-visible shape, default path (pinned): canonical kind, loader None.
    assert structured == {"job": "j1", "place": "p1", "kind": "bootstrap", "loader": None}


async def test_bootstrap_forwards_non_default_loader(tmp_path: Path) -> None:
    """A non-default ``loader`` is forwarded into the submitted job kind --
    the tool only forwards it; TargetManager does the mapping/rejecting
    (DESIGN §11.11 P5 follow-up). The internal "bootstrap:<loader>" encoding
    must NOT leak into the returned payload: kind is normalized back to the
    canonical base and the loader moves to the additive ``loader`` field."""
    file = tmp_path / "boot.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "bootstrap", {"place": "p1", "file": str(file), "loader": "mxs"}
    )

    assert jobs.submit_calls == [("p1", "bootstrap:mxs")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("load", (str(file),))]
    # Client-visible shape (pinned): canonical kind + the additive loader.
    assert structured == {"job": "j1", "place": "p1", "kind": "bootstrap", "loader": "mxs"}


async def test_flash_status_normalizes_encoded_bootstrap_kind() -> None:
    """flash_status on a non-default-loader bootstrap job: the registry's
    stored kind is the internal "bootstrap:mxs" encoding; the client sees the
    canonical kind plus the additive loader field."""
    status: dict[str, object] = {
        "job": "j1",
        "place": "p1",
        "kind": "bootstrap:mxs",
        "state": "running",
        "created": 1.0,
        "finished": None,
        "error": None,
        "truncated": False,
    }
    jobs = _stub_jobs(status_value=status)
    mcp = build_server(
        _flash_config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "flash_status", {"job": "j1"})

    assert structured == {**status, "kind": "bootstrap", "loader": "mxs"}


async def test_sessions_resource_normalizes_encoded_bootstrap_kind() -> None:
    """labgrid://sessions job entries get the same normalization as
    flash_status: canonical kind + additive loader for an encoded bootstrap
    job, loader None for every other kind."""
    jobs_value: list[dict[str, object]] = [
        {"job": "j1", "place": "p1", "kind": "bootstrap:mxs", "state": "running"},
        {"job": "j2", "place": "p2", "kind": "bootstrap", "state": "running"},
        {"job": "j3", "place": "p3", "kind": "dfu", "state": "running"},
    ]
    jobs = _stub_jobs(jobs_value=jobs_value)
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    contents = list(await mcp.read_resource("labgrid://sessions"))

    assert json.loads(contents[0].content) == {
        "consoles": [],
        "forwards": [],
        "jobs": [
            {"job": "j1", "place": "p1", "kind": "bootstrap", "state": "running", "loader": "mxs"},
            {"job": "j2", "place": "p2", "kind": "bootstrap", "state": "running", "loader": None},
            {"job": "j3", "place": "p3", "kind": "dfu", "state": "running", "loader": None},
        ],
    }


async def test_bootstrap_unknown_loader_surfaces_manager_rejection(tmp_path: Path) -> None:
    """The tool does NOT validate ``loader`` itself -- TargetManager's
    rejection (surfaced here as the stub job registry raising the TargetError
    ``flash_driver`` would) propagates as a ToolError naming the valid
    options."""
    file = tmp_path / "boot.bin"
    file.write_bytes(b"data")
    jobs = _stub_jobs(
        submit_value=TargetError(
            "unknown bootstrap loader 'bogus'; valid options: bdimx, imx, mxs, rk, uuu"
        )
    )
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="unknown bootstrap loader 'bogus'"):
        await _call_tool(mcp, "bootstrap", {"place": "p1", "file": str(file), "loader": "bogus"})

    assert jobs.submit_calls == [("p1", "bootstrap:bogus")]


async def test_write_image_submits_job_and_builds_explicit_driver_call(tmp_path: Path) -> None:
    file = tmp_path / "sd.img"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(mcp, "write_image", {"place": "p1", "file": str(file)})

    assert jobs.submit_calls == [("p1", "write_image")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("write_image", (str(file),))]
    # Default kwargs (§11.14) are ALWAYS forwarded, even when the tool's own
    # partition/mode/skip/seek params are all omitted -- reuses target.py's
    # own helper as the source of truth for the expected shape.
    assert jobs.driver.write_image_kwargs_calls == [write_image_kwargs()]


async def test_write_image_forwards_partition_mode_skip_seek(tmp_path: Path) -> None:
    file = tmp_path / "sd.img"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    await _call_tool(
        mcp,
        "write_image",
        {
            "place": "p1",
            "file": str(file),
            "partition": 2,
            "mode": "BMAPTOOL",
            "skip": 4,
            "seek": 8,
        },
    )

    assert jobs.submit_calls == [("p1", "write_image")]
    jobs.built_fns[0]()
    assert jobs.driver.calls == [("write_image", (str(file),))]
    assert jobs.driver.write_image_kwargs_calls == [
        write_image_kwargs(partition=2, mode="BMAPTOOL", skip=4, seek=8)
    ]


async def test_write_image_rejects_bad_mode_before_submit(tmp_path: Path) -> None:
    file = tmp_path / "sd.img"
    file.write_bytes(b"data")
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="unknown write_image mode 'bogus'"):
        await _call_tool(
            mcp, "write_image", {"place": "p1", "file": str(file), "mode": "bogus"}
        )

    assert jobs.submit_calls == []


async def test_write_image_rejects_bad_mode_before_ownership_check(tmp_path: Path) -> None:
    """A bad mode is a tool error even for an unacquired/unknown place and a
    nonexistent file (like set_power's action check, before ANY other
    validation)."""
    jobs = _stub_jobs()
    mcp = build_server(
        _flash_config(),
        _stub(places=[]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="unknown write_image mode 'bogus'"):
        await _call_tool(
            mcp,
            "write_image",
            {"place": "nope", "file": "/nonexistent/image.bin", "mode": "bogus"},
        )

    assert jobs.submit_calls == []


async def test_flash_submit_joberror_is_tool_error(tmp_path: Path) -> None:
    file = tmp_path / "x.bin"
    file.write_bytes(b"x")
    jobs = _stub_jobs(
        submit_value=JobError(
            "place 'p1' already has a running flash job 'j0'; wait for it to finish"
        )
    )
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="already has a running flash job"):
        await _call_tool(mcp, "flash_dfu", {"place": "p1", "alt": 0, "file": str(file)})


async def test_flash_submit_targeterror_is_tool_error(tmp_path: Path) -> None:
    file = tmp_path / "x.bin"
    file.write_bytes(b"x")
    jobs = _stub_jobs(submit_value=TargetError("flash dfu on place 'p1' failed: boom"))
    mcp = build_server(
        _flash_config(),
        _stub(places=[_owned_place()]),
        _session(),
        _targets(),
        _stub_consoles(),
        jobs,
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="flash dfu on place 'p1' failed: boom"):
        await _call_tool(mcp, "flash_dfu", {"place": "p1", "alt": 0, "file": str(file)})


async def test_flash_status_tool_passthrough() -> None:
    status: dict[str, object] = {
        "job": "j1",
        "place": "p1",
        "kind": "dfu",
        "state": "completed",
        "created": 1.0,
        "finished": 2.0,
        "error": None,
        "truncated": False,
    }
    jobs = _stub_jobs(status_value=status)
    mcp = build_server(
        _flash_config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "flash_status", {"job": "j1"})

    # Passthrough plus the additive loader field (None for non-bootstrap).
    assert structured == {**status, "loader": None}
    assert jobs.status_calls == ["j1"]


async def test_flash_status_tool_unknown_job_is_tool_error() -> None:
    jobs = _stub_jobs(status_value=JobError("unknown flash job 'nope'"))
    mcp = build_server(
        _flash_config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    with pytest.raises(ToolError, match="unknown flash job 'nope'"):
        await _call_tool(mcp, "flash_status", {"job": "nope"})


async def test_flash_logs_tool_passthrough_forwards_max_bytes() -> None:
    jobs = _stub_jobs(logs_value={"job": "j1", "data": "hi", "bytes": 2, "truncated": False})
    mcp = build_server(
        _flash_config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "flash_logs", {"job": "j1", "max_bytes": 10})

    assert structured == {"job": "j1", "data": "hi", "bytes": 2, "truncated": False}
    assert jobs.logs_calls == [("j1", 10)]


async def test_flash_logs_tool_unknown_job_is_tool_error() -> None:
    jobs = _stub_jobs(logs_value=JobError("unknown flash job 'nope'"))
    mcp = build_server(
        _flash_config(), _stub(), _session(), _targets(), _stub_consoles(), jobs, _stub_forwards()
    )

    with pytest.raises(ToolError, match="unknown flash job 'nope'"):
        await _call_tool(mcp, "flash_logs", {"job": "nope"})


async def test_flash_submitters_are_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _flash_config(),
        _stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    tools = await mcp.list_tools()

    for tool_name in ["flash_dfu", "flash_fastboot", "flash_script", "bootstrap", "write_image"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is False
        assert tool.annotations.destructiveHint is True
        assert tool.annotations.idempotentHint is False


async def test_flash_status_and_logs_are_readonly_idempotent_annotated() -> None:
    mcp = build_server(
        _flash_config(),
        _stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    tools = await mcp.list_tools()

    for tool_name in ["flash_status", "flash_logs"]:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False
        assert tool.annotations.idempotentHint is True


async def test_release_place_refuses_before_rpc_while_flash_job_runs() -> None:
    """A running flash job refuses release BEFORE the coordinator RPC (fix
    round 1): the release/session call must never be made, so coordinator-side
    ownership is never given up only for the local invalidate to then fail."""
    session = _session(release_value={"name": "p1", "acquired": None})
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    jobs = _stub_jobs()
    jobs.running_job = "j1"
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        session,
        _targets(),
        consoles,
        jobs,
        forwards,
    )

    with pytest.raises(ToolError, match="flash job 'j1' is running"):
        await _call_tool(mcp, "release_place", {"name": "p1"})

    assert session.release_calls == []  # coordinator RPC never made
    assert consoles.close_place_calls == []  # console untouched too
    assert forwards.close_place_calls == []  # forwards untouched too


async def test_release_place_kick_refuses_before_rpc_while_flash_job_runs() -> None:
    """kick does not override the pre-RPC refusal either -- a mid-write flash
    job on OUR side is not the coordinator's business to overrule."""
    session = _session(release_value={"name": "p1", "acquired": None})
    consoles = _stub_consoles()
    forwards = _stub_forwards()
    jobs = _stub_jobs()
    jobs.running_job = "j1"
    mcp = build_server(_config(), _stub(), session, _targets(), consoles, jobs, forwards)

    with pytest.raises(ToolError, match="flash job 'j1' is running"):
        await _call_tool(mcp, "release_place", {"name": "p1", "kick": True})

    assert session.release_calls == []
    assert consoles.close_place_calls == []
    assert forwards.close_place_calls == []


async def test_release_place_surfaces_flash_pin_refusal_as_tool_error() -> None:
    """Backstop for the residual check-then-act window: a job that pins the
    place between the pre-RPC check and invalidate() still surfaces the
    manager's TargetError as a ToolError naming the job (§11.11)."""

    class _PinnedTargets(_StubTargets):
        async def invalidate(self, place: str) -> None:
            raise TargetError(
                f"cannot invalidate place {place!r}: flash job 'j1' is running; cancel it first"
            )

    session = _session(release_value={"name": "p1", "acquired": None})
    mcp = build_server(
        _config(),
        _stub(places=[_owned_place()]),
        session,
        _PinnedTargets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="flash job 'j1' is running"):
        await _call_tool(mcp, "release_place", {"name": "p1"})


async def test_release_place_kick_does_not_override_flash_pin_refusal() -> None:
    """kick releases the place at the coordinator regardless of holder, but it
    must NOT bypass the local pin refusal -- ripping out a mid-write driver
    can brick hardware regardless of who asked."""

    class _PinnedTargets(_StubTargets):
        async def invalidate(self, place: str) -> None:
            raise TargetError(
                f"cannot invalidate place {place!r}: flash job 'j1' is running; cancel it first"
            )

    session = _session(release_value={"name": "p1", "acquired": None})
    mcp = build_server(
        _config(),
        _stub(),
        session,
        _PinnedTargets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="flash job 'j1' is running"):
        await _call_tool(mcp, "release_place", {"name": "p1", "kick": True})


async def test_sessions_resource() -> None:
    sessions_value: list[dict[str, object]] = [
        {
            "session": "s1",
            "place": "p1",
            "state": "open",
            "created": 1.0,
            "last_used": 2.0,
            "buffered_bytes": 3,
        }
    ]
    jobs_value: list[dict[str, object]] = [
        {
            "job": "j1",
            "place": "p2",
            "kind": "dfu",
            "state": "running",
            "created": 1.0,
            "finished": None,
            "buffered_bytes": 0,
        }
    ]
    forwards_value: list[dict[str, object]] = [
        {
            "forward": "f1",
            "place": "p1",
            "local_port": 40000,
            "remote_port": 8080,
            "direction": "local",
        }
    ]
    consoles = _stub_consoles(sessions_value=sessions_value)
    jobs = _stub_jobs(jobs_value=jobs_value)
    forwards = _stub_forwards(sessions_value=forwards_value)
    mcp = build_server(_config(), _stub(), _session(), _targets(), consoles, jobs, forwards)

    contents = list(await mcp.read_resource("labgrid://sessions"))

    assert len(contents) == 1
    # New shape (Task 2): a "consoles"/"forwards"/"jobs" sibling triple, not a
    # bare list. Job entries gain the additive loader field (None non-bootstrap).
    expected_jobs = [{**j, "loader": None} for j in jobs_value]
    assert json.loads(contents[0].content) == {
        "consoles": sessions_value,
        "forwards": forwards_value,
        "jobs": expected_jobs,
    }


async def test_sessions_resource_empty() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    contents = list(await mcp.read_resource("labgrid://sessions"))

    assert json.loads(contents[0].content) == {"consoles": [], "forwards": [], "jobs": []}


async def test_sessions_resource_registered_in_readonly_mode() -> None:
    """Resources stay unconditional even under LABGRID_MCP_READONLY (§gating)."""
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    consoles = _stub_consoles(sessions_value=[{"session": "s1"}])
    jobs = _stub_jobs(jobs_value=[{"job": "j1"}])
    forwards = _stub_forwards(sessions_value=[{"forward": "f1"}])
    mcp = build_server(config, _stub(), _session(), _targets(), consoles, jobs, forwards)

    contents = list(await mcp.read_resource("labgrid://sessions"))

    assert json.loads(contents[0].content) == {
        "consoles": [{"session": "s1"}],
        "forwards": [{"forward": "f1"}],
        "jobs": [{"job": "j1", "loader": None}],
    }


# ---- metadata tools + wait_for_change (Task 2, design §11.12) -------------


def _foreign_place(name: str = "p1", owner: str = "otherhost/otheruser") -> dict[str, object]:
    """A place snapshot acquired by someone OTHER than ``_config()``'s identity."""
    return {"name": name, "acquired": owner}


async def test_add_place_tool() -> None:
    client = _stub()
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "add_place", {"name": "p1"})

    assert structured == {"place": "p1", "added": True}
    assert client.add_place_calls == ["p1"]


async def test_add_place_tool_error() -> None:
    client = _stub()
    client.add_place_error = CoordinatorError("Place p1 already exists")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="already exists"):
        await _call_tool(mcp, "add_place", {"name": "p1"})


async def test_delete_place_tool() -> None:
    client = _stub(places=[{"name": "p1"}])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "delete_place", {"name": "p1"})

    assert structured == {"place": "p1", "deleted": True}
    assert client.delete_place_calls == ["p1"]


async def test_delete_place_maps_coordinator_already_exists_misuse_to_not_found() -> None:
    """§11.12: DeletePlace of a nonexistent place raises the misreported
    ALREADY_EXISTS "does not exist" -- mapped here to a clean not-found error.
    """
    client = _stub()
    client.delete_place_error = CoordinatorError("DeletePlace failed: Place ghost does not exist")
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="place 'ghost' does not exist"):
        await _call_tool(mcp, "delete_place", {"name": "ghost"})


async def test_delete_place_other_error_passes_through() -> None:
    client = _stub()
    client.delete_place_error = CoordinatorError("not connected to coordinator")
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="not connected"):
        await _call_tool(mcp, "delete_place", {"name": "p1"})


async def test_delete_place_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "delete_place", {"name": "p1"})
    assert client.delete_place_calls == []


async def test_delete_place_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "delete_place", {"name": "p1", "force": True})

    assert structured == {"place": "p1", "deleted": True}
    assert client.delete_place_calls == ["p1"]


async def test_delete_place_refuses_own_acquired_without_force() -> None:
    """Unlike the other 7 mutators, delete_place refuses even OUR OWN
    acquisition without force -- deleting it would strand the acquisition.
    """
    client = _stub(places=[_owned_place("p1")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="host/user"):
        await _call_tool(mcp, "delete_place", {"name": "p1"})
    assert client.delete_place_calls == []


async def test_delete_place_force_overrides_own_acquired() -> None:
    client = _stub(places=[_owned_place("p1")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "delete_place", {"name": "p1", "force": True})

    assert structured == {"place": "p1", "deleted": True}


async def test_add_place_alias_tool_returns_refreshed_place() -> None:
    client = _stub(places=[{"name": "p1"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "add_place_alias", {"place": "p1", "alias": "a1"})

    assert client.add_place_alias_calls == [("p1", "a1")]
    assert structured == {"place": {"name": "p1", "aliases": ["a1"]}}


async def test_add_place_alias_allows_own_acquisition_without_force() -> None:
    """Unlike delete_place, editing metadata on a place WE hold needs no force."""
    client = _stub(places=[_owned_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "add_place_alias", {"place": "p1", "alias": "a1"})

    assert client.add_place_alias_calls == [("p1", "a1")]


async def test_add_place_alias_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "add_place_alias", {"place": "p1", "alias": "a1"})
    assert client.add_place_alias_calls == []


async def test_add_place_alias_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "add_place_alias", {"place": "p1", "alias": "a1", "force": True}
    )

    assert client.add_place_alias_calls == [("p1", "a1")]
    assert structured is not None
    assert structured["place"]["aliases"] == ["a1"]


async def test_add_place_alias_tool_error() -> None:
    client = _stub(places=[{"name": "p1"}])
    client.add_place_alias_error = CoordinatorError("boom")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="boom"):
        await _call_tool(mcp, "add_place_alias", {"place": "p1", "alias": "a1"})


async def test_add_place_alias_falls_back_to_stale_snapshot_when_sync_never_lands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bounded catch-up (mirrors session.py's _snapshot_synced): if the RPC
    succeeds but our snapshot never reflects it (ClientStream update lost/
    delayed beyond the bound), return whatever is cached instead of hanging.
    """
    monkeypatch.setattr(server_module, "_METADATA_SYNC_TIMEOUT_S", 0.0)

    class _NoMutateClient(_StubClient):
        async def add_place_alias(self, name: str, alias: str) -> None:
            self.add_place_alias_calls.append((name, alias))
            # Deliberately do NOT mutate places_value.

    client = _NoMutateClient(
        info_value=_stub().info_value,
        places_value=[{"name": "p1"}],
        resources_value=[],
        reservations_value=None,
    )
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "add_place_alias", {"place": "p1", "alias": "a1"})

    assert structured == {"place": {"name": "p1"}}


async def test_delete_place_alias_tool_returns_refreshed_place() -> None:
    client = _stub(places=[{"name": "p1", "aliases": ["a1", "a2"]}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "delete_place_alias", {"place": "p1", "alias": "a1"}
    )

    assert client.delete_place_alias_calls == [("p1", "a1")]
    assert structured == {"place": {"name": "p1", "aliases": ["a2"]}}


async def test_delete_place_alias_prevalidates_missing_alias_zero_rpc_calls() -> None:
    """§11.12 trap: the coordinator raises an uncaught KeyError (gRPC UNKNOWN)
    for a nonexistent alias -- pre-checked here instead, before any RPC.
    """
    client = _stub(places=[{"name": "p1", "aliases": ["a1"]}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="has no alias 'nope'"):
        await _call_tool(mcp, "delete_place_alias", {"place": "p1", "alias": "nope"})
    assert client.delete_place_alias_calls == []


async def test_delete_place_alias_prevalidates_before_ownership_check() -> None:
    """A typo'd alias is reported even on a foreign-acquired place -- shape
    validation runs before the ownership check (mirrors set_power's
    action-before-ownership order)."""
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="has no alias"):
        await _call_tool(mcp, "delete_place_alias", {"place": "p1", "alias": "nope"})


async def test_delete_place_alias_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    client.places_value[0]["aliases"] = ["a1"]
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "delete_place_alias", {"place": "p1", "alias": "a1"})
    assert client.delete_place_alias_calls == []


async def test_delete_place_alias_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    client.places_value[0]["aliases"] = ["a1"]
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "delete_place_alias", {"place": "p1", "alias": "a1", "force": True}
    )

    assert client.delete_place_alias_calls == [("p1", "a1")]
    assert structured is not None
    assert structured["place"]["aliases"] == []


async def test_delete_place_alias_tool_error() -> None:
    client = _stub(places=[{"name": "p1", "aliases": ["a1"]}])
    client.delete_place_alias_error = CoordinatorError("boom")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="boom"):
        await _call_tool(mcp, "delete_place_alias", {"place": "p1", "alias": "a1"})


async def test_set_place_tags_tool_merges_and_deletes_empty_values() -> None:
    client = _stub(places=[{"name": "p1", "tags": {"board": "old", "keep": "yes"}}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "set_place_tags", {"place": "p1", "tags": {"board": "new", "keep": ""}}
    )

    assert client.set_place_tags_calls == [("p1", {"board": "new", "keep": ""})]
    assert structured == {"place": {"name": "p1", "tags": {"board": "new"}}}


async def test_set_place_tags_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "set_place_tags", {"place": "p1", "tags": {"board": "x"}})
    assert client.set_place_tags_calls == []


async def test_set_place_tags_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "set_place_tags", {"place": "p1", "tags": {"board": "x"}, "force": True}
    )

    assert client.set_place_tags_calls == [("p1", {"board": "x"})]
    assert structured is not None


async def test_set_place_tags_tool_error() -> None:
    client = _stub(places=[{"name": "p1"}])
    client.set_place_tags_error = CoordinatorError("boom")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="boom"):
        await _call_tool(mcp, "set_place_tags", {"place": "p1", "tags": {"board": "x"}})


async def test_set_place_comment_tool_returns_refreshed_place() -> None:
    client = _stub(places=[{"name": "p1", "comment": ""}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "set_place_comment", {"place": "p1", "comment": "under repair"}
    )

    assert client.set_place_comment_calls == [("p1", "under repair")]
    assert structured == {"place": {"name": "p1", "comment": "under repair"}}


async def test_set_place_comment_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "set_place_comment", {"place": "p1", "comment": "x"})
    assert client.set_place_comment_calls == []


async def test_set_place_comment_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "set_place_comment", {"place": "p1", "comment": "x", "force": True}
    )

    assert client.set_place_comment_calls == [("p1", "x")]


async def test_set_place_comment_tool_error() -> None:
    client = _stub(places=[{"name": "p1"}])
    client.set_place_comment_error = CoordinatorError("boom")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="boom"):
        await _call_tool(mcp, "set_place_comment", {"place": "p1", "comment": "x"})


async def test_add_place_match_tool_returns_refreshed_place_and_forwards_rename() -> None:
    client = _stub(places=[{"name": "p1"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp,
        "add_place_match",
        {"place": "p1", "pattern": "exp/grp/cls", "rename": "r1"},
    )

    assert client.add_place_match_calls == [("p1", "exp/grp/cls", "r1")]
    assert structured == {
        "place": {
            "name": "p1",
            "matches": [
                {"exporter": "exp", "group": "grp", "cls": "cls", "name": None, "rename": "r1"}
            ],
        }
    }


@pytest.mark.parametrize("bad_pattern", ["exp/grp", "exp/grp/cls/name/extra", "exp//cls"])
async def test_add_place_match_rejects_bad_arity_zero_rpc_calls(bad_pattern: str) -> None:
    client = _stub(places=[{"name": "p1"}])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="3 or 4 non-empty"):
        await _call_tool(mcp, "add_place_match", {"place": "p1", "pattern": bad_pattern})
    assert client.add_place_match_calls == []


async def test_add_place_match_validates_pattern_before_ownership_check() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="3 or 4 non-empty"):
        await _call_tool(mcp, "add_place_match", {"place": "p1", "pattern": "bad"})


async def test_add_place_match_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "add_place_match", {"place": "p1", "pattern": "exp/grp/cls"})
    assert client.add_place_match_calls == []


async def test_add_place_match_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(
        mcp, "add_place_match", {"place": "p1", "pattern": "exp/grp/cls", "force": True}
    )

    assert client.add_place_match_calls == [("p1", "exp/grp/cls", None)]
    assert structured is not None


async def test_add_place_match_tool_error() -> None:
    client = _stub(places=[{"name": "p1"}])
    client.add_place_match_error = CoordinatorError("Match e/g/c already exists")
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    with pytest.raises(ToolError, match="already exists"):
        await _call_tool(mcp, "add_place_match", {"place": "p1", "pattern": "exp/grp/cls"})


async def test_delete_place_match_tool_returns_refreshed_place_and_forwards_rename() -> None:
    client = _stub(
        places=[
            {
                "name": "p1",
                "matches": [
                    {"exporter": "exp", "group": "grp", "cls": "cls", "name": None, "rename": "r1"}
                ],
            }
        ]
    )
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp,
        "delete_place_match",
        {"place": "p1", "pattern": "exp/grp/cls", "rename": "r1"},
    )

    assert client.delete_place_match_calls == [("p1", "exp/grp/cls", "r1")]
    assert structured == {"place": {"name": "p1", "matches": []}}


@pytest.mark.parametrize("bad_pattern", ["exp/grp", "exp/grp/cls/name/extra"])
async def test_delete_place_match_rejects_bad_arity_zero_rpc_calls(bad_pattern: str) -> None:
    client = _stub(places=[{"name": "p1"}])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="3 or 4 non-empty"):
        await _call_tool(mcp, "delete_place_match", {"place": "p1", "pattern": bad_pattern})
    assert client.delete_place_match_calls == []


async def test_delete_place_match_refuses_foreign_acquired_naming_owner() -> None:
    client = _stub(places=[_foreign_place("p1", owner="otherhost/otheruser")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="otherhost/otheruser"):
        await _call_tool(mcp, "delete_place_match", {"place": "p1", "pattern": "exp/grp/cls"})
    assert client.delete_place_match_calls == []


async def test_delete_place_match_force_overrides_foreign_acquired() -> None:
    client = _stub(places=[_foreign_place("p1")])
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "delete_place_match", {"place": "p1", "pattern": "exp/grp/cls", "force": True}
    )

    assert client.delete_place_match_calls == [("p1", "exp/grp/cls", None)]
    assert structured is not None


async def test_delete_place_match_tool_error() -> None:
    client = _stub(places=[{"name": "p1"}])
    client.delete_place_match_error = CoordinatorError("Match e/g/c does not exist in p1")
    mcp = build_server(
        _place_delete_config(),
        client,
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="does not exist"):
        await _call_tool(mcp, "delete_place_match", {"place": "p1", "pattern": "exp/grp/cls"})


async def test_metadata_mutators_are_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()

    for tool_name in _METADATA_TOOL_NAMES:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None, tool_name
        assert tool.annotations.readOnlyHint is False, tool_name
        assert tool.annotations.destructiveHint is True, tool_name
        assert tool.annotations.idempotentHint is False, tool_name


async def test_place_delete_mutators_are_destructive_non_idempotent_annotated() -> None:
    mcp = build_server(
        _place_delete_config(),
        _stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    tools = await mcp.list_tools()

    for tool_name in _PLACE_DELETE_TOOL_NAMES:
        tool = next(t for t in tools if t.name == tool_name)
        assert tool.annotations is not None, tool_name
        assert tool.annotations.readOnlyHint is False, tool_name
        assert tool.annotations.destructiveHint is True, tool_name
        assert tool.annotations.idempotentHint is False, tool_name


# ---- wait_for_change --------------------------------------------------


async def test_wait_for_change_bootstrap_returns_current_cursor_without_waiting() -> None:
    client = _stub()
    client.change_cursor_value = 7
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "wait_for_change", {})

    assert structured == {"cursor": 7, "changed": False}
    assert client.wait_for_change_calls == []


async def test_wait_for_change_returns_new_cursor_when_changed() -> None:
    client = _stub()
    client.wait_for_change_value = 5
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "wait_for_change", {"cursor": 3})

    assert structured == {"cursor": 5, "changed": True}
    assert client.wait_for_change_calls == [(3, 25.0)]


async def test_wait_for_change_reports_unchanged_on_timeout() -> None:
    client = _stub()
    client.wait_for_change_value = 3  # echoes the same cursor back (no change)
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    _content, structured = await _call_tool(mcp, "wait_for_change", {"cursor": 3})

    assert structured == {"cursor": 3, "changed": False}


async def test_wait_for_change_clamps_timeout_to_25s() -> None:
    client = _stub()
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "wait_for_change", {"cursor": 1, "timeout_s": 9999.0})

    assert client.wait_for_change_calls == [(1, 25.0)]


async def test_wait_for_change_does_not_clamp_below_25s() -> None:
    client = _stub()
    mcp = build_server(
        _config(), client, _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    await _call_tool(mcp, "wait_for_change", {"cursor": 1, "timeout_s": 2.5})

    assert client.wait_for_change_calls == [(1, 2.5)]


async def test_wait_for_change_is_readonly_idempotent_annotated() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )

    tools = await mcp.list_tools()
    tool = next(t for t in tools if t.name == "wait_for_change")

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is True


# ---- SSH tools (Category.SSH) + forward tools (§11.13) ---------------------


class _FakeSSHDriver:
    """Fake SSHDriver: scripts ``run`` and records ``put``/``get`` (target.py)."""

    def __init__(
        self,
        run_result: object = (["out"], ["err"], 0),
        put_error: Exception | None = None,
        get_error: Exception | None = None,
        get_writes: bytes | None = None,
    ) -> None:
        self.run_result = run_result
        self.put_error = put_error
        self.get_error = get_error
        self.get_writes = get_writes
        self.run_calls: list[tuple[str, float | None]] = []
        self.put_calls: list[tuple[str, str]] = []
        self.get_calls: list[tuple[str, str]] = []

    def run(self, cmd: str, timeout: float | None = None) -> object:
        self.run_calls.append((cmd, timeout))
        if isinstance(self.run_result, BaseException):
            raise self.run_result
        return self.run_result

    def put(self, filename: str, remotepath: str = "") -> None:
        self.put_calls.append((filename, remotepath))
        if self.put_error is not None:
            raise self.put_error

    def get(self, filename: str, destination: str = ".") -> None:
        self.get_calls.append((filename, destination))
        if self.get_error is not None:
            raise self.get_error
        if self.get_writes is not None:
            Path(destination).write_bytes(self.get_writes)


def _ssh_targets(driver: object) -> _StubTargets:
    return _targets(ssh_driver_value=driver)


def _owned_stub() -> _StubClient:
    return _stub(places=[_owned_place()])


# ssh_run


async def test_ssh_run_joins_line_lists_and_returns_exit_code() -> None:
    driver = _FakeSSHDriver(run_result=(["l1", "l2"], ["e1"], 0))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "echo hi"})

    assert structured == {
        "place": "p1",
        "stdout": "l1\nl2",
        "stderr": "e1",
        "exit_code": 0,
    }
    assert driver.run_calls == [("echo hi", 30.0)]  # default timeout passed through


async def test_ssh_run_unowned_place_errors_before_driver() -> None:
    driver = _FakeSSHDriver()
    mcp = build_server(
        _config(),
        _stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="not acquired by this server"):
        await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "x"})
    assert driver.run_calls == []


async def test_ssh_run_missing_keyfile_target_error_wraps() -> None:
    targets = _targets(ssh_driver_value=TargetError("LABGRID_MCP_SSH_KEYFILE is not set"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        targets,
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="LABGRID_MCP_SSH_KEYFILE is not set"):
        await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "x"})


async def test_ssh_run_driver_failure_wraps_tool_error() -> None:
    driver = _FakeSSHDriver(run_result=RuntimeError("ssh exited 255"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="ssh_run on 'p1' failed: ssh exited 255"):
        await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "x"})


async def test_ssh_run_timeout_surfaces_clean_tool_error() -> None:
    driver = _FakeSSHDriver(run_result=subprocess.TimeoutExpired(cmd="ssh", timeout=1.0))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="timed out after"):
        await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "x", "timeout_s": 1.0})


async def test_ssh_run_outer_wait_for_has_grace_over_inner_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The driver's own ``timeout=timeout_s`` is the sole enforcer; the outer
    ``asyncio.wait_for`` backstop must be given timeout_s + 5.0 (not the same
    timeout_s), so it only fires for a wedged thread and never co-fires with
    the inner timeout (which would otherwise leave the wait_for's own
    exception unretrieved)."""
    driver = _FakeSSHDriver(run_result=(["out"], ["err"], 0))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    seen_timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def _spy_wait_for(fut: object, timeout: float) -> object:
        seen_timeouts.append(timeout)
        return await real_wait_for(fut, timeout)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "wait_for", _spy_wait_for)

    await _call_tool(mcp, "ssh_run", {"place": "p1", "command": "x", "timeout_s": 3.0})

    assert seen_timeouts == [8.0]  # timeout_s + 5.0 grace, not the bare timeout_s


async def test_ssh_run_is_destructive_annotated() -> None:
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(_FakeSSHDriver()),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )
    tool = next(t for t in await mcp.list_tools() if t.name == "ssh_run")
    assert tool.annotations is not None
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.readOnlyHint is False


# put_file


async def test_put_file_returns_local_size(tmp_path: Path) -> None:
    src = tmp_path / "f.bin"
    src.write_bytes(b"hello")
    driver = _FakeSSHDriver()
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "put_file", {"place": "p1", "local_path": str(src), "remote_path": "/tmp/f"}
    )

    assert structured == {"place": "p1", "put": "/tmp/f", "bytes": 5}
    assert driver.put_calls == [(str(src), "/tmp/f")]


async def test_put_file_missing_local_file_errors_before_driver(tmp_path: Path) -> None:
    driver = _FakeSSHDriver()
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="does not exist"):
        await _call_tool(
            mcp,
            "put_file",
            {"place": "p1", "local_path": str(tmp_path / "nope"), "remote_path": "/tmp/f"},
        )
    assert driver.put_calls == []


async def test_put_file_driver_failure_wraps_tool_error(tmp_path: Path) -> None:
    src = tmp_path / "f.bin"
    src.write_bytes(b"x")
    driver = _FakeSSHDriver(put_error=RuntimeError("scp failed"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="put_file to 'p1' failed: scp failed"):
        await _call_tool(
            mcp, "put_file", {"place": "p1", "local_path": str(src), "remote_path": "/tmp/f"}
        )


# get_file


async def test_get_file_writes_local_and_reports_size(tmp_path: Path) -> None:
    dest = tmp_path / "out.bin"
    driver = _FakeSSHDriver(get_writes=b"abcd")
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp, "get_file", {"place": "p1", "remote_path": "/tmp/r", "local_path": str(dest)}
    )

    assert structured == {"place": "p1", "got": str(dest), "bytes": 4}
    assert driver.get_calls == [("/tmp/r", str(dest))]


async def test_get_file_refuses_existing_without_overwrite(tmp_path: Path) -> None:
    dest = tmp_path / "out.bin"
    dest.write_bytes(b"old")
    driver = _FakeSSHDriver()
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="already exists"):
        await _call_tool(
            mcp, "get_file", {"place": "p1", "remote_path": "/tmp/r", "local_path": str(dest)}
        )
    assert driver.get_calls == []


async def test_get_file_overwrite_true_allows_existing(tmp_path: Path) -> None:
    dest = tmp_path / "out.bin"
    dest.write_bytes(b"old")
    driver = _FakeSSHDriver(get_writes=b"new-content")
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    _content, structured = await _call_tool(
        mcp,
        "get_file",
        {"place": "p1", "remote_path": "/tmp/r", "local_path": str(dest), "overwrite": True},
    )

    assert structured == {"place": "p1", "got": str(dest), "bytes": len(b"new-content")}


async def test_get_file_missing_parent_dir_errors(tmp_path: Path) -> None:
    dest = tmp_path / "no_such_dir" / "out.bin"
    driver = _FakeSSHDriver()
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(driver),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )

    with pytest.raises(ToolError, match="parent directory"):
        await _call_tool(
            mcp, "get_file", {"place": "p1", "remote_path": "/tmp/r", "local_path": str(dest)}
        )
    assert driver.get_calls == []


async def test_get_file_is_not_readonly_but_non_destructive_annotated() -> None:
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _ssh_targets(_FakeSSHDriver()),
        _stub_consoles(),
        _stub_jobs(),
        _stub_forwards(),
    )
    tool = next(t for t in await mcp.list_tools() if t.name == "get_file")
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is False
    assert tool.annotations.idempotentHint is False


# forward_open / forward_list / forward_close


async def test_forward_open_returns_registry_handle() -> None:
    forwards = _stub_forwards(
        open_value={"forward": "fwd1", "place": "p1", "local_port": 40000, "remote_port": 8080}
    )
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    _content, structured = await _call_tool(
        mcp, "forward_open", {"place": "p1", "remote_port": 8080}
    )

    assert structured == {
        "forward": "fwd1",
        "place": "p1",
        "local_port": 40000,
        "remote_port": 8080,
    }
    assert forwards.open_calls == [("p1", 8080, 0)]


async def test_forward_open_unowned_place_errors_before_registry() -> None:
    forwards = _stub_forwards()
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    with pytest.raises(ToolError, match="not acquired by this server"):
        await _call_tool(mcp, "forward_open", {"place": "p1", "remote_port": 8080})
    assert forwards.open_calls == []


async def test_forward_open_forward_error_wraps() -> None:
    forwards = _stub_forwards(open_value=ForwardError("ssh -O forward exited 255"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    with pytest.raises(ToolError, match="ssh -O forward exited 255"):
        await _call_tool(mcp, "forward_open", {"place": "p1", "remote_port": 8080})


async def test_forward_open_target_error_wraps() -> None:
    forwards = _stub_forwards(open_value=TargetError("LABGRID_MCP_SSH_KEYFILE is not set"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    with pytest.raises(ToolError, match="LABGRID_MCP_SSH_KEYFILE is not set"):
        await _call_tool(mcp, "forward_open", {"place": "p1", "remote_port": 8080})


# forward_remote_open (-R, §11.14)


async def test_forward_remote_open_returns_registry_handle() -> None:
    forwards = _stub_forwards(
        open_remote_value={
            "forward": "fwd1",
            "place": "p1",
            "direction": "remote",
            "remote_port": 8080,
            "local_port": 8081,
        }
    )
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    _content, structured = await _call_tool(
        mcp, "forward_remote_open", {"place": "p1", "remote_port": 8080, "local_port": 8081}
    )

    assert structured == {
        "forward": "fwd1",
        "place": "p1",
        "direction": "remote",
        "remote_port": 8080,
        "local_port": 8081,
    }
    assert forwards.open_remote_calls == [("p1", 8080, 8081)]


async def test_forward_remote_open_unowned_place_errors_before_registry() -> None:
    forwards = _stub_forwards()
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    with pytest.raises(ToolError, match="not acquired by this server"):
        await _call_tool(
            mcp, "forward_remote_open", {"place": "p1", "remote_port": 8080, "local_port": 8081}
        )
    assert forwards.open_remote_calls == []


async def test_forward_remote_open_forward_error_wraps() -> None:
    forwards = _stub_forwards(open_remote_value=ForwardError("ssh -O forward -R exited 255"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    with pytest.raises(ToolError, match="ssh -O forward -R exited 255"):
        await _call_tool(
            mcp, "forward_remote_open", {"place": "p1", "remote_port": 8080, "local_port": 8081}
        )


async def test_forward_remote_open_target_error_wraps() -> None:
    forwards = _stub_forwards(open_remote_value=TargetError("LABGRID_MCP_SSH_KEYFILE is not set"))
    mcp = build_server(
        _config(),
        _owned_stub(),
        _session(),
        _targets(),
        _stub_consoles(),
        _stub_jobs(),
        forwards,
    )

    with pytest.raises(ToolError, match="LABGRID_MCP_SSH_KEYFILE is not set"):
        await _call_tool(
            mcp, "forward_remote_open", {"place": "p1", "remote_port": 8080, "local_port": 8081}
        )


async def test_forward_list_returns_registry_sessions() -> None:
    entries: list[dict[str, object]] = [
        {
            "forward": "f1",
            "place": "p1",
            "local_port": 40000,
            "remote_port": 8080,
            "direction": "local",
        },
        {
            "forward": "f2",
            "place": "p1",
            "local_port": 8081,
            "remote_port": 8082,
            "direction": "remote",
        },
    ]
    forwards = _stub_forwards(sessions_value=entries)
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    _content, structured = await _call_tool(mcp, "forward_list", {})

    assert structured == {"forwards": entries}


async def test_forward_list_survives_readonly() -> None:
    config = load_config(env={"LG_COORDINATOR": "10.0.0.1:20408", "LABGRID_MCP_READONLY": "1"})
    forwards = _stub_forwards(sessions_value=[])
    mcp = build_server(
        config, _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    _content, structured = await _call_tool(mcp, "forward_list", {})
    assert structured == {"forwards": []}


async def test_forward_close_reports_closed() -> None:
    forwards = _stub_forwards(close_value={"closed": "f1"})
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    _content, structured = await _call_tool(mcp, "forward_close", {"forward": "f1"})

    assert structured == {"closed": "f1"}
    assert forwards.close_calls == ["f1"]


async def test_forward_close_unknown_wraps_tool_error() -> None:
    forwards = _stub_forwards(close_value=ForwardError("unknown forward 'nope'"))
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), forwards
    )

    with pytest.raises(ToolError, match="unknown forward"):
        await _call_tool(mcp, "forward_close", {"forward": "nope"})


async def test_forward_tools_annotations() -> None:
    mcp = build_server(
        _config(), _stub(), _session(), _targets(), _stub_consoles(), _stub_jobs(), _stub_forwards()
    )
    tools = {t.name: t for t in await mcp.list_tools()}

    fl = tools["forward_list"]
    assert fl.annotations is not None
    assert fl.annotations.readOnlyHint is True
    assert fl.annotations.idempotentHint is True

    fo = tools["forward_open"]
    assert fo.annotations is not None
    assert fo.annotations.destructiveHint is False
    assert fo.annotations.idempotentHint is False

    fro = tools["forward_remote_open"]
    assert fro.annotations is not None
    assert fro.annotations.destructiveHint is False
    assert fro.annotations.idempotentHint is False

    fc = tools["forward_close"]
    assert fc.annotations is not None
    assert fc.annotations.destructiveHint is False
    assert fc.annotations.idempotentHint is True
