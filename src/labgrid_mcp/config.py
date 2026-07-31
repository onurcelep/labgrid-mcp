"""Configuration from native labgrid LG_* and labgrid-mcp LABGRID_MCP_* env vars."""

from __future__ import annotations

import getpass
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_COORDINATOR_HOST = "127.0.0.1"
DEFAULT_COORDINATOR_PORT = 20408
DEFAULT_ACQUIRE_TIMEOUT = 120.0

_TRUTHY = frozenset({"1", "true", "yes"})


@dataclass(frozen=True)
class Config:
    coordinator: str
    hostname: str
    username: str
    readonly: bool
    allow: frozenset[str] | None
    acquire_timeout: float
    ssh_keyfile: str | None = None

    @property
    def identity(self) -> str:
        return f"{self.hostname}/{self.username}"


def _normalize_coordinator(raw: str) -> str:
    if ":" in raw:
        return raw
    return f"{raw}:{DEFAULT_COORDINATOR_PORT}"


def load_config(env: Mapping[str, str] | None = None) -> Config:
    e: Mapping[str, str] = os.environ if env is None else env
    allow_raw = e.get("LABGRID_MCP_ALLOW")
    allow = (
        None
        if allow_raw is None
        else frozenset(t.strip().lower() for t in allow_raw.split(",") if t.strip())
    )
    return Config(
        coordinator=_normalize_coordinator(
            e.get("LG_COORDINATOR", f"{DEFAULT_COORDINATOR_HOST}:{DEFAULT_COORDINATOR_PORT}")
        ),
        hostname=e.get("LG_HOSTNAME") or socket.gethostname(),
        username=e.get("LG_USERNAME") or getpass.getuser(),
        readonly=e.get("LABGRID_MCP_READONLY", "").lower() in _TRUTHY,
        allow=allow,
        acquire_timeout=float(e.get("LABGRID_MCP_ACQUIRE_TIMEOUT", DEFAULT_ACQUIRE_TIMEOUT)),
        ssh_keyfile=e.get("LABGRID_MCP_SSH_KEYFILE") or None,
    )
