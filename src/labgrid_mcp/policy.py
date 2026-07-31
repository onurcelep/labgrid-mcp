"""Readonly/allowlist gating and MCP tool-annotation helpers (design decision #5)."""

from enum import StrEnum

from labgrid_mcp.config import Config


class Category(StrEnum):
    READ = "read"
    ACQUIRE = "acquire"
    RESERVATION = "reservation"
    POWER = "power"
    IO = "io"
    MUX = "mux"
    CONSOLE = "console"
    SSH = "ssh"
    FLASH = "flash"
    METADATA = "metadata"
    PLACE_DELETE = "place_delete"


#: categories that cause irreversible cross-user damage; enabled only by
#: explicit LABGRID_MCP_ALLOW entry (design decision #5, #13: FLASH can brick
#: a board, PLACE_DELETE can destroy any lab-wide place with no coordinator
#: ownership guard -- both are opt-in even without readonly).
EXPLICIT_OPT_IN = frozenset({Category.FLASH, Category.PLACE_DELETE})


def is_enabled(category: Category, config: Config) -> bool:
    if category is Category.READ:
        return True
    if config.readonly:
        return False
    if config.allow is not None:
        return category.value in config.allow
    return category not in EXPLICIT_OPT_IN


def annotations(
    *, read_only: bool = False, destructive: bool = False, idempotent: bool = False
) -> dict[str, bool]:
    return {
        "readOnlyHint": read_only,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
    }
