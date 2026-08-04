import re
from importlib.metadata import version

import labgrid_mcp


def test_version() -> None:
    # __version__ must mirror the packaging metadata (pyproject.toml is the
    # single source of truth) and look like a release version.
    assert labgrid_mcp.__version__ == version("labgrid-mcp")
    assert re.fullmatch(r"\d+\.\d+\.\d+", labgrid_mcp.__version__)


def test_labgrid_importable() -> None:
    import labgrid.remote.common  # noqa: F401
    import labgrid.remote.generated  # noqa: F401
