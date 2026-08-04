"""MCP server exposing labgrid hardware-in-the-loop device operations to LLM agents."""

from importlib.metadata import version

# Single source of truth is pyproject.toml's version, read back through the
# installed package metadata -- a hardcoded literal here silently drifted
# from the packaging version once already.
__version__ = version("labgrid-mcp")
