# Green CI proves the locked graph, not fresh installs

Learned: 2026-08-04 (from the 0.1.0 release breaking on install)

A fresh `uvx labgrid-mcp` / `pip install labgrid-mcp` resolves the version
bounds in `pyproject.toml` live against PyPI and never consults `uv.lock`.
CI only ever exercises the locked graph, so a fully green CI run carries
**no fresh-install guarantee** for any dependency with an open upper bound.

This bit for real: `mcp>=1.2` (no upper bound) let fresh installs resolve
the MCP SDK 2.0.0 release, which removed `mcp.server.fastmcp` -- the 0.1.0
package crashed on import for every new user while CI stayed green on the
locked 1.x.

Rules that follow:

- Every direct dependency carries an upper bound at the next major
  (`mcp>=1.2,<2`, `grpcio>=1.60,<2`, `labgrid>=26.0,<27`). Raising a bound
  is a deliberate, tested change -- same philosophy as the labgrid pin
  (DESIGN decision #12).
- After publishing a release, smoke-test the actual published artifact from
  a cold cache (`uvx --refresh labgrid-mcp@<version> demo`) -- it is the
  only check that exercises the live resolution.
