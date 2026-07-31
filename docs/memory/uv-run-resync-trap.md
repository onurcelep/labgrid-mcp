# uv run silently reverts venv overrides — pass --no-sync after uv pip install

Learned: 2026-07-23 (uv behavior; re-verify on major uv upgrades)

`uv run` performs an implicit environment-consistency re-sync against
`uv.lock` before executing, so a venv mutated with `uv pip install X` (e.g.
the canary's labgrid-git-main override) is silently reverted to the pinned
version — the job goes green while testing the wrong thing. Empirically
verified: plain `uv run` saw 26.0 (pin), `uv run --no-sync` saw 26.1.dev68
(override). How to apply: any CI job or script that overrides a locked
dependency before running MUST pass `--no-sync` to every subsequent `uv run`;
comment it as load-bearing so it isn't "cleaned up". See
`.github/workflows/labgrid-canary.yml`.
