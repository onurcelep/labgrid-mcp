# Contributing to labgrid-mcp

Thanks for your interest. Bug reports, fixes, and new tools are welcome.

## Before you start

- For anything bigger than a small fix, open an issue first so we can agree
  on the approach before you write code.
- Read [`docs/DESIGN.md`](docs/DESIGN.md) for the architecture and the
  decision log. Decisions recorded there were made deliberately; if you want
  to revisit one, say so in the issue.

## Workflow

1. Fork the repository and create a branch from `main`.
2. Make your change, with tests.
3. Run the checks below locally.
4. Open a pull request against `main`. Keep it focused on one change.

`main` is protected: every change lands through a pull request, history is
kept linear (rebase merge only), and the `test` and `dco` checks must pass.
CI on a first-time contributor's PR starts after a maintainer approves it.

## Development setup

You need [uv](https://docs.astral.sh/uv/) and `/usr/sbin/sshd` (the
`openssh-server` package) for the SSH end-to-end tests.

```bash
uv sync
uv run pre-commit install   # optional: ruff + whitespace hooks on commit
```

## Checks

CI runs exactly these; please run them before pushing:

```bash
uv run ruff check
uv run ruff format --check
uv run mypy                  # strict, src + tests
uv run pytest -W error       # unit, no warnings tolerated
uv run pytest -m integration # hardware-free end-to-end suite
```

The integration suite starts real labgrid coordinator and exporter
processes against fake hardware. It needs no boards, secrets, or network
services.

## Safety rules

This server lets an agent drive real hardware. Some behavior is a safety
control, not a convenience:

- Flash and place deletion stay opt-in via `LABGRID_MCP_ALLOW`; they are
  never registered by default.
- The client-side force-gate on place metadata is the only ownership
  protection (the coordinator has none). It must not be removed or weakened.
- Changes to the `LABGRID_MCP_READONLY` / `LABGRID_MCP_ALLOW` defaults need
  explicit maintainer sign-off. Call them out in the PR description.

## Commits

- Write a short imperative subject line and explain *why* in the body.
- User-visible changes get an entry in [`CHANGELOG.md`](CHANGELOG.md) under
  an `## [Unreleased]` heading (add the heading if it is missing).

### Developer Certificate of Origin

Every commit must be signed off, certifying that you wrote it or otherwise
have the right to submit it under the project's license, as described in
the [Developer Certificate of Origin](https://developercertificate.org/).
Add the trailer with `git commit -s`:

```
Signed-off-by: Your Name <you@example.com>
```

The email must match the commit's author email. To sign off commits you
already made: `git rebase --signoff origin/main`, then force-push your
branch.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the same license as the project.

## Reporting security issues

Do not open a public issue for a vulnerability. See [`SECURITY.md`](SECURITY.md).
