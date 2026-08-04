# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-08-04

### Fixed

- **`labgrid-mcp demo` now prints the install-agnostic `uvx` config
  snippet.** It previously rendered a `uv run --directory <cwd>` snippet
  that only worked from a source checkout and embedded whatever directory
  the demo happened to be started from.

## [0.1.1] - 2026-08-04

### Fixed

- **Pin `mcp>=1.2,<2`.** The MCP Python SDK's 2.0.0 release removes
  `mcp.server.fastmcp`, so a fresh install (e.g. `uvx labgrid-mcp`) resolved
  a version the server cannot import and crashed on startup. The upper bound
  restores installability; migrating to the 2.x SDK API is future work.

## [0.1.0] - 2026-08-04

Initial implementation: a standalone, coordinator-agnostic MCP server over
[labgrid](https://github.com/labgrid-project/labgrid), covering its full
device-lifecycle surface end to end.

### Added

- **Coordinator client** — persistent async gRPC connection with
  auto-reconnect and place/resource sync (`coordinator_info`).
- **Reads** — `list_places`, `show_place`, `list_resources`, `who`,
  `list_reservations`, plus `labgrid://places`, `labgrid://places/{name}`,
  `labgrid://resources`, `labgrid://reservations`, `labgrid://sessions`
  resources.
- **Reservations & acquisition** — `reserve`/`cancel_reservation` with
  background keepalive so pending tokens never silently expire;
  `acquire_place` (reserve-then-wait-then-acquire in one call),
  `release_place`, `allow_place`.
- **Drivers** — `get_power_state`/`set_power`, `get_io`/`set_io`,
  `set_sd_mux`, `set_usb_mux`, driven through labgrid's own client-side
  Target/driver stack (not reimplemented).
- **Console sessions** — `console_open`/`console_read`/`console_send`/
  `console_close`, backed by a bounded ring buffer and a dedicated reader
  thread that bypasses labgrid's process-global, non-thread-safe `@step`
  stack; sessions auto-close on place release.
- **Flash jobs** — `flash_dfu`, `flash_fastboot`, `flash_script`,
  `bootstrap` (with a `loader` selector across all five
  `BootstrapProtocol` implementations), `write_image`, plus
  `flash_status`/`flash_logs` to poll background jobs to completion. The
  flash family is opt-in only (`LABGRID_MCP_ALLOW=flash`) since it can
  brick real hardware; excluded from the default allowlist even without
  `LABGRID_MCP_READONLY`.
- **Place metadata & change monitoring** — `add_place`/`delete_place`,
  `add_place_alias`/`delete_place_alias`, `set_place_tags` (an empty string
  value deletes that tag key — intentional labgrid semantics, documented
  rather than hidden), `set_place_comment`, `add_place_match`/
  `delete_place_match`, gated by a new default-on `Category.METADATA`. The
  coordinator enforces no ownership guard on any of these eight RPCs, so a
  client-side safety layer refuses a mutation on a place acquired by a
  different identity unless `force=True` (mirroring `release_place`'s
  `kick`); `delete_place` additionally refuses on a place acquired by
  ourselves unless forced, since deleting your own acquired place would
  otherwise strand the acquisition. Pre-validation catches a nonexistent
  alias and a malformed match pattern before they ever reach the coordinator,
  which would otherwise crash with an uncaught error. Also: `wait_for_change`
  — a long-poll tool (registered unconditionally, like the reads) that
  substitutes for a live `monitor` stream, since FastMCP has no subscription
  surface.
- **SSH-bound features** — `ssh_run`, `put_file`/`get_file` (the scp surface),
  and `forward_open`/`forward_list`/`forward_close` (local port-forward
  tunnels via a new `ForwardRegistry`, mirroring the console session
  registry's TTL-sweep and auto-close-on-release conventions; multiple
  tunnels allowed per place, unlike console's one-per-place rule), gated by a
  new default-on `Category.SSH` (`forward_list` is unconditional, like the
  reads — it only lists in-memory tunnel state). Unlike every other
  SSH-shaped tool, labgrid's SSH surface SSHes directly to a statically
  exported `NetworkService` resource's `address:port` rather than tunneling
  through an exporter host, so it needed only a real user-mode `sshd` — no
  VM, no root — to become genuinely hardware-free. A new
  `LABGRID_MCP_SSH_KEYFILE` config env supplies the private key
  (`NetworkService` carries no key field of its own); unset, the SSH tools
  error clearly at call time.
  `labgrid://sessions` gained a `"forwards"` list alongside the existing
  `"consoles"`/`"jobs"`. `sshfs` and `telnet` are skipped permanently (not
  deferred): `sshfs` needs a FUSE client on the MCP server host, absent on
  macOS without an admin+reboot-gated kext, and is redundant with
  `put_file`/`get_file`; `telnet` isn't SSH at all and is redundant with the
  console tools.
- **Capability-gap tools** — closes the highest-value gaps from the
  capability matrix: an optional `resource_name` on `get_power_state`/
  `set_power`/`get_io`/`set_io` selects one of several same-class resources on
  a place (previously unusable — labgrid's raw `get_resource` raises
  "multiple resources matching" with no way to pick one; ambiguous/unknown
  names now surface a tool error naming the candidates instead); an optional
  cycle-only `delay` on `set_power` sets the off/on gap (a tool error if
  passed with `"on"`/`"off"`); `get_sd_mux` reads the current SD-mux mode
  (SD-only — `usb_mux` has no equivalent read); `write_image` gains additive
  `partition`/`mode`/`skip`/`seek` options (`mode` validated against
  labgrid's `Mode` enum before submit); `release_from(place, host, user)`
  releases a place from an arbitrary identity, reading the place back
  before/after to report `released` truthfully since the coordinator's
  `ReleasePlace` silently no-ops on a mismatched `fromuser` (`host`/`user`
  validated non-empty and `/`-free before any RPC); a standalone
  `reservation_wait(token, timeout_s=25.0)` blocks-and-polls a reservation to
  `allocated` (polling is what keeps a not-yet-acquired reservation's TTL
  alive); `forward_remote_open(place, remote_port, local_port)` opens a
  REMOTE (`-R`) tunnel alongside the existing local (`-L`) `forward_open`
  (both ports required, no auto-assign for the local side); `forward_list`
  entries and the `labgrid://sessions` forwards payload gain a `"local"`/
  `"remote"` `direction` field.
- **Guardrails** — `LABGRID_MCP_READONLY` and `LABGRID_MCP_ALLOW` env-gated
  tool registration; every tool carries honest `readOnlyHint`/
  `destructiveHint`/`idempotentHint` MCP annotations.
- **Hardening** — shared bounded-ring/sweeper helper (`ringlog.py`) used by
  both console and job logs; additive wall-clock timestamps
  (`created_at`/`finished_at`/`last_used_at`) alongside the existing
  monotonic fields; a clearer "coordinator disconnected (reconnecting)"
  error during reconnect windows instead of a misleading "unknown place".
- **Demo command** — `labgrid-mcp demo` boots a hardware-free local lab: a
  real `labgrid-coordinator` + `labgrid-exporter` (default port 20499,
  `--port` to override) exporting one place (`demo-place`) backed by an
  in-process fake HTTP power switch and a fake TCP serial console (banner +
  line echo), seeded and ready the moment it prints its banner — which
  includes a paste-ready `.mcp.json` snippet and a few example prompts. Runs
  until Ctrl-C/SIGTERM, tearing every subprocess/thread/temp dir down
  cleanly; a busy coordinator port or a missing labgrid console script exits
  1 with a one-line message instead of a traceback. Productizes the exact
  fixture technique the integration suite below proves against a live
  coordinator (`src/labgrid_mcp/demo.py`), covered by both a unit suite
  (`tests/test_demo.py`) and its own hardware-free integration test
  (`tests/integration/test_demo_stack.py`, acquire/power/console against the
  real stack, plus a no-orphan teardown check).
- **Testing** — a hardware-free integration suite (`pytest -m integration`)
  that exercises the full acquire/power/io/console/flash job stack against
  real `labgrid-coordinator`/`labgrid-exporter` subprocesses, a fake HTTP
  power/io switch, and a fake TCP serial bridge, all run as local
  subprocesses with no real hardware; extended with a bare-coordinator (no
  exporter needed) e2e covering the full place-metadata round trip, the
  foreign-acquired refusal + force override, and the `wait_for_change`
  bootstrap/advance cycle; further extended with `test_ssh_via_user_mode_sshd`
  covering `ssh_run`/`put_file`/`get_file`/`forward_open`/`forward_close`
  (incl. the nonzero-exit-code case, the get_file overwrite refusal, and
  auto-close-on-release for a forward left open) against a real user-mode
  `sshd` confined to a scratch directory — the test probes the sshd with a
  raw `ssh` call first and skips cleanly, with the captured reason, on a host
  whose registered login shell doesn't resolve to a real binary (this rejects
  before pubkey auth is ever evaluated, which looks like an auth failure but
  isn't a labgrid/config bug); further extended for the capability-gap tools:
  a place statically exporting TWO `NetworkPowerPort`s (the `cls:`/named
  -resource recipe) against the fake switch's two independent endpoints
  proves `resource_name` drives each one in isolation and the no-name case on
  that place surfaces a tool error naming both; a timed `set_power(...,
  "cycle", delay=1.0)` proves the off/on gap is actually honored;
  `release_from`/`reservation_wait` are proven hardware-free against a bare
  place (no exporter needed); `forward_remote_open` is folded into the
  user-mode `sshd` e2e with a second scratch listener, round-tripping the
  remote→local direction through a real `ssh -R` tunnel; `mypy --strict`
  covers both `src` and `tests`.
- **CI** — a required workflow running lint, type checks, the unit suite,
  and the full hardware-free integration suite on every pull request and
  push to `main`; a weekly scheduled canary that re-runs the suite against
  labgrid's `main` branch to catch upstream drift early (decision #12).
- **Docs** — `docs/DESIGN.md` (architecture, decision log, verified labgrid
  implementation reference), a newcomer-oriented README (capabilities,
  limits, demo-first quickstart, coordinator compatibility: gRPC-era
  labgrid >= 24, developed/tested against the pinned 26.x), and this
  changelog.
