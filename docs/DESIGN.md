# labgrid-mcp — Design

**Scope:** the tool surface in §5 is fully implemented; §7 records what is
proven hardware-free versus what requires a real device; §11 is an
empirically verified reference of labgrid 26.0 internals (re-verify when the
pin moves).
**License:** Apache-2.0.

A standalone MCP (Model Context Protocol) server that exposes
[labgrid](https://github.com/labgrid-project/labgrid) hardware-in-the-loop
device operations to LLM agents. It works against *any* labgrid coordinator
and has no dependency on any private or lab-specific tooling.

This document records the design: the decisions, the architecture and its
rationale, and the verified labgrid reference the implementation rests on.

---

## 1. Motivation

labgrid is the de-facto framework for controlling embedded Linux test hardware
(power, serial console, flashing, muxes) over a network via a central
coordinator. There is no MCP server for it, so agents cannot drive a lab
directly.

`labgrid-mcp` is the generic, coordinator-agnostic building block: a drop-in
MCP server any labgrid user can point at their coordinator, with no coupling
to any particular lab's deployment, auth setup, or conventions.

---

## 2. Decision log

Each decision below was made deliberately; the rationale matters for future
changes.

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | Positioning | **Standalone generic OSS package** (`labgrid-mcp`), no dependency on any lab-specific tooling | A generic, shareable building block; works against any coordinator. |
| 2 | How it talks to labgrid | **Direct gRPC to the coordinator** (see also #9, hybrid) | Structured data, async-native, binds to the proto contract; an approach already proven in practice. `ClientSession` is argparse/`print()`-coupled and unsuitable as a library. |
| 3 | Operational scope | **Full lifecycle, discrete ops**, mapping labgrid's *full* surface | Deliverable must actually drive a board. |
| 4 | Connection / identity / security | **Native `LG_*` env vars**, insecure channel, security delegated to the network layer | The labgrid coordinator is unauthenticated; identity is self-claimed `host/user`. Matches labgrid-client exactly, zero new auth surface. |
| 5 | Guardrails for destructive ops | **Env-gated modes + MCP annotations**; default allows everything *except the flash family*, which needs explicit `LABGRID_MCP_ALLOW=flash`; `READONLY` and full `ALLOW` restrictions opt-in | An LLM can power-cycle/flash real hardware; honest metadata + opt-in strictness for the common case, but flashing can brick a board — irreversible ops are opt-in. |
| 6 | Tool breadth | **Curated lifecycle subset, fine-grained** (one tool per action) | Fine-grained is required so per-tool `readOnly`/`destructive` annotations are honest. |
| 7 | Repo / license | **GitHub `onurcelep/labgrid-mcp`**, Apache-2.0 | Apache-2.0 is clean when dynamically importing LGPL labgrid (diligence: #14). |
| 8 | Reservation model | **Block-until-acquired primary** (`acquire_place`), generous default max wait; raw reservation tools too | Pending tokens expire without keepalive; the keepalive task (see #10) makes raw reservations reliable as well. |
| 9 | Integration model refinement | **Hybrid, split by domain** (see Architecture) | Driver actions (`power`/`console`/`io`/`flash`/`mux`) are executed **client-side** by the labgrid driver stack, not via coordinator RPCs. Verified in `labgrid/remote/client.py`. |
| 10 | Server statefulness | **Persistent, stateful server** (session/job registries + reservation keepalive) | "Fully usable to deliver" requires interactive console, long flashes, and reliable reservations — none of which a stateless server can hold. |
| 11 | Flash family shape | **Separate fine-grained tool per method** | OSS users use different flows; no single "primary". Each method has distinct args and maps to a distinct driver. |
| 12 | labgrid dependency management | **Pin a labgrid version range in `pyproject.toml`** + a scheduled CI job against labgrid `master` | We import `labgrid.remote.generated` / `labgrid.remote.common` and reuse client internals — not a stable public API, and labgrid's gRPC layer is young. The pin protects users; the canary job catches upstream breakage before they hit it. |
| 13 | Place deletion guardrail | **Split `delete_place`/`delete_place_match` into their own opt-in `Category.PLACE_DELETE`**, out of the default-on `Category.METADATA`; enable explicitly with `LABGRID_MCP_ALLOW=place_delete` | Same irreversible-shared-damage class as flash (decision #5): §11.12 verified the coordinator enforces **no ownership guard** on either RPC, so an unattended delete can destroy any place lab-wide, acquired by anyone or not — irreversible ops are opt-in. |
| 14 | License model (ratifies #7) | **Apache-2.0 confirmed** + `NOTICE` file + `CITATION.cff` | For a tool whose value lies in broad adoption, permissive licensing maximizes corporate-lab uptake, the patent grant eases legal review, and copyleft would rarely trigger for an internally-run tool anyway (GPL obligations fire on distribution; lab users never distribute). A provenance scan verified no labgrid (LGPL) code is copied; labgrid stays a separate unmodified dependency. `NOTICE` carries attribution through redistributions; `CITATION.cff` gives GitHub's "cite this repository" affordance. Re-verified against OSADL's compatibility matrix and legal FAQ: the matrix "No" cells for Apache-2.0↔LGPL-2.1 govern only *incorporating* code into a combined work under one license — prohibited here as two hard invariants (never copy labgrid source into this tree; never bundle labgrid in the wheel). Merely importing the separately-installed library imposes no LGPL obligations on this package; downstream bundlers (containers, frozen binaries) take on LGPL-2.1 §6 duties that Apache-2.0 does not conflict with. labgrid itself verified uniformly LGPL-2.1-or-later: package-level or-later grant, 0/188 modules carry a deviating header (incl. the generated pb2 stubs), and no GPL-only license anywhere in the resolved dependency tree. |

---

## 3. Architecture

### 3.1 Hybrid integration (split by domain)

Two integration surfaces, because labgrid itself splits this way:

- **Coordinator gRPC (thin async client)** — over labgrid's generated proto
  stubs (`labgrid.remote.generated`) + `labgrid.remote.common` dataclasses.
  Covers the coordinator's domain: discovery, `places`, `resources`,
  reservations, `acquire`/`release`/`allow`, `who`.

- **labgrid client-side Target + driver stack** — build a `Target` with a
  `RemotePlace` resource from the acquired place, instantiate the matching
  labgrid driver (`NetworkPowerDriver`, `USBPowerDriver`, console/flash/mux
  drivers, ...) and drive it. Covers device I/O: `power`, `io`, `console`,
  flash family, mux. **This reuses labgrid's own driver implementations** — we
  do not reimplement serial/USB/power protocols.

  Reference: `labgrid/remote/client.py` `_get_target()` (~line 899, no-env
  branch: `Target(place.name); RemotePlace(target, name=place.name)`) and
  `power()` (~line 934) for the resource→driver selection pattern.

We do **not** use `ClientSession` directly — its action methods take no args,
read from `self.args` (argparse) and `print()` results. We reuse its *logic*
(target building, driver selection) but drive drivers programmatically.

### 3.2 Persistent, stateful server

The MCP process is long-lived (spawned by the client), so it holds:

- **One persistent async gRPC connection** to the coordinator, with
  auto-reconnect and coordinator state sync.
- **Long-lived Targets** for acquired places.
- **Session/job registries:**
  - *Console sessions* — a ring buffer per open console; handle-based
    interaction (`console_open` → id, then `console_read`/`console_send`/
    `console_close`).
  - *Flash jobs* — background jobs (`write_image` etc. run for minutes);
    `flash_status`/`flash_logs` poll to completion instead of blocking the
    tool call past the MCP client's request timeout.
  - *Reservation keepalive task* — refreshes pending reservation tokens until
    acquired/cancelled, so tokens don't silently expire between tool calls.
- **Lifecycle management:** TTL cleanup for abandoned sessions/jobs; cleanup on
  `release_place` and on server shutdown.

### 3.3 Transport & SDK

FastMCP (official `mcp` Python SDK) over stdio. The agent client (Claude Code,
Claude Desktop, VS Code, ...) spawns the process and talks stdin/stdout.

Consequence of stdio: **one server instance per MCP client**, each with its own
coordinator identity, console sessions, and jobs — nothing is shared between
agents. That is fine for v1 (labgrid's place locking arbitrates between
instances, like multiple `labgrid-client` users). A shared lab-wide deployment
would use streamable-HTTP transport instead; the tool layer is
transport-agnostic, so this is a future additive change, not a redesign.

### 3.4 Tool results

Every tool returns **structured JSON** (FastMCP structured output — typed dicts
with stable field names), not prose; the agent never needs to parse free text.
Conventions:

- Reads return the underlying labgrid data (places/resources/reservations) as
  plain dicts with labgrid's own field names.
- Mutations return the resulting state (e.g. `set_power` → the new power
  state), so the agent needs no follow-up read.
- `console_read` returns decoded text (`errors="replace"`), plus byte counts
  and a `truncated` flag when the ring buffer overflowed.
- Long-running job tools (`flash_*`) return a job id immediately;
  `flash_status` returns `{job, place, kind, loader, state, created, finished,
  created_at, finished_at, error, truncated}` (`created`/`finished` are
  monotonic, process-relative floats; `created_at`/`finished_at` are the
  wall-clock, epoch-seconds counterparts; progress lives in `flash_logs`'
  drained output, not a dedicated field). `kind` is always the canonical base
  kind; `loader` is the requested
  bootstrap loader for a non-default `bootstrap(loader=...)` job and `null`
  otherwise (default-loader bootstrap included) — the internal
  `"bootstrap:<loader>"` kind encoding never crosses the client boundary
  (normalized in the submitters' returns, `flash_status`, and
  `labgrid://sessions`' jobs list alike).
- Errors are structured MCP tool errors with the labgrid/gRPC cause message
  intact — never a bare stack trace.

---

## 4. Configuration & guardrails

Native labgrid env vars (no bespoke config):

| Var | Meaning |
|-----|---------|
| `LG_COORDINATOR` | coordinator `HOST[:PORT]` (default `127.0.0.1:20408`) |
| `LG_HOSTNAME` | claimed host part of identity (default: real hostname) |
| `LG_USERNAME` | claimed user part of identity (default: real user) |

labgrid-mcp-specific:

| Var | Meaning |
|-----|---------|
| `LABGRID_MCP_READONLY=1` | register read-only tools only |
| `LABGRID_MCP_ALLOW=power,flash,io,...` | allowlist gating destructive ops. Unset = everything allowed **except the flash family** (`flash_*`, `bootstrap`, `write_image`) **and place deletion** (`delete_place`, `delete_place_match`), both irreversible/cross-user and opt-in only — enable explicitly with `flash` / `place_delete` in the list (decisions #5, #13). |
| `LABGRID_MCP_ACQUIRE_TIMEOUT` | max wait for `acquire_place` (generous default; keep below the MCP client's request timeout — the agent can re-call to keep waiting, and `acquire_place` is idempotent if already ours) |
| `LABGRID_MCP_SSH_KEYFILE` | path to the SSH private key used by the SSH-bound tools; no default — unset, they error clearly at call time rather than falling back to whatever key the local `ssh` binary would try |

Every tool carries MCP annotations: `readOnlyHint`, `destructiveHint`,
`idempotentHint`. Transport security is delegated to the network (SSH tunnel /
WireGuard / Tailscale), matching labgrid-client.

Gating is fully wired — every non-read tool registration in `server.py` is
conditioned on `is_enabled(Category…, config)`.

---

## 5. Tool inventory

### v1 tools

**Reads (readOnly):**
`list_places`, `show_place`, `list_resources`, `who`, `list_reservations`,
`coordinator_info` (coordinator version + our identity; also a health check).

**Acquisition:**
`acquire_place` (reserve → wait → acquire in one call), `release_place`,
`allow_place`, `release_from` (§11.14 — releases an *arbitrary* `host`/`user`
identity, not just our own; because of the coordinator's
silent-no-op-on-mismatch trap the tool reads the place back before/after to
report `released` truthfully, and `host`/`user` are validated
non-empty/`/`-free before any RPC).

**Reservations (keepalive-backed, reliable):**
`reserve`, `cancel_reservation`, `reservation_wait` (§11.14 — standalone
block-and-poll to `allocated`, `timeout_s` clamped ≤25s like
`wait_for_change`; polling IS the keepalive for a not-yet-acquired
reservation, which otherwise expires ~60s after creation).

**Drivers:**
`get_power_state` / `set_power`, `get_io` / `set_io`, `get_sd_mux` /
`set_sd_mux`, `set_usb_mux`. (get/set split so read vs destructive annotations
are honest.) `get_power_state`/`set_power`/`get_io`/`set_io` all take an
optional `resource_name` (§11.14 — selects one of several same-class
resources on a place; omitted, unchanged single-resource behavior; ambiguous
with no name given → tool error naming the candidates). `set_power` also
takes an optional cycle-only `delay` (seconds, the off/on gap;
`ToolError` if set with `action` other than `"cycle"`). `get_sd_mux` (§11.14)
is SD-only — `usb_mux` (`LXAUSBMuxDriver`) has no read method, so there is no
`get_usb_mux`.

**Console (session-handle):**
`console_open`, `console_read`, `console_send`, `console_close`.

**SSH (`Category.SSH`, default-on — §11.13):**
`ssh_run`, `put_file`, `get_file` (the scp surface: local file existence /
overwrite refusal pinned before the driver is even bound), `forward_open`
(local `-L`), `forward_remote_open` (remote `-R`, §11.14 — both ports
required, no auto-assign for the local side; a connection to `remote_port` on
the DUT is forwarded to `localhost:local_port` on the MCP host), `forward_close`
(port-forward tunnels, session-handle like console; multiple tunnels per
place allowed, unlike console's one-per-place; each entry carries its
`"local"`/`"remote"` `direction`), `forward_list` (unconditional, like the
reads — lists in-memory tunnel state only, no place ownership or hardware
involved, so it survives readonly). Requires `LABGRID_MCP_SSH_KEYFILE` (§4)
since `NetworkService` carries no key field of its own.

**Flash (per-method, background-job):**
`flash_dfu`, `flash_fastboot`, `flash_script`, `bootstrap`, `write_image`
(additive `partition`/`mode`/`skip`/`seek` kwargs, §11.14 — `mode` validated
against labgrid's `Mode` enum names before submit, default `dd`), plus
`flash_status`, `flash_logs`.

**Place metadata (`Category.METADATA`, default-on, client-side ownership
safety — §11.12):**
`add_place`, `add_place_alias`, `delete_place_alias`,
`set_place_tags`, `set_place_comment`, `add_place_match`.

**Place deletion (`Category.PLACE_DELETE`, opt-in only — decision #13, same
shape as flash):**
`delete_place`, `delete_place_match`. Split out of `Category.METADATA`
because the coordinator enforces no ownership guard on either RPC (§11.12),
so an unattended delete can destroy any place lab-wide; excluded from the
default env even without readonly, enabled with `LABGRID_MCP_ALLOW=place_delete`.

**Change monitoring (unconditional, like the reads):**
`wait_for_change` — long-poll substitute for a live `monitor` stream (no MCP
subscription surface exists; §11.12).

### MCP resources

`labgrid://places`, `labgrid://places/{name}`, `labgrid://resources`,
`labgrid://reservations`, `labgrid://sessions` (active console sessions,
forward tunnels, and jobs).

### Coverage notes and exclusions

**Place metadata & change monitoring:** place-metadata edits (add/delete a place, aliases,
tags incl. empty-value delete, comment, resource matches) via the eight
place-metadata tools above (`Category.METADATA`'s six plus
`Category.PLACE_DELETE`'s two — decision #13), plus the live `monitor` stream
via the `wait_for_change` long-poll tool. Both are hardware-free and verified against
a real coordinator (marker-gated e2e test); see §11.12 for the RPC shapes,
the coordinator's no-ownership-guard finding, and the traps their
implementation works around.

**SSH-bound tools:** `forward` (`forward_open`/`forward_list`/
`forward_close`), `scp` (`put_file`/`get_file`), and `ssh_run` — unlike every
other SSH-shaped tool, `labgrid-client`'s SSH surface SSHes **directly** to a
statically-exported `NetworkService` resource's `address:port`, no exporter
host tunnel and no udev, which makes it hardware-free after all: verified
against a real user-mode `sshd` (no VM, no root) plus a real coordinator +
exporter, CI-verified (marker-gated e2e test, `test_ssh_via_user_mode_sshd`;
executes for real on the ubuntu CI runner, probe-skips locally on a host with
a broken login-shell registration). See §11.13 for the recipe, the
`Subsystem sftp` trap, and the forward-tunnel semantics.

**Skipped, not deferred (§11.13):** `sshfs` needs the `sshfs` binary plus a
FUSE client on the *MCP server* host to mount server-side — absent on macOS
(no macFUSE kext, admin+reboot-gated) and redundant with path-based
`put_file`/`get_file`; not shipped. `telnet` is not SSH at all (raw `telnet`
to port 23) and is redundant with the SerialDriver console tools (§11.10);
not shipped. Both are permanent decisions.

**Capability-gap closures:** the highest-value gaps identified by a
systematic review of `labgrid-client`'s full command surface (§11.14) —
per-resource **name selection** for
power/io (`resource_name` on all four get/set tools; a place
with two same-class resources was previously unusable, not merely
first-match), power **cycle delay** (`set_power`'s `delay`), remote
**forward `-R`** (`forward_remote_open`), conditional **`release_from`**, and
standalone **`reservation_wait`**, plus additive `write_image`
`partition`/`mode`/`skip`/`seek` options and **`get_sd_mux`** (SD-only read).
Verified hardware-free against a real coordinator + exporter: per-resource
name and cycle delay against the fake HTTP power switch (extended with a
second endpoint, DESIGN §11.9's appendix + §11.14's two-same-class exporter
recipe), `release_from`/`reservation_wait` against a bare place (no exporter
needed, §11.8(a)), and `forward_remote_open` folded into the SSH e2e
against the real user-mode `sshd` (CI-verified on ubuntu, per §11.13's
recipe). `write_image`'s options and `get_sd_mux` are unit-only (both SSH/CLI
to a real device on the exporter host — no hardware-free chain exists, same
boundary as the flash drivers, §11.11) but registered-and-asserted in the
hardware-free e2e suite.

**Hardware/DUT-bound (needs a device):** `video`, `audio`, `screen`, `tmc`
remain deferred — every one binds a real instrument or capture resource the
same way flash drivers bind a udev-managed USB export (§11.11's hardware
boundary, generalized). `channel` also remains deferred: unlike the
direct-to-`NetworkService` SSH tools, `channel` tunnels through the *exporter
host's* SSH, so it needs a live exporter/DUT to exercise meaningfully. Out of
scope without a device.

**Remaining known gaps (not covered):** network-
namespace isolation (`netns`), write-files-only variants beyond `put_file`/
`get_file`, `rsync`, extra `fastboot`/`dfu` verbs beyond what `flash_dfu`/
`flash_fastboot` already cover, per-resource-name selection on mux/flash/
console (`resource_name` is scoped to power+io only, by design), and
server-side `resources`/`places` filters (both list tools return everything;
filtering is client-side). None are hardware boundaries — they remain
open scope decisions, not technical blockers.

---

## 6. Repository layout

```
labgrid-mcp/
  pyproject.toml
  LICENSE                 # Apache-2.0
  README.md
  CHANGELOG.md
  .pre-commit-config.yaml
  .gitlint
  .python-version
  docs/
    DESIGN.md             # this file
  src/labgrid_mcp/
    __init__.py
    config.py             # LG_* + LABGRID_MCP_* env parsing
    policy.py             # readonly/allowlist gating + annotation helpers
    coordinator.py        # persistent async gRPC client (channel, reconnect, unary RPCs)
    target.py             # ClientSession-free Target & driver stack for power/io/mux tools
    session.py            # place session orchestration: reservation keepalive + acquire/release
    console.py            # interactive serial-console sessions
    jobs.py               # background flash jobs
    forwards.py           # local/remote SSH port-forward tunnels
    ringlog.py            # shared bounded ring-buffer log + crash-guarded sweeper
    server.py             # FastMCP app: tools, resources, stdio entrypoint
  tests/
    ...
```

Toolchain: uv, ruff, mypy `--strict`, pytest, pre-commit, gitlint.

---

## 7. Coverage and the hardware boundary

Everything in §5's tool inventory is implemented. What separates the
verification tiers is not effort but physics — whether a driver's endpoint
can exist without a device.

**Proven end-to-end hardware-free (CI, every PR).** The integration suite
drives the real stack — a real `labgrid-coordinator` and `labgrid-exporter`
as subprocesses — against fake endpoints:

- **Power / io:** a fake HTTP switch (labgrid's `rest`/`HttpDigitalOutput`
  backends), read back off the switch's own recorded state as an independent
  oracle; includes two-same-class `resource_name` selection and the timed
  cycle `delay` (§11.9, §11.14).
- **Console:** a raw-protocol TCP bridge standing in for a serial port
  (`NetworkSerialPort` with `protocol: raw`, §11.10).
- **SSH (`ssh_run`, `put_file`/`get_file`, forwards both directions):** a
  real user-mode `sshd` — no VM, no root (§11.13). Executes for real on the
  ubuntu CI runner; probe-skips on hosts with a broken login-shell
  registration.
- **Acquisition, reservations, metadata, `wait_for_change`,
  `release_from`/`reservation_wait`:** a bare coordinator place — no
  exporter needed (§11.8, §11.12).
- **Flash job machinery:** submission, gating, background threads, log
  capture, retention — via scripted fake CLIs (§11.11).

The reconnect path, session TTL/sweep conventions, the thread-safety fix
(§11.10), and the demo command are covered by the same suite; `tests.yml`
runs it on every PR and push, and `labgrid-canary.yml` runs it weekly
against labgrid `master` (decision #12).

**Requires a real device (the hardware boundary).** Real flash-driver and
mux runs: every such driver op SSHes to a udev-managed USB export on the
exporter host, so no hardware-free real chain exists (§11.11). The same
boundary generalizes to `video`/`audio`/`screen`/`channel`/`tmc` (§5).
`write_image`'s options and `get_sd_mux` are unit-tested and
registered-asserted only, same boundary.

---

## 8. Standing risks

- **Upstream instability** — `labgrid.remote.generated`/`common` and the
  client internals this server reuses are not a stable public API. Mitigated
  by the version pin plus the weekly canary against labgrid `master`
  (decision #12); §11's facts must be re-verified whenever the pin moves.
- **labgrid's `@step` machinery is process-global and thread-unsafe** —
  mitigated by the thread-local steps rebind installed at startup (§11.10);
  any new background-thread driver use must follow the same discipline.
- **The coordinator enforces no authorization** — identity is self-claimed
  and metadata RPCs carry no ownership guard (§11.12). The client-side
  force-gate and category gating are the only protections; never weaken
  them (CLAUDE.md hard rules).

---

## 9. Testing

- Unit tests against a mocked gRPC stub + registry unit tests (console
  buffering, flash-job lifecycle, reservation keepalive).
- The hardware-free integration suite (`pytest -m integration`) is required,
  not optional — it runs real `labgrid-coordinator`/`labgrid-exporter`
  subprocesses and is CI-enforced on every PR and push (`tests.yml`).

---

## 10. Key labgrid source references

(Verified against the labgrid checkout at design time; re-verify line numbers
before relying on them.)

- `labgrid/remote/client.py`
  - `ClientSession` (~line 89) — argparse/`print()`-coupled; not used directly.
  - `_get_target(place)` (~line 875) — target building; no-env branch is the
    reusable path.
  - `power()` (~line 934) — resource→driver selection pattern for driver tools.
  - identity: `gethostname()`/`getuser()` from `LG_HOSTNAME`/`LG_USERNAME`
    (~lines 101-105); `grpc.aio.insecure_channel` (~line 122).
- `labgrid/remote/coordinator.py`
  - `schedule_reservations()` (~line 953) — reservation TTL/expiry; acquired
    reservations auto-refresh, pending ones need keepalive.
- `labgrid/pyproject.toml` `[project.scripts]` — the five labgrid entrypoints
  and the full capability surface behind `labgrid-client`.

---

## 11. Verified implementation reference (labgrid 26.0)

Facts below were read from the installed labgrid 26.0 package and verified
empirically. This section exists so that code, tests, and maintainers cite
verified behavior by section number instead of re-deriving it. **Re-verify this
section when the labgrid pin moves** (the canary CI job of decision #12 is
the tripwire).

### 11.1 Coordinator RPC surface (`CoordinatorStub`)

Streams: `ClientStream` (bidi, client side), `ExporterStream` (exporters only).

Unary RPCs and their exact request fields → response fields:

| RPC | Request | Response |
|---|---|---|
| `GetPlaces` | (empty) | `repeated Place places` |
| `AcquirePlace` | `placename` | (empty) |
| `ReleasePlace` | `placename`, `fromuser` | (empty) |
| `AllowPlace` | `placename`, `user` | (empty) |
| `CreateReservation` | `filters` (map name→Filter), `prio` (double) | `Reservation reservation` |
| `CancelReservation` | `token` | (empty) |
| `PollReservation` | `token` | `Reservation reservation` |
| `GetReservations` | (empty) | `repeated Reservation reservations` |
| `AddPlace`/`DeletePlace`/`AddPlaceAlias`/`DeletePlaceAlias`/`SetPlaceTags`/`SetPlaceComment`/`AddPlaceMatch`/`DeletePlaceMatch` | (place metadata) | (empty) |

### 11.2 Unary RPCs are session-bound (critical)

The coordinator resolves the caller of every unary RPC via gRPC
`context.peer()` → the `ClientStream` session on the **same channel**
(`coordinator.py` `AcquirePlace`, ~line 851). A unary call on a channel with
no live handshaken `ClientStream` fails with `FAILED_PRECONDITION`
("Peer … does not have a valid session"). Consequences:

- All unary RPCs MUST go through the same `CoordinatorClient` stub/channel
  that runs the subscription stream — never a second channel.
- After a reconnect, unary calls are invalid until the new handshake
  completes.
- Identity for acquire/release is the `startup.name` sent in the handshake
  (our `config.identity`), not an RPC argument.

### 11.3 Error semantics (observed in `coordinator.py`)

gRPC status codes → meaning (these map to structured MCP tool errors, with
the server's `details` string kept):

- `INVALID_ARGUMENT` — entity does not exist (e.g. unknown place).
- `FAILED_PRECONDITION` — invalid state (already acquired; no session).
- `PERMISSION_DENIED` — place reserved for another owner.

### 11.4 ClientStream protocol

- Client sends `ClientInMessage` (oneof-ish: `sync` | `startup` | `subscribe`):
  `startup{version, name}` first (version = `labgrid.util.labgrid_version()`,
  name = `host/user` identity), then `subscribe{all_places=True}`,
  `subscribe{all_resources=True}`, then `sync{id}`.
- Coordinator replies `ClientOutMessage{sync, repeated UpdateResponse updates}`;
  seeing our sync id echoed (`HasField("sync")`) confirms the handshake.
- `UpdateResponse` oneof `kind`: `place` (Place) | `del_place` (string name) |
  `resource` (Resource) | `del_resource` (Path).
- `Resource` = `{Path path, cls, params(map), extra(map), acquired, avail}`;
  `Path` = `{exporter_name, group_name, resource_name}`.
- **No coordinator version ever reaches clients** — `ClientOutMessage` has no
  hello/version field (exporters get one via `ExporterOutMessage.hello`).
  `coordinator_info.version` therefore stays `null` (forward-compat seam in
  `coordinator.py` `_maybe_update_version`).
- Snapshot semantics: a (re)subscribe resends the full current state as
  add/update events only — deletions during a disconnect are silent, so the
  local snapshot MUST be cleared per session (done in `_connect_and_pump`).

### 11.5 Serialization helpers in `labgrid.remote.common` (and their traps)

- `Place.from_pb2(pb2).asdict()` → JSON-ready dict; **omits `name`** — the
  server adds it explicitly (as labgrid's own client does:
  `data["name"] = place.name`).
- `Reservation.from_pb2(pb2)` is correct; `Reservation.asdict()` → dict with
  `state` as the enum *name* string; **omits `token`** — the server adds it
  explicitly from `.token` when returning reservations to agents.
- `ReservationState`: `waiting/allocated/acquired/expired/invalid`.
- **`ResourceEntry.from_pb2` is buggy in labgrid 26.0** — its assert checks
  `isinstance(pb2, …Place)` (copy-paste bug), so it raises `AssertionError`
  on real Resource messages. The server uses
  `ResourceEntry(ResourceEntry.data_from_pb2(resource_pb2))` instead;
  `data_from_pb2` is correct and folds `extra` into `params["extra"]`.
  `ResourceEntry.asdict()` → `{cls, params, acquired, avail}` (no path — the
  `Path` triplet is added explicitly).
- `who` in labgrid-client is **derived from the places snapshot**, not an RPC:
  places with `acquired` set (`"host/user"` string), exporter names from
  `acquired_resources` path tuples.
- `Resource.params`/`extra` map values are `MapValue` messages — tests set
  them via `msg.params[key].string_value = ...`; plain assignment fails.

### 11.6 Channel options

`grpc.aio.insecure_channel(target, options=CHANNEL_OPTIONS)` with labgrid's
exact keepalive options (copied in `coordinator.py::CHANNEL_OPTIONS`):
`keepalive_time_ms=7500`, `keepalive_timeout_ms=10000`,
`http2.ping_timeout_ms=10000`, `http2.max_pings_without_data=0`. Without them
half-open connections never error and reconnect never fires.

### 11.7 Codebase seams and conventions (as implemented)

- Test seams in `coordinator.py`: module-level `_sleep` (patch for backoff),
  `_channel_factory` (patch for channel assertions), constructor kwarg
  `_stub_factory` (inject `FakeCoordinatorStub`). New stream-behavior tests
  reuse `tests/test_coordinator.py`'s `FakeCoordinatorStub`/`Session`/
  `live_session`/`_wait_until` helpers rather than inventing a second fake.
- FastMCP specifics (mcp SDK ≥1.2 as installed): `FastMCP(name, lifespan=…)`;
  tool annotations passed as `mcp.types.ToolAnnotations` built per-field from
  `policy.annotations()` (a `**dict` spread fails `mypy --strict`); resources
  registered with `@mcp.resource("labgrid://…")` returning a JSON string;
  tool errors raised as `mcp.server.fastmcp.exceptions.ToolError` with a
  human-readable message (FastMCP converts to a structured MCP error).
- Gates: `uv run pytest` (unit, 0 warnings tolerated), `uv run pytest -m
  integration` (real coordinator), `uv run ruff check`, `uv run mypy`
  (src-only via `packages`; test files should still be written to pass
  `mypy --strict` when checked directly).

### 11.8 Reservation & acquisition semantics (verified)

Verified against labgrid 26.0 by reading the installed source **and** driving
a real local coordinator over the same gRPC stubs the client uses. Citations are
`remote/<file>.py:line`. **Re-verify when the labgrid pin moves.**

**Reservation filters match tags AND place name** (decisive for
`acquire_place`):

- `CreateReservation` accepts only filter key `"main"`; anything else →
  `UNIMPLEMENTED` (`coordinator.py:1069`). Filter values are `(k, v)` string
  pairs validated against `TAG_KEY`/`TAG_VAL` (`coordinator.py:1074`).
- The scheduler builds each place's tagset as
  `set(place.tags.items()) | {("name", place.name)}` (`coordinator.py:1020-1024`)
  and allocates iff **`filter.tags.issubset(place.tags)`**
  (`scheduler.py:schedule_step`). The place name is a first-class matchable
  key: `filters={"name": "<placename>"}` reserves exactly that place. (The
  `# support place aliases` comment at `coordinator.py:1025` has **no code** —
  aliases are not injected, only `name`.)
- **`acquire_place(name)` design (decision #8): reserve `{"name": name}`, poll
  until state leaves `waiting`, then `AcquirePlace(name)`** — not a bare
  `AcquirePlace` poll loop. The reservation stamps `place.reservation = token`
  (`coordinator.py:1056`) and `AcquirePlace` enforces `res.owner == username`
  (`coordinator.py:866-869`), so holding the reservation blocks other clients
  from winning the place; a plain retry loop has no such guarantee.
  labgrid-client itself has no reserve-by-name convenience (only `KEY=VALUE`
  tag filters), and polls every 1.0 s (`client.py:1594-1609`).

**Keepalive / expiry:**

- `Reservation.timeout` defaults to `created + 60`; `refresh(delta=60)` sets
  `timeout = max(timeout, now+delta)` (`common.py`).
- **`PollReservation` calls `res.refresh()`** (`coordinator.py:1103`) —
  polling is the keepalive. `GetReservations` does not refresh.
- Expiry runs in `schedule_reservations`, invoked on every mutation and by a
  background loop every 15 s (`coordinator.py:242-251`; `:959-974`) — up to
  ~15 s after the 60 s TTL, and two-phase (`state=expired`, then deleted).
- **Only `acquired` reservations auto-refresh**
  (`coordinator.py:962-964`). A `waiting` or `allocated`-but-not-yet-acquired
  reservation is NOT auto-refreshed and expires unless the client polls.
  **Trap:** `PollReservation` has to be polled (≤ ~30 s cadence) from
  `CreateReservation` until `AcquirePlace` succeeds; after acquire no
  keepalive is needed.
- States (`common.py`): `waiting → allocated` (scheduler match, `:1036`);
  `allocated → acquired` (place acquired, `:993-997`); `acquired → allocated`
  (released, `:998-1002`); any → `expired` (TTL, `:967`); allocated place
  deleted → `invalid` (`:985`).

**Acquire / Release / Allow:**

- **(a) A bare place with zero resource matches CAN be acquired** —
  `AcquirePlace` only iterates matching exporter resources
  (`coordinator.py:874-890`); with none, it trivially succeeds. Hardware-free
  e2e of reserve/acquire/release/allow needs only a coordinator, no exporter.
- **(b) `ReleasePlace.fromuser`** (`coordinator.py:896-906`): empty →
  unconditional release (**no owner check at all** — any session can release
  any acquired place); non-empty → conditional, a silent no-op (not an error)
  if `place.acquired != fromuser`. labgrid-client sends empty for `release`,
  `fromuser=<host/user>` for `release-from`.
- **(c) `AllowPlace.user` must be `"host/user"`**; the coordinator requires
  the caller to own the place (`place.acquired == username`), else
  `FAILED_PRECONDITION`.
- **(d) Exact gRPC codes** (mapped per §11.3, `details` kept): acquire
  already-acquired → `FAILED_PRECONDITION`; release non-acquired (empty
  fromuser) → `FAILED_PRECONDITION`; acquire non-existent place →
  `INVALID_ARGUMENT`; acquire a place reserved for someone else →
  `PERMISSION_DENIED`; release a place you don't own (empty fromuser) →
  **no error, succeeds** (kick); poll a cancelled/unknown token →
  `FAILED_PRECONDITION`.
- **(e) After acquire, the place update arrives on the ClientStream** —
  `AcquirePlace`/`ReleasePlace`/`AllowPlace` all call `_publish_place`, so
  every subscribed client's `acquired`/`allowed`/`reservation` fields update.

**AddPlace / DeletePlace / SetPlaceTags** (test fixtures, not session-scoped
to the creator — any client sees/uses them until deleted):

- `AddPlaceRequest{name}` → empty; duplicate → `ALREADY_EXISTS`
  (`coordinator.py:503-515`).
- `DeletePlaceRequest{name}` → empty; unknown name → `ALREADY_EXISTS` (the
  code reuses that status for "does not exist", `coordinator.py:520`). **No
  guard against deleting an acquired place** — it succeeds; any live
  reservation then goes `invalid`.
- `SetPlaceTagsRequest{placename, tags}` (`coordinator.py:564`): `tags` is
  `map<string,string>`, validated against `TAG_KEY`/`TAG_VAL`; **empty value
  deletes the tag** (`coordinator.py:579-583`). These three don't themselves
  need a session peer lookup, but route through the one live channel anyway
  (§11.2 applies to acquire/release/allow/reserve regardless).

**Other acquisition traps:**

- `CreateReservationResponse.reservation.token` is the handle for
  poll/cancel/acquire-guard; `Reservation.asdict()` omits it (§11.5) — add it
  explicitly.
- Two client channels to the same target share a gRPC subchannel by default →
  the coordinator's `assert peer not in self.clients` (`coordinator.py:317`)
  aborts the second with `UNKNOWN`. Only relevant to multi-client *tests*,
  which pass `options=[("grpc.use_local_subchannel_pool", 1)]` per test
  channel. Production uses one channel, so unaffected.
- `prio` is a `double`; the scheduler orders pending reservations by
  `(-prio, created)` (`coordinator.py:1014`) — higher prio/older win a
  contended place first. Default 0.0.
- Coordinator persists places (not reservations) to a file in its CWD via
  `save_later`; test coordinators run in a throwaway dir.

<details>
<summary>Verification (two-client driver against a real coordinator)</summary>

Verified live: bare-place acquire with no resources succeeds and propagates
to a second subscribed client; the documented `FAILED_PRECONDITION`s for
already-acquired/non-acquired acquire/release; a non-owner release (empty
`fromuser`) is a no-op kick that succeeds; non-owner `AllowPlace` is
`FAILED_PRECONDITION`; `{"name": "specific-2"}` allocates exactly that place
and blocks a second client's `AcquirePlace` with `PERMISSION_DENIED`; polling
bumps the timeout; polling a cancelled token is `FAILED_PRECONDITION`;
deleting an acquired place succeeds with no guard. Expiry: an unpolled
`allocated` reservation was still `allocated` at t+62s (TTL 60s, 15s poll
tick not yet due) and `expired` by t+82s, same for an unpolled `waiting` one.

</details>

### 11.9 Client-side Target & driver stack (verified)

Verified against labgrid 26.0 by reading the installed source **and** driving a
real local coordinator + exporter + fake HTTP power/io switch through this
package's own `CoordinatorClient`. Citations are `<file>.py:line` under
`labgrid/`; proven YAML + fake switch in the appendix. **Re-verify when the
labgrid pin moves.**

**The trap: `Target` + `RemotePlace` normally open their OWN `ClientSession`.**
`RemotePlace(target, name=...)` construction eagerly fires
`RemotePlaceManager.on_resource_added` (`resource/common.py:131` →
`resource/remote.py:33`); with no session set it calls `_start()` →
`start_session(...)` (`resource/remote.py:22`), opening labgrid's full gRPC
`ClientSession` off `LG_COORDINATOR`. In-process that second channel shares a
subchannel with our live `CoordinatorClient` → identical `context.peer()` → the
coordinator's `assert peer not in self.clients` (`remote/coordinator.py:317`)
aborts it (the shared-subchannel trap of §11.8). **Never let it self-connect;
never import `ClientSession`.**

**Decision: pre-seat a 3-member adapter on the process-global manager (PROVEN
e2e).** `RemotePlaceManager` is a class-keyed singleton
(`ResourceManager.get()`, `resource/common.py:116`). BEFORE constructing any
`RemotePlace`, the code sets — on `mgr = RemotePlaceManager.get()` —
`mgr.session = adapter` (blocks `_start()`; `on_resource_added` then skips its
if-not-session block), `mgr.loop = stub` (`is_running()` True), and
`mgr.env = None`. The adapter's ENTIRE
surface is the 3 members `on_resource_added`+`poll` touch
(`resource/remote.py:33-88`):

1. `.loop` — anything whose `.is_running()` returns True, so `poll()`'s
   `if not self.loop.is_running(): loop.run_until_complete(...)` branch (crashes
   on a running loop) is skipped. A stub is simplest.
2. `.get_place(name)` → object with `.tags` (dict, copied to `place.tags`) and
   `.acquired_resources` (list of `[exporter, group, cls, name]`).
3. `.get_target_resources(place)` → `dict{(name, cls): entry}` with `.cls`
   (str), `.args` (ctor kwargs), `.avail` (bool), `.extra` (dict) — backed from
   `place.acquired_resources` × the resource snapshot (`params`→args, nested
   `params.extra`→extra, `avail`).

Ownership holds across sessions: labgrid gates on `host/user ==
gethostname()/getuser()` (`remote/client.py:471`), matching our
`Config.identity` — moot for the adapter approach, but it is why the model is
sound.

**Hardware-free power path — `rest` backend (PROVEN).** An exporter statically
exports a `NetworkPowerPort` (a plain `Resource` → `avail=True`, no start/stop,
`remote/exporter.py:1027`); the client binds `NetworkPowerDriver`
(`driver/powerdriver.py:151`, `bindings={"port": NetworkPowerPort}`). Backend
`rest` (`driver/power/rest.py`): `power_get` → HTTP GET `host.format(index=...)`
(`text=="1"`), `power_set` → HTTP PUT body `b"0"`/`b"1"`; `on_activate` resolves
the URL directly (no SSH tunnel) unless `LG_PROXY` / `proxy_required`. `cycle()`
= `off(); time.sleep(delay); on()`, **delay default 2.0 s** (blocking).

**io (PROVEN) + mux.** `HttpDigitalOutput` (`resource/httpdigitalout.py`) is,
like `NetworkPowerPort`, a plain `Resource` (never `NetworkResource`) —
`url`/`body_asserted`/`body_deasserted` required, `method` (default `PUT`) and
`url_get`/`body_get_asserted`/`body_get_deasserted` optional (default to the
set URL/bodies) — so it is a SECOND no-hardware path, same static-export shape
as the power path (io e2e: exported statically right next to a
`NetworkPowerPort` in the same place, against the appendix's fake switch
extended with a second endpoint). `HttpDigitalOutputDriver`
(`driver/httpdigitaloutput.py`, `bindings={"http": "HttpDigitalOutput"}`):
`set(status)` → `requests.request(method, url_set, data=body_asserted or
body_deasserted)`; `get()` → `requests.get(url_get)`, then matches `res.text`
against `body_get_asserted`/`body_get_deasserted` (regex if set, else the
literal `body_asserted`/`body_deasserted` strings) — `True`/`False` on a
match, `ExecutionError` otherwise; direct by default (`proxymanager.get_url`,
no `LG_PROXY`). **sd-mux / usb-mux are HARDWARE-ONLY** — their `Network*`
resources SSH to the exporter host
(`command_prefix`, `resource/common.py:93`) and run a CLI there:
`USBSDMuxDriver.set_mode(mode)` (`dut|host|off|client`) → `usbsdmux <path>
<mode>`; `LXAUSBMuxDriver.set_links(links)` (`⊆{dut-device,host-dut,host-device}`)
→ `usbmuxctl … connect`. Unit-mock by patching `subprocess` /
`processwrapper.check_output`.

**Lifecycle (design rules for the manager).**

- Drivers are SYNCHRONOUS + blocking (`requests` I/O, `time.sleep`,
  `subprocess`) → **every driver call goes through `asyncio.to_thread`** (PROVEN
  for the whole build→activate→drive block); never touch the loop thread.
- Drivers are activated before use (methods are `@Driver.check_active`-guarded).
  The bound resource is already materialized (`on_resource_added` at
  `RemotePlace` construction). `get_driver(cls)` only FINDS an existing driver
  (`target.py:158`) — the code instantiates `cls(target, name=None)` first,
  then `target.activate(drv)`.
- **Manager-leak trap: `RemotePlaceManager` only APPENDS** — no
  `_remove_resource` (`resource/common.py`); `poll()` walks every place ever
  built. So the manager **caches ONE Target per place** (built once, keyed by
  name) and prunes `mgr.resources`/`unmanaged_resources` on invalidate. SSH
  ControlMasters
  likewise accumulate for mux paths (`sshmanager`), not the pure-HTTP paths.
- **Zero-match place** (a bare §11.8 place) → `get_target_resources → {}` →
  driver fails to bind (`NoResourceFoundError`); the tools surface it as a
  clean error rather than crashing.

**Env:** `LG_PROXY` stays unset for direct localhost;
`LG_HOSTNAME`/`LG_USERNAME` stay consistent with our identity; `env=None` is
fine.

<details>
<summary>Appendix — exporter YAML + fake switch (reused verbatim by the e2e suite)</summary>

Exporter YAML (group name is arbitrary; becomes the match middle segment).
The `HttpDigitalOutput` block (io e2e) sits in the SAME group as the
power port — a genuinely different shape (no `model`/`host`/`index`; instead
`url` + the asserted/de-asserted body pair) since it is a different resource
class, not a variant of `NetworkPowerPort`:

```yaml
powerplace:
  NetworkPowerPort:
    model: rest
    host: 'http://127.0.0.1:18080/relay/{index}/value'
    index: 0
  HttpDigitalOutput:
    url: 'http://127.0.0.1:18080/io/0/value'
    body_asserted: '1'
    body_deasserted: '0'
```

Fake switch (`fake_switch.py`; GET→`0`/`1`, PUT sets) with TWO independent
endpoints — `_idx` keys state by the WHOLE stripped request path (e.g.
`relay/0/value` / `io/0/value`) so the power and io paths never collide in the
shared `STATE` dict:

```python
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
STATE = {}
class H(BaseHTTPRequestHandler):
    def _idx(self):
        return self.path.strip("/")
    def do_GET(self):
        v = b"1" if STATE.get(self._idx(), False) else b"0"
        self.send_response(200); self.send_header("Content-Length", str(len(v)))
        self.end_headers(); self.wfile.write(v)
    def do_PUT(self):
        n = int(self.headers.get("Content-Length", 0))
        STATE[self._idx()] = self.rfile.read(n).strip() == b"1"
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
```

The 3-member adapter is implemented as `_CoordinatorAdapter` in
`src/labgrid_mcp/target.py`, following the build sequence above.

Verification — coordinator + exporter (`-n testexporter --hostname 127.0.0.1`)
+ `fake_switch.py`, all on 127.0.0.1 ephemeral ports. Without the adapter the
build fails exactly as predicted (coordinator log: `assert peer not in
self.clients`); with the 3-member adapter it passes: switch state `0→1→0→1`
across `on()/off()/cycle()`, `drv.get()` consistent. The io path is verified
the same way: a `NetworkPowerPort` and a static `HttpDigitalOutput` exported
side by side in one place, driven over MCP (`get_io`/`set_io`) with the
switch's `io/0/value` endpoint as the independent oracle — `false→true→false`
across `set_io`/`get_io`, consistent with (and never cross-contaminating) the
power relay's own `0→1→0` on the same run.

</details>

---

### 11.10 Console via SerialDriver (verified)

Verified against **labgrid 26.0 / pyserial 3.5** by driving a real local
coordinator + exporter + a ~30-line fake TCP console through this package's own
`CoordinatorClient` and the §11.9 `TargetManager`. Citations are `<file>.py:line`
under `labgrid/`; proven exporter YAML + TCP bridge in the appendix (reused
verbatim by the e2e suite). **Re-verify when the labgrid pin moves.**

**The chain is hardware-free: `NetworkSerialPort` + `protocol: raw` →
`socket://` (PROVEN).** A static `NetworkSerialPort` exports exactly like §11.9's
`NetworkPowerPort` — a plain `NetworkResource` (`resource/serialport.py`), NOT in
the exporter `exports` registry (only `RawSerialPort`/`USBSerialPort` map to the
ser2net-spawning `SerialPortExport`, `remote/exporter.py:189,304`), so it is
`avail=True` immediately, no ser2net, no pty, no hardware. The client
`SerialDriver` picks its URL from `protocol` (`driver/serialdriver.py:29-58`):
`protocol == "raw"` → `serial_for_url("socket://")` + `serial.port =
socket://{host}:{port}/` — plain TCP to any listener; `protocol == "rfc2217"`
(the DEFAULT) needs an rfc2217-compliant server (ser2net) we don't have.
**Always set `protocol: raw`** (`port`/`speed` are YAML ints). host/port resolve
DIRECT — no SSH tunnel — via `proxymanager.get_host_and_port` with
`proxy_required=False` (same topology as §11.9). Chain (b)
SSHDriver/NetworkService is NOT needed.

**`ConsoleProtocol` surface** (`driver/serialdriver.py`,
`driver/consoleexpectmixin.py`):

- **`read(size=1, timeout=0.0, max_size=None) -> bytes`** (public, `@step` +
  `@check_active`) delegates to `_read`: drains `max(size, in_waiting)` bounded
  by `max_size`, blocks up to `timeout`, and **raises `pexpect.TIMEOUT` (not
  `b""`) when idle** — a loop that treats idle as `b""` busy-spins. Blocking →
  run off the loop.
- **`write(data: bytes) -> int`** (public, `@step` + `@check_active`); default
  `txdelay=0/txchunk=1` → one `serial.write`.
- activate → `on_activate` connects the socket (`status=1`); deactivate →
  `close()` (idempotent). Public read/write after deactivate raise
  `labgrid.binding.StateError`; the **undecorated `_read`/`_write` have NO
  `@check_active`** — the session object owns the open/closed guard.

**THE TRAP: labgrid's `@step` stack is process-global and NOT thread-safe.**
`step.py` holds a module singleton `steps = Steps()` with one `_stack`; `pop`
does `assert self._stack[-1] is step` (`step.py:22-30`), not thread-local. Every
public console method AND every §11.9 power/io method is `@step`-decorated. A
long-lived reader on the PUBLIC `read()` interleaves its push/pop with concurrent
power ops on ANY place, and the `pop` assertion fails with a bare
`AssertionError` (reproduced deterministically; a reader on the public API
crashes exactly this way). **Fix: the reader and `console_send` MUST call the undecorated
`_read`/`_write` directly** (`serialdriver.py:71-95`) — they touch no shared step
stack, so a continuous reader never contends with any driver call in the process.
pyserial `socket://` is safe for one concurrent reader + one writer (200-iter
stress: clean, no per-socket lock needed); `SerialDriver` + `NetworkPowerDriver`
coexist on one cached Target.

**Safe pattern (extends §11.9's locking invariant).**

- **Under the per-place lock** (serialized with power ops), in a `to_thread`
  worker: build/reuse the cached Target (§11.9), instantiate +
  `Target.activate(SerialDriver)`, return the driver
  (`TargetManager.console_driver`). **Sanctioned exception to the §11.9
  place-lock invariant:** the returned driver's `_read`/`_write` are then used
  OUTSIDE the lock — a reader holding the place lock for its whole lifetime would
  block every power/io op on the place, and bypassing `@step` makes lock-free
  concurrent I/O safe.
- **The reader runs OUTSIDE the place lock** on a dedicated `threading.Thread`,
  looping `sd._read(size=1, timeout=0.1, max_size=4096)` into a bounded
  `deque(maxlen=CONSOLE_RING_BYTES)` of **bytes** (decode `errors="replace"` at
  read time so a chunk boundary never splits a UTF-8 char); catch
  `pexpect.TIMEOUT` → continue, any other exception → error state. It holds NO
  labgrid lock and NO `@step` method, so power ops stay usable (proven). Stop via
  a `threading.Event` + bounded `join`. A dedicated thread (not a `to_thread`
  task) is chosen so a 10-minute blocking reader never occupies a slot in the
  shared default executor that all other `to_thread` driver ops draw from.
- `console_send` → `to_thread(sd._write, data)`.
- **close ordering (proven): stop-event → join reader → `Target.deactivate(sd)`
  under the place lock.** Deactivate closes the socket from another thread; a
  reader mid-`_read` on a closed socket raises, so stop+join FIRST.

**Cleanup traps.** Releasing the place at the coordinator does NOT close our
client-side TCP socket (direct connect); a leaked reader keeps the fd open.
`console_close` and `TargetManager.invalidate`/`shutdown` therefore stop+join
the reader and deactivate the driver (§11.9's `invalidate` already
`deactivate_all_drivers`; the registry closes sessions BEFORE invalidate).
`max_size` is always passed — `in_waiting` on `socket://` can make one `_read`
return far more than `size`.

<details>
<summary>Appendix — exporter YAML + fake TCP console bridge (reused verbatim by the e2e suite)</summary>

Exporter YAML (`protocol: raw` mandatory; `port`/`speed` unquoted ints). Group
`consolegrp` → match `exporter/consolegrp/NetworkSerialPort/serial0`:

```yaml
consolegrp:
  serial0:
    cls: NetworkSerialPort
    host: 127.0.0.1
    port: {{ env['BRIDGE_PORT'] }}    # jinja: exporter renders env into the YAML
    protocol: raw
    speed: 115200
```

Fake console bridge (`bridge.py`, ~30 lines — no ser2net, no pty; echoes lines
and emits an unsolicited banner):

```python
import socket, threading, sys
def _serve_conn(conn):
    conn.sendall(b"BOOT-BANNER labgrid-mcp phase4\r\n")   # unsolicited output
    buf = b""
    while True:
        data = conn.recv(4096)
        if not data: return
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            conn.sendall(b"echo:" + line.rstrip(b"\r") + b"\r\n")
def main(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port)); srv.listen(8)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=_serve_conn, args=(conn,), daemon=True).start()
if __name__ == "__main__":
    main(int(sys.argv[1]))
```

Verification — the full chain: coordinator → exporter (static
`NetworkSerialPort`, `protocol: raw` → the bridge) → `CoordinatorClient.acquire`
→ §11.9 adapter → `Target` + `SerialDriver` → `serial.port ==
socket://127.0.0.1:PORT/`, `is_open=True`. A reader on the PUBLIC API crashed
on `step.py:29 AssertionError`; the `_read` bypass ran clean while concurrent
power `on/off/get` completed (`reader_ok=True power_ok=True`), and the banner +
`hello/world/ping` round-tripped end-to-end.

</details>

---

### 11.11 Flash family & background jobs (verified)

Verified against **labgrid 26.0** by reading the installed source and running
capture/step verification runs on a macOS host. Citations are `<file>.py:line`
under `labgrid/`. **Re-verify when the labgrid pin moves.**

**Headline.** The five flash tools are structurally unlike the power/io/console
families of §11.9–§11.10: every one reaches its CLI **over SSH to the exporter
host** (`NetworkResource.command_prefix`, `resource/common.py:92-103` —
*unconditional* `["ssh", …, host]`, no localhost short-circuit) and stages its
image **over SSH** (`ManagedFile.sync_to_resource` → `sshmanager.open().put_file`,
`util/managedfile.py:55-69`), and every bound resource is a **udev-managed USB
export** (`USBGenericExport` instantiates the local udev class and advertises
`Network<cls>`, `remote/exporter.py:343-363`) that needs a real matching device
to become `avail`. There is **no direct-connect / static-export escape hatch**
(the one that made `rest` power and `socket://` console hardware-free). **Verdict:
no flash-family driver runs hardware-free** — proven two independent ways on the
verification host: `ssh 127.0.0.1` → `Connection refused` (Remote Login off), and
macOS has no udev so no binding ever goes `avail`. Real-hardware flash is the
documented boundary (as
sd/usb-mux stayed hardware-only in §11.9). The flash e2e therefore exercises the
**job registry** against a scripted fake driver plus a real per-job capture proof;
it drives no real flash. This is a *decisive* no, not a "couldn't get it working."

#### Driver inventory (all execute via `processwrapper.check_output`)

`processwrapper` is the process-global singleton `ProcessWrapper()`
(`util/helper.py:197`); every flash driver does `from ..util.helper import
processwrapper`. It runs the command with `subprocess.Popen` **through a pty**,
splits output on `\r`, and for every chunk calls every registered callback with
`(chunk_bytes, Popen)` (`util/helper.py:104-106,140-141`). All five block until
the child exits → **jobs**.

| kind | driver.method | client resource | actual argv | CLI here |
|---|---|---|---|---|
| `dfu` | `DFUDriver.download(altsetting, filename)` (`dfudriver.py:40`) | `NetworkDFUDevice` (udev USB) | `[dfu-util, -p <path>, --alt N, --download <remote>]` | **missing** |
| `fastboot` | `AndroidFastbootDriver.flash(partition, filename)` (`fastbootdriver.py:93`) | `AndroidUSBFastboot` / `AndroidNetFastboot` | `[fastboot, -s …, flash, part, <remote>]` | **missing** |
| `script` | `FlashScriptDriver.flash(script, args)` (`flashscriptdriver.py:37`) | `NetworkUSBFlashableDevice` (udev USB) | `[<remote_script>] + [a.format(device=…, file=…) …]` | our script |
| `bootstrap` | `BootstrapProtocol.load(filename)` — default **`IMXUSBDriver.load`** (`usbloader.py:76`; also MXS/UUU/RK/BDIMX) | `NetworkIMXUSBLoader` (udev USB) | `[imx-usb-loader, -p <path>, -c <remote>]` | **missing** |
| `write_image` | `USBStorageDriver.write_image(filename, …)` (`usbstoragedriver.py:128`) | `NetworkUSBMassStorage` / SDMux (udev USB) | `[dd, if=<remote>, of=<path>, oflag=direct, status=progress, bs=4M, conv=fdatasync]` | `dd` present but runs on the exporter |

`bootstrap` maps to *five* driver classes all exposing `load(filename)` — the
`bootstrap` tool therefore takes a loader selector (default `IMXUSBDriver`).
`getvar`/`oem_getenv` return
parsed values; `flash`/`boot`/`download`/`load`/`write_image` return `None` — the
tools report job success/logs, not a value.

#### Per-job output capture (PROVEN)

ONE callback is registered on the process-global `processwrapper`, with **the
accumulator keyed by `threading.get_ident()`**. Each flash job runs its driver
call on its own thread (below), so the callback — which runs *in that worker
thread* — routes each `\r`-split chunk to that job's bounded byte ring. The
callback also receives the live `Popen` → `process.pid` is captured for cancel.
The callback is registered idempotently ONCE at server init and **returns early
when the thread id isn't a flash job** (labgrid's own
`enable_logging`/`enable_print` register global callbacks; unrelated threads'
chunks must be ignored). **Proven:** two concurrent `check_output` runs of a
`\r`-progress fake CLI → each buffer held only its own tag, zero
cross-contamination, live chunks captured, final `\n`-joined return value
intact.

#### Concurrency: the @step trap, fixed by a thread-local step stack

Every flash public method is `@step`-decorated **and** `processwrapper.check_output`
is itself `@step` (`util/helper.py:42`). The step machinery is the process-global
singleton `labgrid.step.steps` with one shared `_stack`; `pop` asserts
`self._stack[-1] is step` (`step.py:29`) — **not thread-safe**, the §11.10 hazard.
Flash has no `_read`/`_write` escape (§11.10's bypass): the `@step` is on the only
public entry and wraps a second `@step`. A global lock is the wrong fix — it would
make a 5-minute flash block a 50 ms power op on a different place.

**Fix (PROVEN): at server init the server rebinds `labgrid.step.steps` to a
thread-local-stack `Steps` subclass.** `@step` resolves `steps` as a module
global at call time, so one rebind redirects every driver in the process; each
thread gets its own LIFO
stack → push/pop is always locally consistent. Reproduced: two threads calling a
stepped op that yields the GIL mid-call → stock `_stack` fails with `AssertionError`
within a few hundred iters; thread-local → **0 errors**. This removes the hazard
for **all** drivers at once and **also retires the latent power-vs-power on
different places race** the per-place lock never covered — with **no global lock**.
Seam trap: `labgrid.step` the attribute is shadowed by the `step` *function* in
`labgrid/__init__`, so `import labgrid.step as m` binds the function — the
rebind goes via `importlib.import_module("labgrid.step")` (or
`sys.modules[...]`), installed idempotently from every entry path (job registry
AND TargetManager init).

**Job execution model:** each flash runs on a **dedicated `threading.Thread`**
(like the §11.10 reader), NOT `asyncio.to_thread` — a multi-minute flash must not
occupy a slot in the shared default executor every power/io op draws from.
The Target is built/activated under the per-place lock (§11.9); the long
`flash()` call then runs on the job thread. Concurrent flashes on different places are fine
(thread-local steps); two on the *same* place are rejected by the registry.

#### Traps

- **`target.env is None`** (the ClientSession-free Target, §11.9). Any path
  reaching `self.target.env.config.*` crashes: `write_image`/`flash`/`load`/
  `flash_script` with `filename=None` fall back to `env.config.get_image_path`.
  **The tools therefore always pass an explicit local file/script path** and
  never rely on the driver's `image`/`script` attrs; the local file's existence
  is validated before
  submission (`ManagedFile` raises `FileNotFoundError` otherwise, then
  **sha256-hashes the whole file** and copies to `/var/cache/labgrid/<user>/` on
  the exporter — large images = minutes of copy before flashing, part of why these
  are jobs).
- **Cancel mid-flash.** A `Thread` worker can't be force-killed and `check_output`
  has no cancel hook. The only mechanism is **killing the subprocess**: the
  `Popen.pid` captured by the capture callback, then `os.kill(pid, SIGTERM)`;
  `check_output`
  then raises `CalledProcessError` and the thread unwinds → job state `cancelled`.
  **Killing a flash mid-write can brick hardware** — this is why the flash family
  is gated behind `LABGRID_MCP_ALLOW=flash` (decision #5); cancel (used only by
  `shutdown`) is best-effort, with that warning attached.
- **The Target is pinned for the job's lifetime.** `on_activate`/`on_deactivate`
  are no-ops for DFU/fastboot/flashscript/loaders (`USBStorageDriver.on_deactivate`
  closes a `udisks2` `AgentWrapper` — another exporter dependency). A running job
  pins its cached Target so a concurrent `release_place`/`invalidate` can't
  deactivate the driver mid-write: `TargetManager.invalidate` on a pinned place
  raises `TargetError` naming the job.
- **Env:** `LG_PROXY` stays unset; `proxy_required`/`proxy` in a resource's
  `extra` reroutes `command_prefix` to a proxy host — not exercised here.

---

### 11.12 Place metadata & change monitoring (verified)

Verified against **labgrid 26.0** + **mcp 1.28.1** as installed, by reading
installed source and driving a real local `labgrid-coordinator` over the same
gRPC stubs the client uses. **Re-verify when the labgrid/mcp pin moves.**

**Headline — no ownership guard.** None of the eight place-metadata RPCs reads
`place.acquired`, a reservation owner, or the caller's session name (contrast
`AcquirePlace`/`AllowPlace`, which resolve `self.clients[peer].name`,
`coordinator.py:855,925`). Verified empirically: a place **acquired** by the
caller was freely retagged, re-aliased, and **deleted while held** — all `OK`.
§11.8's "DeletePlace has no guard" generalizes to all eight: **any** handshaked
session can retag/re-alias/re-comment/re-match/delete **any** place, acquired
by anyone or not. Ownership safety for metadata edits is entirely **our**
responsibility (readonly gate + our own policy check against the snapshot),
not the coordinator's.

#### Request/response shapes (dumped from installed `labgrid_coordinator_pb2`)

All eight metadata RPCs return an **empty** response message.

| RPC | Request fields | Notes |
|---|---|---|
| `AddPlace` | `name: string` | |
| `DeletePlace` | `name: string` | |
| `AddPlaceAlias` | `placename, alias: string` | |
| `DeletePlaceAlias` | `placename, alias: string` | |
| `SetPlaceTags` | `placename: string`, `tags: map<string,string>` | empty value = delete key |
| `SetPlaceComment` | `placename, comment: string` | comment unvalidated |
| `AddPlaceMatch` | `placename, pattern: string`, `rename: optional string` | `pattern` = `"exporter/group/cls[/name]"` |
| `DeletePlaceMatch` | `placename, pattern: string`, `rename: optional string` | `rename` accepted for symmetry but ignored for matching (see trap 4) |

`Place.matches` is a **list**, `Place.aliases` a **set** (`common.py:223`,
`converter=set`), `Place.tags` a dict.

#### Coordinator semantics + exact gRPC codes (`remote/coordinator.py:503-649`)

Every handler except AddPlace/DeletePlace looks the place up and aborts
`INVALID_ARGUMENT "Place <n> does not exist"` if missing.

| Case | Result (raw gRPC, verified) |
|---|---|
| `AddPlace` duplicate | `ALREADY_EXISTS "Place p1 already exists"` |
| `DeletePlace` nonexistent | `ALREADY_EXISTS "Place ghost does not exist"` ← misuse bug, DeletePlace only |
| `AddPlaceAlias` duplicate | `OK` (idempotent — `set.add`) |
| `DeletePlaceAlias` nonexistent alias | **`UNKNOWN "Unexpected KeyError: 'nope'"`** ← bug, see traps |
| `SetPlaceTags` value `""` | `OK` — deletes that key |
| `SetPlaceTags` 1-char key | `INVALID_ARGUMENT "Key x ... is invalid"` |
| `SetPlaceComment` any string | `OK` — no validation |
| `AddPlaceMatch` 2-segment | **`UNKNOWN`** (uncaught `TypeError`, missing `cls`) |
| `AddPlaceMatch` 5+ segment | **`UNKNOWN`** (uncaught `TypeError`, dup `rename`) |
| `AddPlaceMatch` duplicate | `ALREADY_EXISTS "Match e/g/c already exists"` |
| `DeletePlaceMatch` absent | `INVALID_ARGUMENT "Match z/z/z does not exist in p1"` |

#### Aliases (client-side only) & snapshot auto-refresh (free)

`AcquirePlace("<alias>")` → `INVALID_ARGUMENT "does not exist"` (verified);
acquire/reservation lookups use the exact `place.name` only. Aliases are
resolved solely by labgrid-*client* (`client.py:456-468`), never by the
coordinator (the `# support place aliases` comment at `coordinator.py:1025` is
dead code, confirming §11.8) — **we** must resolve aliases against our
`_places` snapshot before calling any name-taking RPC. Separately: all seven
mutating handlers call `_publish_place` (delete sends `del_place`), each via
`place.touch()` (`common.py:329`) — verified `SetPlaceTags` reflected in
`_places["p3"]` within ~0.4s, broadcast to *all* subscribed clients. Our
existing `_apply_update` handles this with zero new code.

#### Change monitoring: `wait_for_change` long-poll, not MCP subscriptions

Lowlevel `Server` has `subscribe_resource`/`send_resource_updated`, but
`_build_capabilities` hardcodes `ResourcesCapability(subscribe=False)`
(`server/lowlevel/server.py:212`) — never advertised. **FastMCP has no
subscription surface at all** and exposes a session only inside a live
request — no stored handle to push notifications from our background stream
callback. **Decision: a `wait_for_change(cursor, timeout_s)` tool** — zero SDK
subscription support needed, works over stdio, fits §3.4's "tool returns JSON"
model. Mechanism: a monotonic `int` counter bumped once per applied
`UpdateResponse` in `_apply_update`, plus an `asyncio.Event` set on each bump
so waiters block instead of busy-polling. A reconnect (which clears
`_places`/`_resources`, §11.4) also counts as a change — bump on session-start
clear too, so a held cursor never under-reports after a drop. The counter is
process-local state, not persisted: it resets to 0 on every server restart,
so a cursor value a caller held from a previous process is simply stale and
self-heals after at most one full `timeout_s` (the next call either sees an
already-advanced cursor or times out and returns the current one). The
`timeout_s` clamp (max 25.0, server.py) assumes the calling MCP client's own
request timeout is set higher than that; a lower client timeout just means
the client gives up first and our still-running poll finishes harmlessly on
its own with nothing listening.

#### Behavioral traps

1. **`DeletePlaceAlias` of a nonexistent alias raises `UNKNOWN`.** The handler
   catches `ValueError` from `.remove()` (`coordinator.py:558`), but
   `place.aliases` is a `set` and `set.remove()` raises `KeyError` — uncaught.
   The tool pre-checks membership against the snapshot and returns a clean
   error.
2. **Match pattern arity crashes the RPC outside 3-4 segments.** 2 segments and
   5+ segments both surface as `UNKNOWN` (uncaught `TypeError`). Segment count
   (3-4) is validated before the call.
3. **Tag value validation is a no-op** (`TAG_VAL` matches any empty prefix, so
   uppercase/spaces/punctuation all pass) **and an empty value silently
   deletes the key** (verified) — coordinator-side validation cannot be relied
   on; both behaviors are recorded here.
4. **Match dedup does NOT include `rename` — verified.** labgrid 26.0's
   `ResourceMatch` (`common.py:160`) declares
   `rename = attr.ib(default=None, eq=False)`, with the comment "rename is
   just metadata, so don't use it for comparing matches" (`common.py:159`); a
   match's identity is the `(exporter, group, cls, name)` tuple alone.
   Confirmed against the coordinator's own logic (`remote/coordinator.py`):
   `AddPlaceMatch` rejects a duplicate via `rm in place.matches` and
   `DeletePlaceMatch` removes via `place.matches.remove(rm)` — both rely on
   `ResourceMatch.__eq__`, so `rename` plays no role in either duplicate
   detection or delete lookup. A `rename` is still accepted and stored as
   metadata on add (and returned in the match dict), but passing a different
   (or no) `rename` on delete removes the same match just fine.
5. **No ownership guard** (see headline) — the single biggest policy trap.

<details>
<summary>Verification detail</summary>

Setup: ephemeral-port `labgrid-coordinator` subprocess, driven through the
repo's `CoordinatorClient`; raw gRPC codes captured via `client._stub.<RPC>`
directly (table above). Also confirmed: acquired-place edits/delete all `OK`;
`SetPlaceTags` reflected in the snapshot with advanced `changed` ts;
acquire-by-alias errors while acquire-by-name succeeds. MCP SDK facts
confirmed by grep over installed `mcp/server/`.

</details>

---

### 11.13 SSH-bound features via user-mode sshd (verified)

Verified against **labgrid 26.0 / OpenSSH 10.2p1**, by reading installed
source and driving a real `SSHDriver` against a user-mode `sshd`, plus a real
coordinator + exporter statically exporting a `NetworkService`. **Re-verify
when the labgrid pin moves.**

**Headline.** Unlike the flash family (§11.11, which SSHes to the *exporter
host*), `labgrid-client`'s SSH subcommands target a **`NetworkService`
resource** and SSH **directly to `address:port`** — no exporter tunnel, no
udev, no hardware. `NetworkService` (`resource/networkservice.py`) is a plain
`Resource` (not `NetworkResource`) with `address`/`username` (required),
`password` (default `None`), `port` (default 22) — it **static-exports** like
§11.9's `NetworkPowerPort` (`NetworkServiceExport` has no `_start`/`_stop`,
`remote/exporter.py:705-728` → `avail=True` immediately). `proxy_required` is
set only for a `%`-suffixed ifname; a bare `127.0.0.1` stays **direct**
(verified). This makes a **user-mode sshd the entire vehicle** — no VM, no
container — and the recipe is unchanged on ubuntu CI runners. For the MCP we
skip `labgrid-client`'s `_get_ssh` fallback chain (`remote/client.py:1289-
1308`) and **instantiate `SSHDriver` ourselves** (§11.9 pattern), which lets
us set `keyfile`/`username` per-connection — the CLI cannot.

**Auth model (decisive).** `SSHDriver.on_activate` (`sshdriver.py:64-80`)
builds `ssh_prefix`: `-o LogLevel=ERROR`, `-i <keyfile>` iff the `keyfile`
attr is truthy, `-o PasswordAuthentication=no` iff no password, then
`-F none` (**ignores `~/.ssh/config`**) plus `-o ControlPath=…`. `keyfile` is
a plain `attr.ib(default="", validator=instance_of(str))` — **not** resolved
against `env.config` when `target.env` is `None` (our §11.9 Target), so the
raw path is used verbatim; it must be set **at construction**, before
`target.activate()` triggers `on_activate`. `NetworkService` has **no
keyfile field**, so custom-key auth must be set on the driver. The control
master (`sshdriver.py:110-122`) hard-codes `UserKnownHostsFile=/dev/null
StrictHostKeyChecking=no` — no host-key prompt, nothing to prime. Port is
honored end-to-end (`run` via `-p`, `put`/`get` via `-P`, the master via
`-p`; scp/rsync/forward/sshfs ride the master's control socket).

**PROVEN (byte-level).** An env-None Target + real `SSHDriver` against a
user-mode sshd: the sshd log shows labgrid connecting to a non-22 port and
offering the exact configured client key via `PubkeyAuthentication` — port +
keyfile + direct-connect are all wired correctly by `SSHDriver`. (The one
rejection hit locally was a machine-local dangling login shell, unrelated to
labgrid/sshd config — see the login-shell trap below; absent on CI.)

**PROVEN (full chain).** A real `labgrid-coordinator` + `labgrid-exporter`
statically exporting the `NetworkService` YAML below: the resource
registered `avail=True, proxy_required=False`, acquisition succeeded, and
the §11.9 3-member-adapter Target build bound `SSHDriver.networkservice`
with the exported `address`/`port`/`username` — the exporter YAML carries
end-to-end into the driver.

**sshfs / telnet: skipped.** `sshfs` needs the `sshfs` binary + a FUSE
client on the *MCP server* host (`SSHDriver.sshfs`, `sshdriver.py:425-451`
mounts a long-lived FUSE mount server-side) — absent on macOS (no macFUSE
kext, admin+reboot-gated) and redundant with path-based put/get; not
shipped. `labgrid-client telnet` is not SSH at all — raw `telnet` to port 23
(`client.py:1359`) — binary absent on macOS and redundant with the
SerialDriver console tools (§11.10); not shipped.

**Behavioral traps.**

1. **`Subsystem sftp` is required.** OpenSSH ≥9 `scp` speaks SFTP by
   default; put/get/scp silently break without an sftp subsystem line even
   though `run` still works fine.
2. **Login shell must exist.** sshd's `allowed_user` check rejects at the
   `none` auth stage if the connecting user's login shell doesn't resolve —
   evaluated **before** pubkey, so it looks like an auth failure. CI runners
   (bash) are unaffected; any local e2e must run as a user with a valid
   shell.
3. **ControlMaster + keepalive leak if not deactivated.** `on_activate`
   starts a ControlMaster and a keepalive `cat` subprocess (`sshdriver.py:561`)
   and holds a temp `lg-ssh-*` dir; `on_deactivate` stops both and `rmtree`s
   it. `ControlPersist=300` lingers 5 min post-deactivate unless cancelled —
   invalidate/shutdown deactivate the driver (mirroring §11.10's reader
   cleanup).

<details>
<summary>Appendix — user-mode sshd recipe + NetworkService YAML (reused verbatim by the e2e suite)</summary>

**sshd_config** (scratchpad-confined; run as a non-root user on a high
127.0.0.1 port):

```
ListenAddress 127.0.0.1
HostKey <dir>/hostkey
PidFile <dir>/sshd.pid
AuthorizedKeysFile <dir>/authorized_keys
StrictModes no
UsePAM no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
PrintMotd no
Subsystem sftp /usr/libexec/sftp-server
```

(`/usr/lib/openssh/sftp-server` on ubuntu.) Keys: `ssh-keygen -t ed25519 -f
hostkey -N ""` and `-f clientkey -N ""`; `authorized_keys = clientkey.pub`.
Launch: `/usr/sbin/sshd -f sshd_config -p <port>` (`apt-get install -y
openssh-server` on ubuntu). `StrictModes no` is needed since a scratch dir
isn't 0700-audited.

**Exporter YAML** (static `NetworkService`, exports like §11.9's power
port):

```yaml
sshgrp:
  dut-ssh:
    cls: NetworkService
    address: 127.0.0.1
    username: <current user>
    port: <port>
```

Point `SSHDriver.keyfile` at the client key. Cleanup: kill sshd (`kill
$(cat sshd.pid)`), the coordinator, and the exporter; remove the scratch
dir — nothing needs sudo, nothing touches outside `tmp_path`.

</details>

---

### 11.14 Capability-gap APIs (verified)

Verified against **labgrid 26.0** by reading installed source
(`.venv/.../labgrid/`) and cross-reading our bind path. Closes the
highest-value gaps identified by a systematic review of `labgrid-client`'s
full command surface. **Re-verify when the pin moves.**

**Per-resource NAME selection (the one real cross-cut).** Without name
selection, a place with TWO same-class resources (e.g. two
`NetworkPowerPort`s) is **unusable**,
not merely first-match: the bare bind path `driver_cls(target, name=None)` makes
labgrid's `get_resource` raise `NoResourceFoundError("multiple resources
matching …")` (`target.py:150-153`) — unless one resource is literally named
`"default"`. So name selection is the *only* way to drive such a place. The
mechanism (the CLI's `_get_driver_or_new`, `client.py:922-930`) is: take the
driver's single binding key (`[key] = list(cls.bindings)` — `"port"` for
`NetworkPowerDriver`, `"http"` for `HttpDigitalOutputDriver`), then

```python
target.set_binding_map({binding_key: resource_name})   # next driver only
drv = cls(target, name=resource_name)                  # __init__ → bind_driver consumes the map
target.activate(drv)
```

`set_binding_map` (`target.py:285`) sets `_binding_map` for the **next** driver
only; `bind_driver` pops it and calls `get_resource(cls, name=resource_name)`
(`target.py:337`), filtering resources by `res.name`. Multi-binding drivers
can't be named this way (labgrid raises `NotImplementedError`) — not a concern
for power/io/console/mux/flash, all single-binding. **Our plumbing:** an
optional `resource_name` is threaded through and folded into the per-Target
driver cache key
(`power` → `power:name:<name>`, mirroring `bootstrap:<loader>`) so two named
drivers cache independently; `name=None` keeps the byte-identical single-resource
path (same cache key, no `set_binding_map`). **Scoped to power+io.**
Candidate names are enumerated from the place snapshot's `acquired_resources`
(`[exporter, group, cls, name]`) filtered by the driver's binding class(es), and
`TargetError` is raised **by this server** — `name=None` + >1 candidate → "pass
resource_name"; unknown name → "available: …" — so labgrid's raw "multiple
resources matching" never escapes.

**Power cycle delay.** `NetworkPowerDriver.cycle()` takes **no argument**
(`powerdriver.py:227` → `off(); sleep(self.delay); on()`); `delay` is an
`attr.ib(default=2.0, validator=instance_of(float))` (`:154`). The CLI sets it
as an instance attr *before* the action (`client.py:972`), not as a call arg —
so the tool sets `drv.delay = float(delay)` before `cycle()` (int coerced to
float — the validator raises otherwise). Only meaningful for `cycle`.

**write_image options.** `USBStorageDriver.write_image(filename=None,
mode=Mode.DD, partition=None, skip=0, seek=0)` (`usbstoragedriver.py:128`).
`mode` is the `Mode` **enum** (`Mode.DD`/`Mode.BMAPTOOL`, imported from
`labgrid.driver.usbstoragedriver`) — the tool maps string→enum (default DD)
and rejects a
bad name **before** submit. `partition`: `int|None` (None = whole root device).
`skip`/`seek`: N **512-byte** blocks at start of input/output. `BMAPTOOL`
rejects any non-zero skip/seek (`ExecutionError`). Additive kwargs, plumbed
through the existing flash-job path; unit-tested only (SSHes to a real block
device, no hardware-free e2e).

**sd-mux GET (sd only).** `USBSDMuxDriver.get_mode()` **exists**
(`usbsdmuxdriver.py:44`) → `usbsdmux <path> get`, returns a decoded `str`
(`dut`/`host`/`off`/`client`). `LXAUSBMuxDriver` has **no** get method (only
`set_links`) — a "mux get" is SD-only; the absence of `get_usb_mux` is a
deliberate omission, not a gap. Unit-only
(shells to the exporter host).

**forward REMOTE (-R).** `SSHDriver.forward_remote_port(remoteport, localport)`
(`sshdriver.py:313`, a `@contextmanager` + `@Driver.check_active`) emits
`ssh -O forward -R…` over the shared ControlMaster. **Both** ports are required
(no `localport=None` auto-assign, unlike `-L`); a connection to `remoteport` on
the target is forwarded to `localhost:localport` on the MCP host, so a local
service must already be listening. The CM yields **nothing** (the docstring's
"returns the local port" is wrong).

**release_from + reservation wait (coordinator-only).** `release_place_rpc(name,
fromuser="")` (`coordinator.py:218`) already exists; `ReleasePlace` does **no
format validation** on `fromuser` — empty → unconditional kick; non-empty and
`place.acquired != fromuser` → **silent no-op that returns success**
(`coordinator.py:905`); equal → releases. This is why the `release_from` tool
reads back the place state and reports whether the release actually happened. A
standalone `reservation_wait` = `create_reservation` + `poll_reservation` until
`allocated`: only *acquired* reservations auto-refresh, so a `waiting`/
`allocated` one **expires ~60s after creation unless polled** — polling IS the
keepalive (`coordinator.py:1103`); the wait tool block-and-polls internally,
and documents that the reservation dies ~60s after it returns unless the caller
acquires or re-polls.

<details>
<summary>Appendix — two-same-class exporter recipe + release_from readback</summary>

Two `NetworkPowerPort`s in one group via an explicit `cls:` (the exporter parses
each YAML key as the resource *name*, `exporter.py:865`):

```yaml
e2e-group:
  power_a: {cls: NetworkPowerPort, model: rest, host: 'http://127.0.0.1:PORT/relay/{index}/value', index: 0}
  power_b: {cls: NetworkPowerPort, model: rest, host: 'http://127.0.0.1:PORT/relay/{index}/value', index: 1}
```

The place gains two 4-segment matches (`…/NetworkPowerPort/power_a` and
`…/power_b`); names arrive distinct on the client via `acquired_resources`, and
the existing `fake_switch.py` already keys state by full path (`relay/0` vs
`relay/1`) — a ready two-endpoint oracle. **release_from readback:** because a
`fromuser` mismatch is a silent success, the tool re-reads `show_place` and
reports `released` True iff the place is no longer acquired by that identity.

</details>
