# Project memory — index

One line per fact file in this directory. Agents: read this index before
nontrivial work and open only the entries relevant to your task. Write
rules and conventions: `factory:repo-memory` skill.

<!-- - [short title](fact-file.md) — one-line hook: when this matters -->

- [@claude CI runs failing silently](ci-claude-silent-failures.md) — @claude/review runs green but do nothing, or die instantly: diagnosis order (token first), verification traps, max-turns sizing.
- [labgrid coordinator gRPC quirks](labgrid-coordinator-grpc.md) — coordinator version never reaches clients (null by protocol); keepalive channel options are mandatory or reconnect never fires.
- [labgrid unary RPCs are session-bound](labgrid-unary-rpcs-session-bound.md) — unary RPCs must ride the ClientStream channel with a live handshake or fail FAILED_PRECONDITION; matters for anything calling acquire/release/reservations.
- [ResourceEntry.from_pb2 broken in labgrid 26.0](labgrid-resourceentry-frompb2-bug.md) — asserts against Place; use ResourceEntry(ResourceEntry.data_from_pb2(...)) when deserializing resources.
- [ReleasePlace empty-fromuser kick trap](labgrid-release-fromuser-kick-trap.md) — empty fromuser releases unconditionally (no owner check); safe self-release sends fromuser=identity; matters for any release/mutation code.
- [Shared-subchannel test trap](labgrid-shared-subchannel-test-trap.md) — second client channel from one process to one coordinator aborts with UNKNOWN unless grpc.use_local_subchannel_pool is set; test-only.
- [RemotePlace self-connect trap](labgrid-remoteplace-selfconnect-trap.md) — pre-seat RemotePlaceManager.session with our adapter before any RemotePlace, or labgrid self-connects and aborts in-process; matters for all Target/driver code.
- [RemotePlaceManager append-only leak](labgrid-remoteplacemanager-leak-trap.md) — cache one Target per place and prune via parent-match under _MANAGER_LOCK; never rebuild per action.
- [@step thread-unsafety trap](labgrid-step-decorator-thread-trap.md) — labgrid's @step stack is process-global; background threads must use undecorated _read/_write or crash; matters for console/any threaded driver use.
- [protocol:raw for TCP serial](labgrid-serial-protocol-raw.md) — NetworkSerialPort defaults to rfc2217 (needs ser2net); protocol:raw → socket:// plain TCP enables the hardware-free console chain.
- [Thread-local steps fix](labgrid-thread-local-steps-fix.md) — rebind labgrid.step.steps at init to fix cross-thread @step crashes process-wide; install_thread_safe_steps() is the entry point.
- [Flash hardware boundary](labgrid-flash-hardware-boundary.md) — flash resources are udev USB exports + exporter-SSH ops; no hardware-free real chain exists; job machinery tests use scripted fake CLIs.
- [uv run re-sync trap](uv-run-resync-trap.md) — uv run silently reverts uv-pip-installed overrides to the lockfile pin; pass --no-sync after any override (canary workflow depends on it).
- [Metadata RPCs have no ownership guard](labgrid-metadata-no-ownership-guard.md) — any session can edit/delete anyone's place incl. mid-acquisition; our client-side force-gated refusals are the only protection; plus DeletePlace/alias/match traps.
- [User-mode sshd recipe](user-mode-sshd-recipe.md) — SSH e2e needs no VM/root: high-port sshd + keyfile injection; traps: Subsystem sftp mandatory for put/get, dangling login shell rejects auth, CI must install openssh-server.
- [Driver name-select + cycle delay](labgrid-driver-name-and-cycle-delay.md) — two same-class resources need set_binding_map (else "multiple resources matching"); power cycle delay is a sticky driver attr, not an arg — reset it when unset.
- [mypy --strict monkeypatch re-export trap](mypy-strict-monkeypatch-reexport-trap.md) — patching `module.stdlib_name` (e.g. `demo.shutil.which`) from a test fails strict mode; `import` the stdlib module directly in the test and patch that instead.
