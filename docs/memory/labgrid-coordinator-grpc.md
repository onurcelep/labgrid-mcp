# labgrid coordinator gRPC quirks: no client-facing version, keepalive required

Learned: 2026-07-22 (re-verify before acting if the labgrid pin moves past 26.x)

Two non-obvious facts about labgrid 26.0's coordinator protocol, learned by
reading the installed package source during Phase 0:

- **The coordinator never sends its version to clients.** `ClientOutMessage`
  carries only `sync`/`updates`; the version goes to *exporters* only
  (`ExporterOutMessage.hello.version`). So `coordinator_info.version` is
  `null` in production by protocol, not by bug — don't "fix" it, and don't
  build features that assume a client-visible version.
- **gRPC keepalive options are mandatory for reconnect to work.** Without
  labgrid-client's channel options (`coordinator.py::CHANNEL_OPTIONS`), a
  half-open idle connection never surfaces an error, the stream never fails,
  and the reconnect loop never fires. Any new channel must pass the same
  options.
