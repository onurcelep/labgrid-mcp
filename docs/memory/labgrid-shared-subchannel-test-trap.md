# Two labgrid client channels from one process need a local subchannel pool

Learned: 2026-07-22 (test-only trap; production uses one channel and is unaffected)

Two gRPC channels from the same process to the same coordinator target share a
subchannel by default, so the coordinator sees one peer and its
`assert peer not in self.clients` aborts the second ClientStream with an opaque
`UNKNOWN AssertionError`. How to apply: any test or tool opening a second
client to one coordinator must pass `("grpc.use_local_subchannel_pool", 1)` in
its channel options (or spawn a separate process). Details: `docs/DESIGN.md`
§11.8.
