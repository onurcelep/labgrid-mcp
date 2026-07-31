# labgrid unary RPCs are bound to the ClientStream session

Learned: 2026-07-22 (re-verify before acting if the labgrid pin moves past 26.x)

The coordinator resolves every unary RPC caller via gRPC `context.peer()` to
the `ClientStream` session on the **same channel** (labgrid 26.0
`remote/coordinator.py::AcquirePlace`). A unary call on a channel with no live
handshaken stream fails with `FAILED_PRECONDITION` ("Peer … does not have a
valid session"). Why it matters: a second channel, or a call fired between a
reconnect and its handshake, fails even though the coordinator is up. How to
apply: route all unary RPCs through `CoordinatorClient`'s single stub, check
connectedness first, and treat acquire/release identity as coming from the
handshake's `startup.name` — never an RPC argument. Full protocol detail:
`docs/DESIGN.md` §11.2.
