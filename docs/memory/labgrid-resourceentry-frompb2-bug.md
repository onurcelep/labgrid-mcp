# labgrid 26.0 ResourceEntry.from_pb2 is broken — use data_from_pb2

Learned: 2026-07-22 (re-verify on every labgrid version bump — may be fixed upstream)

`labgrid.remote.common.ResourceEntry.from_pb2` asserts
`isinstance(pb2, …Place)` (copy-paste bug in labgrid 26.0), so it raises
`AssertionError` on every real `Resource` message. It looks like the obvious
deserialization entry point and breaks only at runtime. How to apply:
construct with `ResourceEntry(ResourceEntry.data_from_pb2(resource_pb2))` —
`data_from_pb2` is correct and folds `extra` into `params["extra"]`. Related
serialization traps (`Place.asdict()` omits `name`, `Reservation.asdict()`
omits `token`): `docs/DESIGN.md` §11.5.
