# labgrid renamed Reservation.token to .id; the wire field did not change

Learned: 2026-10-12 (labgrid after 26.0; re-verify on every labgrid version bump)

`labgrid.remote.common.Reservation.token` became `Reservation.id` upstream
(with `LG_TOKEN` becoming `LG_RESERVATION`), but the protobuf field on
`Reservation`, `CancelReservationRequest` and `PollReservationRequest` is
still named `token`. Code reading the wrapper attribute breaks on one side of
the rename. How to apply: read the reservation ID from the protobuf
(`res_pb2.token`), never from the `common.Reservation` object, and keep
request construction on `token=`. Public naming and the deprecated `token`
alias: `docs/DESIGN.md` §11.5.
