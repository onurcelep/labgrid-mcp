# labgrid ReleasePlace with empty fromuser is an unconditional kick

Learned: 2026-07-22 (re-verify if the labgrid pin moves past 26.x)

The coordinator's `ReleasePlace` does NO owner check when `fromuser` is empty —
any session can release anyone's place. With a non-empty `fromuser` it
equality-compares against `place.acquired` (the full `"host/user"` holder
string) and silently no-ops on mismatch. How to apply: a safe self-release
sends `fromuser=<own identity>` so a stale local snapshot can never kick the
real owner; reserve empty-`fromuser` for an explicit kick operation. Details +
citations: `docs/DESIGN.md` §11.8.
