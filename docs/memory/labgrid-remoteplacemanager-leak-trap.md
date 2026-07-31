# RemotePlaceManager is append-only — rebuilt Targets leak into every poll()

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

labgrid's `RemotePlaceManager` singleton only ever appends resources and has
no removal API, so naively building a fresh Target per driver action grows the
manager forever and every `poll()` gets slower. How to apply: cache ONE Target
per place, and on invalidate prune the manager by parent-matching the place's
`RemotePlace` (managed children included); serialize ALL singleton mutations
via the process-wide `_MANAGER_LOCK` in `target.py` (Phase 4+ code must hold
it too). Details: `docs/DESIGN.md` §11.9.
