# labgrid coordinator metadata RPCs have NO ownership guard

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

None of the eight place-metadata RPCs (add/delete place, aliases, tags,
comment, matches) consults the caller's identity or the place's
acquired/reservation state — any handshaked session can retag, re-alias, or
DELETE anyone's place, even mid-acquisition (empirically verified). Safety is
entirely client-side: our tools refuse edits on a foreign-acquired place
(and delete of an own-acquired place) unless force=True. Related traps:
DeletePlace misuses ALREADY_EXISTS for "does not exist"; DeletePlaceAlias of
a nonexistent alias crashes with gRPC UNKNOWN (pre-validate);
ResourceMatch.rename is eq=False so match identity is exporter/group/cls/name
only. Details: docs/DESIGN.md §11.12.
