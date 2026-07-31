# RemotePlaceManager must never self-connect in-process — pre-seat the session

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

Letting labgrid's `RemotePlaceManager` create its own coordinator connection
from inside a process that already holds one breaks: its new channel shares
the existing gRPC subchannel, the coordinator sees a duplicate peer, and the
stream aborts (see [Shared-subchannel test trap]). How to apply: set the
singleton's `.session`/`.loop`/`.env` to our 3-member adapter BEFORE
constructing any `RemotePlace`, so labgrid's `_start()` short-circuits. Full
adapter shape + wiring order: `docs/DESIGN.md` §11.9 and `target.py`.
