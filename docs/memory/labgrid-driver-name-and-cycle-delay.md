# labgrid: name-select same-class resources; power cycle delay is an attr

Learned: 2026-07-24 (re-verify if the labgrid pin moves past 26.x)

Two driver-binding traps (both DESIGN §11.14):

- **Multiple same-class resources need `set_binding_map`.** Constructing a
  driver `cls(target, name=None)` on a place with two resources of that class
  raises `NoResourceFoundError("multiple resources matching …")` — the place
  is unusable by default. Select one with
  `Target.set_binding_map({binding_key: resource_name})` BEFORE constructing
  the driver (equivalent to `get_driver(cls, name=…)`); the child resource's
  `.name` equals the coordinator resource name. Our tools take `resource_name`
  and cache the driver per `(place, kind, name)`.
- **Power cycle delay is a sticky instance attr, not an argument.**
  `NetworkPowerDriver.cycle()` takes no args; the off→on gap is `driver.delay`
  (labgrid default 2.0). Set it before each `cycle()` AND reset it to the
  default when unset — a cached driver leaks the prior delay otherwise.
