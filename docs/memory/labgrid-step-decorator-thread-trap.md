# labgrid's @step machinery is process-global and thread-unsafe

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

Public driver methods (`read`/`write`, power `on`/`get`, ...) are `@step`
decorated over one shared process-global stack with an `assert` in pop — a
concurrent call from a second thread crashes with a bare `AssertionError`
(reproduced deterministically). How to apply: any long-lived background
thread using a driver (the console reader) must call the undecorated
internals (`sd._read`/`sd._write`) and own the active-guard/ordering itself;
never mix public driver calls across threads. Details: `docs/DESIGN.md`
§11.10.
