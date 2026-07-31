# Cross-thread @step crashes are fixed by rebinding labgrid.step.steps

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

Concurrent public driver calls from different threads corrupt labgrid's shared
step stack (see [@step thread-unsafety trap]). The process-wide fix: at server
init, rebind `labgrid.step.steps` (via `importlib.import_module("labgrid.step")`
— a plain `import as` alias would shadow) to a thread-local-stack `Steps`.
Proven: 0 assertion errors vs 592 stock under a two-thread hammer. Reporters
holding the old object via from-import don't matter — stack ops resolve the
module global at call time. Installed by `install_thread_safe_steps()`
(idempotent) from both TargetManager and JobRegistry init. Details:
`docs/DESIGN.md` §11.11.
