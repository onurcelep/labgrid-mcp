# mypy --strict blocks patching `module.stdlib_name` from tests

Learned: 2026-07-31

Monkeypatching a stdlib/third-party import through another module's
attribute — e.g. `monkeypatch.setattr(demo.shutil, "which", ...)` to fake
`shutil.which` inside `labgrid_mcp.demo` — works at runtime but fails
`mypy --strict` with `attr-defined: Module "labgrid_mcp.demo" does not
explicitly export attribute "shutil"`: accessing a plain `import shutil`
through another module's namespace is an implicit re-export, which strict
mode disallows even though nothing is actually re-exported on purpose. Fix:
`import shutil` directly in the test file and patch that module object
instead of reaching through the module under test — Python modules are
singletons, so the target module's own `shutil.which(...)` call resolves
through the very same patched object. Applies to any such import (`time`,
`socket`, `subprocess`, ...) a test wants to patch by reaching through
`<module_under_test>.<name>`.
