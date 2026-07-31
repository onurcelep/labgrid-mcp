# @claude CI runs failing silently: diagnosis order

Seeded by factory-init; full playbook in the `factory:ci-agent-ops` skill.

**The action can mask total failure behind a green check.** A run whose
result JSON shows `"is_error": true`, `num_turns: 1`, `total_cost_usd: 0`
did no work at all, whatever the check color says. Symptoms: an issue
answered only by "I'll analyze this and get back to you", or a green
review check with no review comment. The error text is hidden by default;
add `show_full_output: true` to the workflow (must be merged to the
default branch to take effect) to see the SDK stream.

**Diagnosis order for the instant-`is_error`/$0 signature:**

- **Retry once first** (comment "@claude please retry"): transient API
  errors produce the identical signature and clear on retry. Only a
  reproducing failure warrants the steps below.
- **OAuth token first.** Rotate with `claude setup-token` and
  `gh secret set CLAUDE_CODE_OAUTH_TOKEN -R <repo>`. You cannot verify a
  token locally via `CLAUDE_CODE_OAUTH_TOKEN=... claude -p ...` — the CLI
  silently prefers keychain credentials, so that test passes with any
  garbage value. Verify in CI with a cheap "@claude reply pong" issue.
- **Cross-repo experiment discriminates fast:** trigger @claude in another
  repo with the same secret value. Same result → token/account; different
  → this repo's config.
- **Not the action version first.** Version-pinning on a correlation
  across a run-free time gap has been tried and was wrong; get the actual
  error before shipping any fix.

**Separate, honestly-reported failure modes:** `error_max_turns` (raise
the cap; implement-and-PR needs ~15–25 turns) and `git push` 403 as
`github-actions[bot]` (workflow needs `contents/pull-requests/issues:
write`; the App token only covers comments).
