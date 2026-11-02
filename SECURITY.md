# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub:
[Report a vulnerability](https://github.com/onurcelep/labgrid-mcp/security/advisories/new).
Do not open a public issue.

Include what you found, how to reproduce it, and what an attacker could
achieve. You will get an acknowledgement within a week. Fixes are released
as a new version, with a GitHub security advisory crediting you unless you
prefer otherwise.

## Supported versions

Only the latest release receives security fixes.

## Scope

In scope, for example:

- Bypassing the guardrails: running a gated tool while `LABGRID_MCP_READONLY`
  is set or without the matching `LABGRID_MCP_ALLOW` entry.
- Bypassing the client-side force-gate on place metadata.
- Tool input that leads to command execution, file access, or network
  access beyond what the tool documents.

Out of scope:

- Anyone who can reach the labgrid coordinator being able to use it.
  Authentication is network-level by design, as in labgrid itself; protect
  the coordinator with a VPN or tunnel.
- Vulnerabilities in labgrid itself; report those to the
  [labgrid project](https://github.com/labgrid-project/labgrid).
