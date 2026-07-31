# SSH-bound features test against a user-mode sshd — no VM, no root

Learned: 2026-07-24 (recipe in DESIGN §11.13 appendix)

labgrid's SSHDriver connects directly to `NetworkService.address:port` with
`-F none` and known-hosts off, honoring non-22 ports and an injected
`keyfile` — so a user-mode `/usr/sbin/sshd -f <own config>` on a high
localhost port is a full "DUT" for ssh_run/put/get/forward e2e, portable to
ubuntu CI unchanged. Traps: (1) OpenSSH ≥9 scp uses SFTP — the sshd config
MUST have a `Subsystem sftp` line or put/get silently fail while ssh_run
works; (2) sshd rejects at auth if the user's registered login shell binary
is missing (this Mac's dangling fish shell → local e2e skips with evidence);
(3) ubuntu-latest runners ship openssh-client only — CI must
`apt-get install openssh-server` or the e2e silently skips there too.
