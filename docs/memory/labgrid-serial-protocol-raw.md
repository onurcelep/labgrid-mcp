# NetworkSerialPort needs protocol:raw for a plain-TCP (no-ser2net) console

Learned: 2026-07-23 (re-verify if the labgrid pin moves past 26.x)

labgrid's SerialDriver defaults to pyserial `rfc2217://` for a remote
NetworkSerialPort, which requires a compliant RFC2217 server (ser2net).
Setting the resource param `protocol: raw` switches it to `socket://` — a
plain TCP client — so a statically exported NetworkSerialPort can point at
any simple TCP bridge (our hardware-free console e2e). Idle reads raise
`pexpect.TIMEOUT` (not empty bytes); a peer close raises `SerialException`.
Details + proven exporter YAML: `docs/DESIGN.md` §11.10.
