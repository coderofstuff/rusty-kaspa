# Libp2p / DCUtR Operator Notes (feature-gated)

## Modes and defaults
- **off** (default): libp2p disabled; transport is plain TCP. No libp2p deps pulled in unless compiled with `--features libp2p`.
- **full/helper**: libp2p stack enabled (helper == full for now). Requires explicit `--libp2p-mode full|helper`.
- Helper control (relay/DCUtR) binds only when `--libp2p-helper-listen <addr>` is set (**currently stubbed; no control-plane listener is started**).
- Ports: helper listen is explicit; TCP P2P port stays unchanged (`--listen`/default p2p port).

## Identity & privacy
- Default identity is **ephemeral**. Persist only when `--libp2p-identity-path <path>` is provided (or `KASPAD_LIBP2P_IDENTITY_PATH`).
- Status call exposes current PeerId and whether it is persisted; operators should assume persisted IDs reveal linkage across sessions.

## Inbound limits
- Global inbound limit unchanged from TCP settings.
- Libp2p inbound soft caps: per-relay bucket and an “unknown relay” bucket (defaults 4 / 8). Override via CLI/env:
  - `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
  - `KASPAD_LIBP2P_RELAY_INBOUND_CAP`, `KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP`
- Reservation refresh uses exponential backoff to avoid relay spam; failed attempts delay retries, successful reservations refresh on a timer.

## Config surface (env/CLI highlights)
- `--libp2p-mode`, `--libp2p-identity-path`, `--libp2p-helper-listen`
- Reservations: `--libp2p-reservations` (comma-separated) or `KASPAD_LIBP2P_RESERVATIONS`
- External announce: `--libp2p-external-multiaddrs`, `--libp2p-advertise-addresses`
- Inbound caps: `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
- Env overrides all have `KASPAD_LIBP2P_*` prefix; **CLI > env > defaults** for precedence.

## Examples
- Plain TCP public node (libp2p off): default build/run (no flags).
- Private relay (ephemeral ID): `--features libp2p --libp2p-mode full --libp2p-helper-listen 0.0.0.0:38080`
- Public relay with persistent ID + advertised addresses:
  ```
  target/debug/kaspad --simnet --libp2p-mode=full --libp2p-helper-listen=0.0.0.0:38080 \
    --libp2p-identity-path=/var/lib/kaspad/libp2p.id \
    --libp2p-reservations=/ip4/203.0.113.10/tcp/16111/p2p/12D3KooWRelayPeer \
    --libp2p-external-multiaddrs=/ip4/198.51.100.50/tcp/16111 \
    --libp2p-advertise-addresses=198.51.100.50:16111
  ```

## Disable libp2p
- Build: omit `--features libp2p` (or use `--no-default-features` when libp2p is not a default member).
- Run: `--libp2p-mode off` (default) and do not set helper listen.

## Harness
- For libp2p/DCUtR manual checks, build with `--features libp2p` and run the example harness:
  - `cargo run -p kaspa-p2p-libp2p --example dcutr_harness --features libp2p -- <ip:port>`
  - Or set `LIBP2P_TARGET_ADDR=<ip:port>` to attempt an outbound dial; otherwise it prints the local peer ID and waits for inbound streams.
