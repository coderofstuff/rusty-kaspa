# Libp2p / DCUtR Operator Notes (feature-gated)

## Modes and defaults
- **off** (default): libp2p disabled; transport is plain TCP. No libp2p deps pulled in unless compiled with `--features libp2p`.
- **bridge**: hybrid; libp2p runtime runs (helper/reservations/DCUtR) but outbound Kaspa dials use TCP, with a libp2p attempt/cooldown path available in the connector for future hybrid work. Safe for mainnet nodes that need TCP peers.
- **full/helper**: libp2p stack enabled (helper == full for now) and used for outbound; overlay-only mode for the NAT lab.
- Helper API binds only when `--libp2p-helper-listen <addr>` is set (e.g., `127.0.0.1:38080`).
- Ports: TCP P2P port stays unchanged (`--listen`/default p2p port); libp2p uses a dedicated port (`--libp2p-listen-port` or `KASPAD_LIBP2P_LISTEN_PORT`, default `p2p_port+1`). Libp2p is intentionally **not** multiplexed on the P2P TCP port.
- AutoNAT posture: client+server enabled in full/helper modes; server is public-only by default (`server_only_if_public=true`). Labs can opt into private IP reachability with `--libp2p-autonat-allow-private` / `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true`.

## Roles
- `--libp2p-role=public|private|auto` (default `auto`; auto currently resolves to private unless a helper listen address is configured).
- **public**: advertises the libp2p relay service bit/relay_port and keeps the existing libp2p inbound split alongside TCP inbound peers.
- **private/auto**: does **not** advertise relay capability; libp2p inbound peers are capped (default 16) while TCP inbound limits stay unchanged.

## Identity & privacy
- Default identity is **ephemeral**. Persist only when `--libp2p-identity-path <path>` is provided (or `KASPAD_LIBP2P_IDENTITY_PATH`).
- Status call exposes current PeerId and whether it is persisted; operators should assume persisted IDs reveal linkage across sessions.

## Inbound limits
- Global inbound limit unchanged from TCP settings.
- Libp2p inbound soft caps: per-relay bucket and an “unknown relay” bucket (defaults 4 / 8). Override via CLI/env:
  - `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
  - `KASPAD_LIBP2P_RELAY_INBOUND_CAP`, `KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP`
- Private role adds a libp2p-specific inbound cap (default 16) applied **only** to libp2p peers; TCP inbound is unaffected.
- Reservation refresh uses exponential backoff to avoid relay spam; failed attempts delay retries, successful reservations refresh on a timer.

## Config surface (env/CLI highlights)
- `--libp2p-mode`, `--libp2p-role`, `--libp2p-identity-path`, `--libp2p-helper-listen`, `--libp2p-listen-port`
- `--libp2p-autonat-allow-private`: Allow AutoNAT to discover and verify private IPs (e.g. for labs). Default: off (global only).
- Reservations: `--libp2p-reservations` (comma-separated) or `KASPAD_LIBP2P_RESERVATIONS`
- External announce: `--libp2p-external-multiaddrs`, `--libp2p-advertise-addresses`
- Inbound caps: `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
- Env overrides all have `KASPAD_LIBP2P_*` prefix; **CLI > env > defaults** for precedence.

## Address metadata
- NetAddress carries a `services` bitflag and optional `relay_port` to mark peers that can act as libp2p relays and which port to use. Defaults are zero/None so unaware peers ignore them.
- The relay flag is set only when libp2p is enabled; address book merges metadata (bitwise OR on services, relay port upgraded if provided). No change to equality/hashing (still IP:port).
- This wire addition is backwards-compatible (proto fields optional) and does not affect consensus or TCP behaviour when libp2p is off.

## Examples
- Plain TCP public node (libp2p off): default build/run (no flags).
- Mainnet node with bridge mode (default private role, keeps TCP outbound): `kaspad --libp2p-mode=bridge`
- Public relay with persistent ID + advertised addresses (bridge or full):
  ```
  target/debug/kaspad --simnet --libp2p-mode=bridge --libp2p-role=public --libp2p-helper-listen=0.0.0.0:38080 \
    --libp2p-identity-path=/var/lib/kaspad/libp2p.id \
    --libp2p-reservations=/ip4/203.0.113.10/tcp/16111/p2p/12D3KooWRelayPeer \
    --libp2p-external-multiaddrs=/ip4/198.51.100.50/tcp/16111 \
    --libp2p-advertise-addresses=198.51.100.50:16111
  ```
- Private node behind NAT using DCUtR (no relay advertisement):
  ```
  kaspad --libp2p-mode=bridge --libp2p-role=private \
    --libp2p-helper-listen=127.0.0.1:38080 \
    --libp2p-reservations=/ip4/RELAY_IP/tcp/16112/p2p/RELAY_PEER_ID \
    --libp2p-external-multiaddrs=/ip4/MY_PUBLIC_IP/tcp/16112
  ```
- **Lab / Private Relay:** Use `--libp2p-autonat-allow-private` if your relay or peers are on private IPs (e.g. 10.x.x.x, 192.168.x.x). Do NOT use this on public mainnet relays.

## Disable libp2p
- Build: omit `--features libp2p` (or use `--no-default-features` when libp2p is not a default member).
- Run: `--libp2p-mode off` (default) and do not set helper listen.

## Harness
- For libp2p/DCUtR manual checks, build with `--features libp2p` and run the example harness:
  - `cargo run -p kaspa-p2p-libp2p --example dcutr_harness --features libp2p -- <ip:port>`
  - Or set `LIBP2P_TARGET_ADDR=<ip:port>` to attempt an outbound dial; otherwise it prints the local peer ID and waits for inbound streams.
- A DCUtR-focused integration test exists under `components/p2p-libp2p/tests/dcutr.rs` but is `#[ignore]` in CI due to an upstream relay-client drop panic; run manually if you need a quick punch sanity check.

## Helper API (Testing / Debugging)

The helper API provides a simple JSON interface for triggering libp2p operations. **For testing only** - binds to localhost by default.

### Enabling
```bash
kaspad --libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080 ...
```

### Supported Actions

**Status check:**
```bash
echo '{"action":"status"}' | nc -w 5 127.0.0.1 38080
# Response: {"ok":true}
```

**Dial via relay (triggers DCUtR hole punch):**
```bash
echo '{"action":"dial","multiaddr":"/ip4/RELAY_IP/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/TARGET_PEER_ID"}' | nc -w 5 127.0.0.1 38080
# Response: {"ok":true,"msg":"dial successful"}
# On error: {"ok":false,"error":"..."}
```

### Verifying DCUtR Success

After a relay dial, check the kaspad logs for:
```
# Success indicators:
libp2p dcutr event: ... result: Ok(ConnectionId(...))
peer XXXX connected DIRECTLY (no relay)

# Failure indicators:
libp2p dcutr event: ... result: Err(Error { inner: InboundError(Protocol(NoAddresses)) })
```

### Example: Full DCUtR Test

```bash
# 1. Start relay node (public IP, no NAT)
kaspad --testnet --libp2p-mode=full --libp2p-listen-port=16112

# 2. Start Node A (behind NAT)
kaspad --testnet --libp2p-mode=full --libp2p-listen-port=16112 \
  --libp2p-external-multiaddrs=/ip4/WAN_IP_A/tcp/16112 \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/RELAY_IP/tcp/16112/p2p/RELAY_PEER_ID

# 3. Start Node B (behind NAT)
kaspad --testnet --libp2p-mode=full --libp2p-listen-port=16112 \
  --libp2p-external-multiaddrs=/ip4/WAN_IP_B/tcp/16112 \
  --libp2p-reservations=/ip4/RELAY_IP/tcp/16112/p2p/RELAY_PEER_ID

# 4. From Node A, dial Node B via relay (triggers DCUtR)
echo '{"action":"dial","multiaddr":"/ip4/RELAY_IP/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 5 127.0.0.1 38080
```

### Important Notes
- Nodes must have `--libp2p-external-multiaddrs` configured with their WAN IP for DCUtR to have addresses to send
- Both nodes should have reservations on the same relay
- The helper API only binds when `--libp2p-helper-listen` is explicitly set
- Use `--loglevel=debug` to see detailed DCUtR negotiation logs
