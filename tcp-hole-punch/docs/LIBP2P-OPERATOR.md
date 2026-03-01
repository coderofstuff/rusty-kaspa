# Libp2p / DCUtR Operator Notes

## Modes and defaults
- **bridge** (default): hybrid; libp2p runtime runs by default.
- **off**: disable libp2p at runtime with `--no-libp2p` (or `--libp2p-mode=off`).
- **bridge**: hybrid; libp2p runtime runs (helper/reservations/DCUtR) but outbound Kaspa dials use TCP, with a libp2p attempt/cooldown path available in the connector for future hybrid work. Safe for mainnet nodes that need TCP peers.
- **full/helper**: libp2p stack enabled (helper == full for now) and used for outbound; overlay-only mode for the NAT lab.
- Full/helper remain overlay-only; bridge/role changes do **not** alter DCUtR behaviour in full mode.
- Helper API binds only when `--libp2p-helper-listen <addr>` is set (e.g., `127.0.0.1:38080`).
- Ports: TCP P2P port stays unchanged (`--listen`/default p2p port); libp2p uses a dedicated port (`--libp2p-listen-port`, default `p2p_port+1`). Libp2p is intentionally **not** multiplexed on the P2P TCP port.
- AutoNAT posture: client+server enabled in full/helper modes; server is public-only by default (`server_only_if_public=true`). Labs can opt into private IP reachability with `--libp2p-autonat-allow-private`.

## Terminology
- "AutoNAT public escalation" in this branch means: role promotion to **public libp2p relay advertising**.
- It is not generic TCP publicness on the legacy `--listen` port.

## Roles
- `--libp2p-role=public|private|auto` (default `auto`).
- In `auto`, runtime starts from private posture and promotes to `public` only after:
  - AutoNAT public confirmations (threshold from `--libp2p-autonat-confidence-threshold`, default 3),
  - one inbound direct **non-relay libp2p** connection,
  - and at least one usable external address.
- Auto mode demotes back to `private` when those signals age out.
- **public**: advertises the libp2p relay service bit/relay_port and keeps the existing libp2p inbound split alongside TCP inbound peers.
- **private/auto**: does **not** advertise relay capability; libp2p inbound peers are capped (default 8) while TCP inbound limits stay unchanged.

## What AutoNAT proves
- AutoNAT evidence applies to the libp2p transport endpoint (the libp2p listener port), not to legacy Kaspa TCP P2P alone.
- Default port mapping is `libp2p_port = p2p_port + 1`, but operators can override both explicitly.
- If only legacy P2P TCP is reachable and libp2p port is not reachable, auto role should not promote to public relay advertising.

## Identity & privacy
- Default identity is **ephemeral**. Persist only when `--libp2p-identity-path <path>` is provided.
- Status call exposes current PeerId and whether it is persisted; operators should assume persisted IDs reveal linkage across sessions.

## Inbound limits
- Global inbound limit unchanged from TCP settings.
- Libp2p inbound soft caps: per-relay bucket and an “unknown relay” bucket (defaults 4 / 8). Override via CLI/env:
  - `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
- Private role adds a libp2p-specific inbound cap (default 8) applied **only** to libp2p peers; TCP inbound is unaffected.
- Reservation refresh uses exponential backoff to avoid relay spam; failed attempts delay retries, successful reservations refresh on a timer.

## Config surface (CLI/config-file highlights)
- `--libp2p-mode`, `--libp2p-role`, `--libp2p-identity-path`, `--libp2p-helper-listen`, `--libp2p-listen-port`
- `--libp2p-autonat-allow-private`: Allow AutoNAT to discover and verify private IPs (e.g. for labs). Default: off (global only).
- Reservations: `--libp2p-reservations` (comma-separated)
- External announce: `--libp2p-external-multiaddrs`, `--libp2p-advertise-addresses`
- Inbound caps: `--libp2p-relay-inbound-cap`, `--libp2p-relay-inbound-unknown-cap`
- Relay selection seed (deterministic lab/debug runs): `--libp2p-relay-rng-seed`
- Relay advertise metadata: `--libp2p-relay-advertise-capacity`, `--libp2p-relay-advertise-ttl-ms`
- Precedence: **CLI > config-file > defaults**.

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
- Run: `--no-libp2p` (or `--libp2p-mode=off`).

## Harness
- For libp2p/DCUtR manual checks on the `kaspa-p2p-libp2p` crate (not `kaspad`), run the example harness with that crate feature:
  - `cargo run -p kaspa-p2p-libp2p --example dcutr_harness --features libp2p -- <ip:port>`
  - Or set `LIBP2P_TARGET_ADDR=<ip:port>` to attempt an outbound dial; otherwise it prints the local peer ID and waits for inbound streams.
- DCUtR integration smoke tests live under:
  - `components/p2p-libp2p/tests/dcutr.rs`
  - `components/p2p-libp2p/tests/dcutr_advertisement.rs`
- These tests are no longer ignored in crate test runs.

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
