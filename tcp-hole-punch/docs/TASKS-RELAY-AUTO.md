# Relay Auto - Remaining Tasks (Detailed)

Goal: fully automatic private<->private connectivity in public with DCUtR.

## Decisions (locked for implementation)
- **Proto fields:** add `libp2pPeerId` + **compact `relayCircuitHint`** (relay ip:port or /ip4|ip6/.../tcp/...). Defer `relayPeerId` for now; rely on probe to learn relay peer id when needed.
- **Synthetic key scheme (avoid RFC1918 collisions):**
  - Use `BLAKE3(peer_id_bytes)` (or SHA-256 if already preferred), take:
    - `ip = 240.0.0.0/4` + lower 28 bits of hash
    - `port = 1024 + (hash16 % 64512)`
  - Treat synthetic netaddrs as non‑dialable for TCP; only for relay hints.
- **Default enablement:** default libp2p **Bridge** when feature enabled; keep Full opt‑in.
- **Relay advertise gating:** start relay server early, **advertise only after AutoNAT says public + direct inbound evidence in window**. Clear advertising if signal drops.

## A. Reachability gossip (private nodes)
- [ ] **Proto additions (optional fields, additive):**
  - Add `libp2pPeerId` and `relayCircuitHint` to `NetAddress` in `protocol/p2p/proto/p2p.proto`.
  - Keep tags new and optional; do not change existing tags.
- [ ] **Wire conversions:**
  - Update `protocol/p2p/src/convert/net_address.rs` to map new fields to/from `kaspa_addressmanager::NetAddress`.
  - Ensure defaults remain zero/None for older peers.
- [ ] **AddressManager merge:**
  - Merge new fields without clobbering good existing data.
  - Apply TTL semantics where relevant.
- [ ] **FlowContext advertising:**
  - When role=private and reservation active, set new fields on advertised address.
  - When role=public, clear/omit private‑only fields.

**Critical safeguard (avoid private‑IP collisions):**
- [ ] Never store RFC1918/CGNAT/loopback addresses as keys for private reachability.
  - If incoming `NetAddress` has `libp2pPeerId` but unusable IP, **synthesize a stable key** using:
    - `ip = 240.0.0.0/4 + hash(peer_id)`
    - `port = stable hash(peer_id)` into 1024–65535
  - Store metadata against this synthetic key to avoid collisions across private networks.

## B. Outbound relay dial policy
- [ ] **Dial target abstraction (minimal change):**
  - Keep `OutboundConnector::connect(String, ...)` unchanged.
  - Teach `Libp2pOutboundConnector` to treat addresses starting with `/` as multiaddr and call `dial_multiaddr`.
  - In bridge mode, if a multiaddr dial fails, **do not** TCP‑fallback.
- [ ] **ConnectionManager selection:**
  - Classify peers as “private” if `relay_role=Private` or no relay service bit.
  - For private targets with relay hint, prefer relay dial; otherwise keep direct.
  - Ensure fallback to direct only when a direct TCP target exists.
- [ ] **Distinct relay per target (practical meaning):**
  - At peer selection time, avoid filling outbound slots with targets behind the same relay. Limit N targets per relay (N=1 default).
- [ ] **Backoff + retry:**
  - Add per‑target cooldown keyed by `libp2pPeerId`.
  - Add per‑relay cooldown keyed by relay key (ip:port).
  - Add a global relay‑dial rate cap.

## C. Relay selection helpers
- [ ] **Relay assignment helper:**
  - Map `target_peer_id -> relay candidate` using RelayPool scoring.
  - Use `relay_min_sources` + prefix diversity constraints already in `RelayPool`.
- [ ] **Circuit builder:**
  - Build `/ip4|ip6/<relay.ip>/tcp/<relay.port>/p2p/<relayPeerId>/p2p-circuit/p2p/<targetPeerId>` multiaddr.
  - If relay peer id missing, `probe_relay` then retry build.
  - Validate with unit tests for v4/v6 and missing peer id cases.

## D. Defaults + ops
- [ ] **Bridge default wiring:**
  - Ensure Bridge is default when libp2p feature is enabled in public builds.
- [ ] **Relay advertise gating:**
  - Start server early but advertise relay capability only after public reachability confirmed.
  - Ensure AutoNAT promotion triggers advertisement update.

## E. Verification
- [ ] **Lab runbook:**
  - Private A/B + public relay. Confirm: discovery -> relay dial -> DCUtR upgrade -> relay close.
  - Capture logs for: relay selection, reservation, dcutr upgrade, relay close.
- [ ] **Testnet smoke:**
  - Mixed public/private nodes with metrics snapshot.
  - Validate private inbound cap stays ~8 and per‑relay caps hold.
