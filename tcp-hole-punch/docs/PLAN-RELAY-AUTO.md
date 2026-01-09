# DCUtR Relay Discovery + Selection Plan

Goal: follow Michael’s constraints precisely.

## Goals
- Use the existing dedicated relay server port (separate from TCP P2P).
- Private nodes can connect via relays without eclipse risk.
- Private nodes stay near ~8 inbound libp2p peers (no surprise surge).

## Current state (in code today)
Status tags: [present] [partial] [missing]
- Libp2p mode bridge/full; role defaults to private. [present]
- PathUpgradeEvent -> UpgradeCoordinator closes relay after direct path. [present]
- Synthetic address suppression in address manager (prevents DCUtR pollution). [present]
- Private inbound cap exists and enforced (default 8). [present]
- Per‑relay inbound caps exist (config + enforcement). [present]
- Relay capability advertisement (service bit + relay_port + capacity + ttl) exists and is consumed by relay pool. [present]
- Relay reservations support static config and auto selection with backoff. [present]
- Auto role detection wired to AutoNAT + direct reachability. [present]

## Decisions (from Michael + this thread)
- Relay server: dedicated port already exists; enable for public nodes. If public detection unknown, start server but advertise only when public confirmed.
- Relay selection: **one relay per private peer** (hard cap = 1).
- Private inbound cap: default 8 (libp2p only), configurable.

## Architecture (target)
1) Relay capability gossip
   - Public node advertises: relay listen addr + capacity + ttl + role.
   - Private nodes store relay pool with scoring.
   - Note: service bit + relay_port already propagate; missing consumer/selection.

2) Relay pool + selection
   - Maintain N candidates, score by uptime/latency/failure rate.
   - Connection policy:
     - Prefer direct to public peers.
     - For private peers, pick **distinct relay** per peer.
     - Enforce max peers per relay = 1 (default).
   - Diversity constraints: avoid same /16 (v4) or /48 (v6) where alternatives exist.
   - Multi-source discovery: never rely on a single channel or the target peer for relay candidates (min_sources enforced).

3) Rotation
   - Rotate relay on: failures, timeouts, stale ttl, or long-lived bias.
   - Backoff + quarantine relays with repeated failures.
   - Guard against upgrade loops (relay→direct→relay churn) with cooldowns.

4) Private inbound cap
   - Enforce libp2p inbound cap for role=private.
   - Separate from TCP inbound cap (no change to TCP).
   - Note: already enforced in connection manager.

5) Role detection (optional, future)
   - AutoNAT + inbound direct success + stable external addr.
   - Hysteresis (N successes over T minutes) to avoid flapping.
   - If uncertain: keep role=private, no public advertisement.
   - Note: AutoNAT exists but is not wired into role resolution.

## Risk checklist (oracle)
- DCUtR upgrade races: double‑dial, wrong connection closed, half‑upgrade fallback.
- Timeouts + per‑peer/per‑relay backoff + cancellation required to prevent thrash.
- Address hygiene: filter private/loopback unless LAN mode; cap + dedupe advertised addrs.
- DCUtR attempt gating: skip when NAT likely symmetric or no usable public addrs.
- Trust model: open relays require diversity + multi‑source discovery; avoid “fastest wins”.
- Observability: log selection decisions, backoff, upgrade outcomes; add metrics.
- Hard safety limits: cap candidates, parallel dials, reservation count, attempt budgets.

## Test plan (expanded)
- Deterministic relay selection (seeded RNG).
- Adversarial relay pool (many relays in same subnet/ASN) → enforce diversity.
- Poisoned relay (accepts then throttles) → score decay + rotation.
- DCUtR upgrade loop guard (ensure cooldown).

## Defaults
- Relay listen port: existing libp2p relay port (lab uses 16112), override via flag if needed.
- Private inbound cap: 8 (already default).
- Max peers per relay: 1.
- Relay min sources: 2.

## Work plan
### Phase 1: Relay discovery + pool
- Add relay capability gossip message (address + ttl + capacity).
- Store relay pool in libp2p component or connection manager.
- Wire into startup: public role advertises; private role subscribes.

### Phase 2: Selection + rotation
- Implement relay scoring + per-relay caps.
- Integrate into outbound peer selection:
  - direct public peers first
  - private peers via distinct relays
- Rotation logic + backoff.

### Phase 3: Caps + controls
- Enforce private inbound cap in libp2p inbound admission path.
- Flags:
  - `--libp2p-relay-listen-port`
  - `--libp2p-max-relays`
  - `--libp2p-max-peers-per-relay`
  - `--libp2p-inbound-cap-private`

### Phase 4: Auto role (optional)
- AutoNAT event sink + stability window.
- Switch to public only after stable reachability.

## Test plan
- Lab: relay discovery + selection (A/B via different relays).
- Eclipse guard: ensure max 1 peer per relay in steady state.
- Cap test: private inbound stays <= 8.
- Rotation: kill relay, confirm reselect + DCUtR success.

## Open questions
- Exact gossip channel (address flow vs new message).
- Where to store relay pool (address manager vs libp2p service).
- Relay capacity representation (simple int vs weighted).
