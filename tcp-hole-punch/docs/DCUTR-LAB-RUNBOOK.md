# DCUtR Lab Runbook

Tested and verified guide for running DCUtR hole punch tests in the NAT lab.

For lab infrastructure and router/NAT setup record, see `tcp-hole-punch/docs/NAT-LAB-RECORD.md`.

## Lab Topology

```
                    ┌─────────────────┐
                    │     Relay       │
                    │   10.0.3.26     │
                    │   (public)      │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
        ┌─────┴─────┐                 ┌─────┴─────┐
        │  Router A │                 │  Router B │
        │ NAT: Full │                 │ NAT: Full │
        │  Cone     │                 │  Cone     │
        └─────┬─────┘                 └─────┬─────┘
              │                             │
        NAT IP: 10.0.3.61            NAT IP: 10.0.3.62
              │                             │
        ┌─────┴─────┐                 ┌─────┴─────┐
        │  Node A   │                 │  Node B   │
        │192.168.1.10│                │192.168.2.10│
        └───────────┘                 └───────────┘
```

| Node | Private IP | NAT IP | libp2p Port | Role |
|------|------------|--------|-------------|------|
| Relay | 10.0.3.26 | (public) | 16112 | Public relay |
| Node A | 192.168.1.10 | 10.0.3.61 | 16112 | NAT'd client |
| Node B | 192.168.2.10 | 10.0.3.62 | 16112 | NAT'd client |

## SSH Access

```bash
# Relay (direct)
ssh ubuntu@10.0.3.26

# Node A (via jumpbox)
ssh -J root@10.0.3.11 ubuntu@192.168.1.10

# Node B (via jumpbox)
ssh -J root@10.0.3.11 ubuntu@192.168.2.10
```

## Mandatory Prerequisite: Build Latest `kaspad` (All Hosts)

Run this on Relay, Node A, and Node B before Step 1:

```bash
cd ~/rusty-kaspa
cargo build --release --bin kaspad
~/rusty-kaspa/target/release/kaspad --help | grep -E -- "--libp2p-mode|--libp2p-role|--libp2p-helper-listen"
```

Expected: grep prints those flags.

If grep prints nothing, **stop** and rebuild `kaspad` from this branch before continuing.
If you run `cargo clean`, rebuild with the same `cargo build --release --bin kaspad` command again.

## Twist Persistent Mode (Leave Nodes Running)

Use this mode when you want Relay/Node A/Node B to stay up and interact with other devnet nodes over time.

- Keep nodes running for the whole observation window.
- In Steps 1-12, skip the `pkill` and `rm -rf` lines unless you are explicitly testing restart/reseed behavior.
- Keep the same appdirs/identity files so peer identity and history stay stable.
- Monitor continuously with:

```bash
tail -f /tmp/kaspa-relay.log /tmp/kaspa-a.log /tmp/kaspa-b.log
```

### Minimum Cargo command required for devnet connectivity

When running `kaspad` via Cargo, include this baseline command:

```bash
cargo run --bin kaspad --release -- --utxoindex --devnet --connect=23.118.8.163:26611
```

For DCUtR/libp2p lab runs, append runbook flags to that baseline command (or use the built binary commands in this runbook).

Example (Node A style, persistent mode):

```bash
nohup cargo run --bin kaspad --release -- \
  --utxoindex --devnet --connect=23.118.8.163:26611 \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &
```

## Step 1: Start Relay

SSH to relay and run:

`--libp2p-autonat-allow-private` is **lab-only**. It allows AutoNAT to accept private/NATed observations in this topology. Do not keep it set for normal/public deployments.

```bash
# Clean any existing instance
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

# Start relay (MUST include AutoNAT private flag)
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-identity-path=/tmp/relay.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --nologfiles > /tmp/kaspa-relay.log 2>&1 &

# Wait for startup and get peer ID
sleep 12
grep "streaming swarm peer id" /tmp/kaspa-relay.log
```

**Copy the Relay Peer ID** (e.g., `12D3KooWPzNP3yXC83YSR4BotBcGUSTB5dMVZvq7a8gqR3aroFnY`)

## Step 2: Start Node A

SSH to Node A and run (replace `RELAY_PEER_ID`):

```bash
# Clean any existing instance
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

# Start Node A (MUST include AutoNAT private flag)
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

# Wait and verify
sleep 12
grep -E "peer id|reservation" /tmp/kaspa-a.log
```

**Expected output:**
```
libp2p streaming swarm peer id: 12D3KooW... (dcutr enabled)
libp2p reservation accepted by RELAY_PEER_ID, renewal=false
```

**Copy Node A's Peer ID.**

## Step 3: Start Node B

SSH to Node B and run (replace `RELAY_PEER_ID`):

```bash
# Clean any existing instance
pkill -9 kaspad
rm -rf /tmp/kaspa-b /tmp/node-b.key

# Start Node B (MUST include AutoNAT private flag)
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-identity-path=/tmp/node-b.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16112 \
  --nologfiles > /tmp/kaspa-b.log 2>&1 &

# Wait and verify
sleep 12
grep -E "peer id|reservation" /tmp/kaspa-b.log
```

**Copy Node B's Peer ID.**

## Step 4: Trigger DCUtR Hole Punch

From Node A, dial Node B via the relay circuit (replace peer IDs):

```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 15 127.0.0.1 38080
```

**Expected response:**
```json
{"msg":"dial successful","ok":true}
```

## Step 5: Verify Success (Bounded Retry Policy)

Before evaluation, allow a cold run to settle:

```bash
sleep 60
```

### Status labels

- `PASS` (cold): Attempt 1 reaches direct path within 120s.
- `PASS-FLAKY`: Attempt 2 or 3 succeeds after a retry gap.
- `FAIL`: No direct path after 3 total attempts.

### Attempt procedure (max 3 attempts)

For each attempt from Node A:

```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 15 127.0.0.1 38080
sleep 120
grep -E "dcutr event|path: Direct" /tmp/kaspa-a.log | tail -20
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
```

If direct path is not present, wait 45-60s and retry (up to 3 total attempts).

**Success indicators:**
```
libp2p dcutr event: ... result: Ok(ConnectionId(...))
libp2p_bridge: ... path: Direct
```

And helper peers output includes `"path":"direct"` for the other NAT'd node.

## Step 6: Relay Auto-Selection (No Manual Reservations)

This validates automatic relay selection via the Address Manager (new feature).

### 6.1 Restart Relay as Public

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=public \
  --libp2p-identity-path=/tmp/relay.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.26/tcp/16112 \
  --nologfiles > /tmp/kaspa-relay.log 2>&1 &

sleep 12
grep "streaming swarm peer id" /tmp/kaspa-relay.log
```

### 6.2 Start Node A (auto role, no reservations)

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-max-relays=1 \
  --libp2p-max-peers-per-relay=1 \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 12
grep -E "relay auto|reservation" /tmp/kaspa-a.log
```

### 6.3 Start Node B (auto role, no reservations)

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-b /tmp/node-b.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-max-relays=1 \
  --libp2p-max-peers-per-relay=1 \
  --libp2p-identity-path=/tmp/node-b.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-b.log 2>&1 &

sleep 12
grep -E "relay auto|reservation" /tmp/kaspa-b.log
```

**Expected indicators:**
- `libp2p relay auto: reservation accepted for 10.0.3.26:16112`
- `libp2p reservation accepted by RELAY_PEER_ID`

Optional check:
```bash
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
```
Look for `"path":"relay"` connections to the relay.

## Step 7: Auto Role Promotion (Relay Only)

This validates automatic role promotion on the relay when AutoNAT confirms public reachability.

1. Restart relay in `auto` role with external multiaddr:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/relay.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.26/tcp/16112 \
  --nologfiles > /tmp/kaspa-relay.log 2>&1 &
```

2. Start Node A + Node B as in Steps 2–3 (manual reservations). Trigger DCUtR (Step 4).

3. Verify relay log:
```
libp2p autonat: role auto-promoted to public
```
Note: promotion is event-driven, not a fixed timer. In `auto` role, promotion requires:
- enough AutoNAT public probe successes (default threshold: `3`)
- at least one direct (non-relay) inbound libp2p connection
- a usable external address

If the line is missing in the first 10 minutes, keep waiting and re-check. Treat this step as `FAIL` only after an extended window (for example 20-30 minutes) with active peer traffic.

## Step 8: Max Peers Per Relay Cap

This validates that private/auto nodes enforce the per‑relay inbound cap.

1. Start relay as public (Step 6.1).
2. Start Node A with a strict cap:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-max-peers-per-relay=1 \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 12
grep -E "peer id|reservation" /tmp/kaspa-a.log
```

3. Start Node B (reservation to relay as usual).
4. Start an extra node (Node C) on Node B host with unique ports:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-c /tmp/node-c.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-c \
  --listen=192.168.2.10:16121 \
  --rpclisten=192.168.2.10:16120 \
  --libp2p-listen-port=16122 \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-c.key \
  --libp2p-helper-listen=127.0.0.1:38081 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --nologfiles > /tmp/kaspa-c.log 2>&1 &

sleep 12
grep -E "peer id|reservation" /tmp/kaspa-c.log
```

5. Dial Node A via relay from Node B and Node C helper APIs.

**Expected outcome:** Node A enforces the relay cap at steady state. Transient duplicate relay/direct entries can appear during handoff, but should not persist for an upgraded direct peer.
Check Node A helper peers output twice (30s apart) and confirm it does not keep more than one stable relay-path slot per capped relay.

## Step 9: New Feature Checks (Relay Hints + Auto)

Optional but recommended coverage for new relay‑hint behavior.

Notes:
- Backoff/failure logs appear only if a bad relay candidate is actually selected.
- Synthetic TCP skip logs appear only when a relay hint has been ingested into the AddressManager.
- Missing a specific debug line is not an automatic fail if functional behavior matches the step intent.

### 9.1 Relay candidate without `/p2p` peer id (probe path)

Start relay as public (Step 6.1), then start Node A with a candidate missing the peer id:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 20
grep -E "reservation accepted|probe failed" /tmp/kaspa-a.log
```

**Expected:** Reservation accepted (probe filled in the relay peer id).

### 9.2 Multi‑source gating for relay auto

Start Node A with a relay candidate but *no* extra source:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 30
grep -E "reservation accepted" /tmp/kaspa-a.log
```

**Expected:** No reservation accepted yet.

Restart with a second source and confirm it proceeds:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 20
grep -E "reservation accepted" /tmp/kaspa-a.log
```

### 9.3 Bad relay candidate backoff + fallback

Start Node A with one invalid relay candidate and one valid:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.99/tcp/16112,/ip4/10.0.3.26/tcp/16112 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 30
grep -E "probe failed|reservation failed|reservation accepted" /tmp/kaspa-a.log
```

**Expected:** Reservation on the valid relay.
`probe failed`/`reservation failed` for the bad candidate is best-effort observability and may not always surface in a short window.

### 9.4 Synthetic relay hints are non‑dialable for TCP (optional debug)

If you want confirmation that synthetic relay hints are *not* dialed directly:

```bash
RUST_LOG=debug \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &
```

Look for debug lines like `libp2p relay auto: ...` and `Connecting to relay target ...`.
Treat as pass when relay-target dialing is observed and there is no evidence of direct raw TCP dialing to the synthetic relay-hint target.

## Step 10: Gossip‑Only Relay Hint + Rotation

This validates that relay hints from gossip are sufficient for auto‑dial, and that stale hints rotate.

### 10.1 Gossip‑only hint (no manual dial)

To avoid a “single source” block in the lab, run a second relay instance on the same relay VM (different ports). No ports are opened on Node A/B.

Relay #1 (16111/16112) is already running. Start Relay #2:

```bash
pkill -9 -f /tmp/kaspa-relay2 || true
rm -rf /tmp/kaspa-relay2 /tmp/relay2.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay2 \
  --listen=10.0.3.26:16211 \
  --rpclisten=10.0.3.26:16210 \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=public \
  --libp2p-identity-path=/tmp/relay2.key \
  --libp2p-helper-listen=127.0.0.1:38081 \
  --libp2p-listen-port=16212 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.26/tcp/16212 \
  --nodnsseed \
  --nologfiles > /tmp/kaspa-relay2.log 2>&1 &

sleep 12
grep "streaming swarm peer id" /tmp/kaspa-relay2.log
```

1. Start relay as public (Step 6.1).
2. Start Node A + Node B in `auto` role with **no helper dial**, and connect to both relays:

```bash
--connect=10.0.3.26:16111 --connect=10.0.3.26:16211
--libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112,/ip4/10.0.3.26/tcp/16212
```

If you want to keep a single relay, you can set `--libp2p-relay-min-sources=1` as a lab‑only override.

3. Wait 60–120s to allow address gossip.
4. Check Node A logs for relay hint selection and relay dial attempts:

```bash
grep -E "relay hint|relay auto|Connecting to relay target|reservation accepted" /tmp/kaspa-a.log | tail -n 50
```

**Expected:** Node A may establish a relay-path connection to Node B first, then DCUtR upgrades to direct. Once direct is healthy, Node A should not re-open relay path for that same peer unless direct later drops.

### 10.2 Hint staleness + rotation

1. Stop Relay #1 (16111/16112) to force rotation to Relay #2:

```bash
pkill -9 kaspad
```

If Relay #1 is running in a different session, kill only that process.

2. Wait for Node A to attempt a relay dial to the stale hint (should fail) and rotate.

```bash
grep -E "reservation failed|probe failed|skipping .* backoff|relay auto" /tmp/kaspa-a.log | tail -n 50
```

**Expected:** Failure/backoff against stale relay, then selection of a valid relay.

## Step 11: Relay Demotion + Reselection

This validates that a relay that loses public status is removed and peers move to another relay.

Lab note: for practical testing with a single relay, set `--libp2p-relay-min-sources=1` on Node A/B. When the relay stops, you may not see an explicit “releasing reservation” log line; verify by checking helper peers output (empty).
Lab note: to avoid waiting on reservation TTLs, you can restart Node A after the relay goes down to force immediate re‑selection.

1. Start relay in `auto` role and wait for auto‑promotion (Step 7).
2. Simulate demotion (stop relay or remove inbound reachability).
3. Observe Node A/B logs for reservation release + new relay selection.

```bash
grep -E "releasing reservation|relay auto|reservation accepted" /tmp/kaspa-a.log | tail -n 50
```

**Expected:** Reservation release on the demoted relay and a new reservation on another relay.

## Step 12: Additional Relay-Auto Regression Checks

These checks cover behavior added after the original lab flow.

### 12.1 Bridge multiaddr dial failure must not TCP-fallback

1. Ensure Node A is running with helper API and debug logs:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

RUST_LOG=debug \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 15
```

2. Trigger a relay-circuit dial to an unreachable relay IP (valid multiaddr shape, unreachable relay):

```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.99/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 15 127.0.0.1 38080
```

3. Verify Node A did not fall back to raw TCP:

```bash
grep -E "falling back to TCP|bridge mode libp2p dial failed .* falling back to TCP" /tmp/kaspa-a.log
```

**Expected:** no matches.

### 12.2 Outbound relay diversity cap must include existing outbound usage

Goal: steady state should not keep multiple outbound relay-path peers on the same `relay_id` when cap is `1`.

1. Run two relay instances (Step 10) and at least two private targets.
2. On Node A, query peers twice (30s apart):

```bash
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
sleep 30
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
```

If `jq` is available:

```bash
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080 | jq '.peers
  | map(select(.direction=="outbound" and .path=="relay"))
  | group_by(.relay_id)
  | map({relay_id: .[0].relay_id, count: length})'
```

**Expected:** for each `relay_id`, outbound relay-path count stays `<= 1` at steady state.

### 12.3 Auto demotion must clear relay advertising

Goal: relay in `auto` role demotes to private after window expiry and is no longer selected from gossip as a public relay.

1. Start relay in `auto` and first confirm promotion:

```bash
grep -E "role auto-promoted to public" /tmp/kaspa-relay.log
```

2. Stop NAT clients that provide direct inbound signal (Node A/B), then wait at least 11 minutes.
3. Verify demotion:

```bash
grep -E "role auto-demoted to private" /tmp/kaspa-relay.log
```

4. Start a fresh client with gossip-only discovery (no `--libp2p-relay-candidates`, no `--libp2p-reservations`) and set lab override:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-c /tmp/node-c.key

nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-c \
  --listen=192.168.2.10:16121 \
  --rpclisten=192.168.2.10:16120 \
  --libp2p-listen-port=16122 \
  --libp2p-mode=bridge \
  --libp2p-relay-min-sources=1 \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-c.key \
  --libp2p-helper-listen=127.0.0.1:38081 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16122 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-c.log 2>&1 &

sleep 90
grep -E "reservation accepted" /tmp/kaspa-c.log
```

**Expected:** no new reservation accepted after demotion.

### 12.4 Private relay-hint target must not raw-TCP fallback

Goal: when a peer is known via private relay hint (synthetic key path), Node A should try relay targeting only, not direct TCP to synthetic `240.0.0.0/4`.

1. Keep Node A on debug logs.
2. Ensure Node B (private) is discovered via relay hint, then stop relay to force retries.
3. Inspect Node A logs:

```bash
grep -E "Connecting to relay target|Connecting to 24[0-9]\\." /tmp/kaspa-a.log | tail -n 80
```

**Expected:** relay-target attempts may appear; `Connecting to 24[0-9].*` should not appear for synthetic hint targets.

## Step 13: Real-World Bring-Up Handoff (Persistent)

Goal: emulate three independent operators coming online:

- Operator 1 runs one public relay node.
- Operator 2 runs private Node A.
- Operator 3 runs private Node B.
- No helper apparatus and no scripted hole-punch trigger.

### 13.1 Real-world mode rules

- Do not use helper API at all (`--libp2p-helper-listen`, `nc`, helper `dial`, helper `peers`).
- Do not force dials between Node A and Node B.
- Start each node once, then leave it running.
- Keep the baseline devnet connect argument on every node:

```bash
cargo run --bin kaspad --release -- --utxoindex --devnet --connect=23.118.8.163:26611
```

### 13.2 Public-node operator (relay) startup

Use the Step 6.1 relay command pattern (`--libp2p-role=public`) and keep that node online as the single public relay operator for this scenario.
For Step 13 parity, include the same baseline devnet args on relay too: `--utxoindex --devnet --connect=23.118.8.163:26611`.

### 13.3 Private-node operator startup (Node A and Node B)

Start Node A and Node B in `auto` role, with no helper flag, and leave them online.

Minimal autonomous examples:

```bash
# Node A (private operator)
nohup cargo run --bin kaspad --release -- \
  --utxoindex --devnet --connect=23.118.8.163:26611 \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

# Node B (private operator)
nohup cargo run --bin kaspad --release -- \
  --utxoindex --devnet --connect=23.118.8.163:26611 \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
  --libp2p-autonat-allow-private \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-b.key \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16112 \
  --libp2p-relay-candidates=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-b.log 2>&1 &
```

### 13.4 Passive soak and handoff

1. After all 3 nodes are online, wait at least 15 minutes (30-60 minutes preferred) with no intervention.
2. Confirm processes remain alive:

```bash
pgrep -fa "kaspad.*kaspa-a|kaspad.*kaspa-b|kaspad.*kaspa-relay"
```

3. Collect passive evidence only:

```bash
tail -n 80 /tmp/kaspa-a.log
tail -n 80 /tmp/kaspa-b.log
tail -n 80 /tmp/kaspa-relay.log
```

Look for naturally occurring indicators such as `reservation accepted`, `dcutr event`, and `path: Direct` (if/when they appear).

4. End this runbook step by handing the environment back to the operator for ongoing observation. Do not add scripted actions after this point.

### 13.5 Structured periodic check-ins (no helper)

Use a fixed cadence (for example every 10 minutes) and write machine-readable snapshots.

Create and start a check-in loop:

```bash
cat > /tmp/dcutr-checkin.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

interval_sec="${1:-600}" # 600s = 10m
out="${2:-/tmp/dcutr-checkins.tsv}"

if [[ ! -f "$out" ]]; then
  printf "ts_utc\tup_a\tup_b\tup_relay\ta_resv\ta_dcutr\ta_direct\tb_resv\tb_dcutr\tb_direct\n" > "$out"
fi

while true; do
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  up_a=0; pgrep -f "kaspad.*kaspa-a" >/dev/null && up_a=1
  up_b=0; pgrep -f "kaspad.*kaspa-b" >/dev/null && up_b=1
  up_r=0; pgrep -f "kaspad.*kaspa-relay" >/dev/null && up_r=1

  a_resv="$(rg -c "reservation accepted" /tmp/kaspa-a.log 2>/dev/null || echo 0)"
  a_dcutr="$(rg -c "dcutr event" /tmp/kaspa-a.log 2>/dev/null || echo 0)"
  a_direct="$(rg -c "path: Direct" /tmp/kaspa-a.log 2>/dev/null || echo 0)"

  b_resv="$(rg -c "reservation accepted" /tmp/kaspa-b.log 2>/dev/null || echo 0)"
  b_dcutr="$(rg -c "dcutr event" /tmp/kaspa-b.log 2>/dev/null || echo 0)"
  b_direct="$(rg -c "path: Direct" /tmp/kaspa-b.log 2>/dev/null || echo 0)"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$ts" "$up_a" "$up_b" "$up_r" "$a_resv" "$a_dcutr" "$a_direct" "$b_resv" "$b_dcutr" "$b_direct" \
    | tee -a "$out"

  sleep "$interval_sec"
done
EOF

chmod +x /tmp/dcutr-checkin.sh
nohup /tmp/dcutr-checkin.sh 600 /tmp/dcutr-checkins.tsv > /tmp/dcutr-checkin.log 2>&1 &
```

Review snapshots:

```bash
tail -n 20 /tmp/dcutr-checkins.tsv
```

Optional human-readable view:

```bash
column -t -s $'\t' /tmp/dcutr-checkins.tsv | tail -n 20
```

Interpretation for autonomous success:

- `up_a=1`, `up_b=1`, `up_relay=1` throughout the window.
- `a_dcutr`/`b_dcutr` increase over time without helper actions.
- `a_direct` or `b_direct` increase over time, indicating natural direct-path upgrades.

Completion criterion for this step: one public relay operator and two private node operators are online, baseline devnet connect is active, no helper apparatus was used, and passive check-ins are being recorded for operator monitoring.

## Critical Configuration Notes

### NAT Behavior Primer (Brief)

- NAT rewrites private source IP/port to a public mapping and usually blocks unsolicited inbound.
- DCUtR depends on compatible NAT behavior on both private nodes.
- Full-cone (endpoint-independent mapping/filtering) gives the highest success rate for hole punching.
- Symmetric or strict endpoint-dependent NATs commonly cause repeated DCUtR failures even when config is correct.
- In this lab, if direct upgrade is unstable, verify NAT mode first before changing node settings.

### AutoNAT Private-Address Flag (CLI)

```bash
# Valid:
nohup kaspad --libp2p-autonat-allow-private ...
```

This flag enables AutoNAT private-address handling for lab/NAT environments.

### External Multiaddrs Must Be NAT IP

The `--libp2p-external-multiaddrs` must be the **NAT-translated IP**, not the private IP:

| Node | Private IP | External Multiaddr (NAT IP) |
|------|------------|----------------------------|
| Node A | 192.168.1.10 | `/ip4/10.0.3.61/tcp/16112` |
| Node B | 192.168.2.10 | `/ip4/10.0.3.62/tcp/16112` |

### Relay Auto Requires Multiple Sources

Relay auto-selection requires at least two independent sources for a relay candidate.
In the lab, use `--libp2p-relay-candidates` plus AddressManager gossip from `--connect`
to satisfy this requirement.

## Stopping Nodes

If you are running **Twist Persistent Mode**, do not stop nodes here; keep them alive for long-window observation.

```bash
pkill -9 kaspad
```

Or to stop gracefully:
```bash
pkill kaspad
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `AttemptsExceeded(3)` | AutoNAT not allowing private IPs | Add `--libp2p-autonat-allow-private` to node startup args |
| `AttemptsExceeded(3)` on first attempt only | Cold-start candidate/relay convergence | Apply Step 5 bounded retries (up to 3), then classify PASS/PASS-FLAKY/FAIL |
| `AttemptsExceeded(3)` | NAT not full-cone | Load `nft_fullcone` kernel module on routers |
| `unexpected argument '--libp2p-mode'` | Old or wrong `kaspad` binary | Rebuild from this branch with `cargo build --release --bin kaspad` and verify flags in `kaspad --help` |
| `reservation rejected` | Wrong relay peer ID | Verify relay peer ID in reservation multiaddr |
| No `path: Direct` | External multiaddr wrong | Use NAT IP, not private IP |
| Helper API timeout | Helper not started | Check `--libp2p-helper-listen` flag |

## Quick Checklist

- [ ] Full-cone NAT enabled on both routers
- [ ] `kaspad --help` shows `--libp2p-mode` on Relay, Node A, Node B
- [ ] `--libp2p-autonat-allow-private` set on ALL nodes
- [ ] `--libp2p-mode=bridge` on all nodes
- [ ] `--libp2p-external-multiaddrs` set to NAT IP (not private IP)
- [ ] `--libp2p-reservations` points to correct relay IP and peer ID
- [ ] Reservations accepted (check logs)
- [ ] Step 5 classified as PASS, PASS-FLAKY, or FAIL (bounded retries applied)
- [ ] `result: Ok(ConnectionId(...))` in DCUtR event logs (for PASS/PASS-FLAKY)
- [ ] `path: Direct` in bridge connection logs (for PASS/PASS-FLAKY)
- [ ] `"path":"direct"` in helper peers output (for PASS/PASS-FLAKY)
- [ ] Final handoff done: Node A + Node B + public relay left online for passive monitoring (Step 13)
