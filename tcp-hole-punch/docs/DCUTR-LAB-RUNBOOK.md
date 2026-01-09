# DCUtR Lab Runbook

Tested and verified guide for running DCUtR hole punch tests in the NAT lab.

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

## Step 1: Start Relay

SSH to relay and run:

```bash
# Clean any existing instance
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

# Start relay (MUST use env var for AutoNAT)
KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
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

# Start Node A (MUST use env var for AutoNAT)
KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
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

# Start Node B (MUST use env var for AutoNAT)
KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
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

## Step 5: Verify Success

### Check DCUtR Event Logs

```bash
grep -E "dcutr event|path:" /tmp/kaspa-a.log | tail -10
```

**SUCCESS indicators:**
```
libp2p dcutr event: ... result: Ok(ConnectionId(6))
libp2p_bridge: ... path: Direct
```

### Check Peers via Helper API

```bash
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
```

**Success looks like:**
```json
{
  "ok": true,
  "peers": [
    {
      "peer_id": "12D3KooW...",
      "path": "direct",
      "direction": "outbound",
      "dcutr_upgraded": false,
      "state": "connected"
    }
  ]
}
```

Look for `"path":"direct"` connections to the other NAT'd node.

## Step 6: Relay Auto-Selection (No Manual Reservations)

This validates automatic relay selection via the Address Manager (new feature).

### 6.1 Restart Relay as Public

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
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

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-role=auto \
  --libp2p-max-relays=1 \
  --libp2p-max-peers-per-relay=1 \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --connect=10.0.3.26:16111 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 12
grep -E "relay auto|reservation" /tmp/kaspa-a.log
```

### 6.3 Start Node B (auto role, no reservations)

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-b /tmp/node-b.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
  --libp2p-role=auto \
  --libp2p-max-relays=1 \
  --libp2p-max-peers-per-relay=1 \
  --libp2p-identity-path=/tmp/node-b.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16112 \
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

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
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

## Step 8: Max Peers Per Relay Cap

This validates that private/auto nodes enforce the per‑relay inbound cap.

1. Start relay as public (Step 6.1).
2. Start Node A with a strict cap:

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
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

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-c \
  --listen=192.168.2.10:16121 \
  --rpclisten=192.168.2.10:16120 \
  --libp2p-relay-listen-port=16122 \
  --libp2p-mode=bridge \
  --libp2p-role=auto \
  --libp2p-identity-path=/tmp/node-c.key \
  --libp2p-helper-listen=127.0.0.1:38081 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --nologfiles > /tmp/kaspa-c.log 2>&1 &

sleep 12
grep -E "peer id|reservation" /tmp/kaspa-c.log
```

5. Dial Node A via relay from Node B and Node C helper APIs.

**Expected outcome:** Node A stabilizes with a single relay‑path peer (cap=1). Check Node A helper peers output and confirm only one `"path":"relay"` entry remains.

## Critical Configuration Notes

### Environment Variable is REQUIRED

**You MUST use the environment variable, not just the CLI flag:**

```bash
# CORRECT - This works:
KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true nohup kaspad ...

# INCORRECT - This may NOT work:
nohup kaspad --libp2p-autonat-allow-private ...
```

The environment variable ensures AutoNAT properly handles private IP ranges in the lab.

### External Multiaddrs Must Be NAT IP

The `--libp2p-external-multiaddrs` must be the **NAT-translated IP**, not the private IP:

| Node | Private IP | External Multiaddr (NAT IP) |
|------|------------|----------------------------|
| Node A | 192.168.1.10 | `/ip4/10.0.3.61/tcp/16112` |
| Node B | 192.168.2.10 | `/ip4/10.0.3.62/tcp/16112` |

## Stopping Nodes

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
| `AttemptsExceeded(3)` | AutoNAT not allowing private IPs | Use `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true` env var |
| `AttemptsExceeded(3)` | NAT not full-cone | Load `nft_fullcone` kernel module on routers |
| `reservation rejected` | Wrong relay peer ID | Verify relay peer ID in reservation multiaddr |
| No `path: Direct` | External multiaddr wrong | Use NAT IP, not private IP |
| Helper API timeout | Helper not started | Check `--libp2p-helper-listen` flag |

## Quick Checklist

- [ ] Full-cone NAT enabled on both routers
- [ ] `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true` env var set on ALL nodes
- [ ] `--libp2p-mode=bridge` on all nodes
- [ ] `--libp2p-external-multiaddrs` set to NAT IP (not private IP)
- [ ] `--libp2p-reservations` points to correct relay IP and peer ID
- [ ] Reservations accepted (check logs)
- [ ] `result: Ok(ConnectionId(...))` in DCUtR event logs
- [ ] `path: Direct` in bridge connection logs
- [ ] `"path":"direct"` in helper peers output
