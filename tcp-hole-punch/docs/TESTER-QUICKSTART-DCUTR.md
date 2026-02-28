# DCUtR Hole Punch Tester Quickstart

Goal: prove that two private nodes can connect via relay and then upgrade to direct path using DCUtR.

References:
- Full lab runbook: `tcp-hole-punch/docs/DCUTR-LAB-RUNBOOK.md`
- Lab infra record: `tcp-hole-punch/docs/NAT-LAB-RECORD.md`

## 1. Test Snapshot (Pin This Exactly)

- Repository: `https://github.com/elldeeone/rusty-kaspa.git`
- Branch: `tcp-hole-punch-relay-auto`
- Commit: `11259a87196a`
- Rust toolchain: repo default (`rust-toolchain.toml`)

## 2. Success Criteria

- `PASS`: direct path appears on first attempt within 120s.
- `PASS-FLAKY`: direct path appears on attempts 2-3.
- `FAIL`: no direct path after 3 attempts.

Direct path proof must include:
- DCUtR success log (`result: Ok(ConnectionId(...))`)
- bridge path log contains `path: Direct`
- helper API peers output shows `"path":"direct"` for the target peer

## 3. Hard Prerequisites

- Three hosts:
  - Relay (publicly reachable in lab)
  - Node A behind NAT
  - Node B behind NAT
- NAT for Node A and Node B must be full-cone in lab.
- SSH access to all hosts.
- `nc`, `grep`, `pkill` available on hosts.

### NAT Nuance

- NAT rewrites private source IP/port to a public tuple and usually blocks unsolicited inbound traffic.
- DCUtR hole punching is most reliable with endpoint-independent mapping/filtering (full-cone behavior).
- Full-cone: once a mapping exists, inbound packets from any external peer can return on that mapped public port.
- Symmetric or strict endpoint-dependent NATs often break or heavily destabilize hole punching.
- If NAT is not full-cone, treat repeated `FAIL`/`PASS-FLAKY` as likely environment limits, not necessarily node bugs.

## 4. Mandatory Build Requirement (Most Common Failure)

You must build `kaspad` with libp2p feature on **all three hosts**.

Run on Relay, Node A, Node B:

```bash
cd ~/rusty-kaspa
cargo build --release --bin kaspad --features libp2p
~/rusty-kaspa/target/release/kaspad --help | grep -E -- "--libp2p-mode|--libp2p-role|--libp2p-helper-listen"
```

Expected: grep prints those flags.

If you see `unexpected argument '--libp2p-mode'`, rebuild with:

```bash
cargo build --release --bin kaspad --features libp2p
```

If you run `cargo clean`, repeat the same build command before testing.

## 5. Lab Addresses (Example)

| Node | Private IP | NAT IP | libp2p Port | Role |
|------|------------|--------|-------------|------|
| Relay | 10.0.3.26 | (public) | 16112 | Public relay |
| Node A | 192.168.1.10 | 10.0.3.61 | 16112 | NAT client |
| Node B | 192.168.2.10 | 10.0.3.62 | 16112 | NAT client |

Use your actual values if they differ.

## 6. Minimal Repro Flow (Copy/Paste)

All commands below require:

```bash
KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true
```

### 6.1 Start Relay

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-relay /tmp/relay.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-relay \
  --libp2p-mode=bridge \
  --libp2p-identity-path=/tmp/relay.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --nologfiles > /tmp/kaspa-relay.log 2>&1 &

sleep 12
grep "streaming swarm peer id" /tmp/kaspa-relay.log
```

Save relay peer id as `RELAY_PEER_ID`.

### 6.2 Start Node A

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-a /tmp/node-a.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-a \
  --libp2p-mode=bridge \
  --libp2p-identity-path=/tmp/node-a.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.61/tcp/16112 \
  --nologfiles > /tmp/kaspa-a.log 2>&1 &

sleep 12
grep -E "peer id|reservation accepted" /tmp/kaspa-a.log
```

Save node A peer id as `NODE_A_PEER_ID`.

### 6.3 Start Node B

```bash
pkill -9 kaspad
rm -rf /tmp/kaspa-b /tmp/node-b.key

KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true \
nohup ~/rusty-kaspa/target/release/kaspad \
  --appdir=/tmp/kaspa-b \
  --libp2p-mode=bridge \
  --libp2p-identity-path=/tmp/node-b.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-reservations=/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID \
  --libp2p-external-multiaddrs=/ip4/10.0.3.62/tcp/16112 \
  --nologfiles > /tmp/kaspa-b.log 2>&1 &

sleep 12
grep -E "peer id|reservation accepted" /tmp/kaspa-b.log
```

Save node B peer id as `NODE_B_PEER_ID`.

### 6.4 Trigger Relay Dial (A -> B)

Run on Node A:

```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 15 127.0.0.1 38080
```

Expected response:

```json
{"msg":"dial successful","ok":true}
```

### 6.5 Verify with Bounded Retries (Max 3)

Cold settle:

```bash
sleep 60
```

Attempt sequence on Node A:

```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.26/tcp/16112/p2p/RELAY_PEER_ID/p2p-circuit/p2p/NODE_B_PEER_ID"}' | nc -w 15 127.0.0.1 38080
sleep 120
grep -E "dcutr event|path: Direct" /tmp/kaspa-a.log | tail -n 20
echo '{"action":"peers"}' | nc -w 5 127.0.0.1 38080
```

If not direct, wait 45-60s and retry up to 3 total attempts.

## 7. Required Regression Checks Beyond Basic PASS

Run these from `DCUTR-LAB-RUNBOOK.md` Step 12:

- `12.1` bridge multiaddr failure must not TCP-fallback
- `12.2` outbound relay diversity cap with existing outbound relay usage
- `12.3` auto demotion clears relay advertising behavior
- `12.4` private relay-hint target must not raw-TCP fallback

Treat the test as incomplete if Step 12 is skipped.

## 8. Common Mistakes

- Built without `--features libp2p` (most common).
- Used private LAN IP in `--libp2p-external-multiaddrs` instead of NAT IP.
- Forgot `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true`.
- Wrong relay peer id in reservation multiaddr.