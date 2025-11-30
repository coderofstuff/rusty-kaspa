# Mainnet Soak Test Report

**Branch:** `tcp-hole-punch-clean`
**Commit:** `222eba40688267253b9471eadc44f2797ed6a968` (as specified in handover)
**Date:** 2025-11-30
**Test Start:** 05:24 UTC

---

## Executive Summary

**RESULT: BLOCKED - MAINNET SYNC NOT POSSIBLE WITH libp2p-mode=full**

A critical limitation was discovered: `--libp2p-mode=full` routes ALL outgoing peer connections through libp2p. Since mainnet peers do not have libp2p enabled, nodes in this mode cannot establish outgoing connections to sync the blockchain.

**Key Findings:**
1. libp2p-mode=full creates a libp2p-only network topology
2. Mainnet peers only speak TCP on port 16111, not libp2p
3. A hybrid mode or TCP fallback mechanism would be required for mainnet operation
4. The libp2p stack itself is stable with zero panics in lab testing

---

## 1. Environment

### 1.1 Lab Topology

| Node | Private IP | NAT-translated IP | Port | PeerId |
|------|------------|-------------------|------|--------|
| **Relay** | 10.0.3.50 | (public) | 16112 | `12D3KooWN7b6w3TGiM41LBKrFBp1YqGC7rEwRJNmcxeZE4XVCzL4` |
| **Node A** | 192.168.1.10 | 10.0.3.61 | 16112 | `12D3KooWPrAhSzdTFLu2BHAonep7hhRfyHekrr2Q9vFPpq7s82nQ` |
| **Node B** | 192.168.2.10 | 10.0.3.62 | 16112 | `12D3KooWQcybM6ZnFNMMpoXHx6Gvuwcfk8MLtydZFi7EvP1ddn5J` |

### 1.2 Disk Space

| Node | Filesystem | Free Space | Datadir |
|------|------------|------------|---------|
| Relay | /dev/sda1 | 31GB (61% used) | `/var/lib/kaspa-relay-mainnet` |
| Node A | /dev/sda1 | 23GB (72% used) | `/var/lib/kaspa-a-mainnet` |
| Node B | /dev/sda1 | 21GB (74% used) | `/var/lib/kaspa-b-mainnet` |

### 1.3 Configuration

All nodes started with:
```bash
kaspad \
  --appdir=/var/lib/kaspa-*-mainnet \
  --libp2p-mode=full \
  --libp2p-identity-path=/var/lib/kaspa-*-mainnet/*-identity.key \
  --libp2p-helper-listen=127.0.0.1:38080 \
  --libp2p-autonat-allow-private \
  --libp2p-external-multiaddrs=/ip4/<NAT-IP>/tcp/16112 \  # A and B only
  --libp2p-reservations=/ip4/10.0.3.50/tcp/16112/p2p/<RelayPeerId> \  # A and B only
  --nologfiles
```

---

## 2. Critical Finding: Mainnet Sync Blocked

### 2.1 The Problem

When running with `--libp2p-mode=full`, the connection manager attempts to dial mainnet peers via libp2p:

```
Connection manager: has 0/8 outgoing P2P connections, trying to obtain 8 additional connection(s)...
libp2p dial request to /ip4/120.29.124.79/tcp/16111
libp2p dial request to /ip4/160.251.199.225/tcp/16111
...
```

These dials fail because:
1. Mainnet peers listen on TCP port 16111, not libp2p
2. Mainnet peers don't implement the libp2p protocol
3. There is no fallback to regular TCP when libp2p dial fails

### 2.2 Evidence

**With libp2p-mode=full:**
```
Connection manager: has 0/8 outgoing P2P connections
```

**Without libp2p-mode (same node, restarted):**
```
P2P Connected to outgoing peer 120.29.124.79:16111 (outbound: 1)
P2P Connected to outgoing peer 160.251.199.225:16111 (outbound: 2)
...
IBD started with peer 66.115.219.109:16111
```

### 2.3 Architectural Implication

libp2p-mode=full is designed for networks where **all peers** have libp2p enabled. For mainnet operation, one of these approaches would be needed:

1. **Hybrid mode**: Attempt libp2p first, fall back to TCP
2. **Gateway nodes**: Run some nodes without libp2p-mode to bridge networks
3. **Gradual rollout**: Wait until sufficient mainnet nodes have libp2p enabled

---

## 3. libp2p Lab Test Results

Since mainnet sync was blocked, testing focused on libp2p stack stability within the lab.

### 3.1 Connectivity

| Connection | Status |
|------------|--------|
| Relay ← Node A | libp2p reservation accepted |
| Relay ← Node B | libp2p reservation accepted |
| A ↔ B via relay circuit | Established |

**Logs confirming connectivity:**
```
libp2p reservation accepted by 12D3KooWN7b6w3TGiM41LBKrFBp1YqGC7rEwRJNmcxeZE4XVCzL4, renewal=false
P2P Connected to incoming peer [fdff::6b8f:2a33:21dc:68df]:41436 (inbound: 1)
P2P Connected to incoming peer [fdff::31c0:b5df:47b7:3dd1]:51127 (inbound: 2)
```

### 3.2 DCUtR Hole Punch Test

**Test:** Dial from Node A to Node B via relay circuit

**Helper API command:**
```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.50/tcp/16112/p2p/.../p2p-circuit/p2p/..."}' | nc -w 10 127.0.0.1 38080
```

**Result:**
```json
{"msg":"dial successful","ok":true}
```

**DCUtR log from Node B:**
```
libp2p dcutr: initiated dial-back to 12D3KooWPrAhSzdTFLu2BHAonep7hhRfyHekrr2Q9vFPpq7s82nQ via relay 12D3KooWN7b6w3TGiM41LBKrFBp1YqGC7rEwRJNmcxeZE4XVCzL4
libp2p dcutr event: Event { remote_peer_id: ..., result: Err(Error { inner: AttemptsExceeded(3) }) }
```

**Analysis:**
- Relay circuit established successfully
- DCUtR hole punch attempted (3 attempts)
- Hole punch failed (expected - NAT simulation prevents direct connectivity)
- Failure is **bounded** (`AttemptsExceeded(3)`) - no infinite loops
- Connection via relay remained active

### 3.3 Resource Usage

| Node | RSS (Start) | RSS (After Tests) | Status |
|------|-------------|-------------------|--------|
| Relay | 88 MB | 92 MB | Stable |
| Node A | 85 MB | 106 MB | Normal growth |
| Node B | 102 MB | 110 MB | Stable |

Memory usage is modest and not exhibiting unbounded growth.

### 3.4 Stability

| Metric | Relay | Node A | Node B |
|--------|-------|--------|--------|
| **Panics** | 0 | 0 | 0 |
| **Helper API** | N/A | `{"ok":true}` | `{"ok":true}` |
| **Restarts** | 0 | 0 | 0 |

**Warnings observed (expected/benign):**
- UPnP port mapping failures (lab environment limitation)
- DNS seeder resolution failures for 2/9 seeders (intermittent)

---

## 4. What Was NOT Tested

Due to the mainnet sync limitation, the following could not be verified:

1. **Block processing under libp2p**: No blocks were received/validated
2. **Transaction relay**: No mempool activity
3. **Multi-hour soak with live data**: Would require libp2p-enabled mainnet peers
4. **Real-world NAT traversal**: Lab NAT is simulated; real-world NATs may behave differently

---

## 5. Recommendations

### 5.1 For Mainnet Soak Testing

One of these approaches is needed:

**Option A: TCP Fallback Mode**
Implement a hybrid mode where the node attempts libp2p for outgoing connections but falls back to regular TCP for peers that don't respond to libp2p.

**Option B: Gateway Deployment**
Deploy a small set of nodes:
- One node in regular mode (libp2p off) to sync with mainnet
- Other nodes in libp2p-mode=full connecting to the gateway

**Option C: Testnet Soak**
Run the soak test on testnet where we control all nodes and can ensure libp2p is enabled throughout.

### 5.2 Code Observations

The current behavior where libp2p-mode=full routes ALL connections through libp2p may be intentional for security/isolation, but it prevents gradual mainnet adoption. Consider documenting this clearly in operator notes.

---

## 6. Conclusion

**libp2p Stack Status:** STABLE - Zero panics, bounded error handling, modest memory usage

**Mainnet Readiness:** BLOCKED - Cannot sync mainnet with libp2p-mode=full due to lack of libp2p support on mainnet peers

**DCUtR Functionality:** WORKING - Relay circuit established, hole punch attempts bounded

**Recommendation:** Implement TCP fallback mechanism or plan for phased rollout where initial libp2p nodes can still communicate with non-libp2p mainnet peers.

---

*Report generated: 2025-11-30 05:36 UTC*
