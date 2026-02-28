# HYBRID-INTEGRATION-PROPOSAL.md — Hybrid libp2p + TCP Integration Design

**Author**: Claude (Analysis Agent)
**Date**: November 2025
**Status**: Proposal for Review

---

## 1. Goals & Non-Goals

### 1.1 Goals

1. **Hybrid connectivity**: Nodes can connect to both TCP-only peers (current mainnet) and libp2p-capable peers (hole-punched private nodes)

2. **Backwards compatibility**: Default mode remains unchanged — nodes without libp2p flags behave exactly like current master

3. **Private node participation**: NAT'd nodes can participate in the network via DCUtR hole punching, without requiring port forwarding

4. **Public node relay**: Public nodes can optionally act as relay bridges, enabling private-to-private connectivity

5. **Sane peer counts**: Private nodes maintain expected peer limits (e.g., 8 outbound, bounded inbound) and aren't overwhelmed by hole-punch traffic

6. **No eclipse via single relay**: Connection diversity is enforced — no single relay can dominate a private node's connectivity

### 1.2 Non-Goals

1. **Full libp2p-only mode for mainnet**: We don't aim to make `libp2p-mode=full` work on mainnet; it remains for isolated overlay testing

2. **DHT-based peer discovery**: Peer discovery remains via DNS seeders and address manager; no libp2p DHT

3. **Protocol changes**: No changes to Kaspa P2P handshake, version messages, or consensus protocol

4. **Automatic NAT detection**: Users must explicitly enable private-node mode; no automatic detection (yet)

---

## 2. Config & Modes

### 2.1 Proposed Mode Values

I propose three modes for `--libp2p-mode`:

| Mode | Description |
|------|-------------|
| `off` | libp2p completely disabled (current default, unchanged) |
| `bridge` | **NEW**: Hybrid mode — TCP for unknown peers, libp2p for capable peers, optional relay server |
| `full` | Full libp2p transport — for overlay testing only (existing, unchanged) |

**Key insight**: The current `full` mode breaks mainnet because it uses libp2p for all outbound dials. The new `bridge` mode provides intelligent transport selection.

### 2.2 Additional Flags

Building on existing flags, I propose clarifying semantics:

| Flag | Bridge Mode Semantics |
|------|----------------------|
| `--libp2p-role` | **NEW**: `public-relay \| private \| auto` — determines behavior profile |
| `--libp2p-reservations` | For private nodes: relay(s) to reserve on |
| `--libp2p-external-multiaddrs` | For all nodes: addresses to announce for DCUtR |
| `--libp2p-relay-port` | For public-relay role: separate port for relay service |
| `--libp2p-private-inbound-cap` | For private role: max hole-punched inbound connections |

### 2.3 Role Profiles

#### `--libp2p-role=public-relay`
- **Use case**: Public node operator who wants to help private nodes
- **Behavior**:
  - Runs libp2p relay server on `--libp2p-relay-port`
  - Accepts inbound relay reservations (capped)
  - Uses TCP for all outbound connections (mainnet compatible)
  - Libp2p capability advertised in version handshake
  - Can accept hole-punched inbound connections

#### `--libp2p-role=private`
- **Use case**: NAT'd node that needs hole punching
- **Behavior**:
  - Makes reservations on configured relays
  - Uses TCP for known-TCP peers, libp2p for libp2p-capable peers
  - Can initiate DCUtR hole punches
  - Inbound capped at `--libp2p-private-inbound-cap`

#### `--libp2p-role=auto` (default when bridge mode enabled)
- **Use case**: Node that adapts to its environment
- **Behavior**:
  - Detects if publicly routable (via AutoNAT or UPnP)
  - If public: acts like `public-relay` without explicit relay server
  - If private: acts like `private` without reservations (passive only)

---

## 3. Dial Policy

### 3.1 Transport Selection Algorithm

The core problem is: given a peer address, which transport should we use?

**Proposed algorithm** (in `Libp2pOutboundConnector::connect()`):

```
fn select_transport(address: NetAddress, address_manager: &AddressManager) -> Transport {
    // 1. Check if address is known to be libp2p-capable
    if let Some(stored) = address_manager.get(address) {
        if stored.is_libp2p_relay() || stored.relay_port.is_some() {
            return Transport::Libp2p;
        }
    }

    // 2. Check if this is a relay circuit address (multiaddr with /p2p-circuit/)
    if address.is_relay_circuit() {
        return Transport::Libp2p;
    }

    // 3. Default to TCP for unknown peers
    return Transport::Tcp;
}
```

### 3.2 Fallback Strategy

When libp2p dial fails, we need graceful degradation:

```
fn connect_with_fallback(address, metadata) -> Result<Router> {
    let transport = select_transport(address);

    match transport {
        Transport::Libp2p => {
            match libp2p_provider.dial(address).await {
                Ok((md, stream)) => handler.connect_with_stream(stream, md),
                Err(Libp2pError::Disabled) => tcp_fallback(address),
                Err(Libp2pError::DialFailed(_)) => {
                    // Mark as non-libp2p in address manager
                    address_manager.clear_libp2p_capability(address);
                    tcp_fallback(address)
                }
            }
        }
        Transport::Tcp => tcp_dial(address),
    }
}
```

### 3.3 Address Manager Integration

**New methods on `AddressManager`:**

```rust
impl AddressManager {
    /// Mark an address as libp2p-capable (relay or DCUtR)
    pub fn mark_libp2p_capable(&mut self, address: NetAddress, relay_port: Option<u16>) {
        if let Some(stored) = self.address_store.get_mut(address) {
            stored.services |= NET_ADDRESS_SERVICE_LIBP2P_RELAY;
            if let Some(port) = relay_port {
                stored.relay_port = Some(port);
            }
        }
    }

    /// Clear libp2p capability after failed libp2p dial
    pub fn clear_libp2p_capability(&mut self, address: NetAddress) {
        if let Some(stored) = self.address_store.get_mut(address) {
            stored.services &= !NET_ADDRESS_SERVICE_LIBP2P_RELAY;
            stored.relay_port = None;
        }
    }

    /// Get addresses that are known to be libp2p-capable
    pub fn iterate_libp2p_addresses(&self) -> impl Iterator<Item = NetAddress> {
        self.iterate_addresses().filter(|a| a.is_libp2p_relay())
    }
}
```

### 3.4 Relay-Capable Peer Dialing

For reaching private peers via relay, the connection manager needs to understand relay addresses:

```rust
// In ConnectionManager::handle_outbound_connections()

// For each libp2p-capable address that has a relay_port:
let relay_circuit = format!(
    "/ip4/{}/tcp/{}/p2p/{}/p2p-circuit/p2p/{}",
    address.ip,
    address.relay_port.unwrap(),
    relay_peer_id,  // From address metadata
    target_peer_id  // From address metadata
);

// Dial via libp2p using circuit address
libp2p_provider.dial_multiaddr(relay_circuit).await
```

### 3.5 Private Peer Reachability

For private peers to be dialable, we need to:

1. **Advertise relay address**: Private nodes should advertise their relay circuit address, not their local IP
2. **Store in address manager**: When we learn about a private peer (via version handshake), store their circuit address

**Version message extension** (future, non-breaking):
```rust
// In VersionMessage (optional field)
libp2p_circuit_address: Option<String>,  // e.g., "/p2p/RELAY/p2p-circuit/p2p/ME"
```

---

## 4. Relay Usage & Limits

### 4.1 Preventing Eclipse via Single Relay

**Problem**: If all private nodes use the same relay, that relay becomes a single point of eclipse attack.

**Solution**: Multi-relay reservation + connection diversity enforcement.

#### 4.1.1 Multi-Relay Reservations

Private nodes should be encouraged to reserve on multiple relays:

```bash
kaspad --libp2p-mode=bridge --libp2p-role=private \
  --libp2p-reservations="/ip4/RELAY1/tcp/4001/p2p/PEER1" \
  --libp2p-reservations="/ip4/RELAY2/tcp/4001/p2p/PEER2"
```

#### 4.1.2 Outbound Relay Diversity

In connection manager, track relay distribution for outbound connections:

```rust
struct RelayBudget {
    per_relay_limit: usize,  // e.g., 2
    total_relay_limit: usize,  // e.g., 4
    current: HashMap<String, usize>,  // relay_id -> count
}

impl ConnectionManager {
    fn can_dial_via_relay(&self, relay_id: &str) -> bool {
        let current = self.relay_budget.current.get(relay_id).unwrap_or(&0);
        *current < self.relay_budget.per_relay_limit
            && self.relay_budget.current.values().sum::<usize>() < self.relay_budget.total_relay_limit
    }
}
```

### 4.2 Per-Relay Bucket Enforcement

The current implementation already has this (`relay_overflow()` in connection manager). I propose:

- **Per-relay inbound cap**: 4 (default, configurable)
- **Unknown relay bucket cap**: 8 (default, configurable)
- **Total libp2p inbound cap**: 50% of `--max-inbound-peers`

### 4.3 Relay Server Limits

For nodes running `--libp2p-role=public-relay`:

| Limit | Default | Description |
|-------|---------|-------------|
| Max reservations | 32 | Maximum concurrent relay reservations |
| Reservation timeout | 20 min | Auto-expire stale reservations |
| Bandwidth per circuit | 1 MB/s | Prevent single circuit from starving others |
| Max circuits per peer | 4 | Prevent one private node from hogging relay |

---

## 5. Private Node Inbound Limits

### 5.1 The Inbound Explosion Problem

When hole punching works, a private node can suddenly receive many inbound connections. Without limits, this could:
- Exhaust file descriptors
- Consume excessive bandwidth
- Create unfair load distribution

### 5.2 Proposed Limits

```bash
# Private node configuration
kaspad --libp2p-mode=bridge --libp2p-role=private \
  --libp2p-private-inbound-cap=16 \  # Max hole-punched inbound
  --max-inbound-peers=32             # Standard inbound cap (shared with libp2p)
```

**Enforcement in `handle_inbound_connections()`:**

```rust
// Private node heuristic
if self.is_private_role() {
    let libp2p_inbound = inbound.iter()
        .filter(|p| is_libp2p_peer(p))
        .count();

    if libp2p_inbound > self.private_inbound_cap {
        // Drop excess, preferring to keep peers with relay diversity
        let to_drop = select_excess_by_relay_concentration(
            libp2p_inbound,
            self.private_inbound_cap
        );
        terminate_peers(to_drop).await;
    }
}
```

### 5.3 Connection Acceptance Backpressure

Before accepting a hole-punched connection, check capacity:

```rust
// In Libp2pService::start_inbound()
fn should_accept_inbound(&self) -> bool {
    let current = self.current_libp2p_inbound.load(Ordering::SeqCst);
    current < self.private_inbound_cap
}

// In stream event handler
if incoming.direction == StreamDirection::Inbound {
    if !self.should_accept_inbound() {
        log::warn!("Rejecting inbound libp2p connection: at capacity");
        return;  // Drop the stream
    }
    // ... proceed with handshake
}
```

---

## 6. Implementation Plan

### 6.1 Phase 1: Transport Fallback (Critical Path)

**Goal**: Make `libp2p-mode=bridge` work on mainnet without breaking connectivity.

**Changes**:

1. **`components/p2p-libp2p/src/transport.rs`**:
   - Modify `Libp2pOutboundConnector::connect()` to implement fallback logic
   - Add `select_transport()` function
   - Add TCP fallback on libp2p dial failure

2. **`components/connectionmanager/src/lib.rs`**:
   - No changes needed — already uses `OutboundConnector` trait

3. **`kaspad/src/args.rs`**:
   - Add `--libp2p-mode=bridge` variant
   - Add `--libp2p-role` flag

4. **`kaspad/src/daemon.rs`**:
   - Wire bridge mode to use fallback connector

**Estimated scope**: ~200 lines changed

### 6.2 Phase 2: Address Metadata Population

**Goal**: Learn and store libp2p capability from peer interactions.

**Changes**:

1. **`components/addressmanager/src/lib.rs`**:
   - Add `mark_libp2p_capable()` and `clear_libp2p_capability()`
   - Call these from connection success/failure handlers

2. **`protocol/p2p/src/core/hub.rs`** (or appropriate location):
   - After successful libp2p handshake, mark address as capable
   - After libp2p dial failure, clear capability

3. **`utils/src/networking.rs`**:
   - Already has `services` and `relay_port` — just needs usage

**Estimated scope**: ~100 lines changed

### 6.3 Phase 3: Role-Based Configuration

**Goal**: Differentiate public-relay vs private behavior.

**Changes**:

1. **`components/p2p-libp2p/src/config.rs`**:
   - Add `Role` enum: `PublicRelay | Private | Auto`
   - Integrate with existing config builder

2. **`kaspad/src/libp2p.rs`**:
   - Parse `--libp2p-role` flag
   - Configure relay server based on role

3. **`components/p2p-libp2p/src/service.rs`**:
   - Conditional relay server startup for public-relay role
   - Private-node specific reservation logic

**Estimated scope**: ~150 lines changed

### 6.4 Phase 4: Relay Diversity Enforcement

**Goal**: Prevent eclipse via single relay.

**Changes**:

1. **`components/connectionmanager/src/lib.rs`**:
   - Add `RelayBudget` tracking
   - Integrate with outbound peer selection

2. **`components/p2p-libp2p/src/transport.rs`**:
   - Track relay usage in dial decisions

**Estimated scope**: ~100 lines changed

### 6.5 Phase 5: Private Node Inbound Limits

**Goal**: Prevent inbound explosion on private nodes.

**Changes**:

1. **`components/connectionmanager/src/lib.rs`**:
   - Add `private_inbound_cap` field
   - Modify `handle_inbound_connections()` for private role

2. **`components/p2p-libp2p/src/service.rs`**:
   - Add early-reject logic in `start_inbound()`

**Estimated scope**: ~80 lines changed

---

## 7. Testing Strategy

### 7.1 Unit Tests

| Test | Location | Purpose |
|------|----------|---------|
| `transport_fallback_tcp_after_libp2p_fail` | `transport.rs` | Verify fallback works |
| `address_manager_tracks_libp2p_capability` | `addressmanager/lib.rs` | Verify metadata persistence |
| `relay_budget_enforces_diversity` | `connectionmanager/lib.rs` | Verify eclipse prevention |
| `private_inbound_cap_respected` | `connectionmanager/lib.rs` | Verify limit enforcement |

### 7.2 Integration Tests

1. **Mainnet compatibility test**:
   - Start node with `--libp2p-mode=bridge`
   - Verify syncs from mainnet (TCP peers)
   - Verify reaches 8/8 outbound

2. **Hybrid connectivity test**:
   - One TCP-only peer
   - One libp2p-capable peer
   - Verify connects to both appropriately

3. **Relay diversity test**:
   - Two relays
   - Multiple private nodes
   - Verify connections distributed across relays

### 7.3 NAT Lab Scenarios

Reuse existing NAT lab infrastructure (see NAT-LAB-HANDOVER.md):

1. **Preserve existing tests**:
   - DCUtR success/failure scenarios
   - Reservation backoff
   - Helper API functionality

2. **Add hybrid tests**:
   - Mixed TCP + libp2p peer set
   - Transport fallback under load
   - Relay failover (one relay goes down)

### 7.4 Mainnet Soak Test

Repeat soak test from MAINNET-SOAK-REPORT.md with bridge mode:

- Verify sync completes
- Verify stable peer count
- Monitor for memory leaks
- Check relay diversity (if relays configured)

---

## 8. Migration Path

### 8.1 For Existing Node Operators

**Default behavior unchanged**: Nodes without libp2p flags continue working exactly as today.

### 8.2 For Public Node Operators Who Want to Help

```bash
# Minimal relay configuration
kaspad --libp2p-mode=bridge \
  --libp2p-role=public-relay \
  --libp2p-relay-port=4001
```

### 8.3 For Private Node Operators

```bash
# Private node with relay
kaspad --libp2p-mode=bridge \
  --libp2p-role=private \
  --libp2p-reservations="/ip4/RELAY_IP/tcp/4001/p2p/RELAY_PEER_ID" \
  --libp2p-external-multiaddrs="/ip4/MY_PUBLIC_IP/tcp/16111"
```

---

## 9. Risk Analysis

### 9.1 Compatibility Risks

| Risk | Mitigation |
|------|------------|
| Breaking existing TCP connectivity | Fallback-first design; TCP always available |
| Protocol version mismatch | No protocol changes; same gRPC handshake |
| Address manager corruption | Additive metadata; backwards-compatible serialization |

### 9.2 Security Risks

| Risk | Mitigation |
|------|------------|
| Eclipse via relay | Per-relay caps + diversity enforcement |
| Relay DoS | Bandwidth limits + reservation caps |
| Private node resource exhaustion | Inbound caps + backpressure |

### 9.3 Performance Risks

| Risk | Mitigation |
|------|------------|
| Added latency for TCP peers | Only try libp2p for known-capable peers |
| Dial timeout stacking | Fast libp2p timeout (5s) before TCP fallback |
| Memory overhead | Lazy swarm initialization; off by default |

---

## 10. Summary

This proposal introduces a **bridge mode** that enables hybrid TCP + libp2p connectivity:

1. **Default nodes unchanged**: `--libp2p-mode=off` remains default
2. **Bridge mode for hybrid**: `--libp2p-mode=bridge` enables intelligent transport selection
3. **Role-based behavior**: `--libp2p-role` differentiates public-relay vs private nodes
4. **TCP fallback**: libp2p failures gracefully degrade to TCP
5. **Eclipse prevention**: Per-relay caps + diversity enforcement
6. **Private node protection**: Inbound caps prevent resource exhaustion

The implementation is designed to be **incremental** (5 phases) and **testable** at each stage, with minimal risk to existing functionality.
