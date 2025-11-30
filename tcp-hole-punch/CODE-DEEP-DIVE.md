# CODE-DEEP-DIVE.md — libp2p / DCUtR Integration Analysis

**Branch**: `tcp-hole-punch-clean`
**Commit**: `222eba40` (Add TCP hole punch stress test report)
**Date**: November 2025

---

## 1. Overview

This document provides a detailed description of the current libp2p/DCUtR integration in rusty-kaspa. The integration is designed to enable TCP hole punching via the libp2p protocol stack, allowing NAT'd (private) nodes to participate in the Kaspa P2P network by punching through NAT barriers using the DCUtR (Direct Connection Upgrade through Relay) protocol.

**Key design principles:**
- libp2p is **optional** and **off by default** (`--libp2p-mode=off`)
- The integration is **feature-gated** and **isolated** from core consensus/IBD/mempool/mining paths
- When enabled, libp2p provides an alternative transport layer that integrates with the existing Kaspa P2P handshake over gRPC
- The goal is to obtain a bidirectional stream via libp2p, then run a normal Kaspa P2P handshake over that stream

**Current State**: The integration works for **isolated libp2p overlays** (relay + private nodes). However, `libp2p-mode=full` **blocks mainnet sync** because connection manager dials peers via libp2p, but mainnet peers don't speak libp2p — resulting in 0/8 outgoing connections (see MAINNET-SOAK-REPORT.md).

---

## 2. Config & Modes

### 2.1 CLI Flags

All libp2p-related CLI flags are defined in `kaspad/src/args.rs`:

| Flag | Type | Description |
|------|------|-------------|
| `--libp2p-mode` | `off \| full \| helper` | Runtime mode for libp2p adapter |
| `--libp2p-helper-listen` | `SocketAddr` | TCP address for the helper JSON API |
| `--libp2p-identity-path` | `PathBuf` | Path to persist libp2p identity (ed25519 keypair) |
| `--libp2p-reservations` | `Vec<String>` | Multiaddr(s) of relay(s) to reserve on |
| `--libp2p-external-multiaddrs` | `Vec<String>` | External multiaddrs to announce (for DCUtR) |
| `--libp2p-advertise-addresses` | `Vec<SocketAddr>` | Additional advertise addresses |
| `--libp2p-relay-inbound-cap` | `usize` | Per-relay bucket cap for inbound connections |
| `--libp2p-relay-inbound-unknown-cap` | `usize` | Cap for inbound connections with unknown relay |
| `--libp2p-listen` | `Vec<SocketAddr>` | Socket addresses for libp2p listener |
| `--libp2p-autonat-allow-private` | `bool` | Allow AutoNAT server on private IPs |

**Location**: `kaspad/src/args.rs:115-145`

### 2.2 Mode Enum

The `Mode` enum is defined in `components/p2p-libp2p/src/config.rs:141-161`:

```rust
pub enum Mode {
    Off,    // libp2p disabled (default)
    Full,   // Full libp2p transport + relay/DCUtR
    Helper, // Alias for Full (reserved for narrower mode in future)
}
```

**Key methods:**
- `Mode::effective()` — maps `Helper` → `Full` (currently identical)
- `Mode::is_enabled()` — returns `true` unless mode is `Off`

### 2.3 Config Structure

The runtime config is built in `kaspad/src/libp2p.rs` via `build_libp2p_config()`:

```rust
pub fn build_libp2p_config(args: &Args) -> Config {
    ConfigBuilder::new()
        .mode(args.libp2p_mode)
        .identity(if let Some(path) = &args.libp2p_identity_path {
            Identity::Persisted(path.clone())
        } else {
            Identity::Ephemeral
        })
        .helper_listen(args.libp2p_helper_listen)
        .listen_addresses(args.libp2p_listen.clone())
        .relay_inbound_cap(args.libp2p_relay_inbound_cap)
        .relay_inbound_unknown_cap(args.libp2p_relay_inbound_unknown_cap)
        .reservations(args.libp2p_reservations.clone())
        .external_multiaddrs(args.libp2p_external_multiaddrs.clone())
        .advertise_addresses(args.libp2p_advertise_addresses.clone())
        .autonat(autonat_config)
        .build()
}
```

**Location**: `kaspad/src/libp2p.rs:14-48`

### 2.4 Wiring into Daemon

In `kaspad/src/daemon.rs`, the libp2p mode determines how the `outbound_connector` is configured:

1. **Mode = Off**: Uses `TcpConnector` only (line ~285)
2. **Mode = Full/Helper**: Uses `Libp2pOutboundConnector` with fallback to TCP (line ~295)

```rust
let outbound_connector: Arc<dyn OutboundConnector> = if libp2p_config.mode.is_enabled() {
    Arc::new(Libp2pOutboundConnector::with_provider_cell(
        libp2p_config.clone(),
        Arc::new(TcpConnector),
        swarm_provider_cell.clone(),
    ))
} else {
    Arc::new(TcpConnector)
};
```

**Location**: `kaspad/src/daemon.rs:280-310`

### 2.5 Mode Behavior Analysis

#### `libp2p-mode=off` (Default)
- **Outbound connector**: `TcpConnector` (standard TCP dial)
- **Inbound listener**: Standard gRPC server on `--listen`
- **libp2p swarm**: Not created
- **Result**: Node behaves exactly like pre-libp2p master

#### `libp2p-mode=full`
- **Outbound connector**: `Libp2pOutboundConnector`
  - For **every** outbound dial, attempts libp2p first
  - If libp2p provider not yet initialized: **connection fails** (returns `ProtocolError`)
  - If libp2p dial fails: **no TCP fallback** in current implementation
- **Inbound listener**: Both gRPC on `--listen` AND libp2p on `--libp2p-listen`
- **libp2p swarm**: Created and running
- **Result**: **Breaks mainnet sync** because mainnet peers don't speak libp2p

---

## 3. NetAddress & Metadata

### 3.1 NetAddress Structure

The `NetAddress` type is defined in `utils/src/networking.rs:225-278`:

```rust
pub struct NetAddress {
    pub ip: IpAddress,
    pub port: u16,
    pub services: u64,        // Bitflags for capabilities
    pub relay_port: Option<u16>,  // Port for libp2p relay (if applicable)
}
```

**Service flags** (line 222):
```rust
pub const NET_ADDRESS_SERVICE_LIBP2P_RELAY: u64 = 1 << 0;
```

**Key methods:**
- `merge_metadata(&mut self, other: &NetAddress)` — ORs services, updates relay_port
- `is_libp2p_relay(&self)` — checks `NET_ADDRESS_SERVICE_LIBP2P_RELAY` flag
- `with_services()` / `with_relay_port()` — builder pattern

### 3.2 Address Manager Storage

The `AddressManager` in `components/addressmanager/src/lib.rs` stores addresses with metadata:

**Adding addresses (line 260-273):**
```rust
pub fn add_address(&mut self, address: NetAddress) {
    if address.ip.is_loopback() || address.ip.is_unspecified() {
        return;
    }
    if self.address_store.has(address) {
        self.address_store.merge_metadata(address);  // Merge capabilities
        return;
    }
    self.address_store.set(address, 1);  // Initial failed_count = 1
}
```

**Metadata merging** (in `address_store_with_cache::Store::set`, line 383-395):
```rust
let entry = match self.addresses.get(&address.into()) {
    Some(entry) => {
        let mut merged_address = entry.address;
        merged_address.merge_metadata(&address);  // OR services, update relay_port
        Entry { connection_failed_count, address: merged_address }
    }
    None => Entry { connection_failed_count, address },
};
```

### 3.3 How Peers Are Known as libp2p-Capable

**Today**: A node knows a peer is libp2p-capable via:

1. **Services bitflags**: If `NetAddress.services & NET_ADDRESS_SERVICE_LIBP2P_RELAY != 0`
2. **relay_port**: If `NetAddress.relay_port.is_some()`
3. **TransportMetadata at connection time**: The `Capabilities.libp2p` field and `PathKind`

**However**, the current address manager **does not yet populate** `services` or `relay_port` from libp2p peer discovery. The infrastructure exists but is unused — this is part of the future hybrid mode work.

---

## 4. Libp2p Adapter Crate (`components/p2p-libp2p/*`)

### 4.1 Crate Structure

```
components/p2p-libp2p/src/
├── lib.rs          # Re-exports and module declarations
├── config.rs       # Config, Mode, ConfigBuilder, AutoNatConfig
├── transport.rs    # SwarmStreamProvider, Libp2pIdentity, dial/listen logic
├── swarm.rs        # Libp2pBehaviour, DcutrBootstrapBehaviour, StreamBehaviour
├── service.rs      # Libp2pService (reservation worker, helper API, inbound bridge)
├── helper_api.rs   # JSON API for manual dial/status
├── reservations.rs # ReservationManager (backoff logic)
├── metadata.rs     # Re-exports TransportMetadata from kaspa_p2p_lib
```

### 4.2 Dial Lifecycle

**Entry point**: `Libp2pStreamProvider::dial()` in `transport.rs:616-631`

```
dial(address: NetAddress)
    ↓
to_multiaddr(address)  // Convert IP:port to /ip4/x/tcp/y
    ↓
SwarmCommand::Dial { address, respond_to }
    ↓
SwarmDriver::handle_command()
    ↓
libp2p::Swarm::dial(DialOpts)
    ↓
pending_dials.insert(connection_id, DialRequest)
    ↓
... swarm events ...
    ↓
On ConnectionEstablished → request_stream_bridge()
    ↓
On StreamEvent::Outbound → pending_dials.remove() → respond_to.send(Ok(...))
```

**Key structures:**

```rust
struct DialRequest {
    respond_to: oneshot::Sender<Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>,
    started_at: Instant,
    via: DialVia,  // Direct or Relay { target_peer }
}

enum DialVia {
    Direct,
    Relay { target_peer: PeerId },
}
```

**Location**: `transport.rs:734-744`

### 4.3 Pending Dials Management

Pending dials are tracked per-request, not per-peer. Multiple concurrent relay dials to the same peer are supported (each gets its own `DialRequest`).

**Cleanup mechanisms:**
1. **Timeout**: `expire_pending_dials()` runs every 5s, fails dials older than 30s
2. **Connection closed**: `fail_pending()` called on `ConnectionClosed` event
3. **Outgoing error**: `fail_pending()` called on `OutgoingConnectionError` event
4. **Shutdown**: All pending dials failed with "driver stopped"

**Location**: `transport.rs:835-852`

### 4.4 DCUtR Integration

DCUtR (Direct Connection Upgrade through Relay) enables hole punching. The integration:

**1. Address seeding (critical for DCUtR to work):**

The `DcutrBootstrapBehaviour` in `swarm.rs:94-202` seeds external addresses into DCUtR's internal cache:

```rust
pub struct DcutrBootstrapBehaviour {
    pending_candidates: VecDeque<Multiaddr>,
    pending_confirms: VecDeque<Multiaddr>,
    emitted_candidates: HashSet<Multiaddr>,
}
```

This behaviour:
- Emits `NewExternalAddrCandidate` for configured `--libp2p-external-multiaddrs`
- Intercepts `ExternalAddrConfirmed` and re-emits as `NewExternalAddrCandidate` (so DCUtR sees addresses added via `swarm.add_external_address()`)

**Pre-seeding** in `build_streaming_swarm()` (line 387-392):
```rust
let mut dcutr = dcutr::Behaviour::new(peer_id);
for addr in &external_addrs {
    dcutr.on_swarm_event(FromSwarm::NewExternalAddrCandidate(NewExternalAddrCandidate { addr }));
}
```

**2. DCUtR handoff (relay dial → direct connection):**

When a relay dial triggers DCUtR and succeeds, the direct connection is associated with the original pending dial:

```rust
// In SwarmDriver::handle_event(), on ConnectionEstablished:
if !endpoint_uses_relay(&endpoint) {
    if let Some((_old_req, pending)) = self.take_pending_relay_by_peer(&peer_id) {
        // Transfer pending dial to new direct connection
        self.pending_dials.insert(connection_id, pending);
    }
}
```

**Location**: `transport.rs:1016-1023`

**3. Dial-back for bidirectional connectivity:**

`maybe_request_dialback()` in `transport.rs:1162-1215` initiates a return dial via the active relay when:
- Peer supports DCUtR
- Currently connected via relay
- No outgoing connection to peer exists
- Not in cooldown period

### 4.5 Helper API

The helper API provides a JSON-based interface for manual control:

**Location**: `helper_api.rs` and `service.rs:199-247`

**Actions:**
- `{"action":"status"}` → `{"ok":true}`
- `{"action":"dial","multiaddr":"/ip4/x/tcp/y/p2p/..."}` → triggers dial

**Limits (from lifecycle.md):**
- Max line length: 8 KiB (`HELPER_MAX_LINE`)
- Read timeout: 5 seconds (`HELPER_READ_TIMEOUT`)
- One command per connection (closes after response)

**Error handling:**
- Bad JSON → `{"ok":false,"error":"invalid request: ..."}`
- Unknown action → `{"ok":false,"error":"unknown action"}`
- Dial failure → `{"ok":false,"error":"dial failed: ..."}`

### 4.6 Reservation Worker

Reservations maintain relay slots for private nodes:

**Location**: `service.rs:249-358`

**Configuration:**
- `RESERVATION_REFRESH_INTERVAL`: 20 minutes
- `RESERVATION_POLL_INTERVAL`: 30 seconds
- `RESERVATION_BASE_BACKOFF`: 5 seconds
- `RESERVATION_MAX_BACKOFF`: 60 seconds

**Lifecycle:**
1. On startup, if `--libp2p-reservations` non-empty, spawn worker
2. Worker loops: attempt reservations, wait poll interval
3. On shutdown: release all active reservations

**Backoff logic** (`reservations.rs`):
- Exponential backoff on failure (5s → 10s → 20s → 40s → 60s max)
- Reset to base on success

### 4.7 Backpressure

The incoming stream channel has bounded capacity:

```rust
const INCOMING_CHANNEL_BOUND: usize = 32;
```

When full, streams are dropped with a warning:
```rust
fn enqueue_incoming(&self, incoming: IncomingStream) {
    if let Err(err) = self.incoming_tx.try_send(incoming) {
        match err {
            TrySendError::Full(_) => warn!("dropping stream because channel is full"),
            TrySendError::Closed(_) => warn!("dropping stream because receiver is closed"),
        }
    }
}
```

**Location**: `transport.rs:854-862`

---

## 5. Stream → Kaspa Injection Seam

### 5.1 Overview

The injection seam is how libp2p streams become Kaspa P2P connections:

```
libp2p stream (AsyncRead + AsyncWrite)
    ↓
Libp2pService::start_inbound() / connect_with_stream()
    ↓
ConnectionHandler::connect_with_stream() or serve_with_incoming()
    ↓
gRPC handshake over stream
    ↓
Router (standard Kaspa P2P)
```

### 5.2 `connect_with_stream()`

**Location**: `protocol/p2p/src/core/connection_handler.rs:253-295`

```rust
pub async fn connect_with_stream<S>(&self, stream: S, metadata: TransportMetadata)
    -> Result<Arc<Router>, ConnectionError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    // 1. Derive socket address from metadata (synthetic if needed)
    let socket_address = metadata
        .synthetic_socket_addr()
        .or_else(|| metadata.reported_ip.map(|ip| SocketAddr::new(ip.into(), 0)))
        .ok_or(ConnectionError::NoAddress)?;

    // 2. Build h2 channel with libp2p-tuned settings
    let channel = build_libp2p_channel(stream, connect_timeout).await?;

    // 3. Create gRPC client
    let mut client = ProtoP2pClient::with_origin(channel, Uri::from_static("http://kaspa.libp2p"))
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(P2P_MAX_MESSAGE_SIZE);

    // 4. Establish message stream
    let incoming_stream = client.message_stream(ReceiverStream::new(outgoing_receiver)).await?;

    // 5. Create Router with metadata
    let router = Router::new(socket_address, true, metadata, ...).await;

    // 6. Run handshake
    self.initializer.initialize_connection(router.clone()).await?;

    // 7. Notify hub
    self.hub_sender.send(HubEvent::NewPeer(router.clone())).await?;

    Ok(router)
}
```

### 5.3 `serve_with_incoming()`

**Location**: `protocol/p2p/src/core/connection_handler.rs:316-343`

```rust
pub fn serve_with_incoming<S, I>(&self, incoming: I)
where
    S: AsyncRead + AsyncWrite + Connected + Send + Unpin + 'static,
    I: Stream<Item = Result<S, std::io::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        TonicServer::builder()
            // libp2p-tuned settings
            .max_frame_size(Some(LIBP2P_HTTP2_MAX_FRAME_SIZE))
            .initial_stream_window_size(Some(LIBP2P_HTTP2_STREAM_WINDOW))
            // ...
            .serve_with_incoming(incoming)
            .await
    });
}
```

### 5.4 Metadata Passing

The `MetadataConnectInfo` struct carries pre-computed metadata through tonic's `Connected` trait:

```rust
pub struct MetadataConnectInfo {
    pub socket_addr: Option<SocketAddr>,
    pub metadata: TransportMetadata,
}

impl Connected for MetaConnectedStream {
    type ConnectInfo = MetadataConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo { self.info.clone() }
}
```

In the gRPC handler (`message_stream()`), metadata is extracted:

```rust
let (socket_address, metadata) = if let Some(info) = request.extensions().get::<MetadataConnectInfo>() {
    // Use pre-computed metadata from libp2p
    (info.socket_addr.or(info.metadata.synthetic_socket_addr()), info.metadata)
} else {
    // Standard TCP connection - derive metadata from socket
    (request.remote_addr(), self.metadata_factory.for_inbound(ip))
};
```

**Location**: `connection_handler.rs:354-369`

---

## 6. Connection Manager & Dial Policy

### 6.1 Structure

The `ConnectionManager` in `components/connectionmanager/src/lib.rs` manages:
- Outbound peer selection
- Inbound connection limits
- Per-relay bucket enforcement

**Key fields:**
```rust
pub struct ConnectionManager {
    p2p_adaptor: Arc<kaspa_p2p_lib::Adaptor>,
    outbound_target: usize,           // e.g., 8
    inbound_limit: usize,             // e.g., 128
    relay_inbound_cap: usize,         // Default: 4
    relay_inbound_unknown_cap: usize, // Default: 8
    address_manager: Arc<Mutex<AddressManager>>,
    // ...
}
```

### 6.2 Outbound Peer Selection

**Location**: `handle_outbound_connections()` in `lib.rs:170-247`

```rust
async fn handle_outbound_connections(self: &Arc<Self>, peer_by_address: &HashMap<SocketAddr, Peer>) {
    // 1. Count active outbound
    let active_outbound: HashSet<NetAddress> = peer_by_address.values()
        .filter(|peer| peer.is_outbound())
        .map(|peer| peer.net_address().into())
        .collect();

    if active_outbound.len() >= self.outbound_target {
        return;  // Already at target
    }

    // 2. Get prioritized random iterator from address manager
    let mut addr_iter = self.address_manager.lock()
        .iterate_prioritized_random_addresses(active_outbound);

    // 3. Dial peers until target met
    for net_addr in addr_iter {
        let socket_addr = SocketAddr::new(net_addr.ip.into(), net_addr.port);
        let result = self.p2p_adaptor.connect_peer(socket_addr.to_string()).await;
        // ... handle success/failure ...
    }
}
```

**Critical point**: The connection manager calls `p2p_adaptor.connect_peer()`, which uses the configured `outbound_connector`. In `libp2p-mode=full`, this is `Libp2pOutboundConnector`.

### 6.3 How libp2p Connector is Wired

The `Libp2pOutboundConnector` in `components/p2p-libp2p/src/transport.rs:331-432`:

```rust
impl OutboundConnector for Libp2pOutboundConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: CoreTransportMetadata,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        // If disabled, delegate to fallback (TCP)
        if !self.config.mode.is_enabled() {
            return self.fallback.connect(address, metadata, handler);
        }

        // If provider available, dial via libp2p
        if let Some(provider) = &self.provider {
            let (md, stream) = provider.dial(address).await?;
            return handler.connect_with_stream(stream, md).await;
        }

        // Provider not available - ERROR (no fallback!)
        Err(ConnectionError::ProtocolError(ProtocolError::Other(
            "libp2p outbound connector unavailable (provider not initialised)"
        )))
    }
}
```

### 6.4 Why `libp2p-mode=full` Blocks Mainnet Sync

**The exact control flow:**

1. Node starts with `--libp2p-mode=full`
2. `ConnectionManager` event loop ticks
3. `handle_outbound_connections()` calls `p2p_adaptor.connect_peer("mainnet.peer:16111")`
4. `Adaptor::connect_peer()` delegates to `ConnectionHandler::connect()`
5. `ConnectionHandler::connect()` calls `outbound_connector.connect()`
6. `Libp2pOutboundConnector::connect()`:
   - Mode is enabled, so doesn't fall back to TCP
   - Tries to dial via libp2p
   - Mainnet peer doesn't speak libp2p → dial fails
   - Returns `Err(ConnectionError::ProtocolError(...))`
7. Connection manager marks address as failed
8. Repeats for all addresses → **0/8 outgoing connections**

**Root cause**: No TCP fallback when libp2p dial fails. The connector either uses libp2p (if enabled) or TCP (if disabled), but never tries TCP after libp2p failure.

### 6.5 Inbound Limits & Relay Buckets

**Per-relay bucket enforcement** (`relay_overflow()` in `lib.rs:313-332`):

```rust
fn relay_overflow<'a>(peers: &'a [&Peer], per_relay_cap: usize, unknown_cap: usize) -> Vec<&'a Peer> {
    let mut buckets: HashMap<Option<String>, Vec<&Peer>> = HashMap::new();

    for peer in peers {
        let relay_id = match &peer.metadata().path {
            PathKind::Relay { relay_id } => relay_id.clone(),
            _ => None,
        };
        buckets.entry(relay_id).or_default().push(*peer);
    }

    let mut to_drop = Vec::new();
    for (relay_id, peers) in buckets {
        let cap = if relay_id.is_some() { per_relay_cap } else { unknown_cap };
        if peers.len() > cap {
            let drop = peers.choose_multiple(&mut rng(), peers.len() - cap);
            to_drop.extend(drop);
        }
    }
    to_drop
}
```

**Libp2p peer classification** (`is_libp2p_peer()` in `lib.rs:303-306`):

```rust
fn is_libp2p_peer(peer: &Peer) -> bool {
    let md = peer.metadata();
    md.capabilities.libp2p || matches!(md.path, PathKind::Relay { .. })
}
```

**Inbound budget splitting** (in `handle_inbound_connections()`, line 272-278):
```rust
let libp2p_cap = (self.inbound_limit / 2).max(1);
if libp2p_peers.len() > libp2p_cap {
    // Drop excess libp2p peers
}
```

---

## 7. NAT Lab Behaviour vs Code

### 7.1 Lab Topology

From NAT-LAB-HANDOVER.md:
- **Relay**: Public node with `--libp2p-mode=full`
- **Node A**: Private node behind NAT, with `--libp2p-reservations=/ip4/RELAY_IP/tcp/4001/p2p/RELAY_PEER_ID`
- **Node B**: Private node behind NAT, same reservations as A

### 7.2 DCUtR Success Path

**Code realization** (from stress tests and code analysis):

1. A and B both make relay reservations → `reservation_worker()` calls `provider.reserve()`
2. A triggers dial to B via relay circuit → `SwarmCommand::Dial` with p2p-circuit address
3. DCUtR protocol runs:
   - Identify exchange reveals external addresses (via `DcutrBootstrapBehaviour`)
   - DCUtR CONNECT message sent with addresses
   - Simultaneous TCP connect attempts (hole punch)
4. On success, direct connection established:
   - `SwarmEvent::ConnectionEstablished` with non-relay endpoint
   - `take_pending_relay_by_peer()` transfers pending dial to new connection ID
   - Stream request succeeds → Kaspa handshake runs

**Log evidence from STRESS-TEST-REPORT.md:**
```
libp2p dcutr event: DirectConnectionUpgradeSucceeded { remote_peer_id: ... }
```

### 7.3 DCUtR Failure Path

**NoAddresses case** (from stress report):
- Occurs when `--libp2p-external-multiaddrs` not configured
- DCUtR sends CONNECT with empty address list
- Relay responds with error
- Falls back to relay path (no hole punch)

**AttemptsExceeded(3) case:**
- DCUtR made 3 simultaneous connect attempts, all failed
- NAT type incompatible (e.g., symmetric NAT)
- Connection remains relay-only

**Code handling** (`transport.rs:993-997`):
```rust
SwarmEvent::Behaviour(Libp2pEvent::Dcutr(event)) => {
    let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
    info!("libp2p dcutr event: {:?} (swarm has {} external addrs)", event, external_addrs.len());
}
```

### 7.4 Backpressure Behaviour

**Code** (`transport.rs:854-862`):
- `INCOMING_CHANNEL_BOUND = 32`
- On channel full: stream dropped, warning logged
- No retry mechanism

**Matches lifecycle.md specification:**
> "Backpressure: bounded channel of 32, drops on full"

### 7.5 Helper API Behaviour

**Code** (`service.rs:199-247`):
- `HELPER_MAX_LINE = 8 * 1024` (8 KiB)
- `HELPER_READ_TIMEOUT = 5 seconds`
- Single command per connection (closes after response)

**Matches lifecycle.md specification exactly.**

---

## 8. Summary of Current State

### What Works
- ✅ libp2p swarm construction and configuration
- ✅ Relay reservations with backoff
- ✅ DCUtR hole punching (when external addresses configured)
- ✅ Stream → Kaspa P2P bridge
- ✅ Per-relay bucket enforcement
- ✅ Helper API for manual control
- ✅ Isolated libp2p overlay testing (relay + private nodes)

### What Doesn't Work
- ❌ `libp2p-mode=full` on mainnet (0/8 outgoing connections)
- ❌ TCP fallback when libp2p dial fails
- ❌ Automatic transport selection based on peer capabilities
- ❌ Address manager populating libp2p metadata from peer discovery

### Key Missing Pieces for Hybrid Mode
1. **Transport fallback**: Try libp2p first, fall back to TCP on failure
2. **Capability-aware selection**: Use TCP for non-libp2p peers, libp2p for libp2p peers
3. **Address metadata propagation**: Mark addresses with libp2p capability in address manager
4. **Mode granularity**: Separate "use libp2p when available" from "libp2p only"
