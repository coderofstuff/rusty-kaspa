use crate::metrics::{Libp2pMetrics, Libp2pMetricsSnapshot};
use crate::swarm::{Libp2pBehaviour, Libp2pEvent, StreamEvent, StreamRequestId, build_streaming_swarm};
use crate::{config::Config, metadata::TransportMetadata};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PathKind, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use libp2p::autonat;
use libp2p::core::transport::ListenerId;
use libp2p::dcutr;
use libp2p::identify;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Multiaddr;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::SwarmEvent;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::{PeerId, identity, relay};
use log::{debug, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;
use std::{fs, io, path::Path};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, OnceCell, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::task::spawn;
use tokio::time::{Duration, Instant, MissedTickBehavior, interval};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use triggered::{Listener, Trigger};

mod auto_role;
mod driver;
mod multiaddr;
mod provider;
mod provider_types;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::auto_role::{AUTO_ROLE_REQUIRED_DIRECT, AUTO_ROLE_WINDOW, AutoRoleState};
use self::multiaddr::{
    addr_uses_relay, candidate_ip_addr, default_listen_addr, endpoint_uses_relay, extract_circuit_target_peer, extract_relay_peer,
    extract_remote_dcutr_candidates, insert_relay_peer, is_tcp_dialable, parse_multiaddrs, parse_reservation_targets,
    relay_id_from_multiaddr, relay_info_from_multiaddr, relay_probe_base, strip_peer_suffix,
};
pub use self::multiaddr::{multiaddr_to_metadata, to_multiaddr};
pub use self::provider::SwarmStreamProvider;
use self::provider_types::{DialRequest, IncomingStream, PendingProbe, SwarmCommand, metadata_from_endpoint};

#[derive(Debug, thiserror::Error)]
pub enum Libp2pError {
    #[error("libp2p provider unavailable")]
    ProviderUnavailable,
    #[error("libp2p not enabled")]
    Disabled,
    #[error("libp2p dial failed: {0}")]
    DialFailed(String),
    #[error("libp2p listen failed: {0}")]
    ListenFailed(String),
    #[error("libp2p reservation failed: {0}")]
    ReservationFailed(String),
    #[error("libp2p identity error: {0}")]
    Identity(String),
    #[error("invalid multiaddr: {0}")]
    Multiaddr(String),
}

/// Placeholder libp2p transport connector. Will be expanded with real libp2p dial/listen logic.
#[derive(Clone, Default)]
pub struct Libp2pConnector {
    pub config: Config,
}

impl Libp2pConnector {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl TransportConnector for Libp2pConnector {
    type Error = Libp2pError;
    type Future<'a> = BoxFuture<'a, Result<(Arc<Router>, TransportMetadata, PeerKey), Self::Error>>;

    fn connect<'a>(&'a self, _address: NetAddress) -> Self::Future<'a> {
        let _metadata = TransportMetadata::default();
        if !self.config.mode.is_enabled() {
            return Box::pin(async move { Err(Libp2pError::Disabled) });
        }
        Box::pin(async move { Err(Libp2pError::DialFailed("libp2p connector requires runtime provider".into())) })
    }
}

/// Cooldown used in bridge mode after a libp2p dial failure before retrying libp2p for the same address.
const BRIDGE_LIBP2P_RETRY_COOLDOWN: Duration = Duration::from_secs(600);

/// Outbound connector that prefers libp2p when enabled, otherwise falls back to TCP.
pub struct Libp2pOutboundConnector {
    config: Config,
    fallback: Arc<dyn OutboundConnector>,
    provider: Option<Arc<dyn Libp2pStreamProvider>>,
    provider_cell: Option<Arc<OnceCell<Arc<dyn Libp2pStreamProvider>>>>,
    bridge_cooldowns: Mutex<HashMap<String, Instant>>,
}

impl Libp2pOutboundConnector {
    pub fn new(config: Config, fallback: Arc<dyn OutboundConnector>) -> Self {
        Self { config, fallback, provider: None, provider_cell: None, bridge_cooldowns: Mutex::new(HashMap::new()) }
    }

    pub fn with_provider(config: Config, fallback: Arc<dyn OutboundConnector>, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self { config, fallback, provider: Some(provider), provider_cell: None, bridge_cooldowns: Mutex::new(HashMap::new()) }
    }

    pub fn with_provider_cell(
        config: Config,
        fallback: Arc<dyn OutboundConnector>,
        provider_cell: Arc<OnceCell<Arc<dyn Libp2pStreamProvider>>>,
    ) -> Self {
        Self { config, fallback, provider: None, provider_cell: Some(provider_cell), bridge_cooldowns: Mutex::new(HashMap::new()) }
    }

    fn resolve_provider(&self) -> Option<Arc<dyn Libp2pStreamProvider>> {
        if let Some(provider) = &self.provider {
            return Some(provider.clone());
        }
        if let Some(cell) = &self.provider_cell {
            return cell.get().cloned();
        }
        None
    }

    async fn dial_via_provider(
        provider: Arc<dyn Libp2pStreamProvider>,
        address: NetAddress,
        handler: kaspa_p2p_lib::ConnectionHandler,
    ) -> Result<Arc<Router>, ConnectionError> {
        let (mut md, stream) =
            provider.dial(address).await.map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("libp2p dial failed")))?;

        md.capabilities.libp2p = true;
        if matches!(md.path, kaspa_p2p_lib::PathKind::Unknown) {
            md.path = kaspa_p2p_lib::PathKind::Direct;
        }
        handler.connect_with_stream(stream, md).await
    }

    async fn dial_multiaddr_via_provider(
        provider: Arc<dyn Libp2pStreamProvider>,
        address: Multiaddr,
        handler: kaspa_p2p_lib::ConnectionHandler,
    ) -> Result<Arc<Router>, ConnectionError> {
        let address = Self::resolve_relay_multiaddr(provider.clone(), address)
            .await
            .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("libp2p dial failed")))?;
        let (mut md, stream) = provider
            .dial_multiaddr(address)
            .await
            .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("libp2p dial failed")))?;

        md.capabilities.libp2p = true;
        if matches!(md.path, kaspa_p2p_lib::PathKind::Unknown) {
            md.path = kaspa_p2p_lib::PathKind::Direct;
        }
        handler.connect_with_stream(stream, md).await
    }

    async fn resolve_relay_multiaddr(provider: Arc<dyn Libp2pStreamProvider>, address: Multiaddr) -> Result<Multiaddr, Libp2pError> {
        if !addr_uses_relay(&address) {
            return Ok(address);
        }
        if extract_relay_peer(&address).is_some() {
            return Ok(address);
        }
        if extract_circuit_target_peer(&address).is_none() {
            return Err(Libp2pError::Multiaddr("relay circuit missing target peer id".into()));
        }

        let relay_probe_addr = relay_probe_base(&address);
        let relay_peer = provider.probe_relay(relay_probe_addr).await?;
        Ok(insert_relay_peer(&address, relay_peer))
    }

    fn connect_libp2p_only<'a>(
        &'a self,
        address: String,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        let provider = self.resolve_provider();
        let handler = handler.clone();
        Box::pin(async move {
            let provider = provider.ok_or_else(|| {
                ConnectionError::ProtocolError(ProtocolError::Other(
                    "libp2p outbound connector unavailable (provider not initialised)",
                ))
            })?;
            if address.starts_with('/') {
                let multiaddr = Multiaddr::from_str(&address)
                    .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("invalid libp2p multiaddr provided")))?;
                return Self::dial_multiaddr_via_provider(provider, multiaddr, handler).await;
            }

            let address = NetAddress::from_str(&address)
                .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("invalid libp2p address provided")))?;
            Self::dial_via_provider(provider, address, handler).await
        })
    }

    fn connect_bridge<'a>(
        &'a self,
        address: String,
        mut metadata: CoreTransportMetadata,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        let provider = self.resolve_provider();
        let fallback = self.fallback.clone();
        let cooldowns = &self.bridge_cooldowns;
        Box::pin(async move {
            if address.starts_with('/') {
                let provider = provider.ok_or_else(|| {
                    ConnectionError::ProtocolError(ProtocolError::Other(
                        "libp2p outbound connector unavailable (provider not initialised)",
                    ))
                })?;
                let multiaddr = Multiaddr::from_str(&address)
                    .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("invalid libp2p multiaddr provided")))?;
                return Self::dial_multiaddr_via_provider(provider, multiaddr, handler.clone()).await;
            }

            let parsed_address = NetAddress::from_str(&address);
            let now = Instant::now();

            if let Some(deadline) = cooldowns.lock().await.get(&address).cloned()
                && deadline > now
            {
                metadata.capabilities.libp2p = false;
                return fallback.connect(address, metadata, handler).await;
            }

            if let (Ok(net_addr), Some(provider)) = (parsed_address, provider.clone()) {
                match Self::dial_via_provider(provider, net_addr, handler.clone()).await {
                    Ok(router) => {
                        cooldowns.lock().await.remove(&address);
                        return Ok(router);
                    }
                    Err(err) => {
                        debug!("bridge mode libp2p dial failed for {address}: {err}; falling back to TCP");
                        cooldowns.lock().await.insert(address.clone(), now + BRIDGE_LIBP2P_RETRY_COOLDOWN);
                    }
                }
            } else {
                debug!("bridge mode libp2p unavailable for {address}; falling back to TCP");
                cooldowns.lock().await.insert(address.clone(), now + BRIDGE_LIBP2P_RETRY_COOLDOWN);
            }

            metadata.capabilities.libp2p = false;
            fallback.connect(address, metadata, handler).await
        })
    }
}

impl OutboundConnector for Libp2pOutboundConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: CoreTransportMetadata,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        match self.config.mode.effective() {
            crate::Mode::Off => {
                let mut metadata = metadata;
                metadata.capabilities.libp2p = false;
                self.fallback.connect(address, metadata, handler)
            }
            crate::Mode::Full | crate::Mode::Helper => self.connect_libp2p_only(address, handler),
            crate::Mode::Bridge => self.connect_bridge(address, metadata, handler),
        }
    }
}

/// Bound for streams accepted/dialed via libp2p.
pub trait Libp2pStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Libp2pStream for T {}

pub type BoxedLibp2pStream = Box<dyn Libp2pStream>;

/// Handle that can be used to release a reservation listener.
pub struct ReservationHandle {
    closer: Option<BoxFuture<'static, ()>>,
}

impl ReservationHandle {
    pub fn new<Fut>(closer: Fut) -> Self
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        Self { closer: Some(Box::pin(closer)) }
    }

    pub fn noop() -> Self {
        Self { closer: None }
    }

    pub async fn release(mut self) {
        if let Some(closer) = self.closer.take() {
            closer.await;
        }
    }
}

pub type Libp2pListenStream = (TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream);
pub type Libp2pListenFuture<'a> = BoxFuture<'a, Result<Libp2pListenStream, Libp2pError>>;

/// A provider for libp2p streams (dialed or accepted). The real implementation
/// will bridge to the libp2p swarm and return a stream plus transport metadata.
pub trait Libp2pStreamProvider: Send + Sync {
    fn dial<'a>(&'a self, address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>;
    fn dial_multiaddr<'a>(&'a self, address: Multiaddr) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>;
    fn probe_relay<'a>(&'a self, _address: Multiaddr) -> BoxFuture<'a, Result<PeerId, Libp2pError>> {
        Box::pin(async { Err(Libp2pError::DialFailed("relay probe unsupported".into())) })
    }
    fn listen<'a>(&'a self) -> Libp2pListenFuture<'a>;
    fn reserve<'a>(&'a self, target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>>;
    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        Box::pin(async { Vec::new() })
    }
    fn role_updates(&self) -> Option<watch::Receiver<crate::Role>> {
        None
    }
    fn relay_hint_updates(&self) -> Option<watch::Receiver<Option<String>>> {
        None
    }
    fn metrics(&self) -> Option<Arc<Libp2pMetrics>> {
        None
    }
    fn metrics_snapshot(&self) -> Option<Libp2pMetricsSnapshot> {
        self.metrics().map(|metrics| metrics.snapshot())
    }
}

/// Libp2p identity wrapper (ed25519).
#[derive(Clone)]
pub struct Libp2pIdentity {
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub persisted_path: Option<std::path::PathBuf>,
}

impl Libp2pIdentity {
    pub fn from_config(config: &Config) -> Result<Self, Libp2pError> {
        match &config.identity {
            crate::Identity::Ephemeral => {
                let keypair = identity::Keypair::generate_ed25519();
                let peer_id = PeerId::from(keypair.public());
                Ok(Self { keypair, peer_id, persisted_path: None })
            }
            crate::Identity::Persisted(path) => {
                let keypair = load_or_generate_key(path).map_err(|e| Libp2pError::Identity(e.to_string()))?;
                let peer_id = PeerId::from(keypair.public());
                Ok(Self { keypair, peer_id, persisted_path: Some(path.clone()) })
            }
        }
    }

    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }
}

fn load_or_generate_key(path: &Path) -> io::Result<Keypair> {
    if let Ok(bytes) = fs::read(path) {
        return Keypair::from_protobuf_encoding(&bytes).map_err(map_identity_err);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let keypair = identity::Keypair::generate_ed25519();
    let bytes = keypair.to_protobuf_encoding().map_err(map_identity_err)?;
    fs::write(path, bytes)?;
    Ok(keypair)
}

fn map_identity_err(err: impl ToString) -> io::Error {
    io::Error::other(err.to_string())
}

const COMMAND_CHANNEL_BOUND: usize = 16;
const INCOMING_CHANNEL_BOUND: usize = 32;
const PENDING_DIAL_TIMEOUT: Duration = Duration::from_secs(30);
const PENDING_DIAL_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
const DIALBACK_COOLDOWN: Duration = Duration::from_secs(30);
const DIRECT_UPGRADE_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const AUTONAT_PRIVATE_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const DCUTR_PREFLIGHT_RETRY_DELAY: Duration = Duration::from_secs(3);
const DCUTR_LOCAL_OBSERVED_FRESHNESS: Duration = Duration::from_secs(2 * 60);
const DCUTR_REMOTE_CANDIDATE_FRESHNESS: Duration = Duration::from_secs(2 * 60);
const DCUTR_OBSERVED_CANDIDATE_TTL: Duration = Duration::from_secs(15 * 60);
const DCUTR_DYNAMIC_CANDIDATE_TTL: Duration = Duration::from_secs(10 * 60);
const DCUTR_RETRY_BACKOFFS_SECS: [u64; 4] = [2, 5, 10, 20];
const DCUTR_RETRY_JITTER_MS: u64 = 900;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct PeerSnapshot {
    pub peer_id: String,
    pub path: String,
    pub relay_id: Option<String>,
    pub direction: String,
    pub duration_ms: u128,
    pub libp2p: bool,
    pub dcutr_upgraded: bool,
}

#[derive(Clone)]
#[allow(dead_code)]
struct ReservationTarget {
    multiaddr: Multiaddr,
    peer_id: PeerId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialVia {
    Direct,
    Relay { target_peer: PeerId },
}

struct SwarmDriver {
    swarm: libp2p::Swarm<Libp2pBehaviour>,
    command_rx: mpsc::Receiver<SwarmCommand>,
    incoming_tx: mpsc::Sender<IncomingStream>,
    pending_dials: HashMap<StreamRequestId, DialRequest>,
    pending_probes: HashMap<StreamRequestId, PendingProbe>,
    dialback_cooldowns: HashMap<PeerId, Instant>,
    direct_upgrade_cooldowns: HashMap<PeerId, Instant>,
    listen_addrs: Vec<Multiaddr>,
    external_addrs: Vec<Multiaddr>,
    allow_private_addrs: bool,
    local_candidate_meta: HashMap<Multiaddr, LocalCandidateMeta>,
    peer_states: HashMap<PeerId, PeerState>,
    dcutr_retries: HashMap<PeerId, DcutrRetryState>,
    reservation_listeners: HashSet<ListenerId>,
    active_relay: Option<RelayInfo>,
    active_relay_listener: Option<ListenerId>,
    auto_role: Option<AutoRoleState>,
    max_peers_per_relay: usize,
    autonat_private_until: Option<Instant>,
    metrics: Option<Arc<Libp2pMetrics>>,
    listening: bool,
    shutdown: Listener,
    connections: HashMap<StreamRequestId, ConnectionEntry>,
    relay_hint_tx: watch::Sender<Option<String>>,
}

fn default_stream_protocol() -> libp2p::StreamProtocol {
    libp2p::StreamProtocol::new("/kaspad/transport/1.0.0")
}

/// Whether a stream originated from a local outbound dial or from a remote inbound request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDirection {
    Inbound,
    Outbound,
}

fn is_attempts_exceeded(err: &dcutr::Error) -> bool {
    // Upstream error is opaque, relying on Display string for now
    err.to_string().contains("AttemptsExceeded")
}

fn is_dcutr_retry_trigger_error(err: &dcutr::Error) -> bool {
    is_dcutr_retry_trigger_error_text(&err.to_string())
}

fn is_retryable_dcutr_error_text(err: &str) -> bool {
    err.contains("NoAddresses") || err.contains("UnexpectedEof")
}

fn is_dcutr_retry_trigger_error_text(err: &str) -> bool {
    is_retryable_dcutr_error_text(err) || err.contains("AttemptsExceeded")
}

fn local_candidate_priority(source: LocalCandidateSource) -> u8 {
    match source {
        LocalCandidateSource::Observed => 3,
        LocalCandidateSource::Config => 2,
        LocalCandidateSource::Dynamic => 1,
    }
}

fn fallback_old_instant(now: Instant) -> Instant {
    now.checked_sub(Duration::from_secs(24 * 60 * 60)).unwrap_or(now)
}

fn dcutr_retry_jitter(peer_id: PeerId, failures: u8) -> Duration {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    peer_id.hash(&mut hasher);
    failures.hash(&mut hasher);
    Duration::from_millis(hasher.finish() % DCUTR_RETRY_JITTER_MS)
}

#[derive(Clone)]
struct RelayInfo {
    relay_peer: PeerId,
    circuit_base: Multiaddr,
}

#[derive(Default)]
struct PeerState {
    supports_dcutr: bool,
    outgoing: usize,
    connected_via_relay: bool,
    remote_dcutr_candidates: Vec<Multiaddr>,
    remote_candidates_last_seen: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalCandidateSource {
    Config,
    Observed,
    Dynamic,
}

#[derive(Clone, Copy, Debug)]
struct LocalCandidateMeta {
    source: LocalCandidateSource,
    updated_at: Instant,
}

#[derive(Clone, Debug)]
struct DcutrRetryState {
    failures: u8,
    next_retry_at: Instant,
    last_reason: String,
}

#[derive(Clone, Debug)]
struct ConnectionEntry {
    peer_id: PeerId,
    path: PathKind,
    relay_id: Option<String>,
    outbound: bool,
    since: Instant,
    dcutr_upgraded: bool,
}
