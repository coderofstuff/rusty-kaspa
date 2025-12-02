use crate::swarm::{build_streaming_swarm, Libp2pBehaviour, Libp2pEvent, StreamEvent, StreamRequestId};
use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PathKind, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use libp2p::core::transport::ListenerId;
use libp2p::dcutr;
use libp2p::identify;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Multiaddr;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::SwarmEvent;
use libp2p::{identity, relay, PeerId};
use log::{debug, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::{fs, io, path::Path};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::OnceCell;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::spawn;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use triggered::{Listener, Trigger};

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
#[derive(Clone)]
pub struct Libp2pConnector {
    pub config: Config,
}

impl Default for Libp2pConnector {
    fn default() -> Self {
        Self { config: Config::default() }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::executor::block_on;
    use kaspa_utils_tower::counters::TowerConnectionCounters;
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tempfile::tempdir;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Duration;

    #[test]
    fn libp2p_connect_disabled() {
        let connector = Libp2pConnector::default();
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }

    #[test]
    fn libp2p_connect_enabled_stubbed() {
        let mut cfg = Config::default();
        cfg.mode = crate::Mode::Full;
        let connector = Libp2pConnector::new(cfg);
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::DialFailed(_))));
    }

    #[test]
    fn to_multiaddr_ipv4_and_ipv6() {
        let ipv4 = NetAddress::from_str("192.0.2.1:1234").unwrap();
        let m4 = to_multiaddr(ipv4).unwrap();
        assert_eq!(m4.to_string(), "/ip4/192.0.2.1/tcp/1234");

        let ipv6 = NetAddress::from_str("[2001:db8::1]:5678").unwrap();
        let m6 = to_multiaddr(ipv6).unwrap();
        assert_eq!(m6.to_string(), "/ip6/2001:db8::1/tcp/5678");
    }

    #[test]
    fn identity_ephemeral_and_persisted() {
        let cfg = Config::default();
        let id = Libp2pIdentity::from_config(&cfg).expect("ephemeral identity");
        assert!(id.persisted_path.is_none());
        assert!(!id.peer_id.to_string().is_empty());

        let dir = tempdir().unwrap();
        let key_path = dir.path().join("id.key");
        let mut cfg = Config::default();
        cfg.identity = crate::Identity::Persisted(key_path.clone());
        let id1 = Libp2pIdentity::from_config(&cfg).expect("persisted identity");
        let id2 = Libp2pIdentity::from_config(&cfg).expect("persisted identity reload");
        assert_eq!(id1.peer_id, id2.peer_id);
        assert_eq!(id1.persisted_path.as_deref(), Some(key_path.as_path()));
    }

    #[test]
    fn multiaddr_direct_sets_direct_path() {
        let addr: Multiaddr = "/ip4/192.0.2.1/tcp/1234/p2p/12D3KooWPeer".parse().unwrap();
        let (net, path) = multiaddr_to_metadata(&addr);
        assert!(net.is_some());
        assert!(matches!(path, kaspa_p2p_lib::PathKind::Direct));
        let net = net.unwrap();
        assert_eq!(net.ip.0, std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 1)));
        assert_eq!(net.port, 1234);
    }

    #[test]
    fn multiaddr_relay_sets_relay_path_and_id() {
        let relay = PeerId::random();
        let target = PeerId::random();
        let addr: Multiaddr = format!("/p2p/{relay}/p2p-circuit/p2p/{target}/ip4/10.0.0.1/tcp/4001").parse().unwrap();
        let (net, path) = multiaddr_to_metadata(&addr);
        assert!(net.is_some());
        match path {
            kaspa_p2p_lib::PathKind::Relay { relay_id } => assert_eq!(relay_id.as_deref(), Some(relay.to_string().as_str())),
            other => panic!("expected relay path, got {other:?}"),
        }
    }

    #[test]
    fn multiaddr_unknown_has_unknown_path() {
        let addr: Multiaddr = "/dnsaddr/example.com".parse().unwrap();
        let (_net, path) = multiaddr_to_metadata(&addr);
        assert!(matches!(path, kaspa_p2p_lib::PathKind::Unknown));
    }

    #[test]
    fn multiaddr_missing_ip_is_unknown() {
        let mut addr = Multiaddr::empty();
        addr.push(Protocol::P2pCircuit);
        let (net, path) = multiaddr_to_metadata(&addr);
        assert!(net.is_none());
        assert!(matches!(path, kaspa_p2p_lib::PathKind::Relay { relay_id: None }));
    }

    #[test]
    fn multiaddr_relay_without_tcp_port_defaults_to_zero() {
        let relay = PeerId::random();
        let addr: Multiaddr = format!("/p2p/{relay}/p2p-circuit/ip4/10.0.0.1").parse().unwrap();
        let (net, path) = multiaddr_to_metadata(&addr);

        let net = net.expect("ip should be captured even without tcp port");
        assert_eq!(net.port, 0, "missing tcp component should default port to 0");
        match path {
            kaspa_p2p_lib::PathKind::Relay { relay_id } => assert_eq!(relay_id.as_deref(), Some(relay.to_string().as_str())),
            other => panic!("expected relay path, got {other:?}"),
        }
    }

    #[test]
    fn multiaddr_circuit_without_ip_keeps_relay_bucket() {
        let relay = PeerId::random();
        let addr: Multiaddr = format!("/p2p/{relay}/p2p-circuit").parse().unwrap();
        let (net, path) = multiaddr_to_metadata(&addr);
        assert!(net.is_none());
        match path {
            kaspa_p2p_lib::PathKind::Relay { relay_id } => assert_eq!(relay_id.as_deref(), Some(relay.to_string().as_str())),
            other => panic!("expected relay path, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn full_mode_uses_provider_without_tcp_fallback() {
        let cfg = Config { mode: crate::Mode::Full, ..Config::default() };
        let drops = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(MockProvider::with_responses(VecDeque::from([Err(Libp2pError::DialFailed("fail".into()))]), drops));
        let fallback = Arc::new(CountingFallback::default());
        let connector = Libp2pOutboundConnector::with_provider(cfg, fallback.clone(), provider.clone());
        let handler = test_handler();

        let res = connector.connect("127.0.0.1:16110".to_string(), CoreTransportMetadata::default(), &handler).await;
        assert!(res.is_err());
        assert_eq!(provider.attempts(), 1);
        assert_eq!(fallback.calls(), 0);
    }

    #[tokio::test]
    async fn bridge_mode_falls_back_and_cooldowns() {
        let cfg = Config { mode: crate::Mode::Bridge, ..Config::default() };
        let drops = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(MockProvider::with_responses(VecDeque::from([Err(Libp2pError::DialFailed("fail".into()))]), drops));
        let fallback = Arc::new(CountingFallback::default());
        let connector = Libp2pOutboundConnector::with_provider(cfg, fallback.clone(), provider.clone());
        let handler = test_handler();

        let res1 = connector.connect("127.0.0.1:16110".to_string(), CoreTransportMetadata::default(), &handler).await;
        assert!(res1.is_err());
        assert_eq!(provider.attempts(), 1);
        assert_eq!(fallback.calls(), 1);

        // Second attempt should be in cooldown and skip provider.
        let res2 = connector.connect("127.0.0.1:16110".to_string(), CoreTransportMetadata::default(), &handler).await;
        assert!(res2.is_err());
        assert_eq!(provider.attempts(), 1);
        assert_eq!(fallback.calls(), 2);
    }

    #[tokio::test]
    async fn off_mode_delegates_to_tcp_fallback() {
        let cfg = Config { mode: crate::Mode::Off, ..Config::default() };
        let drops = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(MockProvider::with_responses(VecDeque::new(), drops));
        let fallback = Arc::new(CountingFallback::default());
        let connector = Libp2pOutboundConnector::with_provider(cfg, fallback.clone(), provider.clone());
        let handler = test_handler();

        let res = connector.connect("127.0.0.1:16110".to_string(), CoreTransportMetadata::default(), &handler).await;
        assert!(res.is_err());
        assert_eq!(provider.attempts(), 0);
        assert_eq!(fallback.calls(), 1);
    }

    #[derive(Clone, Default)]
    struct CountingFallback {
        calls: Arc<AtomicUsize>,
    }

    impl CountingFallback {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl OutboundConnector for CountingFallback {
        fn connect<'a>(
            &'a self,
            _address: String,
            _metadata: CoreTransportMetadata,
            _handler: &'a kaspa_p2p_lib::ConnectionHandler,
        ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
            let calls = self.calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ConnectionError::ProtocolError(ProtocolError::Other("fallback".into())))
            })
        }
    }

    struct NoopInitializer;

    #[async_trait]
    impl kaspa_p2p_lib::ConnectionInitializer for NoopInitializer {
        async fn initialize_connection(&self, _router: Arc<Router>) -> Result<(), kaspa_p2p_lib::common::ProtocolError> {
            Ok(())
        }
    }

    fn test_handler() -> kaspa_p2p_lib::ConnectionHandler {
        let hub = kaspa_p2p_lib::Hub::new();
        let adaptor = kaspa_p2p_lib::Adaptor::client_only(
            hub,
            Arc::new(NoopInitializer),
            Arc::new(TowerConnectionCounters::default()),
            Arc::new(kaspa_p2p_lib::DirectMetadataFactory::default()),
            Arc::new(kaspa_p2p_lib::TcpConnector),
        );
        adaptor.connection_handler()
    }

    #[tokio::test]
    async fn swarm_provider_requires_runtime() {
        let cfg = Config::default();
        let id = Libp2pIdentity::from_config(&cfg).expect("identity");
        // Should succeed inside a Tokio runtime.
        let res = SwarmStreamProvider::new(cfg, id);
        assert!(res.is_ok());
    }

    fn test_driver(incoming_capacity: usize) -> (SwarmDriver, mpsc::Receiver<IncomingStream>) {
        let cfg = Config::default();
        let identity = Libp2pIdentity::from_config(&cfg).expect("identity");
        let protocol = default_stream_protocol();
        let swarm = build_streaming_swarm(&identity, &cfg, protocol).expect("swarm");
        let (_cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_BOUND);
        let (incoming_tx, incoming_rx) = mpsc::channel(incoming_capacity);
        let (_shutdown_tx, shutdown) = triggered::trigger();
        let driver = SwarmDriver::new(swarm, cmd_rx, incoming_tx, vec![], vec![], vec![], shutdown);
        (driver, incoming_rx)
    }

    fn make_request_id() -> StreamRequestId {
        DialOpts::unknown_peer_id().address(default_listen_addr()).build().connection_id()
    }

    fn insert_relay_pending(
        driver: &mut SwarmDriver,
        peer_id: PeerId,
    ) -> (StreamRequestId, oneshot::Receiver<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>) {
        let (tx, rx) = oneshot::channel();
        let req_id = make_request_id();
        driver
            .pending_dials
            .insert(req_id, DialRequest { respond_to: tx, started_at: Instant::now(), via: DialVia::Relay { target_peer: peer_id } });
        (req_id, rx)
    }

    #[tokio::test]
    async fn multiple_relay_dials_can_succeed() {
        let (mut driver, _) = test_driver(4);
        let peer = PeerId::random();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (req1, rx1) = insert_relay_pending(&mut driver, peer);
        let (req2, rx2) = insert_relay_pending(&mut driver, peer);

        driver.pending_dials.remove(&req1).map(|pending| {
            let _ = pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
        });
        driver.pending_dials.remove(&req2).map(|pending| {
            let _ = pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
        });

        assert!(driver.pending_dials.is_empty(), "all pending dials should be cleared");
        assert!(rx1.await.unwrap().is_ok());
        assert!(rx2.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn multiple_relay_dials_fail_and_succeed_independently() {
        let (mut driver, _) = test_driver(4);
        let peer = PeerId::random();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (req1, rx1) = insert_relay_pending(&mut driver, peer);
        let (req2, rx2) = insert_relay_pending(&mut driver, peer);

        driver.fail_pending(req1, "first failed");
        driver.pending_dials.remove(&req2).map(|pending| {
            let _ = pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
        });

        assert!(driver.pending_dials.is_empty());
        assert!(rx1.await.unwrap().is_err());
        assert!(rx2.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn multiple_relay_dials_timeout_cleanly() {
        let (mut driver, _) = test_driver(4);
        let peer = PeerId::random();
        let (req1, rx1) = insert_relay_pending(&mut driver, peer);
        let (req2, rx2) = insert_relay_pending(&mut driver, peer);
        if let Some(p) = driver.pending_dials.get_mut(&req1) {
            p.started_at = Instant::now() - (PENDING_DIAL_TIMEOUT + Duration::from_secs(1));
        }
        if let Some(p) = driver.pending_dials.get_mut(&req2) {
            p.started_at = Instant::now() - (PENDING_DIAL_TIMEOUT + Duration::from_secs(2));
        }

        driver.expire_pending_dials("timeout");

        assert!(driver.pending_dials.is_empty());
        assert!(rx1.await.unwrap().is_err());
        assert!(rx2.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn dcutr_handoff_preserves_multiple_relays() {
        let (mut driver, _) = test_driver(4);
        let peer = PeerId::random();
        let (_req1, rx1) = insert_relay_pending(&mut driver, peer);
        let (_req2, rx2) = insert_relay_pending(&mut driver, peer);
        // Simulate DCUtR direct connection after relay dials.
        let endpoint = libp2p::core::ConnectedPoint::Dialer {
            address: default_listen_addr(),
            role_override: libp2p::core::Endpoint::Dialer,
            port_use: libp2p::core::transport::PortUse::Reuse,
        };
        driver
            .handle_event(SwarmEvent::ConnectionEstablished {
                peer_id: peer,
                connection_id: make_request_id(),
                endpoint,
                num_established: std::num::NonZeroU32::new(1).unwrap(),
                concurrent_dial_errors: None,
                established_in: Duration::from_millis(0),
            })
            .await;
        // one pending should have been moved, leaving two tracked entries (one moved to new id, one original)
        assert_eq!(driver.pending_dials.len(), 2);
        // Clear remaining to avoid hanging receivers
        for (_, pending) in driver.pending_dials.drain() {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed("dropped in test".into())));
        }
        assert!(matches!(rx1.await, Ok(Err(_))));
        assert!(matches!(rx2.await, Ok(Err(_))));
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
            return cell.get().map(|p| p.clone());
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
            let parsed_address = NetAddress::from_str(&address);
            let now = Instant::now();

            if let Some(deadline) = cooldowns.lock().await.get(&address).cloned() {
                if deadline > now {
                    metadata.capabilities.libp2p = false;
                    return fallback.connect(address, metadata, handler).await;
                }
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

/// A provider for libp2p streams (dialed or accepted). The real implementation
/// will bridge to the libp2p swarm and return a stream plus transport metadata.
pub trait Libp2pStreamProvider: Send + Sync {
    fn dial<'a>(&'a self, address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>;
    fn dial_multiaddr<'a>(&'a self, address: Multiaddr) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>;
    fn listen<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>>;
    fn reserve<'a>(&'a self, target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>>;
    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        Box::pin(async { Vec::new() })
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
    io::Error::new(io::ErrorKind::Other, err.to_string())
}

/// Translate a NetAddress (ip:port) into a libp2p multiaddr.
pub fn to_multiaddr(address: NetAddress) -> Result<Multiaddr, Libp2pError> {
    let multiaddr: Multiaddr = match address.ip {
        kaspa_utils::networking::IpAddress(std::net::IpAddr::V4(v4)) => {
            format!("/ip4/{}/tcp/{}", v4, address.port).parse::<Multiaddr>()
        }
        kaspa_utils::networking::IpAddress(std::net::IpAddr::V6(v6)) => {
            format!("/ip6/{}/tcp/{}", v6, address.port).parse::<Multiaddr>()
        }
    }
    .map_err(|e: libp2p::multiaddr::Error| Libp2pError::Multiaddr(e.to_string()))?;
    Ok(multiaddr)
}

const COMMAND_CHANNEL_BOUND: usize = 16;
const INCOMING_CHANNEL_BOUND: usize = 32;
const PENDING_DIAL_TIMEOUT: Duration = Duration::from_secs(30);
const PENDING_DIAL_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
const DIALBACK_COOLDOWN: Duration = Duration::from_secs(30);

/// Libp2p stream provider backed by a libp2p swarm.
pub struct SwarmStreamProvider {
    config: Config,
    command_tx: mpsc::Sender<SwarmCommand>,
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    shutdown: Trigger,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SwarmStreamProvider {
    pub fn new(config: Config, identity: Libp2pIdentity) -> Result<Self, Libp2pError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| Libp2pError::ListenFailed("missing tokio runtime".into()))?;
        Self::with_handle(config, identity, handle)
    }

    pub fn with_handle(config: Config, identity: Libp2pIdentity, handle: tokio::runtime::Handle) -> Result<Self, Libp2pError> {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_BOUND);
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_CHANNEL_BOUND);
        let (shutdown, shutdown_listener) = triggered::trigger();
        let protocol = default_stream_protocol();
        // Pass config to build_streaming_swarm to configure AutoNAT
        let swarm = build_streaming_swarm(&identity, &config, protocol.clone())?;

        let listen_multiaddrs = if config.listen_addresses.is_empty() {
            vec![default_listen_addr()]
        } else {
            config
                .listen_addresses
                .iter()
                .filter_map(|addr| match to_multiaddr(NetAddress::new((*addr).ip().into(), addr.port())) {
                    Ok(ma) => Some(ma),
                    Err(err) => {
                        warn!("invalid libp2p listen address {}: {err}", addr);
                        None
                    }
                })
                .collect()
        };
        let mut external_multiaddrs = parse_multiaddrs(&config.external_multiaddrs)?;
        external_multiaddrs.extend(config.advertise_addresses.iter().filter_map(|addr| {
            match to_multiaddr(NetAddress::new((*addr).ip().into(), addr.port())) {
                Ok(ma) => Some(ma),
                Err(err) => {
                    warn!("invalid libp2p advertise address {}: {err}", addr);
                    None
                }
            }
        }));
        let reservations = parse_reservation_targets(&config.reservations)?;
        let task = handle.spawn(
            SwarmDriver::new(swarm, command_rx, incoming_tx, listen_multiaddrs, external_multiaddrs, reservations, shutdown_listener)
                .run(),
        );

        Ok(Self { config, command_tx, incoming: Mutex::new(incoming_rx), shutdown, task: Mutex::new(Some(task)) })
    }

    async fn ensure_listening(&self) -> Result<(), Libp2pError> {
        let (tx, rx) = oneshot::channel();
        info!("libp2p ensure listening on configured addresses");
        self.command_tx
            .send(SwarmCommand::EnsureListening { respond_to: tx })
            .await
            .map_err(|_| Libp2pError::ListenFailed("libp2p driver stopped".into()))?;

        rx.await.map_err(|_| Libp2pError::ListenFailed("libp2p driver stopped".into()))?
    }
}

impl Libp2pStreamProvider for SwarmStreamProvider {
    fn dial<'a>(&'a self, address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let multiaddr = to_multiaddr(address)?;
            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Dial { address: multiaddr, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await
                .unwrap_or_else(|_| Err(Libp2pError::DialFailed("libp2p dial cancelled".into())))
                .map(|(metadata, _, stream)| (metadata, stream))
        })
    }

    fn dial_multiaddr<'a>(
        &'a self,
        multiaddr: Multiaddr,
    ) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Dial { address: multiaddr, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await
                .unwrap_or_else(|_| Err(Libp2pError::DialFailed("libp2p dial cancelled".into())))
                .map(|(metadata, _, stream)| (metadata, stream))
        })
    }

    fn listen<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let incoming = &self.incoming;
        let provider = self;
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            provider.ensure_listening().await?;

            let mut rx = incoming.lock().await;
            match rx.recv().await {
                Some(incoming) => {
                    let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
                    Ok((incoming.metadata, incoming.direction, closer, incoming.stream))
                }
                None => Err(Libp2pError::ListenFailed("libp2p incoming channel closed".into())),
            }
        })
    }

    fn reserve<'a>(&'a self, target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Reserve { target, respond_to })
                .await
                .map_err(|_| Libp2pError::ReservationFailed("libp2p driver stopped".into()))?;

            let listener = rx.await.unwrap_or_else(|_| Err(Libp2pError::ReservationFailed("libp2p reservation cancelled".into())))?;
            let release_tx = tx.clone();
            Ok(ReservationHandle::new(async move {
                let (ack_tx, ack_rx) = oneshot::channel();
                if release_tx.send(SwarmCommand::ReleaseReservation { listener_id: listener, respond_to: ack_tx }).await.is_ok() {
                    let _ = ack_rx.await;
                }
            }))
        })
    }

    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        let tx = self.command_tx.clone();
        Box::pin(async move {
            let (respond_to, rx) = oneshot::channel();
            if tx.send(SwarmCommand::PeersSnapshot { respond_to }).await.is_err() {
                return Vec::new();
            }
            rx.await.unwrap_or_default()
        })
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        let trigger = self.shutdown.clone();
        let task = &self.task;
        Box::pin(async move {
            trigger.trigger();
            if let Some(handle) = task.lock().await.take() {
                let _ = handle.await;
            }
        })
    }
}

struct IncomingStream {
    metadata: TransportMetadata,
    direction: StreamDirection,
    stream: BoxedLibp2pStream,
}

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

enum SwarmCommand {
    Dial {
        address: Multiaddr,
        respond_to: oneshot::Sender<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>,
    },
    EnsureListening { respond_to: oneshot::Sender<Result<(), Libp2pError>> },
    Reserve { target: Multiaddr, respond_to: oneshot::Sender<Result<ListenerId, Libp2pError>> },
    ReleaseReservation { listener_id: ListenerId, respond_to: oneshot::Sender<()> },
    PeersSnapshot { respond_to: oneshot::Sender<Vec<PeerSnapshot>> },
}

struct DialRequest {
    respond_to: oneshot::Sender<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>,
    started_at: Instant,
    via: DialVia,
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
    dialback_cooldowns: HashMap<PeerId, Instant>,
    listen_addrs: Vec<Multiaddr>,
    external_addrs: Vec<Multiaddr>,
    peer_states: HashMap<PeerId, PeerState>,
    reservation_listeners: HashSet<ListenerId>,
    active_relay: Option<RelayInfo>,
    listening: bool,
    shutdown: Listener,
    connections: HashMap<StreamRequestId, ConnectionEntry>,
}

impl SwarmDriver {
    fn bootstrap(&mut self) {
        info!("libp2p bootstrap: adding {} external addresses", self.external_addrs.len());
        for addr in self.external_addrs.clone() {
            info!("libp2p bootstrap: registering external address: {}", addr);
            self.swarm.add_external_address(addr);
        }
        // Log the swarm's external addresses after adding
        let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
        info!("libp2p bootstrap: swarm now has {} external addresses: {:?}", external_addrs.len(), external_addrs);
        let _ = self.start_listening();
    }

    fn new(
        swarm: libp2p::Swarm<Libp2pBehaviour>,
        command_rx: mpsc::Receiver<SwarmCommand>,
        incoming_tx: mpsc::Sender<IncomingStream>,
        listen_addrs: Vec<Multiaddr>,
        external_addrs: Vec<Multiaddr>,
        reservations: Vec<ReservationTarget>,
        shutdown: Listener,
    ) -> Self {
        let local_peer_id = *swarm.local_peer_id();
        let active_relay = reservations.into_iter().find_map(|r| relay_info_from_multiaddr(&r.multiaddr, local_peer_id));

        Self {
            swarm,
            command_rx,
            incoming_tx,
            pending_dials: HashMap::new(),
            dialback_cooldowns: HashMap::new(),
            listen_addrs,
            external_addrs,
            peer_states: HashMap::new(),
            reservation_listeners: HashSet::new(),
            active_relay,
            listening: false,
            shutdown,
            connections: HashMap::new(),
        }
    }

    async fn run(mut self) {
        self.bootstrap();
        let mut cleanup = interval(PENDING_DIAL_CLEANUP_INTERVAL);
        cleanup.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.shutdown.clone() => {
                    debug!("libp2p swarm driver received shutdown signal");
                    break;
                }
                _ = cleanup.tick() => {
                    self.expire_pending_dials("dial timed out");
                }
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => break,
                    }
                }
                event = self.swarm.select_next_some() => {
                    self.handle_event(event).await;
                }
            }
        }

        for listener in self.reservation_listeners.drain() {
            let _ = self.swarm.remove_listener(listener);
        }
        for (_, pending) in self.pending_dials.drain() {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed("libp2p driver stopped".into())));
        }
    }

    fn expire_pending_dials(&mut self, reason: &str) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending_dials
            .iter()
            .filter(|(_, pending)| now.saturating_duration_since(pending.started_at) >= PENDING_DIAL_TIMEOUT)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.fail_pending(id, reason);
        }
    }

    fn fail_pending(&mut self, request_id: StreamRequestId, err: impl ToString) {
        if let Some(pending) = self.pending_dials.remove(&request_id) {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
        }
    }

    fn enqueue_incoming(&self, incoming: IncomingStream) {
        let direction = incoming.direction;
        if let Err(err) = self.incoming_tx.try_send(incoming) {
            match err {
                TrySendError::Full(_) => warn!("libp2p_bridge: dropping {:?} stream because channel is full", direction),
                TrySendError::Closed(_) => warn!("libp2p_bridge: dropping {:?} stream because receiver is closed", direction),
            }
        }
    }

    fn take_pending_relay_by_peer(&mut self, peer_id: &PeerId) -> Option<(StreamRequestId, DialRequest)> {
        let candidate = self
            .pending_dials
            .iter()
            .filter(|(_, pending)| matches!(pending.via, DialVia::Relay { target_peer } if target_peer == *peer_id))
            .min_by_key(|(_, pending)| pending.started_at)
            .map(|(id, _)| *id)?;
        // Use FIFO ordering when multiple relay dials are outstanding for the same peer.
        self.pending_dials.remove(&candidate).map(|req| (candidate, req))
    }

    async fn handle_command(&mut self, command: SwarmCommand) {
        match command {
            SwarmCommand::Dial { address, respond_to } => {
                info!("libp2p dial request to {address}");

                // For relay addresses, track by target peer so DCUtR success can resolve the dial
                let is_relay = addr_uses_relay(&address);
                let target_peer = if is_relay { extract_circuit_target_peer(&address) } else { None };

                let dial_opts = DialOpts::unknown_peer_id().address(address).build();
                let request_id = dial_opts.connection_id();
                let started_at = Instant::now();
                let via = target_peer.map_or(DialVia::Direct, |peer| DialVia::Relay { target_peer: peer });
                match self.swarm.dial(dial_opts) {
                    Ok(()) => {
                        self.pending_dials.insert(request_id, DialRequest { respond_to, started_at, via });
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
                    }
                }
            }
            SwarmCommand::EnsureListening { respond_to } => {
                let _ = respond_to.send(self.start_listening());
            }
            SwarmCommand::Reserve { mut target, respond_to } => {
                if self.active_relay.is_none() {
                    if let Some(info) = relay_info_from_multiaddr(&target, *self.swarm.local_peer_id()) {
                        self.active_relay = Some(info);
                    }
                }
                if !target.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
                    target.push(Protocol::P2pCircuit);
                }
                match self.swarm.listen_on(target) {
                    Ok(listener) => {
                        self.reservation_listeners.insert(listener);
                        let _ = respond_to.send(Ok(listener));
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(Libp2pError::ReservationFailed(err.to_string())));
                    }
                }
            }
            SwarmCommand::ReleaseReservation { listener_id, respond_to } => {
                self.reservation_listeners.remove(&listener_id);
                let _ = self.swarm.remove_listener(listener_id);
                let _ = respond_to.send(());
            }
            SwarmCommand::PeersSnapshot { respond_to } => {
                let _ = respond_to.send(self.peers_snapshot());
            }
        }
    }

    async fn handle_event(&mut self, event: SwarmEvent<Libp2pEvent>) {
        match event {
            SwarmEvent::Behaviour(Libp2pEvent::Stream(event)) => self.handle_stream_event(event).await,
            SwarmEvent::Behaviour(Libp2pEvent::Ping(event)) => {
                let _ = event;
            }
            SwarmEvent::Behaviour(Libp2pEvent::Identify(event)) => match event {
                identify::Event::Received { peer_id, ref info, .. } => {
                    let supports_dcutr = info.protocols.iter().any(|p| p.as_ref() == dcutr::PROTOCOL_NAME.as_ref());
                    info!(
                        target: "libp2p_identify",
                        "identify received from {peer_id}: protocols={:?} (dcutr={supports_dcutr}) listen_addrs={:?}",
                        info.protocols,
                        info.listen_addrs
                    );
                    // Only add observed address if it's a valid TCP address (has IP+TCP).
                    // Relay circuit peers may report observed addresses like `/p2p/<peer_id>` without
                    // any IP information, which are useless for DCUtR hole punching and pollute
                    // the external address set.
                    if !addr_uses_relay(&info.observed_addr) && is_tcp_dialable(&info.observed_addr) {
                        self.swarm.add_external_address(info.observed_addr.clone());
                    }
                    // NOTE: We intentionally do NOT add info.listen_addrs as external addresses.
                    // Those are the REMOTE peer's addresses, not ours. Adding them would pollute
                    // our external address set with unreachable addresses, breaking DCUtR hole punch.
                    self.mark_dcutr_support(peer_id, supports_dcutr);
                    self.maybe_request_dialback(peer_id);
                }
                identify::Event::Pushed { peer_id, ref info, .. } => {
                    let supports_dcutr = info.protocols.iter().any(|p| p.as_ref() == dcutr::PROTOCOL_NAME.as_ref());
                    info!(
                        target: "libp2p_identify",
                        "identify pushed to {peer_id}: protocols={:?} (dcutr={supports_dcutr}) listen_addrs={:?}",
                        info.protocols,
                        info.listen_addrs
                    );
                }
                identify::Event::Sent { peer_id, .. } => {
                    info!(
                        target: "libp2p_identify",
                        "identify sent to {peer_id}; expecting advertisement of {}",
                        dcutr::PROTOCOL_NAME
                    );
                }
                other => debug!("libp2p identify event: {:?}", other),
            },
            SwarmEvent::Behaviour(Libp2pEvent::RelayClient(event)) =>
            {
                #[allow(unreachable_patterns)]
                match event {
                    relay::client::Event::ReservationReqAccepted { relay_peer_id, renewal, .. } => {
                        info!("libp2p reservation accepted by {relay_peer_id}, renewal={renewal}");
                    }
                    relay::client::Event::OutboundCircuitEstablished { relay_peer_id, .. } => {
                        info!("libp2p outbound circuit established via {relay_peer_id}");
                    }
                    relay::client::Event::InboundCircuitEstablished { src_peer_id, .. } => {
                        info!("libp2p inbound circuit established from {src_peer_id}");
                        self.mark_relay_path(src_peer_id);
                        self.maybe_request_dialback(src_peer_id);
                    }
                    _ => {}
                }
            }
            SwarmEvent::Behaviour(Libp2pEvent::RelayServer(event)) => {
                debug!("libp2p relay server event: {:?}", event);
            }
            SwarmEvent::Behaviour(Libp2pEvent::Dcutr(event)) => {
                // Enhanced DCUtR logging to diagnose NoAddresses issue
                let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
                info!("libp2p dcutr event: {:?} (swarm has {} external addrs: {:?})", event, external_addrs.len(), external_addrs);
            }
            SwarmEvent::Behaviour(Libp2pEvent::Autonat(event)) => {
                debug!("libp2p autonat event: {:?}", event);
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("libp2p listening on {address}");
                if self.active_relay.is_none() {
                    if let Some(info) = relay_info_from_multiaddr(&address, *self.swarm.local_peer_id()) {
                        self.active_relay = Some(info);
                    }
                }
                self.listening = true;
            }
            SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, .. } => {
                debug!("libp2p connection established with {peer_id} on {connection_id:?}");
                self.track_established(peer_id, &endpoint);

                // For DCUtR direct connections spawned from relay dials, transfer the earliest
                // pending relay dial for this peer onto the new connection_id.
                let mut had_pending_relay = false;
                if !endpoint_uses_relay(&endpoint) {
                    if let Some((_old_req, pending)) = self.take_pending_relay_by_peer(&peer_id) {
                        info!("libp2p DCUtR success: direct connection to {peer_id} resolves pending relay dial");
                        self.pending_dials.insert(connection_id, pending);
                        had_pending_relay = true;
                    }
                }

                if endpoint.is_dialer() {
                    info!("libp2p initiating stream to {peer_id} (as dialer)");
                    self.request_stream_bridge(peer_id, connection_id);
                } else if had_pending_relay {
                    // DCUtR succeeded but we're the listener - still need to initiate stream
                    // because we had a pending outbound dial that needs to be resolved
                    info!("libp2p DCUtR: initiating stream to {peer_id} (as listener with pending dial)");
                    self.request_stream_bridge(peer_id, connection_id);
                } else {
                    debug!("libp2p waiting for stream from {peer_id} (as listener)");
                    // If we are only a listener on a relayed connection and the peer supports DCUtR,
                    // initiate a bidirectional dial-back via the active relay so we become a dialer too.
                    self.maybe_request_dialback(peer_id);
                }
                self.record_connection(connection_id, peer_id, &endpoint, had_pending_relay);
            }
            SwarmEvent::ConnectionClosed { peer_id, connection_id, endpoint, .. } => {
                self.fail_pending(connection_id, "connection closed before stream");
                self.track_closed(peer_id, &endpoint);
                self.connections.remove(&connection_id);
            }
            SwarmEvent::OutgoingConnectionError { connection_id, error, .. } => {
                self.fail_pending(connection_id, error.to_string());
                self.connections.remove(&connection_id);
            }
            _ => {}
        }
    }

    fn start_listening(&mut self) -> Result<(), Libp2pError> {
        if self.listening {
            return Ok(());
        }

        let addrs = if self.listen_addrs.is_empty() { vec![default_listen_addr()] } else { self.listen_addrs.clone() };
        info!("libp2p starting listen on {:?}", addrs);

        for addr in addrs {
            if let Err(err) = self.swarm.listen_on(addr) {
                warn!("libp2p failed to listen: {err}");
                return Err(Libp2pError::ListenFailed(err.to_string()));
            }
        }

        self.listening = true;
        Ok(())
    }

    fn track_established(&mut self, peer_id: PeerId, endpoint: &libp2p::core::ConnectedPoint) {
        let state = self.peer_states.entry(peer_id).or_default();
        if matches!(endpoint, libp2p::core::ConnectedPoint::Dialer { .. }) {
            state.outgoing = state.outgoing.saturating_add(1);
        }
        if endpoint_uses_relay(endpoint) {
            state.connected_via_relay = true;
            debug!("libp2p track_established: peer {} connected via relay", peer_id);
        } else {
            debug!("libp2p track_established: peer {} connected DIRECTLY (no relay)", peer_id);
        }
    }

    fn track_closed(&mut self, peer_id: PeerId, endpoint: &libp2p::core::ConnectedPoint) {
        if let Some(state) = self.peer_states.get_mut(&peer_id) {
            if matches!(endpoint, libp2p::core::ConnectedPoint::Dialer { .. }) && state.outgoing > 0 {
                state.outgoing -= 1;
            }
            if endpoint_uses_relay(endpoint) {
                state.connected_via_relay = false;
            }
        }
    }

    fn record_connection(
        &mut self,
        connection_id: StreamRequestId,
        peer_id: PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
        dcutr_upgraded: bool,
    ) {
        let path = if endpoint_uses_relay(endpoint) { PathKind::Relay { relay_id: None } } else { PathKind::Direct };
        let relay_id = match endpoint {
            libp2p::core::ConnectedPoint::Dialer { address, .. } => relay_id_from_multiaddr(address),
            libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => relay_id_from_multiaddr(send_back_addr),
        };
        let outbound = endpoint.is_dialer();
        self.connections
            .insert(connection_id, ConnectionEntry { peer_id, path, relay_id, outbound, since: Instant::now(), dcutr_upgraded });
    }

    async fn handle_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::Inbound { peer_id, _connection_id: connection_id, endpoint, stream } => {
                if matches!(endpoint, libp2p::core::ConnectedPoint::Dialer { .. }) {
                    debug!("libp2p_bridge: skipping inbound stream on dialed connection to {peer_id} to avoid duplicate bridging");
                    return;
                }
                info!(
                    "libp2p_bridge: StreamEvent::Inbound peer={} endpoint={:?}",
                    peer_id, endpoint
                );
                let mut metadata = metadata_from_endpoint(&peer_id, &endpoint);
                // If endpoint-based path detection returned Unknown, fall back to our
                // connection records which track whether a connection uses a relay circuit.
                // This is needed because the send_back_addr for relay circuit listeners
                // may not contain the P2pCircuit protocol marker.
                if matches!(metadata.path, kaspa_p2p_lib::PathKind::Unknown) {
                    if let Some(conn) = self.connections.get(&connection_id) {
                        metadata.path = conn.path.clone();
                    }
                }
                info!("libp2p_bridge: inbound stream from {peer_id} over {:?}, handing to Kaspa", metadata.path);
                let incoming = IncomingStream { metadata, direction: StreamDirection::Inbound, stream: Box::new(stream.compat()) };
                self.enqueue_incoming(incoming);
            }
            StreamEvent::Outbound { peer_id, request_id, endpoint, stream, .. } => {
                let metadata = metadata_from_endpoint(&peer_id, &endpoint);
                let direction = match &endpoint {
                    libp2p::core::ConnectedPoint::Dialer { role_override, .. }
                        if matches!(role_override, libp2p::core::Endpoint::Listener) =>
                    {
                        info!("libp2p_bridge: DCUtR role_override detected, treating outbound stream as inbound (h2 server) for {peer_id}");
                        StreamDirection::Inbound
                    }
                    _ => StreamDirection::Outbound,
                };
                info!(
                    "libp2p_bridge: StreamEvent::Outbound peer={} req_id={:?} endpoint={:?} direction={:?}",
                    peer_id, request_id, endpoint, direction
                );
                if let Some(pending) = self.pending_dials.remove(&request_id) {
                    let _ = pending.respond_to.send(Ok((metadata, direction, Box::new(stream.compat()))));
                } else {
                    info!(
                        "libp2p_bridge: outbound stream with no pending dial (req {request_id:?}) from {peer_id}; handing to Kaspa (direction={:?})",
                        direction
                    );
                    let incoming = IncomingStream { metadata, direction, stream: Box::new(stream.compat()) };
                    let _ = self.incoming_tx.send(incoming).await;
                }
            }
        }
    }

    fn request_stream_bridge(&mut self, peer_id: PeerId, connection_id: StreamRequestId) {
        // Check if there's already a pending dial for this connection (e.g., from relay dial transfer)
        // If so, don't create a new channel - the existing one will be resolved via StreamEvent::Outbound
        if self.pending_dials.contains_key(&connection_id) {
            debug!("libp2p request_stream_bridge: reusing existing pending dial for {connection_id:?}");
            self.swarm.behaviour_mut().streams.request_stream(peer_id, connection_id, connection_id);
            return;
        }
        info!(
            "libp2p_bridge: request_stream_bridge peer={} conn_id={:?} (requesting substream)",
            peer_id, connection_id
        );

        let (respond_to, rx) = oneshot::channel();
        self.pending_dials.insert(connection_id, DialRequest { respond_to, started_at: Instant::now(), via: DialVia::Direct });
        self.swarm.behaviour_mut().streams.request_stream(peer_id, connection_id, connection_id);

        let tx = self.incoming_tx.clone();
        spawn(async move {
            if let Ok(Ok((metadata, direction, stream))) = rx.await {
                info!(
                    "libp2p_bridge: established stream with {peer_id} (req {connection_id:?}); handing to Kaspa (direction={:?})",
                    direction
                );
                let incoming = IncomingStream { metadata, direction, stream };
                if let Err(err) = tx.try_send(incoming) {
                    match err {
                        TrySendError::Full(_) => {
                            warn!("libp2p_bridge: dropping outbound stream for {peer_id} because channel is full")
                        }
                        TrySendError::Closed(_) => {
                            warn!("libp2p_bridge: dropping outbound stream for {peer_id} because receiver is closed")
                        }
                    }
                }
            }
        });
    }

    fn mark_dcutr_support(&mut self, peer_id: PeerId, supports: bool) {
        if supports {
            self.peer_states.entry(peer_id).or_default().supports_dcutr = true;
        }
    }

    fn mark_relay_path(&mut self, peer_id: PeerId) {
        self.peer_states.entry(peer_id).or_default().connected_via_relay = true;
    }

    fn maybe_request_dialback(&mut self, peer_id: PeerId) {
        let Some(state) = self.peer_states.get(&peer_id) else {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no peer state");
            return;
        };
        if !state.supports_dcutr {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: peer does not support dcutr");
            return;
        }
        if !state.connected_via_relay {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: not connected via relay");
            return;
        }
        if state.outgoing > 0 {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: already have outgoing connection");
            return;
        }
        let now = Instant::now();
        if let Some(next_allowed) = self.dialback_cooldowns.get(&peer_id) {
            if *next_allowed > now {
                debug!("libp2p dcutr: skipping dial-back to {peer_id}: cooldown until {:?}", *next_allowed);
                return;
            }
        }

        let Some(relay) = &self.active_relay else {
            debug!("libp2p dcutr: no active relay available for dial-back to {peer_id}");
            return;
        };

        let mut circuit_addr = relay.circuit_base.clone();
        strip_peer_suffix(&mut circuit_addr, *self.swarm.local_peer_id());
        if !circuit_addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
            circuit_addr.push(Protocol::P2pCircuit);
        }
        circuit_addr.push(Protocol::P2p(peer_id));

        let opts = DialOpts::peer_id(peer_id)
            .addresses(vec![circuit_addr.clone()])
            .condition(PeerCondition::Always)
            .extend_addresses_through_behaviour()
            .build();

        match self.swarm.dial(opts) {
            Ok(()) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                info!("libp2p dcutr: initiated dial-back to {peer_id} via relay {}", relay.relay_peer);
            }
            Err(err) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                warn!("libp2p dcutr: failed to dial {peer_id} via relay {}: {err}", relay.relay_peer);
            }
        }
    }

    fn peers_snapshot(&self) -> Vec<PeerSnapshot> {
        let now = Instant::now();
        self.connections
            .values()
            .map(|entry| PeerSnapshot {
                peer_id: entry.peer_id.to_string(),
                path: match &entry.path {
                    PathKind::Direct => "direct".to_string(),
                    PathKind::Relay { .. } => "relay".to_string(),
                    PathKind::Unknown => "unknown".to_string(),
                },
                relay_id: entry.relay_id.clone(),
                direction: if entry.outbound { "outbound".to_string() } else { "inbound".to_string() },
                duration_ms: now.saturating_duration_since(entry.since).as_millis(),
                libp2p: true,
                dcutr_upgraded: entry.dcutr_upgraded,
            })
            .collect()
    }
}

fn metadata_from_endpoint(peer_id: &PeerId, endpoint: &libp2p::core::ConnectedPoint) -> TransportMetadata {
    let mut md = TransportMetadata::default();
    md.capabilities.libp2p = true;
    md.libp2p_peer_id = Some(peer_id.to_string());
    let (addr, path) = connected_point_to_metadata(endpoint);
    md.path = path;
    md.reported_ip = addr.map(|a| a.ip);

    md
}

fn connected_point_to_metadata(endpoint: &libp2p::core::ConnectedPoint) -> (Option<NetAddress>, kaspa_p2p_lib::PathKind) {
    match endpoint {
        libp2p::core::ConnectedPoint::Dialer { address, .. } => multiaddr_to_metadata(address),
        libp2p::core::ConnectedPoint::Listener { local_addr, send_back_addr } => {
            // For relay circuit listeners, send_back_addr may be just "/p2p/<peer_id>" without
            // the circuit marker. Check local_addr for circuit information since it contains
            // the full relay path (e.g., "/ip4/.../p2p-circuit").
            let (_, local_path) = multiaddr_to_metadata(local_addr);
            if matches!(local_path, kaspa_p2p_lib::PathKind::Relay { .. }) {
                // local_addr has circuit info; use its path. Address from send_back_addr is
                // typically unusable for relay circuits (just peer ID, no IP).
                let (addr, _) = multiaddr_to_metadata(send_back_addr);
                (addr, local_path)
            } else {
                // No circuit in local_addr; use send_back_addr as before
                multiaddr_to_metadata(send_back_addr)
            }
        }
    }
}

/// Extract networking metadata (NetAddress + path info) from a multiaddr.
pub fn multiaddr_to_metadata(address: &Multiaddr) -> (Option<NetAddress>, kaspa_p2p_lib::PathKind) {
    let mut ip: Option<std::net::IpAddr> = None;
    let mut port: Option<u16> = None;
    let mut relay_id: Option<String> = None;
    let mut saw_circuit = false;
    let mut last_peer_id: Option<String> = None;

    for component in address.iter() {
        match component {
            Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
            Protocol::Tcp(p) => port = Some(p),
            Protocol::P2p(hash) => {
                let pid = hash.to_string();
                if saw_circuit && relay_id.is_none() {
                    relay_id = last_peer_id.take();
                }
                last_peer_id = Some(pid);
            }
            Protocol::P2pCircuit => {
                saw_circuit = true;
                relay_id = last_peer_id.take();
            }
            _ => {}
        }
    }

    let net = ip.map(|i| NetAddress::new(i.into(), port.unwrap_or(0)));
    let path = if saw_circuit {
        kaspa_p2p_lib::PathKind::Relay { relay_id }
    } else if net.is_some() {
        kaspa_p2p_lib::PathKind::Direct
    } else {
        kaspa_p2p_lib::PathKind::Unknown
    };

    (net, path)
}

fn parse_multiaddrs(addrs: &[String]) -> Result<Vec<Multiaddr>, Libp2pError> {
    addrs.iter().map(|raw| Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))).collect()
}

fn parse_reservation_targets(reservations: &[String]) -> Result<Vec<ReservationTarget>, Libp2pError> {
    reservations
        .iter()
        .map(|raw| {
            let multiaddr: Multiaddr = Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))?;
            let peer_id = multiaddr
                .iter()
                .find_map(|p| if let Protocol::P2p(peer_id) = p { Some(peer_id) } else { None })
                .ok_or_else(|| Libp2pError::Multiaddr("reservation multiaddr missing peer id".into()))?;
            Ok(ReservationTarget { multiaddr, peer_id })
        })
        .collect()
}

fn default_stream_protocol() -> libp2p::StreamProtocol {
    libp2p::StreamProtocol::new("/kaspad/transport/1.0.0")
}

fn default_listen_addr() -> Multiaddr {
    Multiaddr::from_str("/ip4/0.0.0.0/tcp/0").expect("static multiaddr should parse")
}

/// Whether a stream originated from a local outbound dial or from a remote inbound request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDirection {
    Inbound,
    Outbound,
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

fn extract_relay_peer(addr: &Multiaddr) -> Option<PeerId> {
    let components: Vec<_> = addr.iter().collect();
    for window in components.windows(2) {
        if let [Protocol::P2p(peer), Protocol::P2pCircuit] = window {
            return Some(peer.clone());
        }
    }
    None
}

fn relay_id_from_multiaddr(addr: &Multiaddr) -> Option<String> {
    extract_relay_peer(addr).map(|p| p.to_string())
}

/// Extracts the target peer from a relay circuit address.
/// For `/ip4/.../p2p/RELAY/p2p-circuit/p2p/TARGET`, returns TARGET.
fn extract_circuit_target_peer(addr: &Multiaddr) -> Option<PeerId> {
    let components: Vec<_> = addr.iter().collect();
    // Find p2p-circuit, then look for the next P2p component
    let mut after_circuit = false;
    for p in components {
        if matches!(p, Protocol::P2pCircuit) {
            after_circuit = true;
        } else if after_circuit {
            if let Protocol::P2p(peer_id) = p {
                return Some(peer_id);
            }
        }
    }
    None
}

fn relay_info_from_multiaddr(addr: &Multiaddr, local_peer_id: PeerId) -> Option<RelayInfo> {
    let relay_peer = extract_relay_peer(addr)?;
    let mut circuit_base = addr.clone();
    if !circuit_base.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
        circuit_base.push(Protocol::P2pCircuit);
    }
    strip_peer_suffix(&mut circuit_base, local_peer_id);
    Some(RelayInfo { relay_peer, circuit_base })
}

fn strip_peer_suffix(addr: &mut Multiaddr, peer_id: PeerId) {
    if let Some(Protocol::P2p(last)) = addr.iter().last() {
        if last == peer_id {
            addr.pop();
        }
    }
}

fn addr_uses_relay(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| matches!(p, Protocol::P2pCircuit))
}

fn is_tcp_dialable(addr: &Multiaddr) -> bool {
    let mut has_ip = false;
    let mut has_tcp = false;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(_) | Protocol::Ip6(_) => has_ip = true,
            Protocol::Tcp(_) => has_tcp = true,
            Protocol::P2pCircuit => return false,
            _ => {}
        }
    }
    has_ip && has_tcp
}

fn endpoint_uses_relay(endpoint: &libp2p::core::ConnectedPoint) -> bool {
    let addr = match endpoint {
        libp2p::core::ConnectedPoint::Dialer { address, .. } => address,
        libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
    };
    addr.iter().any(|p| matches!(p, Protocol::P2pCircuit))
}

#[cfg(test)]
#[allow(dead_code)]
struct MockProvider {
    responses: std::sync::Mutex<std::collections::VecDeque<Result<(), Libp2pError>>>,
    attempts: std::sync::atomic::AtomicUsize,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
fn make_test_stream(drops: Arc<std::sync::atomic::AtomicUsize>) -> BoxedLibp2pStream {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{duplex, ReadBuf};
    use tokio::io::{AsyncRead, AsyncWrite};

    struct DropStream {
        inner: tokio::io::DuplexStream,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for DropStream {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl AsyncRead for DropStream {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DropStream {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    let (client, _server) = duplex(64);
    Box::new(DropStream { inner: client, drops })
}

#[cfg(test)]
#[allow(dead_code)]
impl MockProvider {
    fn with_responses(
        responses: std::collections::VecDeque<Result<(), Libp2pError>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self { responses: std::sync::Mutex::new(responses), attempts: std::sync::atomic::AtomicUsize::new(0), drops }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl Libp2pStreamProvider for MockProvider {
    fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or_else(|| Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| (TransportMetadata::default(), make_test_stream(self.drops.clone())))
        })
    }

    fn dial_multiaddr<'a>(
        &'a self,
        _address: Multiaddr,
    ) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or_else(|| Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| (TransportMetadata::default(), make_test_stream(self.drops.clone())))
        })
    }

    fn listen<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let drops = self.drops.clone();
        Box::pin(async move {
            let stream = make_test_stream(drops);
            let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
            Ok((TransportMetadata::default(), StreamDirection::Inbound, closer, stream))
        })
    }

    fn reserve<'a>(&'a self, _target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or_else(|| Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| ReservationHandle::noop())
        })
    }

    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        Box::pin(async { Vec::new() })
    }
}
