use super::test_support::{MockProvider, make_test_stream};
use super::*;
use async_trait::async_trait;
use futures::executor::block_on;
use kaspa_utils_tower::counters::TowerConnectionCounters;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::tempdir;
use tokio::sync::{mpsc, oneshot, watch};
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
    let cfg = Config { mode: crate::Mode::Full, ..Config::default() };
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
fn endpoint_uses_relay_checks_local_addr_for_listener() {
    let relay = PeerId::random();
    let peer = PeerId::random();
    let local_addr: Multiaddr = format!("/ip4/10.0.0.1/tcp/16112/p2p/{relay}/p2p-circuit").parse().unwrap();
    let send_back_addr: Multiaddr = format!("/p2p/{peer}").parse().unwrap();
    let endpoint = libp2p::core::ConnectedPoint::Listener { local_addr, send_back_addr };
    assert!(endpoint_uses_relay(&endpoint));
}

#[test]
fn identity_ephemeral_and_persisted() {
    let cfg = Config::default();
    let id = Libp2pIdentity::from_config(&cfg).expect("ephemeral identity");
    assert!(id.persisted_path.is_none());
    assert!(!id.peer_id.to_string().is_empty());

    let dir = tempdir().unwrap();
    let key_path = dir.path().join("id.key");
    let cfg = Config { identity: crate::Identity::Persisted(key_path.clone()), ..Config::default() };
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
async fn bridge_mode_multiaddr_failure_does_not_fallback_to_tcp() {
    let cfg = Config { mode: crate::Mode::Bridge, ..Config::default() };
    let drops = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MockProvider::with_responses(VecDeque::from([Err(Libp2pError::DialFailed("fail".into()))]), drops));
    let fallback = Arc::new(CountingFallback::default());
    let connector = Libp2pOutboundConnector::with_provider(cfg, fallback.clone(), provider.clone());
    let handler = test_handler();

    let relay = PeerId::random();
    let target = PeerId::random();
    let multiaddr = format!("/ip4/203.0.113.9/tcp/16112/p2p/{relay}/p2p-circuit/p2p/{target}");
    let res = connector.connect(multiaddr, CoreTransportMetadata::default(), &handler).await;
    assert!(res.is_err());
    assert_eq!(provider.attempts(), 1);
    assert_eq!(fallback.calls(), 0);
}

#[tokio::test]
async fn resolve_relay_multiaddr_probes_missing_relay_peer_v4() {
    let relay_peer = PeerId::random();
    let target_peer = PeerId::random();
    let drops = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MockProvider::with_probe_peer(VecDeque::new(), drops, relay_peer));
    let addr: Multiaddr = format!("/ip4/203.0.113.9/tcp/16112/p2p-circuit/p2p/{target_peer}").parse().unwrap();

    let resolved = Libp2pOutboundConnector::resolve_relay_multiaddr(provider.clone(), addr).await.expect("resolve should succeed");
    let expected_probe: Multiaddr = "/ip4/203.0.113.9/tcp/16112".parse().unwrap();
    assert_eq!(provider.last_probe(), Some(expected_probe));

    let expected: Multiaddr = format!("/ip4/203.0.113.9/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{target_peer}").parse().unwrap();
    assert_eq!(resolved, expected);
}

#[tokio::test]
async fn resolve_relay_multiaddr_probes_missing_relay_peer_v6() {
    let relay_peer = PeerId::random();
    let target_peer = PeerId::random();
    let drops = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MockProvider::with_probe_peer(VecDeque::new(), drops, relay_peer));
    let addr: Multiaddr = format!("/ip6/2001:db8::9/tcp/16112/p2p-circuit/p2p/{target_peer}").parse().unwrap();

    let resolved = Libp2pOutboundConnector::resolve_relay_multiaddr(provider.clone(), addr).await.expect("resolve should succeed");
    let expected_probe: Multiaddr = "/ip6/2001:db8::9/tcp/16112".parse().unwrap();
    assert_eq!(provider.last_probe(), Some(expected_probe));

    let expected: Multiaddr = format!("/ip6/2001:db8::9/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{target_peer}").parse().unwrap();
    assert_eq!(resolved, expected);
}

#[tokio::test]
async fn resolve_relay_multiaddr_rejects_missing_target_peer() {
    let relay_peer = PeerId::random();
    let drops = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MockProvider::with_probe_peer(VecDeque::new(), drops, relay_peer));
    let addr: Multiaddr = "/ip4/203.0.113.9/tcp/16112/p2p-circuit".parse().unwrap();

    let err = Libp2pOutboundConnector::resolve_relay_multiaddr(provider.clone(), addr).await.expect_err("expected error");
    assert!(matches!(err, Libp2pError::Multiaddr(msg) if msg.contains("missing target peer id")));
    assert!(provider.last_probe().is_none(), "probe should not run without target peer id");
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
            Err(ConnectionError::ProtocolError(ProtocolError::Other("fallback")))
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
        Arc::new(kaspa_p2p_lib::DirectMetadataFactory),
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
    test_driver_with_allow_private(incoming_capacity, false)
}

fn test_driver_with_allow_private(
    incoming_capacity: usize,
    allow_private_addrs: bool,
) -> (SwarmDriver, mpsc::Receiver<IncomingStream>) {
    let mut cfg = Config::default();
    cfg.autonat.server_only_if_public = !allow_private_addrs;
    let identity = Libp2pIdentity::from_config(&cfg).expect("identity");
    let protocol = default_stream_protocol();
    let swarm = build_streaming_swarm(&identity, &cfg, protocol).expect("swarm");
    let (_cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_BOUND);
    let (incoming_tx, incoming_rx) = mpsc::channel(incoming_capacity);
    let (_shutdown_tx, shutdown) = triggered::trigger();
    let (role_tx, _role_rx) = watch::channel(crate::Role::Private);
    let (relay_hint_tx, _relay_hint_rx) = watch::channel(None);
    let driver = SwarmDriver::new(
        swarm,
        cmd_rx,
        incoming_tx,
        vec![],
        vec![],
        allow_private_addrs,
        vec![],
        role_tx,
        relay_hint_tx,
        crate::Role::Private,
        1,
        AUTO_ROLE_WINDOW,
        1,
        1,
        shutdown,
        None,
    );
    (driver, incoming_rx)
}

fn dialback_ready_driver_with_allow_private(allow_private_addrs: bool) -> (SwarmDriver, PeerId) {
    let (mut driver, _) = test_driver_with_allow_private(1, allow_private_addrs);
    let peer = PeerId::random();
    let remote_candidate: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    driver.peer_states.insert(
        peer,
        PeerState {
            supports_dcutr: true,
            connected_via_relay: true,
            outgoing: 0,
            remote_dcutr_candidates: vec![remote_candidate],
            remote_candidates_last_seen: Some(Instant::now()),
        },
    );
    let relay_peer = PeerId::random();
    let circuit_base: Multiaddr = format!("/ip4/198.51.100.1/tcp/16112/p2p/{relay_peer}").parse().unwrap();
    driver.active_relay = Some(RelayInfo { relay_peer, circuit_base });
    let local_observed: Multiaddr = "/ip4/203.0.113.1/tcp/16112".parse().unwrap();
    driver.swarm.add_external_address(local_observed.clone());
    driver.record_local_candidate(local_observed, LocalCandidateSource::Observed);
    driver.record_connection(
        make_request_id(),
        peer,
        &libp2p::core::ConnectedPoint::Dialer {
            address: format!("/ip4/198.51.100.1/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{peer}").parse().unwrap(),
            role_override: libp2p::core::Endpoint::Dialer,
            port_use: libp2p::core::transport::PortUse::Reuse,
        },
        false,
    );
    (driver, peer)
}

fn dialback_ready_driver() -> (SwarmDriver, PeerId) {
    dialback_ready_driver_with_allow_private(false)
}

fn make_request_id() -> StreamRequestId {
    DialOpts::unknown_peer_id().address(default_listen_addr()).build().connection_id()
}

type PendingDialResult = Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>;
type PendingDialReceiver = oneshot::Receiver<PendingDialResult>;

fn insert_relay_pending(driver: &mut SwarmDriver, peer_id: PeerId) -> (StreamRequestId, PendingDialReceiver) {
    let (tx, rx) = oneshot::channel();
    let req_id = make_request_id();
    driver
        .pending_dials
        .insert(req_id, DialRequest { respond_to: tx, started_at: Instant::now(), via: DialVia::Relay { target_peer: peer_id } });
    (req_id, rx)
}

mod auto_role;
mod driver;
