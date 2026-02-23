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

#[tokio::test]
async fn multiple_relay_dials_can_succeed() {
    let (mut driver, _) = test_driver(4);
    let peer = PeerId::random();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (req1, rx1) = insert_relay_pending(&mut driver, peer);
    let (req2, rx2) = insert_relay_pending(&mut driver, peer);

    if let Some(pending) = driver.pending_dials.remove(&req1) {
        let _ =
            pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
    }
    if let Some(pending) = driver.pending_dials.remove(&req2) {
        let _ =
            pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
    }

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
    if let Some(pending) = driver.pending_dials.remove(&req2) {
        let _ =
            pending.respond_to.send(Ok((TransportMetadata::default(), StreamDirection::Outbound, make_test_stream(drops.clone()))));
    }

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

#[tokio::test]
async fn direct_connection_with_existing_relay_marks_dcutr_upgraded() {
    let (mut driver, _) = test_driver(4);
    let peer = PeerId::random();
    let relay_peer = PeerId::random();

    let relay_conn_id = make_request_id();
    let relay_endpoint = libp2p::core::ConnectedPoint::Dialer {
        address: format!("/ip4/198.51.100.1/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{peer}").parse().unwrap(),
        role_override: libp2p::core::Endpoint::Dialer,
        port_use: libp2p::core::transport::PortUse::Reuse,
    };
    driver.record_connection(relay_conn_id, peer, &relay_endpoint, false);

    let direct_conn_id = make_request_id();
    let direct_endpoint = libp2p::core::ConnectedPoint::Dialer {
        address: "/ip4/203.0.113.10/tcp/16112".parse().unwrap(),
        role_override: libp2p::core::Endpoint::Dialer,
        port_use: libp2p::core::transport::PortUse::Reuse,
    };

    driver
        .handle_event(SwarmEvent::ConnectionEstablished {
            peer_id: peer,
            connection_id: direct_conn_id,
            endpoint: direct_endpoint,
            num_established: std::num::NonZeroU32::new(1).unwrap(),
            concurrent_dial_errors: None,
            established_in: Duration::from_millis(0),
        })
        .await;

    let direct = driver.connections.get(&direct_conn_id).expect("direct connection entry");
    assert!(matches!(direct.path, PathKind::Direct));
    assert!(direct.dcutr_upgraded, "direct connection should be marked as DCUtR-upgraded when relay path existed");
}

#[test]
fn dcutr_dialback_skips_when_autonat_private() {
    let (mut driver, peer) = dialback_ready_driver();
    driver.autonat_private_until = Some(Instant::now() + Duration::from_secs(60));

    driver.maybe_request_dialback(peer);
    assert!(driver.dialback_cooldowns.is_empty());
}

#[tokio::test]
async fn dcutr_dialback_allows_private_autonat_when_private_addrs_allowed() {
    let (mut driver, peer) = dialback_ready_driver_with_allow_private(true);
    driver.autonat_private_until = Some(Instant::now() + Duration::from_secs(60));

    driver.maybe_request_dialback(peer);
    assert!(driver.dialback_cooldowns.contains_key(&peer));
}

#[test]
fn dcutr_dialback_skips_when_direct_upgrade_cooldown_active() {
    let (mut driver, peer) = dialback_ready_driver();
    driver.direct_upgrade_cooldowns.insert(peer, Instant::now() + Duration::from_secs(60));

    driver.maybe_request_dialback(peer);
    assert!(driver.dialback_cooldowns.is_empty());
}

#[test]
fn dcutr_preflight_defers_without_fresh_observed_candidate() {
    let (mut driver, peer) = dialback_ready_driver();
    let observed = driver
        .local_candidate_meta
        .iter()
        .find_map(|(addr, meta)| matches!(meta.source, LocalCandidateSource::Observed).then_some(addr.clone()))
        .expect("observed candidate");
    if let Some(meta) = driver.local_candidate_meta.get_mut(&observed) {
        meta.updated_at = fallback_old_instant(Instant::now());
    }

    driver.maybe_request_dialback(peer);
    assert!(driver.dialback_cooldowns.is_empty());
    assert!(driver.dcutr_retries.contains_key(&peer));
}

#[test]
fn dcutr_dialback_skips_for_active_relay_peer() {
    let (mut driver, _peer) = dialback_ready_driver();
    let relay_peer = driver.active_relay.as_ref().expect("active relay").relay_peer;
    driver
        .dcutr_retries
        .insert(relay_peer, DcutrRetryState { failures: 1, next_retry_at: Instant::now(), last_reason: "test".to_string() });

    driver.request_dialback(relay_peer, true, "test");

    assert!(!driver.dcutr_retries.contains_key(&relay_peer));
    assert!(!driver.dialback_cooldowns.contains_key(&relay_peer));
}

#[test]
fn force_dialback_does_not_assume_relay_path() {
    let (mut driver, _) = test_driver(1);
    let peer = PeerId::random();
    let remote_candidate: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    driver.peer_states.insert(
        peer,
        PeerState {
            supports_dcutr: false,
            connected_via_relay: false,
            outgoing: 0,
            remote_dcutr_candidates: vec![remote_candidate],
            remote_candidates_last_seen: Some(Instant::now()),
        },
    );
    let relay_peer = PeerId::random();
    let circuit_base: Multiaddr = format!("/ip4/198.51.100.1/tcp/16112/p2p/{relay_peer}").parse().unwrap();
    driver.active_relay = Some(RelayInfo { relay_peer, circuit_base });
    let observed: Multiaddr = "/ip4/203.0.113.1/tcp/16112".parse().unwrap();
    driver.swarm.add_external_address(observed.clone());
    driver.record_local_candidate(observed, LocalCandidateSource::Observed);

    driver.request_dialback(peer, true, "test");

    let state = driver.peer_states.get(&peer).expect("peer state");
    assert!(state.supports_dcutr);
    assert!(!state.connected_via_relay);
    assert!(!driver.dcutr_retries.contains_key(&peer));
    assert!(!driver.dialback_cooldowns.contains_key(&peer));
}

#[test]
fn scheduled_retry_is_consumed_when_preconditions_fail() {
    let (mut driver, _) = test_driver(1);
    let peer = PeerId::random();
    driver.peer_states.insert(
        peer,
        PeerState {
            supports_dcutr: true,
            connected_via_relay: false,
            outgoing: 0,
            remote_dcutr_candidates: vec![],
            remote_candidates_last_seen: None,
        },
    );
    driver.dcutr_retries.insert(
        peer,
        DcutrRetryState {
            failures: 0,
            next_retry_at: Instant::now().checked_sub(Duration::from_secs(1)).unwrap_or_else(Instant::now),
            last_reason: "test".to_string(),
        },
    );

    driver.process_scheduled_dcutr_retries();

    assert!(!driver.dcutr_retries.contains_key(&peer));
}

#[test]
fn observed_candidates_dedupe_by_ip() {
    let (mut driver, _) = test_driver(1);
    let first: Multiaddr = "/ip4/8.8.8.8/tcp/41001".parse().unwrap();
    let second: Multiaddr = "/ip4/8.8.8.8/tcp/41002".parse().unwrap();

    driver.swarm.add_external_address(first.clone());
    driver.record_local_candidate(first.clone(), LocalCandidateSource::Observed);
    driver.swarm.add_external_address(second.clone());
    driver.record_local_candidate(second.clone(), LocalCandidateSource::Observed);

    let observed: Vec<_> = driver
        .local_candidate_meta
        .iter()
        .filter_map(|(addr, meta)| (meta.source == LocalCandidateSource::Observed).then_some(addr.clone()))
        .collect();
    assert_eq!(observed.len(), 1);
    assert!(observed.contains(&second));
    assert!(!observed.contains(&first));

    let local = driver.local_dcutr_candidates();
    assert!(local.contains(&second));
    assert!(!local.contains(&first));
}

#[test]
fn retryable_dcutr_error_detection_matches_known_failures() {
    assert!(is_retryable_dcutr_error_text("NoAddresses"));
    assert!(is_retryable_dcutr_error_text("io error: UnexpectedEof"));
    assert!(!is_retryable_dcutr_error_text("AttemptsExceeded"));
}

#[test]
fn dcutr_retry_trigger_detection_includes_attempts_exceeded() {
    assert!(is_dcutr_retry_trigger_error_text("NoAddresses"));
    assert!(is_dcutr_retry_trigger_error_text("io error: UnexpectedEof"));
    assert!(is_dcutr_retry_trigger_error_text("AttemptsExceeded(3)"));
}

#[test]
fn local_dcutr_candidates_prioritize_observed_over_config() {
    let (mut driver, _) = test_driver(1);
    let config_addr: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    let observed_addr: Multiaddr = "/ip4/9.9.9.9/tcp/16112".parse().unwrap();

    driver.swarm.add_external_address(config_addr.clone());
    driver.record_local_candidate(config_addr, LocalCandidateSource::Config);
    driver.swarm.add_external_address(observed_addr.clone());
    driver.record_local_candidate(observed_addr.clone(), LocalCandidateSource::Observed);

    let candidates = driver.local_dcutr_candidates();
    let expected_first: Multiaddr = "/ip4/9.9.9.9/tcp/16112".parse().unwrap();
    assert_eq!(candidates.first(), Some(&expected_first));
}

#[test]
fn observed_candidate_with_configured_ip_and_different_port_is_retained() {
    let (mut driver, _) = test_driver(1);
    let config_addr: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    let observed_addr: Multiaddr = "/ip4/8.8.8.8/tcp/34190".parse().unwrap();

    driver.swarm.add_external_address(config_addr.clone());
    driver.record_local_candidate(config_addr.clone(), LocalCandidateSource::Config);
    driver.swarm.add_external_address(observed_addr.clone());
    driver.record_local_candidate(observed_addr.clone(), LocalCandidateSource::Observed);

    let candidates = driver.local_dcutr_candidates();
    assert!(candidates.contains(&config_addr));
    assert!(candidates.contains(&observed_addr));
    assert_eq!(candidates.first(), Some(&observed_addr));
}

#[test]
fn observed_candidate_equal_to_config_does_not_drop_config() {
    let (mut driver, _) = test_driver(1);
    let config_addr: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();

    driver.swarm.add_external_address(config_addr.clone());
    driver.record_local_candidate(config_addr.clone(), LocalCandidateSource::Config);

    // Relay observation can match the configured address exactly.
    driver.record_local_candidate(config_addr.clone(), LocalCandidateSource::Observed);

    let candidates = driver.local_dcutr_candidates();
    assert_eq!(candidates, vec![config_addr]);
}

#[tokio::test]
async fn dcutr_preflight_allows_config_candidates_without_fresh_observed() {
    let (mut driver, peer) = dialback_ready_driver();
    let observed = driver
        .local_candidate_meta
        .iter()
        .find_map(|(addr, meta)| matches!(meta.source, LocalCandidateSource::Observed).then_some(addr.clone()))
        .expect("observed candidate");
    if let Some(meta) = driver.local_candidate_meta.get_mut(&observed) {
        meta.updated_at = fallback_old_instant(Instant::now());
    }
    let config_addr: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    driver.swarm.add_external_address(config_addr.clone());
    driver.record_local_candidate(config_addr, LocalCandidateSource::Config);

    driver.maybe_request_dialback(peer);
    assert!(driver.dialback_cooldowns.contains_key(&peer));
}

#[test]
fn extract_remote_dcutr_candidates_filters_undialable_addrs() {
    let relay_peer = PeerId::random();
    let target_peer = PeerId::random();
    let public_addr: Multiaddr = "/ip4/8.8.8.8/tcp/16112".parse().unwrap();
    let private_addr: Multiaddr = "/ip4/192.168.1.20/tcp/16112".parse().unwrap();
    let udp_addr: Multiaddr = "/ip4/8.8.4.4/udp/16112".parse().unwrap();
    let relay_addr: Multiaddr = format!("/ip4/8.8.8.8/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{target_peer}").parse().unwrap();

    let remote =
        extract_remote_dcutr_candidates(&[public_addr.clone(), private_addr.clone(), udp_addr.clone(), relay_addr.clone()], false);
    assert_eq!(remote, vec![public_addr.clone()]);

    let remote_allow_private = extract_remote_dcutr_candidates(std::slice::from_ref(&private_addr), true);
    assert_eq!(remote_allow_private, vec![private_addr]);
}

#[test]
fn auto_role_promotes_after_signals() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    assert_eq!(state.update_role(now, true), None);

    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);
}

#[test]
fn auto_role_requires_external_addr() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, false), None);
    assert_eq!(*role_rx.borrow(), crate::Role::Private);
}

#[test]
fn auto_role_requires_hysteresis_hits() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 2, 2);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), None);

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);
}

#[test]
fn auto_role_demotes_when_window_signal_expires() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, Duration::from_secs(1), 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);

    let later = now + Duration::from_secs(2);
    assert_eq!(state.update_role(later, true), Some(crate::Role::Private));
    assert_eq!(*role_rx.borrow(), crate::Role::Private);
}

#[test]
fn usable_external_addr_respects_private_setting() {
    let (driver_public, _) = test_driver_with_allow_private(1, false);
    let global: Multiaddr = "/ip4/8.8.8.8/tcp/1234".parse().unwrap();
    let private: Multiaddr = "/ip4/192.168.1.10/tcp/1234".parse().unwrap();
    let loopback: Multiaddr = "/ip4/127.0.0.1/tcp/1234".parse().unwrap();

    assert!(driver_public.is_usable_external_addr(&global));
    assert!(!driver_public.is_usable_external_addr(&private));
    assert!(!driver_public.is_usable_external_addr(&loopback));

    let (driver_private, _) = test_driver_with_allow_private(1, true);
    assert!(driver_private.is_usable_external_addr(&private));
    assert!(!driver_private.is_usable_external_addr(&loopback));
}
