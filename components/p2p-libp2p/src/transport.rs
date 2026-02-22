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
use std::collections::{HashMap, HashSet, VecDeque};
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

#[cfg(test)]
mod tests {
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
            let _ = pending.respond_to.send(Ok((
                TransportMetadata::default(),
                StreamDirection::Outbound,
                make_test_stream(drops.clone()),
            )));
        }
        if let Some(pending) = driver.pending_dials.remove(&req2) {
            let _ = pending.respond_to.send(Ok((
                TransportMetadata::default(),
                StreamDirection::Outbound,
                make_test_stream(drops.clone()),
            )));
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
            let _ = pending.respond_to.send(Ok((
                TransportMetadata::default(),
                StreamDirection::Outbound,
                make_test_stream(drops.clone()),
            )));
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
        assert_eq!(candidates.first(), Some(&observed_addr));
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
        assert!(!state.maybe_promote(now, true));

        state.record_direct_inbound(now);
        assert!(state.maybe_promote(now, true));
        assert_eq!(*role_rx.borrow(), crate::Role::Public);
    }

    #[test]
    fn auto_role_requires_external_addr() {
        let (role_tx, role_rx) = watch::channel(crate::Role::Private);
        let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 1, 1);
        let now = Instant::now();

        state.record_autonat_public(now);
        state.record_direct_inbound(now);
        assert!(!state.maybe_promote(now, false));
        assert_eq!(*role_rx.borrow(), crate::Role::Private);
    }

    #[test]
    fn auto_role_requires_hysteresis_hits() {
        let (role_tx, role_rx) = watch::channel(crate::Role::Private);
        let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 2, 2);
        let now = Instant::now();

        state.record_autonat_public(now);
        state.record_direct_inbound(now);
        assert!(!state.maybe_promote(now, true));

        state.record_autonat_public(now);
        state.record_direct_inbound(now);
        assert!(state.maybe_promote(now, true));
        assert_eq!(*role_rx.borrow(), crate::Role::Public);
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
const DIRECT_UPGRADE_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const AUTONAT_PRIVATE_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const DCUTR_PREFLIGHT_RETRY_DELAY: Duration = Duration::from_secs(3);
const DCUTR_LOCAL_OBSERVED_FRESHNESS: Duration = Duration::from_secs(2 * 60);
const DCUTR_REMOTE_CANDIDATE_FRESHNESS: Duration = Duration::from_secs(2 * 60);
const DCUTR_OBSERVED_CANDIDATE_TTL: Duration = Duration::from_secs(15 * 60);
const DCUTR_DYNAMIC_CANDIDATE_TTL: Duration = Duration::from_secs(10 * 60);
const DCUTR_RETRY_BACKOFFS_SECS: [u64; 4] = [2, 5, 10, 20];
const DCUTR_RETRY_JITTER_MS: u64 = 900;

/// Libp2p stream provider backed by a libp2p swarm.
pub struct SwarmStreamProvider {
    config: Config,
    command_tx: mpsc::Sender<SwarmCommand>,
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    shutdown: Trigger,
    task: Mutex<Option<JoinHandle<()>>>,
    role_updates: Option<watch::Receiver<crate::Role>>,
    relay_hint_updates: Option<watch::Receiver<Option<String>>>,
    metrics: Arc<Libp2pMetrics>,
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
        let metrics = Libp2pMetrics::new();
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
        let effective_role = if matches!(config.role, crate::Role::Auto) { crate::Role::Private } else { config.role };
        let (role_tx, role_rx) = watch::channel(effective_role);
        let (relay_hint_tx, relay_hint_rx) = watch::channel(None);
        let auto_role_required_autonat = config.autonat.confidence_threshold.max(1);
        let auto_role_required_direct = AUTO_ROLE_REQUIRED_DIRECT.max(1);
        let allow_private_addrs = !config.autonat.server_only_if_public;
        let task = handle.spawn(
            SwarmDriver::new(
                swarm,
                command_rx,
                incoming_tx,
                listen_multiaddrs,
                external_multiaddrs,
                allow_private_addrs,
                reservations,
                role_tx,
                relay_hint_tx,
                config.role,
                config.max_peers_per_relay,
                AUTO_ROLE_WINDOW,
                auto_role_required_autonat,
                auto_role_required_direct,
                shutdown_listener,
                Some(metrics.clone()),
            )
            .run(),
        );

        Ok(Self {
            config,
            command_tx,
            incoming: Mutex::new(incoming_rx),
            shutdown,
            task: Mutex::new(Some(task)),
            role_updates: Some(role_rx),
            relay_hint_updates: Some(relay_hint_rx),
            metrics,
        })
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

    fn probe_relay<'a>(&'a self, address: Multiaddr) -> BoxFuture<'a, Result<PeerId, Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::ProbeRelay { address, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await.unwrap_or_else(|_| Err(Libp2pError::DialFailed("relay probe cancelled".into())))
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

    fn role_updates(&self) -> Option<watch::Receiver<crate::Role>> {
        self.role_updates.clone()
    }

    fn relay_hint_updates(&self) -> Option<watch::Receiver<Option<String>>> {
        self.relay_hint_updates.clone()
    }

    fn metrics(&self) -> Option<Arc<Libp2pMetrics>> {
        Some(self.metrics.clone())
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
    ProbeRelay {
        address: Multiaddr,
        respond_to: oneshot::Sender<Result<PeerId, Libp2pError>>,
    },
    EnsureListening {
        respond_to: oneshot::Sender<Result<(), Libp2pError>>,
    },
    Reserve {
        target: Multiaddr,
        respond_to: oneshot::Sender<Result<ListenerId, Libp2pError>>,
    },
    ReleaseReservation {
        listener_id: ListenerId,
        respond_to: oneshot::Sender<()>,
    },
    PeersSnapshot {
        respond_to: oneshot::Sender<Vec<PeerSnapshot>>,
    },
}

struct DialRequest {
    respond_to: oneshot::Sender<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>,
    started_at: Instant,
    via: DialVia,
}

struct PendingProbe {
    respond_to: oneshot::Sender<Result<PeerId, Libp2pError>>,
    started_at: Instant,
}

const AUTO_ROLE_WINDOW: Duration = Duration::from_secs(10 * 60);
const AUTO_ROLE_REQUIRED_DIRECT: usize = 1;

struct AutoRoleState {
    current: crate::Role,
    window: Duration,
    required_autonat: usize,
    required_direct: usize,
    autonat_public_hits: VecDeque<Instant>,
    direct_inbound_hits: VecDeque<Instant>,
    role_tx: watch::Sender<crate::Role>,
}

impl AutoRoleState {
    fn new(role_tx: watch::Sender<crate::Role>, window: Duration, required_autonat: usize, required_direct: usize) -> Self {
        Self {
            current: crate::Role::Private,
            window,
            required_autonat: required_autonat.max(1),
            required_direct: required_direct.max(1),
            autonat_public_hits: VecDeque::new(),
            direct_inbound_hits: VecDeque::new(),
            role_tx,
        }
    }

    fn record_autonat_public(&mut self, now: Instant) {
        self.autonat_public_hits.push_back(now);
        self.prune(now);
    }

    fn record_direct_inbound(&mut self, now: Instant) {
        self.direct_inbound_hits.push_back(now);
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while self.autonat_public_hits.front().map(|t| now.saturating_duration_since(*t) > self.window).unwrap_or(false) {
            self.autonat_public_hits.pop_front();
        }
        while self.direct_inbound_hits.front().map(|t| now.saturating_duration_since(*t) > self.window).unwrap_or(false) {
            self.direct_inbound_hits.pop_front();
        }
    }

    fn maybe_promote(&mut self, now: Instant, has_external_addr: bool) -> bool {
        if self.current == crate::Role::Public {
            return false;
        }
        self.prune(now);
        if self.autonat_public_hits.len() >= self.required_autonat
            && self.direct_inbound_hits.len() >= self.required_direct
            && has_external_addr
        {
            self.current = crate::Role::Public;
            let _ = self.role_tx.send(crate::Role::Public);
            return true;
        }
        false
    }
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
        self.publish_relay_hint();
    }

    fn publish_relay_hint(&self) {
        let hint = self.active_relay.as_ref().map(|relay| relay.circuit_base.to_string());
        let _ = self.relay_hint_tx.send(hint);
    }

    fn set_active_relay(&mut self, relay: RelayInfo, listener: Option<ListenerId>) {
        let relay_changed = self.active_relay.as_ref().map(|current| current.relay_peer) != Some(relay.relay_peer);
        self.active_relay = Some(relay);
        self.active_relay_listener = listener;
        if relay_changed {
            self.invalidate_dcutr_cached_candidates("relay_changed");
        }
        self.publish_relay_hint();
    }

    fn clear_active_relay(&mut self) {
        if self.active_relay.is_some() {
            self.invalidate_dcutr_cached_candidates("relay_cleared");
        }
        self.active_relay = None;
        self.active_relay_listener = None;
        self.publish_relay_hint();
    }

    fn invalidate_dcutr_cached_candidates(&mut self, source: &str) {
        let peers_with_candidates = self.peer_states.values().filter(|state| !state.remote_dcutr_candidates.is_empty()).count();
        for state in self.peer_states.values_mut() {
            state.remote_dcutr_candidates.clear();
            state.remote_candidates_last_seen = None;
        }

        let observed_addrs: Vec<_> = self
            .local_candidate_meta
            .iter()
            .filter(|(_, meta)| matches!(meta.source, LocalCandidateSource::Observed | LocalCandidateSource::Dynamic))
            .map(|(addr, _)| addr.clone())
            .collect();
        for addr in &observed_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }

        self.dialback_cooldowns.clear();
        self.direct_upgrade_cooldowns.clear();
        self.dcutr_retries.clear();

        info!(
            "libp2p dcutr candidate invalidation ({source}): peers_cleared={} local_removed={}",
            peers_with_candidates,
            observed_addrs.len()
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        swarm: libp2p::Swarm<Libp2pBehaviour>,
        command_rx: mpsc::Receiver<SwarmCommand>,
        incoming_tx: mpsc::Sender<IncomingStream>,
        listen_addrs: Vec<Multiaddr>,
        external_addrs: Vec<Multiaddr>,
        allow_private_addrs: bool,
        reservations: Vec<ReservationTarget>,
        role_tx: watch::Sender<crate::Role>,
        relay_hint_tx: watch::Sender<Option<String>>,
        config_role: crate::Role,
        max_peers_per_relay: usize,
        auto_role_window: Duration,
        auto_role_required_autonat: usize,
        auto_role_required_direct: usize,
        shutdown: Listener,
        metrics: Option<Arc<Libp2pMetrics>>,
    ) -> Self {
        let local_peer_id = *swarm.local_peer_id();
        let active_relay = reservations.into_iter().find_map(|r| relay_info_from_multiaddr(&r.multiaddr, local_peer_id));
        let auto_role = if matches!(config_role, crate::Role::Auto) {
            Some(AutoRoleState::new(role_tx, auto_role_window, auto_role_required_autonat, auto_role_required_direct))
        } else {
            None
        };
        let now = Instant::now();
        let local_candidate_meta = external_addrs
            .iter()
            .cloned()
            .map(|addr| (addr, LocalCandidateMeta { source: LocalCandidateSource::Config, updated_at: now }))
            .collect();

        Self {
            swarm,
            command_rx,
            incoming_tx,
            pending_dials: HashMap::new(),
            pending_probes: HashMap::new(),
            dialback_cooldowns: HashMap::new(),
            direct_upgrade_cooldowns: HashMap::new(),
            listen_addrs,
            external_addrs,
            allow_private_addrs,
            local_candidate_meta,
            peer_states: HashMap::new(),
            dcutr_retries: HashMap::new(),
            reservation_listeners: HashSet::new(),
            active_relay,
            active_relay_listener: None,
            auto_role,
            max_peers_per_relay: max_peers_per_relay.max(1),
            autonat_private_until: None,
            metrics,
            listening: false,
            shutdown,
            connections: HashMap::new(),
            relay_hint_tx,
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
                    self.expire_pending_probes("relay probe timed out");
                    self.process_scheduled_dcutr_retries();
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
        for (_, pending) in self.pending_probes.drain() {
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

    fn expire_pending_probes(&mut self, reason: &str) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, pending)| now.saturating_duration_since(pending.started_at) >= PENDING_DIAL_TIMEOUT)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.fail_probe(id, reason);
        }
    }

    fn process_scheduled_dcutr_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<_> =
            self.dcutr_retries.iter().filter(|(_, state)| state.next_retry_at <= now).map(|(peer_id, _)| *peer_id).collect();

        for peer_id in due {
            let Some(state) = self.dcutr_retries.remove(&peer_id) else {
                continue;
            };
            info!(
                "libp2p dcutr scheduled retry firing for {}: failures={} last_reason={}",
                peer_id, state.failures, state.last_reason
            );
            self.force_identify_refresh(peer_id, "scheduled_retry");
            self.refresh_relay_connection(peer_id, "scheduled_retry");
            self.request_dialback(peer_id, true, "scheduled_retry");
        }
    }

    fn schedule_dcutr_retry(&mut self, peer_id: PeerId, reason: &str, count_failure: bool) {
        let now = Instant::now();
        let state = self.dcutr_retries.entry(peer_id).or_insert_with(|| DcutrRetryState {
            failures: 0,
            next_retry_at: now + DCUTR_PREFLIGHT_RETRY_DELAY,
            last_reason: String::new(),
        });

        if count_failure {
            state.failures = state.failures.saturating_add(1);
        }

        let attempt_index =
            if count_failure { usize::from(state.failures.saturating_sub(1)).min(DCUTR_RETRY_BACKOFFS_SECS.len() - 1) } else { 0 };
        let backoff = Duration::from_secs(DCUTR_RETRY_BACKOFFS_SECS[attempt_index]);
        let jitter = dcutr_retry_jitter(peer_id, state.failures);
        let next_retry = now + backoff + jitter;

        if state.next_retry_at <= now || next_retry < state.next_retry_at {
            state.next_retry_at = next_retry;
        }
        state.last_reason = reason.to_string();

        info!(
            "libp2p dcutr retry scheduled for {}: reason={} failures={} next_retry_in_ms={}",
            peer_id,
            reason,
            state.failures,
            state.next_retry_at.saturating_duration_since(now).as_millis()
        );
    }

    fn clear_dcutr_retry(&mut self, peer_id: PeerId) {
        if self.dcutr_retries.remove(&peer_id).is_some() {
            debug!("libp2p dcutr retry state cleared for {}", peer_id);
        }
    }

    fn fail_pending(&mut self, request_id: StreamRequestId, err: impl ToString) {
        if let Some(pending) = self.pending_dials.remove(&request_id) {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
        }
    }

    fn fail_probe(&mut self, request_id: StreamRequestId, err: impl ToString) {
        if let Some(pending) = self.pending_probes.remove(&request_id) {
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
            SwarmCommand::ProbeRelay { address, respond_to } => {
                info!("libp2p relay probe request to {address}");
                let dial_opts = DialOpts::unknown_peer_id().address(address).build();
                let request_id = dial_opts.connection_id();
                let started_at = Instant::now();
                match self.swarm.dial(dial_opts) {
                    Ok(()) => {
                        self.pending_probes.insert(request_id, PendingProbe { respond_to, started_at });
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
                if !target.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
                    target.push(Protocol::P2pCircuit);
                }
                let target_for_info = target.clone();
                match self.swarm.listen_on(target) {
                    Ok(listener) => {
                        self.reservation_listeners.insert(listener);
                        if let Some(info) = relay_info_from_multiaddr(&target_for_info, *self.swarm.local_peer_id()) {
                            self.set_active_relay(info, Some(listener));
                        }
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
                if self.active_relay_listener == Some(listener_id) {
                    self.clear_active_relay();
                }
                let _ = respond_to.send(());
            }
            SwarmCommand::PeersSnapshot { respond_to } => {
                let _ = respond_to.send(self.peers_snapshot());
            }
        }
    }

    async fn handle_event(&mut self, event: SwarmEvent<Libp2pEvent>) {
        match event {
            SwarmEvent::Behaviour(event) => self.handle_behaviour_event(event).await,
            SwarmEvent::NewListenAddr { address, .. } => self.handle_new_listen_addr_event(address),
            SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, .. } => {
                self.handle_connection_established_event(peer_id, connection_id, endpoint)
            }
            SwarmEvent::ConnectionClosed { peer_id, connection_id, endpoint, .. } => {
                self.handle_connection_closed_event(peer_id, connection_id, endpoint)
            }
            SwarmEvent::OutgoingConnectionError { connection_id, error, .. } => {
                self.handle_outgoing_connection_error_event(connection_id, error.to_string())
            }
            _ => {}
        }
    }

    async fn handle_behaviour_event(&mut self, event: Libp2pEvent) {
        match event {
            Libp2pEvent::Stream(event) => self.handle_stream_event(event).await,
            Libp2pEvent::Ping(event) => {
                let _ = event;
            }
            Libp2pEvent::Identify(event) => self.handle_identify_event(event),
            Libp2pEvent::RelayClient(event) => self.handle_relay_client_event(event),
            Libp2pEvent::RelayServer(event) => {
                debug!("libp2p relay server event: {:?}", event);
            }
            Libp2pEvent::Dcutr(event) => self.handle_dcutr_event(event),
            Libp2pEvent::DcutrBootstrap(_) => {}
            Libp2pEvent::Autonat(event) => self.handle_autonat_event(event),
        }
    }

    fn handle_identify_event(&mut self, event: identify::Event) {
        match event {
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
                self.update_remote_dcutr_candidates(peer_id, &info.listen_addrs);
                let observed_refreshed = self.refresh_local_dcutr_candidates(peer_id, &info.observed_addr);
                // NOTE: We intentionally do NOT add info.listen_addrs as external addresses.
                // Those are the REMOTE peer's addresses, not ours. Adding them would pollute
                // our external address set with unreachable addresses, breaking DCUtR hole punch.
                self.prune_unusable_external_addrs("identify_received");
                self.prune_stale_external_addrs("identify_received");
                self.mark_dcutr_support(peer_id, supports_dcutr);
                if self.active_relay.as_ref().is_some_and(|relay| relay.relay_peer == peer_id) {
                    debug!("libp2p dcutr: skipping dial-back trigger for active relay peer {}", peer_id);
                    return;
                }
                if observed_refreshed {
                    self.request_dialback(peer_id, true, "observed_addr_refresh");
                } else {
                    self.maybe_request_dialback(peer_id);
                }
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
        }
    }

    fn handle_relay_client_event(&mut self, event: relay::client::Event) {
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

    fn handle_dcutr_event(&mut self, event: dcutr::Event) {
        let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
        let is_spurious_attempts = self
            .connections
            .values()
            .any(|conn| conn.peer_id == event.remote_peer_id && matches!(conn.path, PathKind::Direct) && conn.dcutr_upgraded);
        match &event.result {
            Err(e) if is_dcutr_retry_trigger_error(e) && !(is_attempts_exceeded(e) && is_spurious_attempts) => {
                warn!(
                    "libp2p dcutr retry-trigger failure for {}: {:?} (local_candidates={} remote_candidates={})",
                    event.remote_peer_id,
                    e,
                    self.local_dcutr_candidates().len(),
                    self.peer_states.get(&event.remote_peer_id).map_or(0, |state| state.remote_dcutr_candidates.len())
                );
                self.refresh_dcutr_retry_path(event.remote_peer_id, e);
            }
            Err(e) if is_attempts_exceeded(e) && is_spurious_attempts => {
                debug!(
                    "Ignored spurious DCUtR error for connected peer {}: {:?} (swarm has {} external addrs: {:?})",
                    event.remote_peer_id,
                    e,
                    external_addrs.len(),
                    external_addrs
                );
            }
            _ => {
                info!("libp2p dcutr event: {:?} (swarm has {} external addrs: {:?})", event, external_addrs.len(), external_addrs);
            }
        }
    }

    fn handle_autonat_event(&mut self, event: autonat::Event) {
        debug!("libp2p autonat event: {:?}", event);
        let has_external_addr = self.has_usable_external_addr();
        match &event {
            autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Response { .. }) => {
                self.autonat_private_until = None;
            }
            autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Error { error, .. }) => {
                if !self.allow_private_addrs
                    && matches!(error, autonat::OutboundProbeError::Response(autonat::ResponseError::DialError))
                {
                    self.autonat_private_until = Some(Instant::now() + AUTONAT_PRIVATE_COOLDOWN);
                }
            }
            _ => {}
        }
        if let Some(auto_role) = self.auto_role.as_mut() {
            let is_public_probe = matches!(event, autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Response { .. }));
            if is_public_probe {
                auto_role.record_autonat_public(Instant::now());
                if auto_role.maybe_promote(Instant::now(), has_external_addr) {
                    info!("libp2p autonat: role auto-promoted to public");
                }
            }
        }
    }

    fn handle_new_listen_addr_event(&mut self, address: Multiaddr) {
        info!("libp2p listening on {address}");
        if self.active_relay.is_none()
            && let Some(info) = relay_info_from_multiaddr(&address, *self.swarm.local_peer_id())
        {
            self.set_active_relay(info, None);
        }
        if self.is_usable_external_addr(&address) {
            self.record_local_candidate(address.clone(), LocalCandidateSource::Dynamic);
        }
        self.prune_unusable_external_addrs("new_listen_addr");
        self.prune_stale_external_addrs("new_listen_addr");
        self.listening = true;
    }

    fn handle_connection_established_event(
        &mut self,
        peer_id: PeerId,
        connection_id: StreamRequestId,
        endpoint: libp2p::core::ConnectedPoint,
    ) {
        debug!("libp2p connection established with {peer_id} on {connection_id:?}");
        self.track_established(peer_id, &endpoint);

        let has_external_addr = self.has_usable_external_addr();
        if let Some(auto_role) = self.auto_role.as_mut()
            && !endpoint.is_dialer()
            && !endpoint_uses_relay(&endpoint)
        {
            auto_role.record_direct_inbound(Instant::now());
            if auto_role.maybe_promote(Instant::now(), has_external_addr) {
                info!("libp2p auto role: promoted to public after direct inbound");
            }
        }

        if let Some(pending) = self.pending_probes.remove(&connection_id) {
            info!("libp2p relay probe connected to {peer_id}");
            self.record_connection(connection_id, peer_id, &endpoint, false);
            let _ = pending.respond_to.send(Ok(peer_id));
            return;
        }

        // For DCUtR direct connections spawned from relay dials, transfer the earliest
        // pending relay dial for this peer onto the new connection_id.
        let mut had_pending_relay = false;
        if !endpoint_uses_relay(&endpoint)
            && let Some((_old_req, pending)) = self.take_pending_relay_by_peer(&peer_id)
        {
            info!("libp2p DCUtR success: direct connection to {peer_id} resolves pending relay dial");
            self.pending_dials.insert(connection_id, pending);
            had_pending_relay = true;
        }
        if had_pending_relay && let Some(metrics) = self.metrics.as_ref() {
            metrics.dcutr().record_dialback_success();
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
        if !endpoint_uses_relay(&endpoint) {
            self.note_direct_upgrade(peer_id, had_pending_relay);
        }
        if endpoint_uses_relay(&endpoint) {
            self.enforce_relay_cap(connection_id);
        } else {
            self.close_relay_connections_for_peer(peer_id, connection_id);
        }
    }

    fn handle_connection_closed_event(
        &mut self,
        peer_id: PeerId,
        connection_id: StreamRequestId,
        endpoint: libp2p::core::ConnectedPoint,
    ) {
        self.fail_pending(connection_id, "connection closed before stream");
        self.fail_probe(connection_id, "relay probe connection closed");
        self.track_closed(peer_id, &endpoint);
        self.connections.remove(&connection_id);
    }

    fn handle_outgoing_connection_error_event(&mut self, connection_id: StreamRequestId, error: String) {
        self.fail_pending(connection_id, &error);
        self.fail_probe(connection_id, &error);
        self.connections.remove(&connection_id);
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

    fn has_usable_external_addr(&self) -> bool {
        self.swarm.external_addresses().any(|addr| self.is_usable_external_addr(addr))
    }

    fn is_usable_external_addr(&self, addr: &Multiaddr) -> bool {
        if !is_tcp_dialable(addr) {
            return false;
        }
        let mut ip: Option<std::net::IpAddr> = None;
        for protocol in addr.iter() {
            match protocol {
                Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
                Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
                Protocol::P2pCircuit => return false,
                _ => {}
            }
        }
        let Some(ip) = ip else {
            return false;
        };
        if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
            return false;
        }
        if self.allow_private_addrs {
            return true;
        }
        kaspa_utils::networking::IpAddress::new(ip).is_publicly_routable()
    }

    fn has_relay_connection(&self, peer_id: PeerId) -> bool {
        self.connections.values().any(|conn| conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. }))
    }

    fn force_identify_refresh(&mut self, peer_id: PeerId, reason: &str) {
        if tokio::runtime::Handle::try_current().is_err() {
            debug!("libp2p dcutr identify refresh skipped for {} (reason={}): no tokio runtime", peer_id, reason);
            return;
        }
        self.swarm.behaviour_mut().identify.push([peer_id]);
        info!("libp2p dcutr identify refresh triggered for {} (reason={})", peer_id, reason);
    }

    fn refresh_relay_connection(&mut self, peer_id: PeerId, reason: &str) {
        if tokio::runtime::Handle::try_current().is_err() {
            debug!("libp2p dcutr relay refresh skipped for {} (reason={}): no tokio runtime", peer_id, reason);
            return;
        }
        let Some(relay) = &self.active_relay else {
            debug!("libp2p dcutr relay refresh skipped for {}: no active relay (reason={})", peer_id, reason);
            return;
        };

        let relay_addr = relay_probe_base(&relay.circuit_base);
        let opts = DialOpts::peer_id(relay.relay_peer)
            .addresses(vec![relay_addr.clone()])
            .condition(PeerCondition::Disconnected)
            .extend_addresses_through_behaviour()
            .build();
        match self.swarm.dial(opts) {
            Ok(()) => {
                info!(
                    "libp2p dcutr relay refresh dial started for {} via {} using {} (reason={})",
                    peer_id, relay.relay_peer, relay_addr, reason
                );
            }
            Err(err) => {
                warn!(
                    "libp2p dcutr relay refresh dial failed for {} via {} using {} (reason={}): {}",
                    peer_id, relay.relay_peer, relay_addr, reason, err
                );
            }
        }
    }

    fn note_direct_upgrade(&mut self, peer_id: PeerId, had_pending_relay: bool) {
        self.clear_dcutr_retry(peer_id);
        if !had_pending_relay && !self.has_relay_connection(peer_id) {
            return;
        }
        self.direct_upgrade_cooldowns.insert(peer_id, Instant::now() + DIRECT_UPGRADE_COOLDOWN);
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
            libp2p::core::ConnectedPoint::Listener { send_back_addr, local_addr } => {
                relay_id_from_multiaddr(send_back_addr).or_else(|| relay_id_from_multiaddr(local_addr))
            }
        };
        let outbound = endpoint.is_dialer();
        self.connections
            .insert(connection_id, ConnectionEntry { peer_id, path, relay_id, outbound, since: Instant::now(), dcutr_upgraded });
    }

    fn close_relay_connections_for_peer(&mut self, peer_id: PeerId, keep: StreamRequestId) {
        let relay_ids: Vec<_> = self
            .connections
            .iter()
            .filter(|(id, conn)| **id != keep && conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. }))
            .map(|(id, _)| *id)
            .collect();
        for id in relay_ids {
            if self.swarm.close_connection(id) {
                info!("libp2p: closing relay connection {id:?} to {peer_id} after direct path established");
            }
            self.connections.remove(&id);
        }
    }

    fn enforce_relay_cap(&mut self, connection_id: StreamRequestId) {
        let Some(conn) = self.connections.get(&connection_id) else {
            return;
        };
        if !matches!(conn.path, PathKind::Relay { .. }) {
            return;
        }
        let relay_id = conn.relay_id.clone();
        let mut relay_peers: HashSet<PeerId> = self
            .connections
            .iter()
            .filter(|(id, entry)| **id != connection_id && matches!(entry.path, PathKind::Relay { .. }) && entry.relay_id == relay_id)
            .map(|(_, entry)| entry.peer_id)
            .collect();
        relay_peers.insert(conn.peer_id);
        let unique_peers = relay_peers.len();
        if unique_peers > self.max_peers_per_relay {
            if self.swarm.close_connection(connection_id) {
                info!("libp2p: closing relay connection {connection_id:?} for relay cap");
            }
            self.connections.remove(&connection_id);
        }
    }

    async fn handle_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::Inbound { peer_id, _connection_id: connection_id, endpoint, stream } => {
                // For dialed connections, we normally skip inbound streams to avoid duplicates.
                // However, for DCUtR-upgraded connections where this node has role_override = Listener,
                // we ARE the server and MUST accept inbound streams - the remote will send us data.
                if let libp2p::core::ConnectedPoint::Dialer { role_override, .. } = &endpoint {
                    if !matches!(role_override, libp2p::core::Endpoint::Listener) {
                        debug!("libp2p_bridge: skipping inbound stream on dialed connection to {peer_id} (no role_override)");
                        return;
                    }
                    info!("libp2p_bridge: accepting inbound stream on DCUtR connection (role_override=Listener) from {peer_id}");
                }
                info!("libp2p_bridge: StreamEvent::Inbound peer={} endpoint={:?}", peer_id, endpoint);
                let mut metadata = metadata_from_endpoint(&peer_id, &endpoint);
                // If endpoint-based path detection returned Unknown, fall back to our
                // connection records which track whether a connection uses a relay circuit.
                // This is needed because the send_back_addr for relay circuit listeners
                // may not contain the P2pCircuit protocol marker.
                if matches!(metadata.path, kaspa_p2p_lib::PathKind::Unknown)
                    && let Some(conn) = self.connections.get(&connection_id)
                {
                    metadata.path = conn.path.clone();
                }
                info!("libp2p_bridge: inbound stream from {peer_id} over {:?}, handing to Kaspa", metadata.path);
                let incoming = IncomingStream { metadata, direction: StreamDirection::Inbound, stream: Box::new(stream.compat()) };
                self.enqueue_incoming(incoming);
            }
            StreamEvent::Outbound { peer_id, request_id, endpoint, stream, .. } => {
                let metadata = metadata_from_endpoint(&peer_id, &endpoint);
                let direction = match &endpoint {
                    libp2p::core::ConnectedPoint::Dialer { role_override: libp2p::core::Endpoint::Listener, .. } => {
                        info!(
                            "libp2p_bridge: DCUtR role_override detected, treating outbound stream as inbound (h2 server) for {peer_id}"
                        );
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
        info!("libp2p_bridge: request_stream_bridge peer={} conn_id={:?} (requesting substream)", peer_id, connection_id);

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

    fn update_remote_dcutr_candidates(&mut self, peer_id: PeerId, listen_addrs: &[Multiaddr]) {
        let remote_dcutr_candidates = extract_remote_dcutr_candidates(listen_addrs, self.allow_private_addrs);
        let remote_count = remote_dcutr_candidates.len();
        let state = self.peer_states.entry(peer_id).or_default();
        state.remote_dcutr_candidates = remote_dcutr_candidates;
        state.remote_candidates_last_seen = Some(Instant::now());
        info!(
            "libp2p dcutr candidates refreshed from identify for {peer_id}: local_candidates={} remote_candidates={remote_count}",
            self.local_dcutr_candidates().len()
        );
    }

    fn local_dcutr_candidates(&self) -> Vec<Multiaddr> {
        let now = Instant::now();
        let mut candidates: Vec<_> =
            self.swarm.external_addresses().filter(|addr| self.is_usable_external_addr(addr)).cloned().collect();
        candidates.sort_by_key(|addr| {
            let meta = self
                .local_candidate_meta
                .get(addr)
                .copied()
                .unwrap_or(LocalCandidateMeta { source: LocalCandidateSource::Dynamic, updated_at: fallback_old_instant(now) });
            (
                std::cmp::Reverse(local_candidate_priority(meta.source)),
                now.saturating_duration_since(meta.updated_at),
                addr.to_string(),
            )
        });
        candidates.dedup();
        candidates
    }

    fn prune_unusable_external_addrs(&mut self, source: &str) {
        let stale_addrs: Vec<_> =
            self.swarm.external_addresses().filter(|addr| !self.is_usable_external_addr(addr)).cloned().collect();
        if stale_addrs.is_empty() {
            return;
        }
        for addr in &stale_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }
        info!(
            "libp2p dcutr candidate prune ({source}): removed={} remaining_usable={} removed_addrs={:?}",
            stale_addrs.len(),
            self.local_dcutr_candidates().len(),
            stale_addrs
        );
    }

    fn prune_stale_external_addrs(&mut self, source: &str) {
        let now = Instant::now();
        let stale_addrs: Vec<_> = self
            .local_candidate_meta
            .iter()
            .filter_map(|(addr, meta)| {
                let max_age = match meta.source {
                    LocalCandidateSource::Config => return None,
                    LocalCandidateSource::Observed => DCUTR_OBSERVED_CANDIDATE_TTL,
                    LocalCandidateSource::Dynamic => DCUTR_DYNAMIC_CANDIDATE_TTL,
                };
                if now.saturating_duration_since(meta.updated_at) > max_age { Some(addr.clone()) } else { None }
            })
            .collect();
        if stale_addrs.is_empty() {
            return;
        }
        for addr in &stale_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }
        info!(
            "libp2p dcutr stale candidate prune ({source}): removed={} remaining_usable={} removed_addrs={:?}",
            stale_addrs.len(),
            self.local_dcutr_candidates().len(),
            stale_addrs
        );
    }

    fn record_local_candidate(&mut self, addr: Multiaddr, source: LocalCandidateSource) {
        if matches!(source, LocalCandidateSource::Observed)
            && let Some(candidate_ip) = candidate_ip_addr(&addr)
        {
            // Keep one observed candidate per IP so frequent observed port updates
            // cannot grow the local candidate set without bound.
            let stale_same_ip: Vec<_> = self
                .local_candidate_meta
                .iter()
                .filter_map(|(existing, meta)| {
                    if existing != &addr
                        && meta.source == LocalCandidateSource::Observed
                        && candidate_ip_addr(existing).is_some_and(|existing_ip| existing_ip == candidate_ip)
                    {
                        Some(existing.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for stale in stale_same_ip {
                self.swarm.remove_external_address(&stale);
                self.local_candidate_meta.remove(&stale);
            }
        }
        self.local_candidate_meta.insert(addr, LocalCandidateMeta { source, updated_at: Instant::now() });
    }

    fn has_fresh_local_observed_candidate(&self, now: Instant) -> bool {
        self.local_candidate_meta.iter().any(|(addr, meta)| {
            matches!(meta.source, LocalCandidateSource::Observed)
                && self.is_usable_external_addr(addr)
                && now.saturating_duration_since(meta.updated_at) <= DCUTR_LOCAL_OBSERVED_FRESHNESS
        })
    }

    fn refresh_local_dcutr_candidates(&mut self, peer_id: PeerId, observed_addr: &Multiaddr) -> bool {
        if !self.is_usable_external_addr(observed_addr) {
            return false;
        }
        let now = Instant::now();
        let was_fresh = self
            .local_candidate_meta
            .get(observed_addr)
            .is_some_and(|meta| now.saturating_duration_since(meta.updated_at) <= Duration::from_secs(1));
        self.swarm.add_external_address(observed_addr.clone());
        self.record_local_candidate(observed_addr.clone(), LocalCandidateSource::Observed);
        // Force immediate re-attempt for this peer when a fresh observed address appears.
        self.dialback_cooldowns.remove(&peer_id);
        self.direct_upgrade_cooldowns.remove(&peer_id);
        let remote_count = self.peer_states.get(&peer_id).map_or(0, |state| state.remote_dcutr_candidates.len());
        info!(
            "libp2p dcutr local candidate refresh for {peer_id}: observed_addr={} local_candidates={} remote_candidates={remote_count}",
            observed_addr,
            self.local_dcutr_candidates().len()
        );
        !was_fresh
    }

    fn refresh_dcutr_retry_path(&mut self, peer_id: PeerId, error: &dcutr::Error) {
        let relay_connections =
            self.connections.values().filter(|conn| conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. })).count();
        self.mark_relay_path(peer_id);
        self.dialback_cooldowns.remove(&peer_id);
        self.direct_upgrade_cooldowns.remove(&peer_id);
        info!(
            "libp2p dcutr retry-path refresh for {peer_id}: error={error} relay_connections={relay_connections} local_candidates={} remote_candidates={}",
            self.local_dcutr_candidates().len(),
            self.peer_states.get(&peer_id).map_or(0, |state| state.remote_dcutr_candidates.len())
        );
        self.force_identify_refresh(peer_id, "dcutr_error");
        self.refresh_relay_connection(peer_id, "dcutr_error");
        self.schedule_dcutr_retry(peer_id, &error.to_string(), true);
        self.request_dialback(peer_id, true, "retryable_dcutr_error");
    }

    fn maybe_request_dialback(&mut self, peer_id: PeerId) {
        self.request_dialback(peer_id, false, "standard");
    }

    fn request_dialback(&mut self, peer_id: PeerId, force: bool, reason: &str) {
        self.prune_unusable_external_addrs("request_dialback");
        self.prune_stale_external_addrs("request_dialback");
        if self.active_relay.as_ref().is_some_and(|relay| relay.relay_peer == peer_id) {
            self.clear_dcutr_retry(peer_id);
            debug!("libp2p dcutr: skipping dial-back to active relay peer {}", peer_id);
            return;
        }
        if force {
            let has_relay_connection = self.has_relay_connection(peer_id);
            let state = self.peer_states.entry(peer_id).or_default();
            state.supports_dcutr = true;
            if has_relay_connection {
                state.connected_via_relay = true;
            }
        }

        let Some(state) = self.peer_states.get(&peer_id) else {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no peer state");
            return;
        };
        let supports_dcutr = state.supports_dcutr;
        let connected_via_relay = state.connected_via_relay;
        let outgoing = state.outgoing;
        let remote_candidates_count = state.remote_dcutr_candidates.len();
        let remote_candidates_last_seen = state.remote_candidates_last_seen;

        if !supports_dcutr {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: peer does not support dcutr");
            return;
        }
        if !connected_via_relay {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: not connected via relay");
            return;
        }
        if !force && outgoing > 0 {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: already have outgoing connection");
            return;
        }
        if !self.has_relay_connection(peer_id) {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no active relay circuit connection");
            self.force_identify_refresh(peer_id, "missing_relay_circuit");
            self.refresh_relay_connection(peer_id, "missing_relay_circuit");
            self.schedule_dcutr_retry(peer_id, "missing_relay_circuit", false);
            return;
        }

        let now = Instant::now();
        if !self.allow_private_addrs
            && let Some(until) = self.autonat_private_until
        {
            if until > now {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.dcutr().record_dialback_skipped_private();
                }
                debug!("libp2p dcutr: skipping dial-back to {peer_id}: autonat private until {:?}", until);
                return;
            }
            self.autonat_private_until = None;
        }
        if !force
            && let Some(next_allowed) = self.dialback_cooldowns.get(&peer_id)
            && *next_allowed > now
        {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: cooldown until {:?}", *next_allowed);
            return;
        }
        if let Some(until) = self.direct_upgrade_cooldowns.get(&peer_id).copied() {
            if until > now {
                if force {
                    self.direct_upgrade_cooldowns.remove(&peer_id);
                } else {
                    debug!("libp2p dcutr: skipping dial-back to {peer_id}: direct-upgrade cooldown until {:?}", until);
                    return;
                }
            } else {
                self.direct_upgrade_cooldowns.remove(&peer_id);
            }
        }

        let local_has_fresh_observed = self.has_fresh_local_observed_candidate(now);
        let remote_is_fresh =
            remote_candidates_last_seen.is_some_and(|seen| now.saturating_duration_since(seen) <= DCUTR_REMOTE_CANDIDATE_FRESHNESS);
        if !local_has_fresh_observed || remote_candidates_count == 0 || !remote_is_fresh {
            info!(
                "libp2p dcutr preflight defer for {}: reason={} local_fresh_observed={} local_candidates={} remote_candidates={} remote_fresh={}",
                peer_id,
                reason,
                local_has_fresh_observed,
                self.local_dcutr_candidates().len(),
                remote_candidates_count,
                remote_is_fresh
            );
            self.force_identify_refresh(peer_id, "preflight_defer");
            self.refresh_relay_connection(peer_id, "preflight_defer");
            self.schedule_dcutr_retry(peer_id, "preflight_defer", false);
            return;
        }

        let local_candidates = self.local_dcutr_candidates();
        if local_candidates.is_empty() {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.dcutr().record_dialback_skipped_no_external();
            }
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no usable external address");
            self.schedule_dcutr_retry(peer_id, "no_usable_local_candidates", false);
            return;
        }

        let Some(relay) = &self.active_relay else {
            debug!("libp2p dcutr: no active relay available for dial-back to {peer_id}");
            self.schedule_dcutr_retry(peer_id, "no_active_relay", false);
            return;
        };
        let relay_peer = relay.relay_peer;

        let mut circuit_addr = relay.circuit_base.clone();
        strip_peer_suffix(&mut circuit_addr, *self.swarm.local_peer_id());
        if !circuit_addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
            circuit_addr.push(Protocol::P2pCircuit);
        }
        circuit_addr.push(Protocol::P2p(peer_id));
        let chosen_dial_addrs = vec![circuit_addr.clone()];

        info!(
            "libp2p dcutr dial-back attempt to {peer_id} reason={reason} local_candidates={} remote_candidates={} chosen_dial_addrs={:?}",
            local_candidates.len(),
            remote_candidates_count,
            chosen_dial_addrs
        );

        let opts = DialOpts::peer_id(peer_id)
            .addresses(chosen_dial_addrs)
            .condition(PeerCondition::Always)
            .extend_addresses_through_behaviour()
            .build();

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.dcutr().record_dialback_attempt();
        }
        match self.swarm.dial(opts) {
            Ok(()) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                self.clear_dcutr_retry(peer_id);
                info!("libp2p dcutr: initiated dial-back to {peer_id} via relay {}", relay_peer);
            }
            Err(err) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.dcutr().record_dialback_failure();
                }
                warn!("libp2p dcutr: failed to dial {peer_id} via relay {}: {err}", relay_peer);
                self.force_identify_refresh(peer_id, "dialback_dial_error");
                self.refresh_relay_connection(peer_id, "dialback_dial_error");
                self.schedule_dcutr_retry(peer_id, "dialback_dial_error", true);
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

fn extract_remote_dcutr_candidates(listen_addrs: &[Multiaddr], allow_private_addrs: bool) -> Vec<Multiaddr> {
    let mut candidates: Vec<_> = listen_addrs
        .iter()
        .filter(|addr| {
            if !is_tcp_dialable(addr) {
                return false;
            }
            let Some(ip) = candidate_ip_addr(addr) else {
                return false;
            };
            is_usable_dcutr_candidate_ip(ip, allow_private_addrs)
        })
        .cloned()
        .collect();
    candidates.sort_by_key(|addr| {
        let score = candidate_ip_addr(addr)
            .map(|ip| {
                if kaspa_utils::networking::IpAddress::new(ip).is_publicly_routable() {
                    3_u8
                } else if allow_private_addrs {
                    2_u8
                } else {
                    1_u8
                }
            })
            .unwrap_or(0_u8);
        (std::cmp::Reverse(score), addr.to_string())
    });
    candidates.dedup();
    candidates
}

fn candidate_ip_addr(addr: &Multiaddr) -> Option<std::net::IpAddr> {
    let mut ip = None;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
            _ => {}
        }
    }
    ip
}

fn is_usable_dcutr_candidate_ip(ip: std::net::IpAddr, allow_private_addrs: bool) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    if allow_private_addrs {
        return true;
    }
    kaspa_utils::networking::IpAddress::new(ip).is_publicly_routable()
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

fn extract_relay_peer(addr: &Multiaddr) -> Option<PeerId> {
    let components: Vec<_> = addr.iter().collect();
    for window in components.windows(2) {
        if let [Protocol::P2p(peer), Protocol::P2pCircuit] = window {
            return Some(*peer);
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
        } else if after_circuit && let Protocol::P2p(peer_id) = p {
            return Some(peer_id);
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
    if let Some(Protocol::P2p(last)) = addr.iter().last()
        && last == peer_id
    {
        addr.pop();
    }
}

fn addr_uses_relay(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| matches!(p, Protocol::P2pCircuit))
}

fn relay_probe_base(addr: &Multiaddr) -> Multiaddr {
    let mut base = Multiaddr::empty();
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2pCircuit) {
            break;
        }
        base.push(protocol);
    }
    base
}

fn insert_relay_peer(addr: &Multiaddr, relay_peer: PeerId) -> Multiaddr {
    let mut out = Multiaddr::empty();
    let mut inserted = false;
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2pCircuit) && !inserted {
            out.push(Protocol::P2p(relay_peer));
            inserted = true;
        }
        out.push(protocol);
    }
    if !inserted {
        out.push(Protocol::P2p(relay_peer));
        out.push(Protocol::P2pCircuit);
    }
    out
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
    match endpoint {
        libp2p::core::ConnectedPoint::Dialer { address, .. } => addr_uses_relay(address),
        libp2p::core::ConnectedPoint::Listener { send_back_addr, local_addr } => {
            addr_uses_relay(send_back_addr) || addr_uses_relay(local_addr)
        }
    }
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
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::io::{ReadBuf, duplex};

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
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
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
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
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
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| ReservationHandle::noop())
        })
    }

    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        Box::pin(async { Vec::new() })
    }
}
