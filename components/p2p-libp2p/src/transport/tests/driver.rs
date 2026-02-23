use super::*;

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
