use super::*;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;

fn make_update(ip: IpAddr, port: u16, source: RelaySource) -> RelayCandidateUpdate {
    let key = relay_key_from_parts(ip, port);
    let address = Multiaddr::from_str(&format!("/ip4/{}/tcp/{}", ip, port)).unwrap();
    RelayCandidateUpdate {
        key,
        address,
        net_address: Some(NetAddress::new(ip.into(), port)),
        relay_peer_id: None,
        capacity: Some(1),
        ttl: Some(Duration::from_secs(60)),
        source,
    }
}

#[test]
fn relay_update_from_multiaddr_uses_peer_before_circuit() {
    let relay_peer = PeerId::random();
    let target_peer = PeerId::random();
    let address = Multiaddr::from_str(&format!("/ip4/203.0.113.9/tcp/16112/p2p/{relay_peer}/p2p-circuit/p2p/{target_peer}"))
        .expect("valid relay circuit multiaddr");

    let update = relay_update_from_multiaddr(address, Duration::from_secs(60), RelaySource::Config, Some(1))
        .expect("relay update should parse");
    assert_eq!(update.relay_peer_id, Some(relay_peer));
}

#[test]
fn relay_update_from_multiaddr_uses_peer_for_plain_multiaddr() {
    let relay_peer = PeerId::random();
    let address = Multiaddr::from_str(&format!("/ip4/203.0.113.9/tcp/16112/p2p/{relay_peer}")).expect("valid relay multiaddr");

    let update = relay_update_from_multiaddr(address, Duration::from_secs(60), RelaySource::Config, Some(1))
        .expect("relay update should parse");
    assert_eq!(update.relay_peer_id, Some(relay_peer));
}

#[test]
fn selection_prefers_diverse_prefixes() {
    let mut config = RelayPoolConfig::new(2, 1);
    config.min_sources = 1;
    config.rng_seed = Some(42);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let updates = vec![
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 2)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 1)), 16112, RelaySource::AddressGossip),
    ];
    pool.update_candidates(now, updates);

    let selected = pool.select_relays(now);
    assert_eq!(selected.len(), 2);
    let prefixes: HashSet<_> =
        selected.iter().filter_map(|sel| pool.entries.get(&sel.key)).filter_map(|entry| entry.prefix_key()).collect();
    assert_eq!(prefixes.len(), 2);
}

#[test]
fn selection_avoids_backoff() {
    let mut config = RelayPoolConfig::new(1, 1);
    config.rng_seed = Some(1);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let update = make_update(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 16112, RelaySource::AddressGossip);
    let key = update.key.clone();
    pool.update_candidates(now, vec![update]);
    pool.record_failure(&key, now);

    let selected = pool.select_relays(now);
    assert!(selected.is_empty());
}

#[test]
fn selection_prefers_high_confidence_sources() {
    let mut config = RelayPoolConfig::new(1, 1);
    config.min_sources = 2;
    config.rng_seed = Some(7);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let a = make_update(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)), 16112, RelaySource::AddressGossip);
    let b = make_update(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), 16112, RelaySource::AddressGossip);
    pool.update_candidates(now, vec![a.clone(), b.clone()]);

    let mut b_extra = b.clone();
    b_extra.source = RelaySource::Config;
    pool.update_candidates(now, vec![b_extra]);

    let selected = pool.select_relays(now);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].key, b.key);
}

#[test]
fn selection_requires_min_sources() {
    let mut config = RelayPoolConfig::new(1, 1);
    config.min_sources = 2;
    config.rng_seed = Some(9);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let update = make_update(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11)), 16112, RelaySource::AddressGossip);
    let key = update.key.clone();
    pool.update_candidates(now, vec![update]);
    assert!(pool.select_relays(now).is_empty());

    let mut update = make_update(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11)), 16112, RelaySource::Config);
    update.key = key.clone();
    pool.update_candidates(now, vec![update]);

    let selected = pool.select_relays(now);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].key, key);
}

#[test]
fn selection_is_deterministic_with_seed() {
    let now = Instant::now();
    let updates = vec![
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 16112, RelaySource::AddressGossip),
    ];

    let mut config_a = RelayPoolConfig::new(2, 1);
    config_a.rng_seed = Some(42);
    let mut pool_a = RelayPool::new(config_a);
    pool_a.update_candidates(now, updates.clone());

    let mut config_b = RelayPoolConfig::new(2, 1);
    config_b.rng_seed = Some(42);
    let mut pool_b = RelayPool::new(config_b);
    pool_b.update_candidates(now, updates);

    let selected_a: Vec<String> = pool_a.select_relays(now).into_iter().map(|sel| sel.key).collect();
    let selected_b: Vec<String> = pool_b.select_relays(now).into_iter().map(|sel| sel.key).collect();
    assert_eq!(selected_a, selected_b);
}

#[test]
fn selection_prefers_diversity_in_adversarial_pool() {
    let now = Instant::now();
    let mut updates = vec![
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 2)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 3)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 4)), 16112, RelaySource::AddressGossip),
    ];
    updates.push(make_update(IpAddr::V4(Ipv4Addr::new(10, 2, 0, 1)), 16112, RelaySource::AddressGossip));
    updates.push(make_update(IpAddr::V4(Ipv4Addr::new(10, 3, 0, 1)), 16112, RelaySource::AddressGossip));

    let mut config = RelayPoolConfig::new(3, 1);
    config.min_sources = 1;
    config.rng_seed = Some(11);
    let mut pool = RelayPool::new(config);
    pool.update_candidates(now, updates);

    let selected = pool.select_relays(now);
    assert_eq!(selected.len(), 3);
    let prefixes: HashSet<_> =
        selected.iter().filter_map(|sel| pool.entries.get(&sel.key)).filter_map(|entry| entry.prefix_key()).collect();
    assert_eq!(prefixes.len(), 3);
}

#[test]
fn poisoned_relay_scores_decay() {
    let now = Instant::now();
    let update_a = make_update(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)), 16112, RelaySource::AddressGossip);
    let update_b = make_update(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2)), 16112, RelaySource::AddressGossip);

    let mut config = RelayPoolConfig::new(1, 1);
    config.min_sources = 1;
    config.rng_seed = Some(5);
    config.score_half_life = Duration::from_secs(5);
    let mut pool = RelayPool::new(config);
    pool.update_candidates(now, vec![update_a.clone(), update_b.clone()]);

    pool.record_success(&update_a.key, Some(Duration::from_millis(20)), now);
    let initial = pool.select_relays(now);
    assert_eq!(initial[0].key, update_a.key);

    let later = now + Duration::from_secs(20);
    pool.record_success(&update_b.key, Some(Duration::from_millis(30)), later);
    pool.record_success(&update_a.key, Some(Duration::from_secs(5)), later);

    let selected = pool.select_relays(later);
    assert_eq!(selected[0].key, update_b.key);
}

#[test]
fn candidate_pool_is_capped() {
    let now = Instant::now();
    let mut config = RelayPoolConfig::new(1, 1);
    config.max_candidates = 3;
    let mut pool = RelayPool::new(config);

    let updates = vec![
        make_update(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 3)), 16112, RelaySource::AddressGossip),
        make_update(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 4)), 16112, RelaySource::AddressGossip),
    ];
    pool.update_candidates(now, updates);
    assert!(pool.entries.len() <= 3);
}

#[test]
fn reservation_multiaddr_appends_peer_id() {
    let mut config = RelayPoolConfig::new(1, 1);
    config.rng_seed = Some(3);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let mut update = make_update(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)), 16112, RelaySource::AddressGossip);
    let key = update.key.clone();
    let peer_id = PeerId::random();
    update.relay_peer_id = Some(peer_id);
    pool.update_candidates(now, vec![update]);

    let addr = pool.reservation_multiaddr(&key).expect("reservation addr");
    assert!(addr.iter().any(|p| matches!(p, Protocol::P2p(id) if id == peer_id)));
}

#[test]
fn candidate_observability_reports_backoff_and_sources() {
    let mut config = RelayPoolConfig::new(1, 1);
    config.min_sources = 2;
    config.rng_seed = Some(13);
    let mut pool = RelayPool::new(config);
    let now = Instant::now();

    let update = make_update(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 20)), 16112, RelaySource::AddressGossip);
    let key = update.key.clone();
    pool.update_candidates(now, vec![update]);
    pool.record_failure(&key, now);

    let entries = pool.candidate_observability(now);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, key);
    assert_eq!(entries[0].source_count, 1);
    assert!(entries[0].in_backoff);
    assert!(!entries[0].has_peer_id);
}
