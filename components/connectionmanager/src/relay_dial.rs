use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use kaspa_addressmanager::NetAddress;
use kaspa_p2p_lib::{PathKind, Peer};
use kaspa_utils::networking::{NET_ADDRESS_SERVICE_LIBP2P_RELAY, RelayRole, is_synthetic_relay_ip};
use tokio::sync::Mutex as AsyncMutex;

use crate::ConnectionManager;

mod hint;

pub(super) const RELAY_TARGETS_PER_RELAY_LIMIT: usize = 1;
const RELAY_DIAL_COOLDOWN: Duration = Duration::from_secs(60);
const RELAY_DIAL_WINDOW: Duration = Duration::from_secs(60);
const RELAY_DIAL_MAX_PER_WINDOW: usize = 4;

#[derive(Debug)]
pub(super) struct RelayDialBudget {
    window_start: Instant,
    count: usize,
}

impl RelayDialBudget {
    pub(super) fn new() -> Self {
        Self { window_start: Instant::now(), count: 0 }
    }

    fn allow(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.window_start) >= RELAY_DIAL_WINDOW {
            self.window_start = now;
            self.count = 0;
        }
        if self.count >= RELAY_DIAL_MAX_PER_WINDOW {
            return false;
        }
        self.count += 1;
        true
    }
}

#[derive(Clone, Debug)]
pub(super) struct RelayDialTarget {
    pub(super) target_peer_id: String,
    pub(super) relay_key: String,
    pub(super) relay_peer_id: Option<String>,
    pub(super) diversity_key: String,
}

#[derive(Clone, Debug)]
pub(super) struct DialPlan {
    pub(super) address: NetAddress,
    pub(super) relay: Option<RelayDialTarget>,
}

impl ConnectionManager {
    pub(super) fn relay_dial_plan(address: &NetAddress) -> Option<(String, RelayDialTarget)> {
        if !Self::is_private_relay_target(address) {
            return None;
        }
        let target_peer_id = address.libp2p_peer_id.as_ref()?;
        let hint = address.relay_circuit_hint.as_ref()?;
        let relay_key = Self::relay_hint_key(hint)?;
        let relay_peer_id = Self::relay_hint_peer_id(hint);
        let diversity_key = relay_peer_id.clone().unwrap_or_else(|| relay_key.clone());
        let dial_addr = Self::relay_circuit_addr(hint, target_peer_id)?;
        Some((dial_addr, RelayDialTarget { target_peer_id: target_peer_id.clone(), relay_key, relay_peer_id, diversity_key }))
    }

    pub(super) fn is_private_relay_target(address: &NetAddress) -> bool {
        matches!(address.relay_role, Some(RelayRole::Private))
            || (!address.has_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY) && address.libp2p_peer_id.is_some())
    }

    pub(super) fn has_direct_tcp_target(address: &NetAddress) -> bool {
        !is_synthetic_relay_ip(address.ip)
            && !address.is_synthetic_relay_hint()
            && address.ip.is_publicly_routable()
            && address.port != 0
    }

    pub(super) fn assign_relay_for_target(
        assigned_relays_by_target: &mut HashMap<String, String>,
        target: &RelayDialTarget,
        diversity_keys: &[String],
    ) -> bool {
        if diversity_keys.is_empty() {
            return false;
        }
        if let Some(assigned) = assigned_relays_by_target.get(&target.target_peer_id)
            && !diversity_keys.iter().any(|key| key == assigned)
        {
            return false;
        }
        assigned_relays_by_target.entry(target.target_peer_id.clone()).or_insert_with(|| target.diversity_key.clone());
        true
    }

    pub(super) fn relay_diversity_keys(target: &RelayDialTarget, relay_peer_id_by_key: &HashMap<String, String>) -> Vec<String> {
        let mut keys = Vec::with_capacity(2);
        Self::push_unique_key(&mut keys, target.diversity_key.clone());
        if target.relay_peer_id.is_some() {
            Self::push_unique_key(&mut keys, target.relay_key.clone());
        } else if let Some(relay_peer_id) = relay_peer_id_by_key.get(&target.relay_key) {
            Self::push_unique_key(&mut keys, relay_peer_id.clone());
        }
        keys
    }

    fn push_unique_key(keys: &mut Vec<String>, key: String) {
        if !keys.iter().any(|candidate| candidate == &key) {
            keys.push(key);
        }
    }

    pub(super) fn relay_usage_count(usage: &HashMap<String, usize>, keys: &[String]) -> usize {
        keys.iter().filter_map(|key| usage.get(key).copied()).max().unwrap_or_default()
    }

    pub(super) fn reserve_relay_usage(usage: &mut HashMap<String, usize>, keys: &[String], value: usize) {
        for key in keys {
            usage.insert(key.clone(), value);
        }
    }

    pub(super) fn active_relay_usage(peer_by_address: &HashMap<SocketAddr, Peer>) -> HashMap<String, usize> {
        let mut usage: HashMap<String, usize> = HashMap::new();
        for peer in peer_by_address.values() {
            if !peer.is_outbound() {
                continue;
            }
            if let PathKind::Relay { relay_id: Some(relay_id) } = &peer.metadata().path {
                *usage.entry(relay_id.clone()).or_default() += 1;
            }
        }
        usage
    }

    pub(super) fn relay_circuit_addr(hint: &str, target_peer_id: &str) -> Option<String> {
        hint::relay_circuit_addr(hint, target_peer_id)
    }

    fn relay_hint_key(hint: &str) -> Option<String> {
        hint::relay_hint_key(hint)
    }

    pub(super) fn relay_hint_peer_id(hint: &str) -> Option<String> {
        hint::relay_hint_peer_id(hint)
    }

    pub(super) fn relay_hint_requires_bootstrap(relay_peer_id: Option<&str>, active_libp2p_peer_ids: &HashSet<String>) -> bool {
        relay_peer_id.is_some_and(|relay_peer_id| !active_libp2p_peer_ids.contains(relay_peer_id))
    }

    pub(super) fn relay_cooldown_allows(cooldowns: &mut HashMap<String, Instant>, key: &str, now: Instant) -> bool {
        if let Some(deadline) = cooldowns.get(key).copied() {
            if deadline > now {
                return false;
            }
            cooldowns.remove(key);
        }
        true
    }

    pub(super) fn set_relay_cooldown(cooldowns: &mut HashMap<String, Instant>, key: &str, deadline: Instant) {
        cooldowns.insert(key.to_string(), deadline);
    }

    pub(super) fn clear_relay_cooldown(cooldowns: &mut HashMap<String, Instant>, key: &str) {
        cooldowns.remove(key);
    }

    pub(super) async fn relay_dial_allowed(&self, target: &RelayDialTarget, now: Instant) -> bool {
        Self::relay_dial_allowed_with_gates(
            &self.relay_target_cooldowns,
            &self.relay_hint_cooldowns,
            &self.relay_dial_budget,
            target,
            now,
        )
        .await
    }

    pub(super) async fn relay_dial_allowed_with_gates(
        relay_target_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        relay_hint_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        relay_dial_budget: &AsyncMutex<RelayDialBudget>,
        target: &RelayDialTarget,
        now: Instant,
    ) -> bool {
        {
            let mut cooldowns = relay_target_cooldowns.lock().await;
            if !Self::relay_cooldown_allows(&mut cooldowns, &target.target_peer_id, now) {
                return false;
            }
        }
        {
            let mut cooldowns = relay_hint_cooldowns.lock().await;
            if !Self::relay_cooldown_allows(&mut cooldowns, &target.relay_key, now) {
                return false;
            }
        }

        let mut budget = relay_dial_budget.lock().await;
        budget.allow(now)
    }

    pub(super) async fn record_relay_dial_success(&self, target: &RelayDialTarget) {
        Self::record_relay_dial_success_with_gates(
            &self.relay_target_cooldowns,
            &self.relay_hint_cooldowns,
            &self.relay_peer_id_by_key,
            target,
        )
        .await;
    }

    pub(super) async fn record_relay_dial_success_with_gates(
        relay_target_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        relay_hint_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        relay_peer_id_by_key: &AsyncMutex<HashMap<String, String>>,
        target: &RelayDialTarget,
    ) {
        if let Some(relay_peer_id) = target.relay_peer_id.as_ref() {
            relay_peer_id_by_key.lock().await.insert(target.relay_key.clone(), relay_peer_id.clone());
        }
        {
            let mut cooldowns = relay_target_cooldowns.lock().await;
            Self::clear_relay_cooldown(&mut cooldowns, &target.target_peer_id);
        }
        {
            let mut cooldowns = relay_hint_cooldowns.lock().await;
            Self::clear_relay_cooldown(&mut cooldowns, &target.relay_key);
        }
    }

    pub(super) async fn record_relay_dial_failure(&self, target: &RelayDialTarget, now: Instant) {
        Self::record_relay_dial_failure_with_gates(&self.relay_target_cooldowns, &self.relay_hint_cooldowns, target, now).await;
    }

    pub(super) async fn record_relay_dial_failure_with_gates(
        relay_target_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        relay_hint_cooldowns: &AsyncMutex<HashMap<String, Instant>>,
        target: &RelayDialTarget,
        now: Instant,
    ) {
        let deadline = now + RELAY_DIAL_COOLDOWN;
        {
            let mut cooldowns = relay_target_cooldowns.lock().await;
            Self::set_relay_cooldown(&mut cooldowns, &target.target_peer_id, deadline);
        }
        {
            let mut cooldowns = relay_hint_cooldowns.lock().await;
            Self::set_relay_cooldown(&mut cooldowns, &target.relay_key, deadline);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use kaspa_p2p_lib::PeerProperties;
    use kaspa_p2p_lib::transport::{Capabilities, TransportMetadata};
    use kaspa_utils::networking::{IpAddress, PeerId};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;

    fn make_peer_with_path_and_direction(path: PathKind, ip: Ipv4Addr, caps: Capabilities, is_outbound: bool) -> Peer {
        let metadata = TransportMetadata { path, capabilities: caps, ..Default::default() };
        Peer::new(
            PeerId::default(),
            SocketAddr::from((ip, 16000)),
            is_outbound,
            Instant::now(),
            Arc::new(PeerProperties::default()),
            0,
            metadata,
        )
    }

    #[test]
    fn relay_circuit_addr_builds_from_ip_hint() {
        let addr = ConnectionManager::relay_circuit_addr("203.0.113.9:16112", "12D3KooTarget").unwrap();
        assert_eq!(addr, "/ip4/203.0.113.9/tcp/16112/p2p-circuit/p2p/12D3KooTarget");
    }

    #[test]
    fn relay_circuit_addr_builds_from_ipv6_hint() {
        let addr = ConnectionManager::relay_circuit_addr("[2001:db8::9]:16112", "12D3KooTarget").unwrap();
        assert_eq!(addr, "/ip6/2001:db8::9/tcp/16112/p2p-circuit/p2p/12D3KooTarget");
    }

    #[test]
    fn relay_circuit_addr_extends_multiaddr_hint() {
        let hint = "/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay";
        let addr = ConnectionManager::relay_circuit_addr(hint, "12D3KooTarget").unwrap();
        assert_eq!(addr, "/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay/p2p-circuit/p2p/12D3KooTarget");
    }

    #[test]
    fn relay_hint_peer_id_extracts_first_multiaddr_peer() {
        let hint = "/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay/p2p-circuit";
        assert_eq!(ConnectionManager::relay_hint_peer_id(hint).as_deref(), Some("12D3KooRelay"));
        assert!(ConnectionManager::relay_hint_peer_id("203.0.113.9:16112").is_none());
    }

    #[test]
    fn relay_hint_bootstrap_detection_requires_active_relay_session() {
        let relay_peer_id = "12D3KooRelay";
        let mut active = HashSet::new();
        assert!(ConnectionManager::relay_hint_requires_bootstrap(Some(relay_peer_id), &active));

        active.insert(relay_peer_id.to_string());
        assert!(!ConnectionManager::relay_hint_requires_bootstrap(Some(relay_peer_id), &active));
        assert!(!ConnectionManager::relay_hint_requires_bootstrap(None, &active));
    }

    #[test]
    fn relay_dial_plan_requires_fields() {
        let addr = NetAddress::new(IpAddress::from_str("10.0.0.1").unwrap(), 16112)
            .with_relay_role(Some(RelayRole::Private))
            .with_libp2p_peer_id(Some("12D3KooTarget".to_string()))
            .with_relay_circuit_hint(Some("/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay".to_string()));
        let (dial_addr, target) = ConnectionManager::relay_dial_plan(&addr).unwrap();
        assert_eq!(dial_addr, "/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay/p2p-circuit/p2p/12D3KooTarget");
        assert_eq!(target.target_peer_id, "12D3KooTarget");
        assert_eq!(target.relay_key, "203.0.113.9:16112");
        assert_eq!(target.relay_peer_id.as_deref(), Some("12D3KooRelay"));
        assert_eq!(target.diversity_key, "12D3KooRelay");
    }

    #[test]
    fn relay_dial_plan_accepts_private_targets_without_relay_service_bit() {
        let addr = NetAddress::new(IpAddress::from_str("10.0.0.1").unwrap(), 16112)
            .with_libp2p_peer_id(Some("12D3KooTarget".to_string()))
            .with_relay_circuit_hint(Some("/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay".to_string()));
        let (_dial_addr, target) = ConnectionManager::relay_dial_plan(&addr).expect("should classify target as private");
        assert_eq!(target.target_peer_id, "12D3KooTarget");
        assert_eq!(target.relay_key, "203.0.113.9:16112");
    }

    #[test]
    fn relay_dial_plan_rejects_public_relay_targets_without_private_role() {
        let addr = NetAddress::new(IpAddress::from_str("203.0.113.8").unwrap(), 16112)
            .with_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY)
            .with_libp2p_peer_id(Some("12D3KooTarget".to_string()))
            .with_relay_circuit_hint(Some("/ip4/203.0.113.9/tcp/16112/p2p/12D3KooRelay".to_string()));
        assert!(ConnectionManager::relay_dial_plan(&addr).is_none());
    }

    #[test]
    fn direct_fallback_requires_public_tcp_target() {
        let private = NetAddress::new(IpAddress::from_str("10.0.0.1").unwrap(), 16112);
        let synthetic_private =
            NetAddress::new(IpAddress::from_str("240.0.0.1").unwrap(), 16112).with_libp2p_peer_id(Some("12D3KooTarget".to_string()));
        let public = NetAddress::new(IpAddress::from_str("8.8.8.8").unwrap(), 16112);
        assert!(!ConnectionManager::has_direct_tcp_target(&private));
        assert!(!ConnectionManager::has_direct_tcp_target(&synthetic_private));
        assert!(ConnectionManager::has_direct_tcp_target(&public));
    }

    #[test]
    fn relay_assignment_helper_keeps_first_relay_per_target() {
        let mut assigned = HashMap::new();
        let first = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: Some("12D3KooRelayA".to_string()),
            diversity_key: "12D3KooRelayA".to_string(),
        };
        let second = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.10:16112".to_string(),
            relay_peer_id: Some("12D3KooRelayB".to_string()),
            diversity_key: "12D3KooRelayB".to_string(),
        };
        let relay_peer_id_by_key = HashMap::new();
        let first_keys = ConnectionManager::relay_diversity_keys(&first, &relay_peer_id_by_key);
        let second_keys = ConnectionManager::relay_diversity_keys(&second, &relay_peer_id_by_key);

        assert!(ConnectionManager::assign_relay_for_target(&mut assigned, &first, &first_keys));
        assert!(!ConnectionManager::assign_relay_for_target(&mut assigned, &second, &second_keys));
        assert_eq!(assigned.get("12D3KooTarget").map(String::as_str), Some("12D3KooRelayA"));
    }

    #[test]
    fn relay_usage_count_matches_known_relay_peer_alias_for_hint_without_peer_id() {
        let target = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: None,
            diversity_key: "203.0.113.9:16112".to_string(),
        };

        let mut relay_peer_id_by_key = HashMap::new();
        relay_peer_id_by_key.insert(target.relay_key.clone(), "12D3KooRelay".to_string());
        let keys = ConnectionManager::relay_diversity_keys(&target, &relay_peer_id_by_key);

        let mut usage = HashMap::new();
        usage.insert("12D3KooRelay".to_string(), 1);
        assert_eq!(ConnectionManager::relay_usage_count(&usage, &keys), 1);
    }

    #[test]
    fn relay_assignment_accepts_same_relay_via_key_alias() {
        let mut assigned = HashMap::new();
        let with_peer_id = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: Some("12D3KooRelay".to_string()),
            diversity_key: "12D3KooRelay".to_string(),
        };
        let without_peer_id = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: None,
            diversity_key: "203.0.113.9:16112".to_string(),
        };

        let mut relay_peer_id_by_key = HashMap::new();
        relay_peer_id_by_key.insert(with_peer_id.relay_key.clone(), "12D3KooRelay".to_string());
        let first_keys = ConnectionManager::relay_diversity_keys(&with_peer_id, &relay_peer_id_by_key);
        let second_keys = ConnectionManager::relay_diversity_keys(&without_peer_id, &relay_peer_id_by_key);

        assert!(ConnectionManager::assign_relay_for_target(&mut assigned, &with_peer_id, &first_keys));
        assert!(ConnectionManager::assign_relay_for_target(&mut assigned, &without_peer_id, &second_keys));
    }

    #[test]
    fn active_relay_usage_counts_outbound_relay_ids() {
        let outbound_r1_a = make_peer_with_path_and_direction(
            PathKind::Relay { relay_id: Some("relay-1".to_string()) },
            Ipv4Addr::new(10, 10, 0, 1),
            Capabilities { libp2p: true },
            true,
        );
        let outbound_r1_b = make_peer_with_path_and_direction(
            PathKind::Relay { relay_id: Some("relay-1".to_string()) },
            Ipv4Addr::new(10, 10, 0, 2),
            Capabilities { libp2p: true },
            true,
        );
        let outbound_r2 = make_peer_with_path_and_direction(
            PathKind::Relay { relay_id: Some("relay-2".to_string()) },
            Ipv4Addr::new(10, 10, 0, 3),
            Capabilities { libp2p: true },
            true,
        );
        let inbound_r1 = make_peer_with_path_and_direction(
            PathKind::Relay { relay_id: Some("relay-1".to_string()) },
            Ipv4Addr::new(10, 10, 0, 4),
            Capabilities { libp2p: true },
            false,
        );
        let outbound_unknown = make_peer_with_path_and_direction(
            PathKind::Relay { relay_id: None },
            Ipv4Addr::new(10, 10, 0, 5),
            Capabilities { libp2p: true },
            true,
        );

        let mut peers = HashMap::new();
        for peer in [outbound_r1_a, outbound_r1_b, outbound_r2, inbound_r1, outbound_unknown] {
            peers.insert(peer.net_address(), peer);
        }

        let usage = ConnectionManager::active_relay_usage(&peers);
        assert_eq!(usage.get("relay-1"), Some(&2));
        assert_eq!(usage.get("relay-2"), Some(&1));
        assert_eq!(usage.len(), 2);
    }

    #[test]
    fn relay_dial_budget_enforces_window_and_resets() {
        let now = Instant::now();
        let mut budget = RelayDialBudget { window_start: now, count: 0 };

        for _ in 0..RELAY_DIAL_MAX_PER_WINDOW {
            assert!(budget.allow(now), "dial should be allowed while under budget");
        }
        assert!(!budget.allow(now), "dial should be blocked once budget is exhausted");

        let before_reset = now + RELAY_DIAL_WINDOW - Duration::from_millis(1);
        assert!(!budget.allow(before_reset), "budget should still block before window expires");

        let after_reset = now + RELAY_DIAL_WINDOW;
        assert!(budget.allow(after_reset), "budget should reset once window expires");
    }

    #[test]
    fn relay_cooldown_allows_after_expiry_and_cleans_entry() {
        let now = Instant::now();
        let mut cooldowns = HashMap::new();
        cooldowns.insert("target".to_string(), now + Duration::from_secs(5));

        assert!(!ConnectionManager::relay_cooldown_allows(&mut cooldowns, "target", now));
        assert!(cooldowns.contains_key("target"));

        let at_deadline = now + Duration::from_secs(5);
        assert!(ConnectionManager::relay_cooldown_allows(&mut cooldowns, "target", at_deadline));
        assert!(!cooldowns.contains_key("target"));
    }

    #[test]
    fn relay_failure_and_success_update_target_and_hint_cooldowns() {
        let now = Instant::now();
        let deadline = now + RELAY_DIAL_COOLDOWN;
        let target = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: Some("12D3KooRelay".to_string()),
            diversity_key: "12D3KooRelay".to_string(),
        };

        let mut target_cooldowns = HashMap::new();
        let mut hint_cooldowns = HashMap::new();
        ConnectionManager::set_relay_cooldown(&mut target_cooldowns, &target.target_peer_id, deadline);
        ConnectionManager::set_relay_cooldown(&mut hint_cooldowns, &target.relay_key, deadline);
        assert_eq!(target_cooldowns.get(&target.target_peer_id).copied(), Some(deadline));
        assert_eq!(hint_cooldowns.get(&target.relay_key).copied(), Some(deadline));

        ConnectionManager::clear_relay_cooldown(&mut target_cooldowns, &target.target_peer_id);
        ConnectionManager::clear_relay_cooldown(&mut hint_cooldowns, &target.relay_key);
        assert!(!target_cooldowns.contains_key(&target.target_peer_id));
        assert!(!hint_cooldowns.contains_key(&target.relay_key));
    }

    #[tokio::test]
    async fn relay_dial_allowed_with_gates_honors_cooldowns_and_budget() {
        let target = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: Some("12D3KooRelay".to_string()),
            diversity_key: "12D3KooRelay".to_string(),
        };
        let now = Instant::now();

        let target_cooldowns = AsyncMutex::new(HashMap::new());
        let hint_cooldowns = AsyncMutex::new(HashMap::new());
        let budget = AsyncMutex::new(RelayDialBudget { window_start: now, count: 0 });

        {
            let mut cooldowns = target_cooldowns.lock().await;
            cooldowns.insert(target.target_peer_id.clone(), now + Duration::from_secs(30));
        }

        assert!(!ConnectionManager::relay_dial_allowed_with_gates(&target_cooldowns, &hint_cooldowns, &budget, &target, now).await);

        {
            let mut cooldowns = target_cooldowns.lock().await;
            cooldowns.insert(target.target_peer_id.clone(), now - Duration::from_secs(1));
        }

        for _ in 0..RELAY_DIAL_MAX_PER_WINDOW {
            assert!(ConnectionManager::relay_dial_allowed_with_gates(&target_cooldowns, &hint_cooldowns, &budget, &target, now).await);
        }
        assert!(!ConnectionManager::relay_dial_allowed_with_gates(&target_cooldowns, &hint_cooldowns, &budget, &target, now).await);
    }

    #[tokio::test]
    async fn record_relay_dial_failure_with_gates_sets_cooldowns() {
        let target = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: None,
            diversity_key: "203.0.113.9:16112".to_string(),
        };
        let now = Instant::now();

        let target_cooldowns = AsyncMutex::new(HashMap::new());
        let hint_cooldowns = AsyncMutex::new(HashMap::new());

        ConnectionManager::record_relay_dial_failure_with_gates(&target_cooldowns, &hint_cooldowns, &target, now).await;

        let target_deadline = target_cooldowns.lock().await.get(&target.target_peer_id).copied();
        let hint_deadline = hint_cooldowns.lock().await.get(&target.relay_key).copied();
        assert_eq!(target_deadline, Some(now + RELAY_DIAL_COOLDOWN));
        assert_eq!(hint_deadline, Some(now + RELAY_DIAL_COOLDOWN));
    }

    #[tokio::test]
    async fn record_relay_dial_success_with_gates_clears_cooldowns_and_persists_peer_id() {
        let target = RelayDialTarget {
            target_peer_id: "12D3KooTarget".to_string(),
            relay_key: "203.0.113.9:16112".to_string(),
            relay_peer_id: Some("12D3KooRelay".to_string()),
            diversity_key: "12D3KooRelay".to_string(),
        };
        let now = Instant::now();

        let target_cooldowns = AsyncMutex::new(HashMap::from([(target.target_peer_id.clone(), now + Duration::from_secs(30))]));
        let hint_cooldowns = AsyncMutex::new(HashMap::from([(target.relay_key.clone(), now + Duration::from_secs(30))]));
        let relay_peer_id_by_key = AsyncMutex::new(HashMap::new());

        ConnectionManager::record_relay_dial_success_with_gates(&target_cooldowns, &hint_cooldowns, &relay_peer_id_by_key, &target)
            .await;

        assert!(!target_cooldowns.lock().await.contains_key(&target.target_peer_id));
        assert!(!hint_cooldowns.lock().await.contains_key(&target.relay_key));
        assert_eq!(relay_peer_id_by_key.lock().await.get(&target.relay_key).cloned(), target.relay_peer_id);
    }
}
