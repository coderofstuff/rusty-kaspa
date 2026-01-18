use futures_util::future::{join_all, BoxFuture};
use kaspa_utils::networking::NetAddress;
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelaySource {
    AddressGossip,
    Config,
    Manual,
}

impl RelaySource {
    fn bit(self) -> u8 {
        match self {
            RelaySource::AddressGossip => 1 << 0,
            RelaySource::Config => 1 << 1,
            RelaySource::Manual => 1 << 2,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RelayCandidateUpdate {
    pub key: String,
    pub address: Multiaddr,
    pub net_address: Option<NetAddress>,
    pub relay_peer_id: Option<PeerId>,
    pub capacity: Option<usize>,
    pub ttl: Option<Duration>,
    pub source: RelaySource,
}

pub trait RelayCandidateSource: Send + Sync {
    fn fetch_candidates<'a>(&'a self) -> BoxFuture<'a, Vec<RelayCandidateUpdate>>;
}

pub struct CompositeRelaySource {
    sources: Vec<Arc<dyn RelayCandidateSource>>,
}

impl CompositeRelaySource {
    pub fn new(sources: Vec<Arc<dyn RelayCandidateSource>>) -> Self {
        Self { sources }
    }
}

impl RelayCandidateSource for CompositeRelaySource {
    fn fetch_candidates<'a>(&'a self) -> BoxFuture<'a, Vec<RelayCandidateUpdate>> {
        Box::pin(async move {
            let futures = self.sources.iter().map(|source| source.fetch_candidates());
            let batches = join_all(futures).await;
            batches.into_iter().flatten().collect()
        })
    }
}

pub struct StaticRelaySource {
    candidates: Vec<RelayCandidateUpdate>,
}

impl StaticRelaySource {
    pub fn new(candidates: Vec<RelayCandidateUpdate>) -> Self {
        Self { candidates }
    }
}

impl RelayCandidateSource for StaticRelaySource {
    fn fetch_candidates<'a>(&'a self) -> BoxFuture<'a, Vec<RelayCandidateUpdate>> {
        let candidates = self.candidates.clone();
        Box::pin(async move { candidates })
    }
}

#[derive(Clone, Debug)]
pub struct RelayPoolConfig {
    pub max_relays: usize,
    pub max_peers_per_relay: usize,
    pub candidate_ttl: Duration,
    pub rotation_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub min_sources: usize,
    pub max_candidates: usize,
    pub score_half_life: Duration,
    pub rng_seed: Option<u64>,
}

impl RelayPoolConfig {
    pub fn new(max_relays: usize, max_peers_per_relay: usize) -> Self {
        Self {
            max_relays: max_relays.max(1),
            max_peers_per_relay: max_peers_per_relay.max(1),
            candidate_ttl: Duration::from_secs(30 * 60),
            rotation_interval: Duration::from_secs(45 * 60),
            backoff_base: Duration::from_secs(10),
            backoff_max: Duration::from_secs(10 * 60),
            min_sources: 2,
            max_candidates: 512,
            score_half_life: Duration::from_secs(30 * 60),
            rng_seed: None,
        }
    }

    fn decay_factor(&self, now: Instant, last_update: Instant) -> f64 {
        let half_life = self.score_half_life.as_secs_f64();
        if half_life <= f64::EPSILON {
            return 1.0;
        }
        let elapsed = now.saturating_duration_since(last_update).as_secs_f64();
        0.5_f64.powf(elapsed / half_life)
    }
}

#[derive(Clone, Debug)]
pub struct RelaySelection {
    pub key: String,
    pub address: Multiaddr,
    pub relay_peer_id: Option<PeerId>,
}

#[derive(Clone, Debug, Default)]
pub struct RelayCandidateStats {
    pub total: usize,
    pub eligible: usize,
    pub high_confidence: usize,
}

#[derive(Clone, Debug)]
struct RelayEntry {
    key: String,
    address: Multiaddr,
    net_address: Option<NetAddress>,
    relay_peer_id: Option<PeerId>,
    capacity: usize,
    expires_at: Instant,
    sources: u8,
    successes: u32,
    failures: u32,
    success_score: f64,
    failure_score: f64,
    last_latency_ms: Option<u64>,
    backoff_until: Option<Instant>,
    backoff_current: Duration,
    last_selected: Option<Instant>,
    first_seen: Instant,
    last_score_update: Instant,
}

impl RelayEntry {
    fn score(&self, now: Instant, config: &RelayPoolConfig) -> f64 {
        let decay = config.decay_factor(now, self.last_score_update);
        let success_score = self.success_score * decay;
        let failure_score = self.failure_score * decay;
        let mut score = (success_score * 10.0) - (failure_score * 20.0);
        if let Some(latency) = self.last_latency_ms {
            score -= latency as f64 / 100.0;
        }
        let sources = self.sources.count_ones() as f64;
        score += sources * 2.0;
        let uptime_minutes = now.saturating_duration_since(self.first_seen).as_secs_f64() / 60.0;
        score += uptime_minutes;
        score
    }

    fn apply_decay(&mut self, now: Instant, config: &RelayPoolConfig) {
        let decay = config.decay_factor(now, self.last_score_update);
        if (decay - 1.0).abs() > f64::EPSILON {
            self.success_score *= decay;
            self.failure_score *= decay;
            self.last_score_update = now;
        }
    }

    fn prefix_key(&self) -> Option<PrefixKey> {
        self.net_address.as_ref().map(|addr| PrefixKey::from(addr.ip.0))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum PrefixKey {
    V4([u8; 2]),
    V6([u8; 6]),
}

impl From<IpAddr> for PrefixKey {
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => {
                let octets = v4.octets();
                PrefixKey::V4([octets[0], octets[1]])
            }
            IpAddr::V6(v6) => {
                let octets = v6.octets();
                PrefixKey::V6([octets[0], octets[1], octets[2], octets[3], octets[4], octets[5]])
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RelayPool {
    config: RelayPoolConfig,
    entries: HashMap<String, RelayEntry>,
    rng: StdRng,
}

impl RelayPool {
    pub fn new(config: RelayPoolConfig) -> Self {
        let rng = match config.rng_seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };
        Self { config, entries: HashMap::new(), rng }
    }

    pub fn update_candidates(&mut self, now: Instant, updates: Vec<RelayCandidateUpdate>) {
        for update in updates {
            let ttl = update.ttl.unwrap_or(self.config.candidate_ttl);
            let capacity = update.capacity.unwrap_or(self.config.max_peers_per_relay);
            let key = update.key.clone();
            let address = update.address.clone();
            let net_address = update.net_address.clone();
            let relay_peer_id = update.relay_peer_id;
            let entry = self.entries.entry(key.clone()).or_insert_with(|| RelayEntry {
                key: key.clone(),
                address: address.clone(),
                net_address,
                relay_peer_id,
                capacity,
                expires_at: now + ttl,
                sources: update.source.bit(),
                successes: 0,
                failures: 0,
                success_score: 0.0,
                failure_score: 0.0,
                last_latency_ms: None,
                backoff_until: None,
                backoff_current: Duration::from_secs(0),
                last_selected: None,
                first_seen: now,
                last_score_update: now,
            });

            entry.address = update.address;
            if update.net_address.is_some() {
                entry.net_address = update.net_address;
            }
            if update.relay_peer_id.is_some() {
                entry.relay_peer_id = update.relay_peer_id;
            }
            entry.capacity = capacity;
            entry.expires_at = now + ttl;
            entry.sources |= update.source.bit();
        }

        self.trim_candidates(now);
    }

    pub fn prune_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    pub fn set_peer_id(&mut self, key: &str, peer_id: PeerId) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.relay_peer_id = Some(peer_id);
        }
    }

    pub fn record_success(&mut self, key: &str, latency: Option<Duration>, now: Instant) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.apply_decay(now, &self.config);
            entry.successes = entry.successes.saturating_add(1);
            entry.success_score += 1.0;
            entry.backoff_current = Duration::from_secs(0);
            entry.backoff_until = None;
            if let Some(latency) = latency {
                entry.last_latency_ms = Some(latency.as_millis() as u64);
            }
        }
    }

    pub fn record_failure(&mut self, key: &str, now: Instant) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.apply_decay(now, &self.config);
            entry.failures = entry.failures.saturating_add(1);
            entry.failure_score += 1.0;
            let next = if entry.backoff_current.is_zero() {
                self.config.backoff_base
            } else {
                (entry.backoff_current * 2).min(self.config.backoff_max)
            };
            entry.backoff_current = next;
            entry.backoff_until = Some(now + next);
        }
    }

    pub fn candidate_stats(&self, now: Instant) -> RelayCandidateStats {
        let total = self.entries.len();
        let eligible: Vec<&RelayEntry> = self
            .entries
            .values()
            .filter(|entry| entry.expires_at > now)
            .filter(|entry| entry.backoff_until.map(|until| until <= now).unwrap_or(true))
            .collect();
        let min_sources = self.config.min_sources.max(1);
        let high_confidence = eligible.iter().filter(|entry| entry.sources.count_ones() as usize >= min_sources).count();
        RelayCandidateStats { total, eligible: eligible.len(), high_confidence }
    }

    pub fn select_relays(&mut self, now: Instant) -> Vec<RelaySelection> {
        let mut eligible: Vec<&RelayEntry> = self
            .entries
            .values()
            .filter(|entry| entry.expires_at > now)
            .filter(|entry| entry.backoff_until.map(|until| until <= now).unwrap_or(true))
            .collect();

        if eligible.is_empty() {
            return Vec::new();
        }

        let min_sources = self.config.min_sources.max(1);
        let high_confidence: Vec<&RelayEntry> =
            eligible.iter().copied().filter(|entry| entry.sources.count_ones() as usize >= min_sources).collect();

        if min_sources > 1 && high_confidence.is_empty() {
            return Vec::new();
        }

        if !high_confidence.is_empty() {
            eligible = high_confidence;
        }

        eligible.sort_by(|a, b| {
            b.score(now, &self.config)
                .partial_cmp(&a.score(now, &self.config))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        let mut selected = Vec::new();
        let mut used_prefixes: HashSet<PrefixKey> = HashSet::new();
        let total_unique_prefixes = eligible.iter().filter_map(|e| e.prefix_key()).collect::<HashSet<_>>().len();

        for entry in eligible.iter() {
            if selected.len() >= self.config.max_relays {
                break;
            }
            if let Some(prefix) = entry.prefix_key() {
                if used_prefixes.contains(&prefix) && used_prefixes.len() < total_unique_prefixes {
                    continue;
                }
                used_prefixes.insert(prefix);
            }
            selected.push(*entry);
        }

        if selected.len() < self.config.max_relays {
            let mut remaining: Vec<&RelayEntry> =
                eligible.iter().filter(|entry| !selected.iter().any(|chosen| chosen.key == entry.key)).copied().collect();
            remaining.shuffle(&mut self.rng);
            for entry in remaining {
                if selected.len() >= self.config.max_relays {
                    break;
                }
                selected.push(entry);
            }
        }

        selected
            .into_iter()
            .map(|entry| RelaySelection { key: entry.key.clone(), address: entry.address.clone(), relay_peer_id: entry.relay_peer_id })
            .collect()
    }

    pub fn reservation_multiaddr(&self, key: &str) -> Option<Multiaddr> {
        let entry = self.entries.get(key)?;
        let peer_id = entry.relay_peer_id?;
        let mut addr = entry.address.clone();
        if !addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
            addr.push(Protocol::P2p(peer_id));
        }
        Some(addr)
    }

    pub fn probe_multiaddr(&self, key: &str) -> Option<Multiaddr> {
        self.entries.get(key).map(|entry| entry.address.clone())
    }

    pub fn is_rotation_due(&self, key: &str, now: Instant) -> bool {
        self.entries
            .get(key)
            .and_then(|entry| entry.last_selected)
            .map(|selected| now.saturating_duration_since(selected) >= self.config.rotation_interval)
            .unwrap_or(false)
    }

    pub fn mark_selected(&mut self, key: &str, now: Instant) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_selected = Some(now);
        }
    }

    fn trim_candidates(&mut self, now: Instant) {
        if self.entries.len() <= self.config.max_candidates {
            return;
        }
        let mut scored: Vec<(String, f64)> =
            self.entries.values().map(|entry| (entry.key.clone(), entry.score(now, &self.config))).collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let remove_count = self.entries.len().saturating_sub(self.config.max_candidates);
        for (key, _) in scored.into_iter().take(remove_count) {
            self.entries.remove(&key);
        }
    }
}

pub fn relay_key_from_parts(ip: IpAddr, port: u16) -> String {
    format!("{ip}:{port}")
}

pub fn relay_update_from_netaddr(
    net_address: NetAddress,
    relay_port: u16,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Result<RelayCandidateUpdate, libp2p::multiaddr::Error> {
    let key = relay_key_from_parts(net_address.ip.0, relay_port);
    let address = match net_address.ip.0 {
        IpAddr::V4(v4) => Multiaddr::from_str(&format!("/ip4/{}/tcp/{}", v4, relay_port))?,
        IpAddr::V6(v6) => Multiaddr::from_str(&format!("/ip6/{}/tcp/{}", v6, relay_port))?,
    };
    Ok(RelayCandidateUpdate {
        key,
        address,
        net_address: Some(NetAddress::new(net_address.ip, relay_port)),
        relay_peer_id: None,
        capacity,
        ttl: Some(ttl),
        source,
    })
}

pub fn relay_update_from_multiaddr(
    address: Multiaddr,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Option<RelayCandidateUpdate> {
    let mut ip: Option<IpAddr> = None;
    let mut port: Option<u16> = None;
    let mut relay_peer_id: Option<PeerId> = None;
    for protocol in address.iter() {
        match protocol {
            Protocol::Ip4(v4) => ip = Some(IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(IpAddr::V6(v6)),
            Protocol::Tcp(p) => port = Some(p),
            Protocol::P2p(peer) => relay_peer_id = Some(peer),
            _ => {}
        }
    }
    let ip = ip?;
    let port = port?;
    let key = relay_key_from_parts(ip, port);
    let net_address = NetAddress::new(ip.into(), port);
    Some(RelayCandidateUpdate { key, address, net_address: Some(net_address), relay_peer_id, capacity, ttl: Some(ttl), source })
}

pub fn relay_update_from_multiaddr_str(
    address: &str,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Option<RelayCandidateUpdate> {
    let addr = Multiaddr::from_str(address).ok()?;
    relay_update_from_multiaddr(addr, ttl, source, capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

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
}
