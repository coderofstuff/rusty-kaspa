use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, Instant, SystemTime},
};

use duration_string::DurationString;
use futures_util::future::{join_all, try_join_all};
use itertools::Itertools;
use kaspa_addressmanager::{AddressManager, NetAddress};
use kaspa_core::{debug, info, warn};
use kaspa_p2p_lib::{ConnectionError, PathKind, Peer, common::ProtocolError};
use kaspa_utils::{
    networking::{NET_ADDRESS_SERVICE_LIBP2P_RELAY, RelayRole},
    triggers::SingleTrigger,
};
use parking_lot::Mutex as ParkingLotMutex;
use rand::{seq::SliceRandom, thread_rng};
use tokio::{
    select,
    sync::{
        Mutex as TokioMutex,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time::{MissedTickBehavior, interval},
};

const DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE: usize = 8;
const RELAY_DIAL_COOLDOWN: Duration = Duration::from_secs(60);
const RELAY_DIAL_WINDOW: Duration = Duration::from_secs(60);
const RELAY_DIAL_MAX_PER_WINDOW: usize = 4;
const RELAY_TARGETS_PER_RELAY_LIMIT: usize = 1;

#[derive(Debug)]
struct RelayDialBudget {
    window_start: Instant,
    count: usize,
}

impl RelayDialBudget {
    fn new() -> Self {
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
pub struct Libp2pRoleConfig {
    pub is_private: bool,
    pub libp2p_inbound_cap_private: usize,
}

static ROLE_CONFIG: OnceLock<RwLock<Libp2pRoleConfig>> = OnceLock::new();

fn role_config() -> &'static RwLock<Libp2pRoleConfig> {
    ROLE_CONFIG.get_or_init(|| {
        RwLock::new(Libp2pRoleConfig { is_private: false, libp2p_inbound_cap_private: DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE })
    })
}

pub fn set_libp2p_role_config(config: Libp2pRoleConfig) {
    *role_config().write().unwrap() = config;
}

fn current_role_config() -> Libp2pRoleConfig {
    role_config().read().unwrap().clone()
}

pub struct ConnectionManager {
    p2p_adaptor: Arc<kaspa_p2p_lib::Adaptor>,
    outbound_target: usize,
    inbound_limit: usize,
    /// Soft cap per relay identity for inbound libp2p connections.
    relay_inbound_cap: usize,
    /// Soft cap for inbound libp2p connections without a parsed relay id.
    relay_inbound_unknown_cap: usize,
    dns_seeders: &'static [&'static str],
    default_port: u16,
    address_manager: Arc<ParkingLotMutex<AddressManager>>,
    connection_requests: TokioMutex<HashMap<SocketAddr, ConnectionRequest>>,
    relay_target_cooldowns: TokioMutex<HashMap<String, Instant>>,
    relay_hint_cooldowns: TokioMutex<HashMap<String, Instant>>,
    relay_dial_budget: TokioMutex<RelayDialBudget>,
    force_next_iteration: UnboundedSender<()>,
    shutdown_signal: SingleTrigger,
}

#[derive(Clone, Debug)]
struct ConnectionRequest {
    next_attempt: SystemTime,
    is_permanent: bool,
    attempts: u32,
}

impl ConnectionRequest {
    fn new(is_permanent: bool) -> Self {
        Self { next_attempt: SystemTime::now(), is_permanent, attempts: 0 }
    }
}

#[derive(Clone, Debug)]
struct RelayDialTarget {
    target_peer_id: String,
    relay_key: String,
    relay_peer_id: Option<String>,
    diversity_key: String,
}

#[derive(Clone, Debug)]
struct DialPlan {
    address: NetAddress,
    relay: Option<RelayDialTarget>,
}

impl ConnectionManager {
    pub fn new(
        p2p_adaptor: Arc<kaspa_p2p_lib::Adaptor>,
        outbound_target: usize,
        inbound_limit: usize,
        relay_inbound_cap: Option<usize>,
        relay_inbound_unknown_cap: Option<usize>,
        dns_seeders: &'static [&'static str],
        default_port: u16,
        address_manager: Arc<ParkingLotMutex<AddressManager>>,
    ) -> Arc<Self> {
        let (tx, rx) = unbounded_channel::<()>();
        let manager = Arc::new(Self {
            p2p_adaptor,
            outbound_target,
            inbound_limit,
            relay_inbound_cap: relay_inbound_cap.unwrap_or(4),
            relay_inbound_unknown_cap: relay_inbound_unknown_cap.unwrap_or(8),
            address_manager,
            connection_requests: Default::default(),
            relay_target_cooldowns: TokioMutex::new(HashMap::new()),
            relay_hint_cooldowns: TokioMutex::new(HashMap::new()),
            relay_dial_budget: TokioMutex::new(RelayDialBudget::new()),
            force_next_iteration: tx,
            shutdown_signal: SingleTrigger::new(),
            dns_seeders,
            default_port,
        });
        manager.clone().start_event_loop(rx);
        manager.force_next_iteration.send(()).unwrap();
        manager
    }

    fn start_event_loop(self: Arc<Self>, mut rx: UnboundedReceiver<()>) {
        let mut ticker = interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(async move {
            loop {
                if self.shutdown_signal.trigger.is_triggered() {
                    break;
                }
                select! {
                    _ = rx.recv() => self.clone().handle_event().await,
                    _ = ticker.tick() => self.clone().handle_event().await,
                    _ = self.shutdown_signal.listener.clone() => break,
                }
            }
            debug!("Connection manager event loop exiting");
        });
    }

    async fn handle_event(self: Arc<Self>) {
        debug!("Starting connection loop iteration");
        let peers = self.p2p_adaptor.active_peers();
        let peer_by_address: HashMap<SocketAddr, Peer> = peers.into_iter().map(|peer| (peer.net_address(), peer)).collect();

        self.handle_connection_requests(&peer_by_address).await;
        self.handle_outbound_connections(&peer_by_address).await;
        self.handle_inbound_connections(&peer_by_address).await;
    }

    pub async fn add_connection_request(&self, address: SocketAddr, is_permanent: bool) {
        // If the request already exists, it resets the attempts count and overrides the `is_permanent` setting.
        self.connection_requests.lock().await.insert(address, ConnectionRequest::new(is_permanent));
        self.force_next_iteration.send(()).unwrap(); // We force the next iteration of the connection loop.
    }

    pub async fn stop(&self) {
        self.shutdown_signal.trigger.trigger()
    }

    async fn handle_connection_requests(self: &Arc<Self>, peer_by_address: &HashMap<SocketAddr, Peer>) {
        let mut requests = self.connection_requests.lock().await;
        let mut new_requests = HashMap::with_capacity(requests.len());
        for (address, request) in requests.iter() {
            let address = *address;
            let request = request.clone();
            let is_connected = peer_by_address.contains_key(&address);
            if is_connected && !request.is_permanent {
                // The peer is connected and the request is not permanent - no need to keep the request
                continue;
            }

            if !is_connected && request.next_attempt <= SystemTime::now() {
                debug!("Connecting to peer request {}", address);
                match self.p2p_adaptor.connect_peer(address.to_string()).await {
                    Err(err) => {
                        debug!("Failed connecting to peer request: {}, {}", address, err);
                        if request.is_permanent {
                            const MAX_ACCOUNTABLE_ATTEMPTS: u32 = 4;
                            let retry_duration =
                                Duration::from_secs(30u64 * 2u64.pow(min(request.attempts, MAX_ACCOUNTABLE_ATTEMPTS)));
                            debug!("Will retry peer request {} in {}", address, DurationString::from(retry_duration));
                            new_requests.insert(
                                address,
                                ConnectionRequest {
                                    next_attempt: SystemTime::now() + retry_duration,
                                    attempts: request.attempts + 1,
                                    is_permanent: true,
                                },
                            );
                        }
                    }
                    Ok(_) if request.is_permanent => {
                        // Permanent requests are kept forever
                        new_requests.insert(address, ConnectionRequest::new(true));
                    }
                    Ok(_) => {}
                }
            } else {
                new_requests.insert(address, request);
            }
        }

        *requests = new_requests;
    }

    async fn handle_outbound_connections(self: &Arc<Self>, peer_by_address: &HashMap<SocketAddr, Peer>) {
        let active_outbound: HashSet<kaspa_addressmanager::NetAddress> =
            peer_by_address.values().filter(|peer| peer.is_outbound()).map(|peer| peer.net_address().into()).collect();
        if active_outbound.len() >= self.outbound_target {
            return;
        }
        let active_libp2p_peer_ids: HashSet<String> = peer_by_address
            .values()
            .filter_map(|peer| {
                let md = peer.metadata();
                if md.capabilities.libp2p { md.libp2p_peer_id.clone() } else { None }
            })
            .collect();

        let mut missing_connections = self.outbound_target - active_outbound.len();
        let mut addr_iter = self.address_manager.lock().iterate_prioritized_random_addresses(active_outbound);
        let mut used_relays = Self::active_relay_usage(peer_by_address);
        let mut assigned_relays_by_target: HashMap<String, String> = HashMap::new();
        let mut progressing = true;
        let mut connecting = true;
        while connecting && missing_connections > 0 {
            if self.shutdown_signal.trigger.is_triggered() {
                return;
            }
            let mut addrs_to_connect = Vec::with_capacity(missing_connections);
            let mut jobs = Vec::with_capacity(missing_connections);
            for _ in 0..missing_connections {
                let Some(net_addr) = addr_iter.next() else {
                    connecting = false;
                    break;
                };
                if let Some((dial_addr, relay_target)) = Self::relay_dial_plan(&net_addr) {
                    if Self::relay_hint_requires_bootstrap(relay_target.relay_peer_id.as_deref(), &active_libp2p_peer_ids) {
                        debug!(
                            "Relay hint {} references relay peer {:?} without active libp2p session; allowing bootstrap dial",
                            relay_target.relay_key, relay_target.relay_peer_id
                        );
                    }
                    if !Self::assign_relay_for_target(&mut assigned_relays_by_target, &relay_target) {
                        continue;
                    }
                    let used = used_relays.get(&relay_target.diversity_key).copied().unwrap_or_default();
                    if used < RELAY_TARGETS_PER_RELAY_LIMIT {
                        let now = Instant::now();
                        if self.relay_dial_allowed(&relay_target, now).await {
                            used_relays.insert(relay_target.diversity_key.clone(), used + 1);
                            debug!("Connecting to relay target {}", &dial_addr);
                            addrs_to_connect.push(DialPlan { address: net_addr, relay: Some(relay_target) });
                            jobs.push(self.p2p_adaptor.connect_peer(dial_addr));
                            continue;
                        }
                    }
                }
                if net_addr.is_synthetic_relay_hint() {
                    continue;
                }
                if Self::is_private_relay_target(&net_addr) && !Self::has_direct_tcp_target(&net_addr) {
                    continue;
                }
                let socket_addr = SocketAddr::new(net_addr.ip.into(), net_addr.port).to_string();
                debug!("Connecting to {}", &socket_addr);
                addrs_to_connect.push(DialPlan { address: net_addr, relay: None });
                jobs.push(self.p2p_adaptor.connect_peer(socket_addr.clone()));
            }

            if progressing && !jobs.is_empty() {
                // Log only if progress was made
                info!(
                    "Connection manager: has {}/{} outgoing P2P connections, trying to obtain {} additional connection(s)...",
                    self.outbound_target - missing_connections,
                    self.outbound_target,
                    jobs.len(),
                );
                progressing = false;
            } else {
                debug!(
                    "Connection manager: outgoing: {}/{} , connecting: {}, iterator: {}",
                    self.outbound_target - missing_connections,
                    self.outbound_target,
                    jobs.len(),
                    addr_iter.len(),
                );
            }
            for (res, plan) in (join_all(jobs).await).into_iter().zip(addrs_to_connect) {
                match res {
                    Ok(_) => {
                        self.address_manager.lock().mark_connection_success(plan.address);
                        missing_connections -= 1;
                        progressing = true;
                        if let Some(relay) = plan.relay.as_ref() {
                            self.record_relay_dial_success(relay).await;
                        }
                    }
                    Err(ConnectionError::ProtocolError(ProtocolError::PeerAlreadyExists(_))) => {
                        // We avoid marking the existing connection as connection failure
                        debug!("Failed connecting to {:?}, peer already exists", plan.address);
                    }
                    Err(err) => {
                        debug!("Failed connecting to {:?}, err: {}", plan.address, err);
                        self.address_manager.lock().mark_connection_failure(plan.address);
                        if let Some(relay) = plan.relay.as_ref() {
                            self.record_relay_dial_failure(relay, Instant::now()).await;
                        }
                    }
                }
            }
        }

        if missing_connections > 0 && !self.dns_seeders.is_empty() {
            if missing_connections > self.outbound_target / 2 {
                // If we are missing more than half of our target, query all in parallel.
                // This will always be the case on new node start-up and is the most resilient strategy in such a case.
                self.dns_seed_many(self.dns_seeders.len()).await;
            } else {
                // Try to obtain at least twice the number of missing connections
                self.dns_seed_with_address_target(2 * missing_connections).await;
            }
        }
    }

    fn relay_dial_plan(address: &NetAddress) -> Option<(String, RelayDialTarget)> {
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

    fn is_private_relay_target(address: &NetAddress) -> bool {
        matches!(address.relay_role, Some(RelayRole::Private))
            || (!address.has_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY) && address.libp2p_peer_id.is_some())
    }

    fn has_direct_tcp_target(address: &NetAddress) -> bool {
        !address.is_synthetic_relay_hint() && address.ip.is_publicly_routable() && address.port != 0
    }

    fn assign_relay_for_target(assigned_relays_by_target: &mut HashMap<String, String>, target: &RelayDialTarget) -> bool {
        if let Some(assigned) = assigned_relays_by_target.get(&target.target_peer_id)
            && assigned != &target.diversity_key
        {
            return false;
        }
        assigned_relays_by_target.entry(target.target_peer_id.clone()).or_insert_with(|| target.diversity_key.clone());
        true
    }

    fn active_relay_usage(peer_by_address: &HashMap<SocketAddr, Peer>) -> HashMap<String, usize> {
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

    fn relay_circuit_addr(hint: &str, target_peer_id: &str) -> Option<String> {
        let mut base = if hint.starts_with('/') {
            hint.trim_end_matches('/').to_string()
        } else {
            let socket = SocketAddr::from_str(hint).ok()?;
            match socket.ip() {
                IpAddr::V4(ip) => format!("/ip4/{ip}/tcp/{}", socket.port()),
                IpAddr::V6(ip) => format!("/ip6/{ip}/tcp/{}", socket.port()),
            }
        };

        if !base.contains("/p2p-circuit") {
            base.push_str("/p2p-circuit");
        }
        base.push_str("/p2p/");
        base.push_str(target_peer_id);
        Some(base)
    }

    fn relay_hint_key(hint: &str) -> Option<String> {
        if hint.starts_with('/') {
            if let Some((ip, port)) = Self::parse_multiaddr_ip_port(hint) {
                return Some(SocketAddr::new(ip, port).to_string());
            }
            return Some(hint.trim_end_matches('/').to_string());
        }

        SocketAddr::from_str(hint).ok().map(|socket| socket.to_string())
    }

    fn relay_hint_peer_id(hint: &str) -> Option<String> {
        if !hint.starts_with('/') {
            return None;
        }
        let parts: Vec<&str> = hint.split('/').filter(|part| !part.is_empty()).collect();
        for idx in 0..parts.len().saturating_sub(1) {
            if parts[idx] == "p2p" {
                return Some(parts[idx + 1].to_string());
            }
        }
        None
    }

    fn relay_hint_requires_bootstrap(relay_peer_id: Option<&str>, active_libp2p_peer_ids: &HashSet<String>) -> bool {
        relay_peer_id.is_some_and(|relay_peer_id| !active_libp2p_peer_ids.contains(relay_peer_id))
    }

    fn parse_multiaddr_ip_port(hint: &str) -> Option<(IpAddr, u16)> {
        let parts: Vec<&str> = hint.split('/').filter(|part| !part.is_empty()).collect();
        let mut ip: Option<IpAddr> = None;
        let mut port: Option<u16> = None;
        for idx in 0..parts.len().saturating_sub(1) {
            match parts[idx] {
                "ip4" | "ip6" if ip.is_none() => {
                    if let Ok(parsed) = IpAddr::from_str(parts[idx + 1]) {
                        ip = Some(parsed);
                    }
                }
                "tcp" if port.is_none() => {
                    if let Ok(parsed) = parts[idx + 1].parse::<u16>() {
                        port = Some(parsed);
                    }
                }
                _ => {}
            }
        }
        match (ip, port) {
            (Some(ip), Some(port)) => Some((ip, port)),
            _ => None,
        }
    }

    fn relay_cooldown_allows(cooldowns: &mut HashMap<String, Instant>, key: &str, now: Instant) -> bool {
        if let Some(deadline) = cooldowns.get(key).copied() {
            if deadline > now {
                return false;
            }
            cooldowns.remove(key);
        }
        true
    }

    fn set_relay_cooldown(cooldowns: &mut HashMap<String, Instant>, key: &str, deadline: Instant) {
        cooldowns.insert(key.to_string(), deadline);
    }

    fn clear_relay_cooldown(cooldowns: &mut HashMap<String, Instant>, key: &str) {
        cooldowns.remove(key);
    }

    async fn relay_dial_allowed(&self, target: &RelayDialTarget, now: Instant) -> bool {
        {
            let mut cooldowns = self.relay_target_cooldowns.lock().await;
            if !Self::relay_cooldown_allows(&mut cooldowns, &target.target_peer_id, now) {
                return false;
            }
        }
        {
            let mut cooldowns = self.relay_hint_cooldowns.lock().await;
            if !Self::relay_cooldown_allows(&mut cooldowns, &target.relay_key, now) {
                return false;
            }
        }

        let mut budget = self.relay_dial_budget.lock().await;
        budget.allow(now)
    }

    async fn record_relay_dial_success(&self, target: &RelayDialTarget) {
        {
            let mut cooldowns = self.relay_target_cooldowns.lock().await;
            Self::clear_relay_cooldown(&mut cooldowns, &target.target_peer_id);
        }
        {
            let mut cooldowns = self.relay_hint_cooldowns.lock().await;
            Self::clear_relay_cooldown(&mut cooldowns, &target.relay_key);
        }
    }

    async fn record_relay_dial_failure(&self, target: &RelayDialTarget, now: Instant) {
        let deadline = now + RELAY_DIAL_COOLDOWN;
        {
            let mut cooldowns = self.relay_target_cooldowns.lock().await;
            Self::set_relay_cooldown(&mut cooldowns, &target.target_peer_id, deadline);
        }
        {
            let mut cooldowns = self.relay_hint_cooldowns.lock().await;
            Self::set_relay_cooldown(&mut cooldowns, &target.relay_key, deadline);
        }
    }

    async fn handle_inbound_connections(self: &Arc<Self>, peer_by_address: &HashMap<SocketAddr, Peer>) {
        let inbound: Vec<&Peer> = peer_by_address.values().filter(|peer| !peer.is_outbound()).collect();
        if self.inbound_limit == 0 {
            let futures = inbound.iter().map(|peer| self.p2p_adaptor.terminate(peer.key()));
            join_all(futures).await;
            return;
        }

        let mut libp2p_peers: Vec<&Peer> = inbound.iter().copied().filter(|p| Self::is_libp2p_peer(p)).collect();
        let direct_peers: Vec<&Peer> = inbound.into_iter().filter(|p| !Self::is_libp2p_peer(p)).collect();

        // Enforce per-relay buckets before global caps.
        let to_drop_relay = Self::relay_overflow(&libp2p_peers, self.relay_inbound_cap, self.relay_inbound_unknown_cap);
        if !to_drop_relay.is_empty() {
            let keep: HashSet<_> = to_drop_relay.iter().map(|p| p.key()).collect();
            Self::terminate_peers(&self.p2p_adaptor, &to_drop_relay).await;
            libp2p_peers.retain(|p| !keep.contains(&p.key()));
        }

        let role_config = current_role_config();
        if role_config.is_private {
            let private_cap = Self::private_libp2p_cap(self.inbound_limit, role_config.libp2p_inbound_cap_private);
            let to_drop = Self::drop_libp2p_over_cap(&libp2p_peers, private_cap);
            if !to_drop.is_empty() {
                let drop_keys: HashSet<_> = to_drop.iter().map(|peer| peer.key()).collect();
                Self::terminate_peers(&self.p2p_adaptor, &to_drop).await;
                libp2p_peers.retain(|p| !drop_keys.contains(&p.key()));
            }
        } else {
            let libp2p_cap = Self::public_libp2p_cap(self.inbound_limit);
            let to_drop = Self::drop_libp2p_over_cap(&libp2p_peers, libp2p_cap);
            if !to_drop.is_empty() {
                let drop_keys: HashSet<_> = to_drop.iter().map(|peer| peer.key()).collect();
                Self::terminate_peers(&self.p2p_adaptor, &to_drop).await;
                libp2p_peers.retain(|p| !drop_keys.contains(&p.key()));
            }
        }

        let total = libp2p_peers.len() + direct_peers.len();
        if total > self.inbound_limit {
            let mut remaining_to_drop = total.saturating_sub(self.inbound_limit);
            let mut futures = Vec::new();

            if remaining_to_drop > 0 && !libp2p_peers.is_empty() {
                let drop =
                    libp2p_peers.choose_multiple(&mut thread_rng(), remaining_to_drop.min(libp2p_peers.len())).cloned().collect_vec();
                futures.extend(drop.iter().map(|peer| self.p2p_adaptor.terminate(peer.key())));
                libp2p_peers.retain(|p| !drop.iter().any(|d| d.key() == p.key()));
                remaining_to_drop = libp2p_peers.len().saturating_add(direct_peers.len()).saturating_sub(self.inbound_limit);
            }

            if remaining_to_drop > 0 {
                let drop =
                    direct_peers.choose_multiple(&mut thread_rng(), remaining_to_drop.min(direct_peers.len())).cloned().collect_vec();
                futures.extend(drop.iter().map(|peer| self.p2p_adaptor.terminate(peer.key())));
            }

            join_all(futures).await;
        }
    }

    fn is_libp2p_peer(peer: &Peer) -> bool {
        let md = peer.metadata();
        md.capabilities.libp2p || matches!(md.path, PathKind::Relay { .. })
    }

    async fn terminate_peers(adaptor: &kaspa_p2p_lib::Adaptor, peers: &[&Peer]) {
        let futures = peers.iter().map(|peer| adaptor.terminate(peer.key()));
        join_all(futures).await;
    }

    fn drop_libp2p_over_cap<'a>(peers: &'a [&Peer], cap: usize) -> Vec<&'a Peer> {
        if peers.len() > cap { peers.choose_multiple(&mut thread_rng(), peers.len() - cap).cloned().collect_vec() } else { Vec::new() }
    }

    fn private_libp2p_cap(inbound_limit: usize, private_role_cap: usize) -> usize {
        private_role_cap.min(inbound_limit.max(1))
    }

    fn public_libp2p_cap(inbound_limit: usize) -> usize {
        (inbound_limit / 2).max(1)
    }

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
                let drop = peers.choose_multiple(&mut thread_rng(), peers.len() - cap).cloned().collect_vec();
                to_drop.extend(drop);
            }
        }
        to_drop
    }

    /// Queries DNS seeders in random order, one after the other, until obtaining `min_addresses_to_fetch` addresses
    async fn dns_seed_with_address_target(self: &Arc<Self>, min_addresses_to_fetch: usize) {
        let cmgr = self.clone();
        tokio::task::spawn_blocking(move || cmgr.dns_seed_with_address_target_blocking(min_addresses_to_fetch)).await.unwrap();
    }

    fn dns_seed_with_address_target_blocking(self: &Arc<Self>, mut min_addresses_to_fetch: usize) {
        let shuffled_dns_seeders = self.dns_seeders.choose_multiple(&mut thread_rng(), self.dns_seeders.len());
        for &seeder in shuffled_dns_seeders {
            // Query seeders sequentially until reaching the desired number of addresses
            let addrs_len = self.dns_seed_single(seeder);
            if addrs_len >= min_addresses_to_fetch {
                break;
            } else {
                min_addresses_to_fetch -= addrs_len;
            }
        }
    }

    /// Queries `num_seeders_to_query` random DNS seeders in parallel
    async fn dns_seed_many(self: &Arc<Self>, num_seeders_to_query: usize) -> usize {
        info!("Querying {} DNS seeders", num_seeders_to_query);
        let shuffled_dns_seeders = self.dns_seeders.choose_multiple(&mut thread_rng(), num_seeders_to_query);
        let jobs = shuffled_dns_seeders.map(|seeder| {
            let cmgr = self.clone();
            tokio::task::spawn_blocking(move || cmgr.dns_seed_single(seeder))
        });
        try_join_all(jobs).await.unwrap().into_iter().sum()
    }

    /// Query a single DNS seeder and add the obtained addresses to the address manager.
    ///
    /// DNS lookup is a blocking i/o operation so this function is assumed to be called
    /// from a blocking execution context.
    fn dns_seed_single(self: &Arc<Self>, seeder: &str) -> usize {
        info!("Querying DNS seeder {}", seeder);
        // Since the DNS lookup protocol doesn't come with a port, we must assume that the default port is used.
        let addrs = match (seeder, self.default_port).to_socket_addrs() {
            Ok(addrs) => addrs,
            Err(e) => {
                warn!("Error connecting to DNS seeder {}: {}", seeder, e);
                return 0;
            }
        };

        let addrs_len = addrs.len();
        info!("Retrieved {} addresses from DNS seeder {}", addrs_len, seeder);
        let mut amgr_lock = self.address_manager.lock();
        for addr in addrs {
            amgr_lock.add_address(NetAddress::new(addr.ip().into(), addr.port()));
        }

        addrs_len
    }

    /// Bans the given IP and disconnects from all the peers with that IP.
    ///
    /// _GO-KASPAD: BanByIP_
    pub async fn ban(&self, ip: IpAddr) {
        if self.ip_has_permanent_connection(ip).await {
            return;
        }
        for peer in self.p2p_adaptor.active_peers() {
            if peer.net_address().ip() == ip {
                self.p2p_adaptor.terminate(peer.key()).await;
            }
        }
        self.address_manager.lock().ban(ip.into());
    }

    /// Returns whether the given address is banned.
    pub async fn is_banned(&self, address: &SocketAddr) -> bool {
        !self.is_permanent(address).await && self.address_manager.lock().is_banned(address.ip().into())
    }

    /// Returns whether the given address is a permanent request.
    pub async fn is_permanent(&self, address: &SocketAddr) -> bool {
        self.connection_requests.lock().await.contains_key(address)
    }

    /// Returns whether the given IP has some permanent request.
    pub async fn ip_has_permanent_connection(&self, ip: IpAddr) -> bool {
        self.connection_requests.lock().await.iter().any(|(address, request)| request.is_permanent && address.ip() == ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaspa_p2p_lib::PeerProperties;
    use kaspa_p2p_lib::transport::{Capabilities, TransportMetadata};
    use kaspa_utils::networking::{IpAddress, PeerId, RelayRole};
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::str::FromStr;
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;

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

    fn make_peer_with_path(path: PathKind, ip: Ipv4Addr, caps: Capabilities) -> Peer {
        make_peer_with_path_and_direction(path, ip, caps, false)
    }

    fn make_relay_peer(relay_id: Option<&str>) -> Peer {
        let path = PathKind::Relay { relay_id: relay_id.map(|id| id.to_string()) };
        make_peer_with_path(path, Ipv4Addr::new(127, 0, 0, 1), Capabilities { libp2p: true })
    }

    static ROLE_CONFIG_TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn relay_overflow_drops_expected_counts() {
        let r1 = vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r1")), make_relay_peer(Some("r1"))]; // overflow by 1 when cap=2
        let r2 = vec![make_relay_peer(Some("r2")), make_relay_peer(Some("r2"))]; // fits cap
        let unknown = vec![make_relay_peer(None), make_relay_peer(None)]; // overflow by 1 when cap=1

        let mut all = Vec::new();
        all.extend(r1.iter());
        all.extend(r2.iter());
        all.extend(unknown.iter());

        let to_drop = ConnectionManager::relay_overflow(&all, 2, 1);
        let mut counts: HashMap<Option<String>, usize> = HashMap::new();
        for peer in to_drop {
            let relay = match &peer.metadata().path {
                PathKind::Relay { relay_id } => relay_id.clone(),
                _ => None,
            };
            *counts.entry(relay).or_default() += 1;
        }

        assert_eq!(counts.get(&Some("r1".to_string())), Some(&1));
        assert_eq!(counts.get(&Some("r2".to_string())), None);
        assert_eq!(counts.get(&None), Some(&1));
    }

    #[test]
    fn relay_overflow_enforces_per_relay_cap() {
        let relay_a = PathKind::Relay { relay_id: Some("relay-a".into()) };
        let relay_b = PathKind::Relay { relay_id: Some("relay-b".into()) };
        let peers = vec![
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 1), Capabilities { libp2p: true }),
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 2), Capabilities { libp2p: true }),
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 3), Capabilities { libp2p: true }),
            make_peer_with_path(relay_b.clone(), Ipv4Addr::new(10, 0, 0, 4), Capabilities { libp2p: true }),
        ];
        let refs: Vec<&Peer> = peers.iter().collect();
        let dropped = ConnectionManager::relay_overflow(&refs, 2, 8);
        assert_eq!(dropped.len(), 1, "only one peer from relay-a should be dropped");
        assert!(dropped.iter().all(|p| matches!(p.metadata().path, PathKind::Relay { .. })));
    }

    #[test]
    fn relay_overflow_enforces_unknown_bucket() {
        let relay_unknown = PathKind::Relay { relay_id: None };
        let peers = vec![
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 1), Capabilities { libp2p: true }),
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 2), Capabilities { libp2p: true }),
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 3), Capabilities { libp2p: true }),
        ];
        let refs: Vec<&Peer> = peers.iter().collect();
        let dropped = ConnectionManager::relay_overflow(&refs, 4, 2);
        assert_eq!(dropped.len(), 1, "unknown relay bucket should drop overflow");
        assert!(matches!(dropped[0].metadata().path, PathKind::Relay { relay_id: None }));
    }

    #[test]
    fn libp2p_classification_detects_capability_and_relay_path() {
        // Direct path + libp2p capability => libp2p
        let direct_libp2p = make_peer_with_path(PathKind::Direct, Ipv4Addr::new(10, 0, 2, 1), Capabilities { libp2p: true });
        assert!(ConnectionManager::is_libp2p_peer(&direct_libp2p));

        // Relay path without capability still counts as libp2p for accounting.
        let relay_path =
            make_peer_with_path(PathKind::Relay { relay_id: None }, Ipv4Addr::new(10, 0, 2, 2), Capabilities { libp2p: false });
        assert!(ConnectionManager::is_libp2p_peer(&relay_path));

        // Direct path with no capability => non-libp2p.
        let direct_plain = make_peer_with_path(PathKind::Direct, Ipv4Addr::new(10, 0, 2, 3), Capabilities { libp2p: false });
        assert!(!ConnectionManager::is_libp2p_peer(&direct_plain));
    }

    #[test]
    fn drop_libp2p_over_cap_limits_set() {
        let peers =
            vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r2")), make_relay_peer(Some("r3")), make_relay_peer(Some("r4"))];
        let refs: Vec<&Peer> = peers.iter().collect();
        let to_drop = ConnectionManager::drop_libp2p_over_cap(&refs, 2);
        assert_eq!(to_drop.len(), 2);
    }

    #[test]
    fn drop_libp2p_over_cap_no_drop_when_under_cap() {
        let peers = vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r2"))];
        let refs: Vec<&Peer> = peers.iter().collect();
        let to_drop = ConnectionManager::drop_libp2p_over_cap(&refs, 4);
        assert!(to_drop.is_empty());
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
        let public = NetAddress::new(IpAddress::from_str("8.8.8.8").unwrap(), 16112);
        assert!(!ConnectionManager::has_direct_tcp_target(&private));
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

        assert!(ConnectionManager::assign_relay_for_target(&mut assigned, &first));
        assert!(!ConnectionManager::assign_relay_for_target(&mut assigned, &second));
        assert_eq!(assigned.get("12D3KooTarget").map(String::as_str), Some("12D3KooRelayA"));
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

    #[test]
    fn private_libp2p_cap_respects_inbound_limit() {
        assert_eq!(ConnectionManager::private_libp2p_cap(16, 8), 8);
        assert_eq!(ConnectionManager::private_libp2p_cap(4, 8), 4);
        assert_eq!(ConnectionManager::private_libp2p_cap(0, 8), 1);
    }

    #[test]
    fn public_libp2p_cap_has_floor_of_one() {
        assert_eq!(ConnectionManager::public_libp2p_cap(16), 8);
        assert_eq!(ConnectionManager::public_libp2p_cap(3), 1);
        assert_eq!(ConnectionManager::public_libp2p_cap(0), 1);
    }

    #[test]
    fn role_config_updates_are_visible_immediately() {
        let guard = ROLE_CONFIG_TEST_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
        let original = current_role_config();

        set_libp2p_role_config(Libp2pRoleConfig { is_private: true, libp2p_inbound_cap_private: 5 });
        let updated_private = current_role_config();
        assert!(updated_private.is_private);
        assert_eq!(updated_private.libp2p_inbound_cap_private, 5);

        set_libp2p_role_config(Libp2pRoleConfig { is_private: false, libp2p_inbound_cap_private: 11 });
        let updated_public = current_role_config();
        assert!(!updated_public.is_private);
        assert_eq!(updated_public.libp2p_inbound_cap_private, 11);

        set_libp2p_role_config(original);
        drop(guard);
    }
}
