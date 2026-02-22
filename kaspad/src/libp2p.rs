use clap::ValueEnum;
#[cfg(feature = "libp2p")]
use futures_util::future::BoxFuture;
#[cfg(feature = "libp2p")]
use kaspa_addressmanager::AddressManager;
#[cfg(feature = "libp2p")]
use kaspa_connectionmanager::{Libp2pRoleConfig, set_libp2p_role_config};
use kaspa_core::task::service::{AsyncService, AsyncServiceError, AsyncServiceFuture};
#[cfg(feature = "libp2p")]
use kaspa_p2p_flows::flow_context::FlowContext;
use kaspa_p2p_lib::{OutboundConnector, TcpConnector};
use kaspa_p2p_libp2p::SwarmStreamProvider;
#[cfg(feature = "libp2p")]
use kaspa_p2p_libp2p::relay_pool::{
    CompositeRelaySource, RelayCandidateSource, RelayCandidateUpdate, RelaySource, StaticRelaySource, relay_update_from_multiaddr_str,
    relay_update_from_netaddr,
};
use kaspa_p2p_libp2p::{
    AutoNatConfig, Config as AdapterConfig, ConfigBuilder as AdapterConfigBuilder, Identity as AdapterIdentity,
    Libp2pOutboundConnector, Mode as AdapterMode, Role as AdapterRole,
};
use kaspa_rpc_core::{GetLibp2pStatusResponse, RpcLibp2pIdentity, RpcLibp2pMode};
#[cfg(feature = "libp2p")]
use kaspa_utils::networking::{NET_ADDRESS_SERVICE_LIBP2P_RELAY, RelayRole};
#[cfg(feature = "libp2p")]
use kaspa_utils::triggers::SingleTrigger;
#[cfg(feature = "libp2p")]
use parking_lot::Mutex;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::OnceCell;
#[cfg(feature = "libp2p")]
use tokio::time::{Duration, sleep};

pub(crate) const DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE: usize = 8;
#[cfg(feature = "libp2p")]
const DEFAULT_RELAY_CANDIDATE_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Libp2pMode {
    Off,
    Full,
    /// Alias for full until a narrower helper-only mode is introduced.
    Helper,
    #[default]
    Bridge,
}

impl From<Libp2pMode> for AdapterMode {
    fn from(value: Libp2pMode) -> Self {
        match value {
            Libp2pMode::Off => AdapterMode::Off,
            Libp2pMode::Full => AdapterMode::Full,
            Libp2pMode::Helper => AdapterMode::Helper,
            Libp2pMode::Bridge => AdapterMode::Bridge,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Libp2pRole {
    Public,
    Private,
    #[default]
    Auto,
}

impl From<Libp2pRole> for AdapterRole {
    fn from(value: Libp2pRole) -> Self {
        match value {
            Libp2pRole::Public => AdapterRole::Public,
            Libp2pRole::Private => AdapterRole::Private,
            Libp2pRole::Auto => AdapterRole::Auto,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
pub struct Libp2pArgs {
    #[serde(default)]
    pub libp2p_mode: Libp2pMode,
    #[serde(default)]
    pub libp2p_role: Libp2pRole,
    /// True when `--libp2p-mode` was set on the CLI (including explicit default values like `off`).
    #[serde(skip)]
    pub libp2p_mode_set_from_cli: bool,
    /// True when `--libp2p-role` was set on the CLI (including explicit default values like `auto`).
    #[serde(skip)]
    pub libp2p_role_set_from_cli: bool,
    pub libp2p_identity_path: Option<PathBuf>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub libp2p_helper_listen: Option<SocketAddr>,
    /// Optional dedicated libp2p relay listen port (defaults to the p2p port + 1).
    pub libp2p_relay_listen_port: Option<u16>,
    /// Optional dedicated libp2p listen port (defaults to p2p port + 1).
    pub libp2p_listen_port: Option<u16>,
    /// Optional inbound caps for libp2p relay connections (per relay / unknown relay bucket).
    pub libp2p_relay_inbound_cap: Option<usize>,
    pub libp2p_relay_inbound_unknown_cap: Option<usize>,
    /// Max relay reservations to keep active (auto selection).
    pub libp2p_max_relays: Option<usize>,
    /// Max peers per relay (eclipse guard).
    pub libp2p_max_peers_per_relay: Option<usize>,
    /// Optional RNG seed for relay selection (deterministic tests).
    pub libp2p_relay_rng_seed: Option<u64>,
    /// Private-role inbound cap for libp2p connections.
    pub libp2p_inbound_cap_private: Option<usize>,
    /// Relay reservation multiaddrs.
    pub libp2p_reservations: Vec<String>,
    /// Static relay candidate multiaddrs for auto selection.
    pub libp2p_relay_candidates: Vec<String>,
    /// External multiaddrs to announce.
    pub libp2p_external_multiaddrs: Vec<String>,
    /// Addresses to advertise (non-libp2p aware).
    pub libp2p_advertise_addresses: Vec<SocketAddr>,
    /// Optional relay capacity to advertise via gossip.
    pub libp2p_relay_advertise_capacity: Option<u32>,
    /// Optional relay TTL (ms) to advertise via gossip.
    pub libp2p_relay_advertise_ttl_ms: Option<u64>,
    /// Allow AutoNAT to discover private IPs (for lab environments).
    pub libp2p_autonat_allow_private: bool,
    /// AutoNAT confidence threshold (successful probes required).
    pub libp2p_autonat_confidence_threshold: Option<usize>,
}

impl Default for Libp2pArgs {
    fn default() -> Self {
        Self {
            libp2p_mode: Libp2pMode::Bridge,
            libp2p_role: Libp2pRole::Auto,
            libp2p_mode_set_from_cli: false,
            libp2p_role_set_from_cli: false,
            libp2p_identity_path: None,
            libp2p_helper_listen: None,
            libp2p_relay_listen_port: None,
            libp2p_listen_port: None,
            libp2p_relay_inbound_cap: None,
            libp2p_relay_inbound_unknown_cap: None,
            libp2p_max_relays: None,
            libp2p_max_peers_per_relay: None,
            libp2p_relay_rng_seed: None,
            libp2p_inbound_cap_private: None,
            libp2p_reservations: Vec::new(),
            libp2p_relay_candidates: Vec::new(),
            libp2p_external_multiaddrs: Vec::new(),
            libp2p_advertise_addresses: Vec::new(),
            libp2p_relay_advertise_capacity: None,
            libp2p_relay_advertise_ttl_ms: None,
            libp2p_autonat_allow_private: false,
            libp2p_autonat_confidence_threshold: None,
        }
    }
}

/// Translate CLI/config args into the adapter config.
pub fn libp2p_config_from_args(args: &Libp2pArgs, app_dir: &Path, p2p_listen: SocketAddr) -> AdapterConfig {
    let env_mode = env::var("KASPAD_LIBP2P_MODE").ok().and_then(|s| parse_libp2p_mode(&s));
    let env_role = env::var("KASPAD_LIBP2P_ROLE").ok().and_then(|s| parse_libp2p_role(&s));
    let env_identity_path = env::var("KASPAD_LIBP2P_IDENTITY_PATH").ok().map(PathBuf::from);
    let env_helper_listen = env::var("KASPAD_LIBP2P_HELPER_LISTEN").ok().and_then(|s| s.parse::<SocketAddr>().ok());
    let env_relay_listen_port = env::var("KASPAD_LIBP2P_RELAY_LISTEN_PORT").ok().and_then(|s| s.parse::<u16>().ok());
    let env_listen_port = env::var("KASPAD_LIBP2P_LISTEN_PORT").ok().and_then(|s| s.parse::<u16>().ok());
    let env_max_relays = env::var("KASPAD_LIBP2P_MAX_RELAYS").ok().and_then(|s| s.parse::<usize>().ok());
    let env_max_peers_per_relay = env::var("KASPAD_LIBP2P_MAX_PEERS_PER_RELAY").ok().and_then(|s| s.parse::<usize>().ok());
    let env_relay_rng_seed = env::var("KASPAD_LIBP2P_RELAY_RNG_SEED").ok().and_then(|s| s.parse::<u64>().ok());
    let env_inbound_cap_private = env::var("KASPAD_LIBP2P_INBOUND_CAP_PRIVATE").ok().and_then(|s| s.parse::<usize>().ok());
    let env_relay_advertise_capacity = env::var("KASPAD_LIBP2P_RELAY_ADVERTISE_CAPACITY").ok().and_then(|s| s.parse::<u32>().ok());
    let env_relay_advertise_ttl_ms = env::var("KASPAD_LIBP2P_RELAY_ADVERTISE_TTL_MS").ok().and_then(|s| s.parse::<u64>().ok());
    let env_autonat_allow_private =
        env::var("KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE").ok().map(|s| s == "1" || s.eq_ignore_ascii_case("true")).unwrap_or(false);
    let env_autonat_confidence_threshold =
        env::var("KASPAD_LIBP2P_AUTONAT_CONFIDENCE_THRESHOLD").ok().and_then(|s| s.parse::<usize>().ok());

    let mode = if args.libp2p_mode_set_from_cli { args.libp2p_mode } else { env_mode.unwrap_or(args.libp2p_mode) };
    let role = if args.libp2p_role_set_from_cli { args.libp2p_role } else { env_role.unwrap_or(args.libp2p_role) };

    let identity_path = args.libp2p_identity_path.clone().or(env_identity_path);
    let helper_listen = args.libp2p_helper_listen.or(env_helper_listen);
    let listen_port = args
        .libp2p_relay_listen_port
        .or(env_relay_listen_port)
        .or(args.libp2p_listen_port)
        .or(env_listen_port)
        .unwrap_or_else(|| p2p_listen.port().saturating_add(1));
    let listen_addr = SocketAddr::new(p2p_listen.ip(), listen_port);
    let relay_inbound_cap =
        args.libp2p_relay_inbound_cap.or(env::var("KASPAD_LIBP2P_RELAY_INBOUND_CAP").ok().and_then(|s| s.parse().ok()));
    let relay_inbound_unknown_cap = args
        .libp2p_relay_inbound_unknown_cap
        .or(env::var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP").ok().and_then(|s| s.parse().ok()));
    let inbound_cap_private =
        args.libp2p_inbound_cap_private.or(env_inbound_cap_private).unwrap_or(DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE);
    let max_relays = args.libp2p_max_relays.or(env_max_relays).unwrap_or(1);
    let max_peers_per_relay = args.libp2p_max_peers_per_relay.or(env_max_peers_per_relay).unwrap_or(1);
    let reservations =
        merge_list(&args.libp2p_reservations, env::var("KASPAD_LIBP2P_RESERVATIONS").ok().as_deref(), |s| Some(s.to_string()));
    let relay_candidates =
        merge_list(&args.libp2p_relay_candidates, env::var("KASPAD_LIBP2P_RELAY_CANDIDATES").ok().as_deref(), |s| Some(s.to_string()));
    let external_multiaddrs =
        merge_list(&args.libp2p_external_multiaddrs, env::var("KASPAD_LIBP2P_EXTERNAL_MULTIADDRS").ok().as_deref(), |s| {
            Some(s.to_string())
        });
    let advertise_addresses =
        merge_list(&args.libp2p_advertise_addresses, env::var("KASPAD_LIBP2P_ADVERTISE_ADDRESSES").ok().as_deref(), |s| {
            s.parse::<SocketAddr>().ok()
        });
    let relay_advertise_capacity =
        args.libp2p_relay_advertise_capacity.or(env_relay_advertise_capacity).or(Some(max_peers_per_relay as u32));
    let relay_advertise_ttl_ms =
        args.libp2p_relay_advertise_ttl_ms.or(env_relay_advertise_ttl_ms).or(Some(DEFAULT_RELAY_CANDIDATE_TTL.as_millis() as u64));
    let autonat_allow_private = args.libp2p_autonat_allow_private || env_autonat_allow_private;
    let autonat_confidence_threshold =
        args.libp2p_autonat_confidence_threshold.or(env_autonat_confidence_threshold).filter(|value| *value > 0);
    let relay_min_sources = env::var("KASPAD_LIBP2P_RELAY_MIN_SOURCES").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(2);
    let relay_rng_seed = args.libp2p_relay_rng_seed.or(env_relay_rng_seed);
    let resolved_role = resolve_role(role, &reservations, helper_listen);

    let identity = identity_path
        .as_ref()
        .map(|path| resolve_identity_path(path, app_dir))
        .map(AdapterIdentity::Persisted)
        .unwrap_or(AdapterIdentity::Ephemeral);

    let mut autonat_config = AutoNatConfig { server_only_if_public: !autonat_allow_private, ..AutoNatConfig::default() };
    if let Some(threshold) = autonat_confidence_threshold {
        autonat_config.confidence_threshold = threshold;
    }

    AdapterConfigBuilder::new()
        .mode(AdapterMode::from(mode).effective())
        .role(AdapterRole::from(resolved_role))
        .identity(identity)
        .helper_listen(helper_listen)
        .listen_addresses(vec![listen_addr])
        .relay_inbound_cap(relay_inbound_cap)
        .relay_inbound_unknown_cap(relay_inbound_unknown_cap)
        .libp2p_inbound_cap_private(inbound_cap_private)
        .max_relays(max_relays)
        .max_peers_per_relay(max_peers_per_relay)
        .relay_min_sources(relay_min_sources)
        .relay_rng_seed(relay_rng_seed)
        .reservations(reservations)
        .relay_candidates(relay_candidates)
        .external_multiaddrs(external_multiaddrs)
        .advertise_addresses(advertise_addresses)
        .relay_advertise_capacity(relay_advertise_capacity)
        .relay_advertise_ttl_ms(relay_advertise_ttl_ms)
        .autonat(autonat_config)
        .build()
}

pub fn libp2p_status_from_config(config: &AdapterConfig, peer_id: Option<String>) -> GetLibp2pStatusResponse {
    let identity = match &config.identity {
        AdapterIdentity::Ephemeral => RpcLibp2pIdentity::Ephemeral,
        AdapterIdentity::Persisted(path) => RpcLibp2pIdentity::Persisted { path: path.display().to_string() },
    };

    let mode = match config.mode.effective() {
        AdapterMode::Off => RpcLibp2pMode::Off,
        AdapterMode::Full => RpcLibp2pMode::Full,
        AdapterMode::Helper => RpcLibp2pMode::Helper,
        AdapterMode::Bridge => RpcLibp2pMode::Full,
    };

    GetLibp2pStatusResponse { mode, peer_id, identity }
}

pub struct Libp2pRuntime {
    pub outbound: Arc<dyn OutboundConnector>,
    pub peer_id: Option<String>,
    pub identity: Option<kaspa_p2p_libp2p::Libp2pIdentity>,
    pub(crate) init_service: Option<Arc<Libp2pInitService>>,
    pub(crate) provider_cell: Option<Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>>,
}

pub fn libp2p_runtime_from_config(config: &AdapterConfig) -> Libp2pRuntime {
    if config.mode.is_enabled() {
        match kaspa_p2p_libp2p::Libp2pIdentity::from_config(config) {
            Ok(identity) => {
                let peer_id = Some(identity.peer_id_string());
                let provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>> = Arc::new(OnceCell::new());
                let outbound = Arc::new(Libp2pOutboundConnector::with_provider_cell(
                    config.clone(),
                    Arc::new(TcpConnector),
                    provider_cell.clone(),
                ));
                let init_service = Some(Arc::new(Libp2pInitService {
                    config: config.clone(),
                    identity: identity.clone(),
                    provider_cell: provider_cell.clone(),
                }));
                Libp2pRuntime { outbound, peer_id, identity: Some(identity), init_service, provider_cell: Some(provider_cell.clone()) }
            }
            Err(err) => {
                log::warn!("libp2p identity setup failed: {err}; falling back to TCP only");
                Libp2pRuntime {
                    outbound: Arc::new(TcpConnector),
                    peer_id: None,
                    identity: None,
                    init_service: None,
                    provider_cell: None,
                }
            }
        }
    } else {
        Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None, identity: None, init_service: None, provider_cell: None }
    }
}

#[cfg(not(feature = "libp2p"))]
pub fn libp2p_runtime_from_config(_config: &AdapterConfig) -> Libp2pRuntime {
    Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None, identity: None, init_service: None, provider_cell: None }
}

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
}

fn parse_libp2p_mode(s: &str) -> Option<Libp2pMode> {
    match s.to_ascii_lowercase().as_str() {
        "off" => Some(Libp2pMode::Off),
        "full" => Some(Libp2pMode::Full),
        "helper" => Some(Libp2pMode::Helper),
        "bridge" => Some(Libp2pMode::Bridge),
        _ => None,
    }
}

fn parse_libp2p_role(s: &str) -> Option<Libp2pRole> {
    match s.to_ascii_lowercase().as_str() {
        "public" => Some(Libp2pRole::Public),
        "private" => Some(Libp2pRole::Private),
        "auto" => Some(Libp2pRole::Auto),
        _ => None,
    }
}

fn resolve_role(role: Libp2pRole, reservations: &[String], helper_listen: Option<SocketAddr>) -> Libp2pRole {
    match role {
        Libp2pRole::Auto => {
            let _ = (reservations, helper_listen);
            Libp2pRole::Auto
        }
        other => other,
    }
}

fn merge_list<T, F: Fn(&str) -> Option<T>>(cli: &[T], env_val: Option<&str>, parse: F) -> Vec<T>
where
    T: Clone,
{
    if !cli.is_empty() {
        return cli.to_vec();
    }
    env_val.map(|s| s.split(',').filter_map(|item| parse(item.trim())).collect()).unwrap_or_default()
}

#[cfg(feature = "libp2p")]
struct AddressManagerRelaySource {
    address_manager: Arc<Mutex<AddressManager>>,
    ttl: Duration,
}

#[cfg(feature = "libp2p")]
impl AddressManagerRelaySource {
    fn new(address_manager: Arc<Mutex<AddressManager>>) -> Self {
        Self { address_manager, ttl: DEFAULT_RELAY_CANDIDATE_TTL }
    }
}

#[cfg(feature = "libp2p")]
impl RelayCandidateSource for AddressManagerRelaySource {
    fn fetch_candidates<'a>(&'a self) -> BoxFuture<'a, Vec<RelayCandidateUpdate>> {
        Box::pin(async move {
            let addresses = self.address_manager.lock().get_all_addresses();
            let mut updates = Vec::new();
            for addr in addresses {
                if !addr.has_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY) {
                    continue;
                }
                if matches!(addr.relay_role, Some(RelayRole::Private)) {
                    continue;
                }
                let Some(relay_port) = addr.relay_port else {
                    continue;
                };
                let ttl = addr.relay_ttl_ms.map(Duration::from_millis).unwrap_or(self.ttl);
                let capacity = addr.relay_capacity.map(|cap| cap as usize);
                match relay_update_from_netaddr(addr, relay_port, ttl, RelaySource::AddressGossip, capacity) {
                    Ok(update) => updates.push(update),
                    Err(err) => log::debug!("libp2p relay source: invalid relay candidate: {err}"),
                }
            }
            updates
        })
    }
}

#[cfg(feature = "libp2p")]
fn apply_role_update(
    flow_context: &FlowContext,
    role: AdapterRole,
    relay_port: Option<u16>,
    relay_capacity: Option<u32>,
    relay_ttl_ms: Option<u64>,
    inbound_cap_private: usize,
    libp2p_peer_id: Option<String>,
    relay_hint: Option<String>,
) {
    let (services, relay_port, relay_role, relay_capacity, relay_ttl_ms, libp2p_peer_id, relay_hint) =
        if matches!(role, AdapterRole::Public) {
            (NET_ADDRESS_SERVICE_LIBP2P_RELAY, relay_port, Some(RelayRole::Public), relay_capacity, relay_ttl_ms, None, None)
        } else {
            (0, None, Some(RelayRole::Private), None, None, libp2p_peer_id, relay_hint)
        };
    flow_context.set_libp2p_advertisement(services, relay_port, relay_capacity, relay_ttl_ms, relay_role, libp2p_peer_id, relay_hint);
    let is_private = !matches!(role, AdapterRole::Public);
    set_libp2p_role_config(Libp2pRoleConfig { is_private, libp2p_inbound_cap_private: inbound_cap_private });
}

pub(crate) struct Libp2pInitService {
    config: AdapterConfig,
    identity: kaspa_p2p_libp2p::Libp2pIdentity,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
}

impl AsyncService for Libp2pInitService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-init"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            let handle = tokio::runtime::Handle::current();
            log::info!("libp2p init: listen addresses {:?}", self.config.listen_addresses);
            let provider = match SwarmStreamProvider::with_handle(self.config.clone(), self.identity.clone(), handle) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("libp2p init failed to build provider: {e}");
                    return Err(AsyncServiceError::Service(e.to_string()));
                }
            };
            let _ = self.provider_cell.set(Arc::new(provider));
            log::info!("libp2p init: provider ready");
            Ok(())
        })
    }

    fn signal_exit(self: Arc<Self>) {}

    fn stop(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if let Some(provider) = self.provider_cell.get() {
                provider.shutdown().await;
            }
            Ok(())
        })
    }
}

#[cfg(feature = "libp2p")]
pub struct Libp2pNodeService {
    config: AdapterConfig,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
    flow_context: Arc<FlowContext>,
    libp2p_peer_id: Option<String>,
    shutdown: SingleTrigger,
}

#[cfg(feature = "libp2p")]
impl Libp2pNodeService {
    pub fn new(
        config: AdapterConfig,
        provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
        flow_context: Arc<FlowContext>,
        libp2p_peer_id: Option<String>,
    ) -> Self {
        Self { config, provider_cell, flow_context, libp2p_peer_id, shutdown: SingleTrigger::new() }
    }
}

#[cfg(feature = "libp2p")]
impl AsyncService for Libp2pNodeService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-node"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if !self.config.mode.is_enabled() {
                return Ok(());
            }
            log::info!("libp2p node service starting; waiting for provider and connection handler");

            let provider = loop {
                if let Some(provider) = self.provider_cell.get() {
                    log::info!("libp2p provider initialised; starting node service");
                    break provider.clone();
                }
                tokio::select! {
                    _ = self.shutdown.listener.clone() => {
                        return Err(AsyncServiceError::Service(
                            "libp2p node service stopped while waiting for provider initialization".to_owned(),
                        ));
                    }
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            };

            let relay_port = self.config.listen_addresses.first().map(|addr| addr.port());
            let relay_capacity = self.config.relay_advertise_capacity;
            let relay_ttl_ms = self.config.relay_advertise_ttl_ms;
            let inbound_cap_private = self.config.libp2p_inbound_cap_private;
            let libp2p_peer_id = self.libp2p_peer_id.clone();
            let current_role = Arc::new(Mutex::new(self.config.role));
            let current_hint: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

            apply_role_update(
                &self.flow_context,
                *current_role.lock(),
                relay_port,
                relay_capacity,
                relay_ttl_ms,
                inbound_cap_private,
                libp2p_peer_id.clone(),
                current_hint.lock().clone(),
            );

            if matches!(self.config.role, AdapterRole::Auto)
                && let Some(mut role_rx) = provider.role_updates()
            {
                let flow_context = self.flow_context.clone();
                let current_role = current_role.clone();
                let current_hint = current_hint.clone();
                let libp2p_peer_id = libp2p_peer_id.clone();
                tokio::spawn(async move {
                    let mut current = *role_rx.borrow();
                    loop {
                        if role_rx.changed().await.is_err() {
                            break;
                        }
                        let role = *role_rx.borrow();
                        if role == current {
                            continue;
                        }
                        current = role;
                        *current_role.lock() = role;
                        apply_role_update(
                            &flow_context,
                            role,
                            relay_port,
                            relay_capacity,
                            relay_ttl_ms,
                            inbound_cap_private,
                            libp2p_peer_id.clone(),
                            current_hint.lock().clone(),
                        );
                    }
                });
            }

            if let Some(mut hint_rx) = provider.relay_hint_updates() {
                let flow_context = self.flow_context.clone();
                let current_role = current_role.clone();
                let current_hint = current_hint.clone();
                let libp2p_peer_id = libp2p_peer_id.clone();
                let initial_hint = hint_rx.borrow().clone();
                if initial_hint.is_some() {
                    *current_hint.lock() = initial_hint.clone();
                    apply_role_update(
                        &flow_context,
                        *current_role.lock(),
                        relay_port,
                        relay_capacity,
                        relay_ttl_ms,
                        inbound_cap_private,
                        libp2p_peer_id.clone(),
                        initial_hint.clone(),
                    );
                }
                tokio::spawn(async move {
                    let mut current = initial_hint;
                    loop {
                        if hint_rx.changed().await.is_err() {
                            break;
                        }
                        let hint = hint_rx.borrow().clone();
                        if hint == current {
                            continue;
                        }
                        current = hint.clone();
                        *current_hint.lock() = hint.clone();
                        apply_role_update(
                            &flow_context,
                            *current_role.lock(),
                            relay_port,
                            relay_capacity,
                            relay_ttl_ms,
                            inbound_cap_private,
                            libp2p_peer_id.clone(),
                            hint,
                        );
                    }
                });
            }

            let handler = loop {
                if let Some(handler) = self.flow_context.connection_handler() {
                    log::info!("libp2p connection handler available; wiring inbound bridge");
                    break handler;
                }
                tokio::select! {
                    _ = self.shutdown.listener.clone() => {
                        return Err(AsyncServiceError::Service(
                            "libp2p node service stopped while waiting for connection handler".to_owned(),
                        ));
                    }
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            };

            let mut svc = kaspa_p2p_libp2p::Libp2pService::with_provider(self.config.clone(), provider)
                .with_shutdown(self.shutdown.listener.clone());
            if matches!(self.config.role, AdapterRole::Private | AdapterRole::Auto) {
                let mut sources: Vec<Arc<dyn RelayCandidateSource>> =
                    vec![Arc::new(AddressManagerRelaySource::new(self.flow_context.address_manager()))];
                if !self.config.relay_candidates.is_empty() {
                    let mut updates = Vec::new();
                    for candidate in &self.config.relay_candidates {
                        if let Some(update) =
                            relay_update_from_multiaddr_str(candidate, DEFAULT_RELAY_CANDIDATE_TTL, RelaySource::Config, None)
                        {
                            updates.push(update);
                        } else {
                            log::warn!("libp2p relay source: invalid relay candidate {}", candidate);
                        }
                    }
                    if !updates.is_empty() {
                        sources.push(Arc::new(StaticRelaySource::new(updates)));
                    }
                }
                let relay_source: Arc<dyn RelayCandidateSource> =
                    if sources.len() == 1 { sources[0].clone() } else { Arc::new(CompositeRelaySource::new(sources)) };
                svc = svc.with_relay_source(relay_source);
            }
            svc.start().await.map_err(|e| AsyncServiceError::Service(e.to_string()))?;
            if let Err(err) = svc.start_inbound(Arc::new(handler)).await {
                log::error!("libp2p inbound bridge failed to start: {err}");
                return Err(AsyncServiceError::Service(err.to_string()));
            }
            log::info!("libp2p node service started: bridging libp2p streams into Kaspa");

            self.shutdown.listener.clone().await;
            svc.shutdown().await;
            Ok(())
        })
    }

    fn signal_exit(self: Arc<Self>) {
        self.shutdown.trigger.trigger();
    }

    fn stop(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if let Some(provider) = self.provider_cell.get() {
                provider.shutdown().await;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::AdapterIdentity;
    use super::*;
    use std::env;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn set_env_var(key: &str, value: &str) {
        // SAFETY: ENV_LOCK serializes all test env mutations in this module.
        unsafe { env::set_var(key, value) }
    }

    fn remove_env_var(key: &str) {
        // SAFETY: ENV_LOCK serializes all test env mutations in this module.
        unsafe { env::remove_var(key) }
    }

    #[test]
    fn libp2p_env_overrides_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env_var("KASPAD_LIBP2P_MODE", "full");
        set_env_var("KASPAD_LIBP2P_IDENTITY_PATH", "/tmp/libp2p-id.key");
        set_env_var("KASPAD_LIBP2P_HELPER_LISTEN", "127.0.0.1:12345");
        set_env_var("KASPAD_LIBP2P_RELAY_INBOUND_CAP", "5");
        set_env_var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP", "7");
        set_env_var("KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE", "true");
        set_env_var("KASPAD_LIBP2P_AUTONAT_CONFIDENCE_THRESHOLD", "2");
        set_env_var("KASPAD_LIBP2P_RELAY_MIN_SOURCES", "3");
        set_env_var("KASPAD_LIBP2P_RELAY_RNG_SEED", "42");

        let cfg = libp2p_config_from_args(&Libp2pArgs::default(), Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg.mode, AdapterMode::Full);
        assert!(matches!(cfg.identity, AdapterIdentity::Persisted(ref path) if path.ends_with("libp2p-id.key")));
        assert_eq!(cfg.helper_listen, Some("127.0.0.1:12345".parse().unwrap()));
        assert_eq!(cfg.relay_inbound_cap, Some(5));
        assert_eq!(cfg.relay_inbound_unknown_cap, Some(7));
        assert!(!cfg.autonat.server_only_if_public); // allow_private=true -> server_only_if_public=false
        assert_eq!(cfg.autonat.confidence_threshold, 2);
        assert_eq!(cfg.role, AdapterRole::Auto);
        assert_eq!(cfg.libp2p_inbound_cap_private, DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE);
        assert_eq!(cfg.relay_min_sources, 3);
        assert_eq!(cfg.relay_rng_seed, Some(42));

        remove_env_var("KASPAD_LIBP2P_MODE");
        remove_env_var("KASPAD_LIBP2P_IDENTITY_PATH");
        remove_env_var("KASPAD_LIBP2P_HELPER_LISTEN");
        remove_env_var("KASPAD_LIBP2P_RELAY_INBOUND_CAP");
        remove_env_var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP");
        remove_env_var("KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE");
        remove_env_var("KASPAD_LIBP2P_AUTONAT_CONFIDENCE_THRESHOLD");
        remove_env_var("KASPAD_LIBP2P_RELAY_MIN_SOURCES");
        remove_env_var("KASPAD_LIBP2P_RELAY_RNG_SEED");
        remove_env_var("KASPAD_LIBP2P_ROLE");
    }

    #[test]
    fn libp2p_default_mode_is_bridge() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env_var("KASPAD_LIBP2P_MODE");
        let cfg = libp2p_config_from_args(&Libp2pArgs::default(), Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg.mode, AdapterMode::Bridge);
    }

    #[test]
    fn libp2p_cli_mode_overrides_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env_var("KASPAD_LIBP2P_MODE", "full");
        let args = Libp2pArgs { libp2p_mode: Libp2pMode::Helper, libp2p_mode_set_from_cli: true, ..Libp2pArgs::default() };

        let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        // Helper aliases to Full in the adapter config (effective mode).
        assert_eq!(cfg.mode, AdapterMode::Full);

        remove_env_var("KASPAD_LIBP2P_MODE");
        remove_env_var("KASPAD_LIBP2P_ROLE");
    }

    #[test]
    fn libp2p_explicit_default_mode_and_role_override_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env_var("KASPAD_LIBP2P_MODE", "full");
        set_env_var("KASPAD_LIBP2P_ROLE", "public");

        let args = Libp2pArgs {
            libp2p_mode: Libp2pMode::Off,
            libp2p_role: Libp2pRole::Auto,
            libp2p_mode_set_from_cli: true,
            libp2p_role_set_from_cli: true,
            ..Libp2pArgs::default()
        };

        let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg.mode, AdapterMode::Off);
        assert_eq!(cfg.role, AdapterRole::Auto);

        remove_env_var("KASPAD_LIBP2P_MODE");
        remove_env_var("KASPAD_LIBP2P_ROLE");
    }

    #[test]
    fn libp2p_role_auto_resolution() {
        let _guard = ENV_LOCK.lock().unwrap();
        let args_public = Libp2pArgs {
            libp2p_mode: Libp2pMode::Full,
            libp2p_helper_listen: Some("127.0.0.1:12345".parse().unwrap()),
            ..Libp2pArgs::default()
        };
        let cfg_public = libp2p_config_from_args(&args_public, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg_public.role, AdapterRole::Auto);

        let args_private = Libp2pArgs {
            libp2p_mode: Libp2pMode::Full,
            libp2p_reservations: vec!["/ip4/10.0.0.1/tcp/4001/p2p/peer".into()],
            ..Libp2pArgs::default()
        };
        let cfg_private = libp2p_config_from_args(&args_private, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg_private.role, AdapterRole::Auto);

        let args_default = Libp2pArgs { libp2p_mode: Libp2pMode::Full, ..Libp2pArgs::default() };
        let cfg_default = libp2p_config_from_args(&args_default, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg_default.role, AdapterRole::Auto);
    }

    #[cfg(feature = "libp2p")]
    mod relay_source_tests {
        use super::*;
        use kaspa_addressmanager::AddressManager;
        use kaspa_consensus_core::config::{Config as ConsensusConfig, params::SIMNET_PARAMS};
        use kaspa_core::task::tick::TickService;
        use kaspa_database::create_temp_db;
        use kaspa_database::prelude::ConnBuilder;
        use kaspa_utils::networking::{IpAddress, NET_ADDRESS_SERVICE_LIBP2P_RELAY, NetAddress};
        use std::str::FromStr;
        use std::sync::Arc;

        #[tokio::test]
        async fn relay_source_filters_by_service_and_port() {
            let db = create_temp_db!(ConnBuilder::default().with_files_limit(1));
            let config = ConsensusConfig::new(SIMNET_PARAMS);
            let (am, _) = AddressManager::new(Arc::new(config), db.1, Arc::new(TickService::default()));

            {
                let mut guard = am.lock();
                let good = NetAddress::new(IpAddress::from_str("10.0.0.1").unwrap(), 16111)
                    .with_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY)
                    .with_relay_port(Some(16112));
                let missing_port =
                    NetAddress::new(IpAddress::from_str("10.0.0.2").unwrap(), 16111).with_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY);
                let missing_service = NetAddress::new(IpAddress::from_str("10.0.0.3").unwrap(), 16111).with_relay_port(Some(16112));

                guard.add_address(good);
                guard.add_address(missing_port);
                guard.add_address(missing_service);
            }

            let source = AddressManagerRelaySource::new(am);
            let updates = source.fetch_candidates().await;
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].key, "10.0.0.1:16112");
        }
    }
}
