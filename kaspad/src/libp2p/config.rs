use super::args::{Libp2pArgs, Libp2pRole};
use kaspa_p2p_libp2p::{
    AutoNatConfig, Config as AdapterConfig, ConfigBuilder as AdapterConfigBuilder, Identity as AdapterIdentity, Mode as AdapterMode,
    Role as AdapterRole,
};
use kaspa_rpc_core::{GetLibp2pStatusResponse, RpcLibp2pIdentity, RpcLibp2pMode};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

pub(crate) const DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE: usize = 8;
pub(crate) const DEFAULT_RELAY_CANDIDATE_TTL: Duration = Duration::from_secs(30 * 60);

/// Translate CLI/config args into the adapter config.
pub fn libp2p_config_from_args(args: &Libp2pArgs, app_dir: &Path, p2p_listen: SocketAddr) -> AdapterConfig {
    let mode = args.libp2p_mode;
    let role = args.libp2p_role;

    let identity_path = args.libp2p_identity_path.clone();
    let helper_listen = args.libp2p_helper_listen;
    let listen_port = args.libp2p_listen_port.unwrap_or_else(|| p2p_listen.port().saturating_add(1));
    let listen_addr = SocketAddr::new(p2p_listen.ip(), listen_port);
    let relay_inbound_cap = args.libp2p_relay_inbound_cap;
    let relay_inbound_unknown_cap = args.libp2p_relay_inbound_unknown_cap;
    let inbound_cap_private = args.libp2p_inbound_cap_private.unwrap_or(DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE);
    let max_relays = args.libp2p_max_relays.unwrap_or(1);
    let max_peers_per_relay = args.libp2p_max_peers_per_relay.unwrap_or(1);
    let reservations = args.libp2p_reservations.clone();
    let relay_candidates = args.libp2p_relay_candidates.clone();
    let external_multiaddrs = args.libp2p_external_multiaddrs.clone();
    let advertise_addresses = args.libp2p_advertise_addresses.clone();
    let relay_advertise_capacity = args.libp2p_relay_advertise_capacity.or(Some(max_peers_per_relay as u32));
    let relay_advertise_ttl_ms = args.libp2p_relay_advertise_ttl_ms.or(Some(DEFAULT_RELAY_CANDIDATE_TTL.as_millis() as u64));
    let autonat_allow_private = args.libp2p_autonat_allow_private;
    let autonat_confidence_threshold = args.libp2p_autonat_confidence_threshold.filter(|value| *value > 0);
    let relay_min_sources = args.libp2p_relay_min_sources.unwrap_or(2);
    let relay_rng_seed = args.libp2p_relay_rng_seed;
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

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
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
