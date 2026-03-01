use super::config::DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE;
use super::{Libp2pArgs, Libp2pMode, Libp2pRole, libp2p_config_from_args};
use kaspa_p2p_libp2p::{Identity as AdapterIdentity, Mode as AdapterMode, Role as AdapterRole};
use std::path::Path;

#[test]
fn libp2p_args_override_defaults() {
    let args = Libp2pArgs {
        libp2p_mode: Libp2pMode::Full,
        libp2p_identity_path: Some("/tmp/libp2p-id.key".into()),
        libp2p_helper_listen: Some("127.0.0.1:12345".parse().unwrap()),
        libp2p_relay_inbound_cap: Some(5),
        libp2p_relay_inbound_unknown_cap: Some(7),
        libp2p_autonat_allow_private: true,
        libp2p_autonat_confidence_threshold: Some(2),
        libp2p_relay_min_sources: Some(3),
        libp2p_relay_rng_seed: Some(42),
        ..Libp2pArgs::default()
    };

    let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
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
}

#[test]
fn libp2p_default_mode_is_bridge() {
    let cfg = libp2p_config_from_args(&Libp2pArgs::default(), Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
    assert_eq!(cfg.mode, AdapterMode::Bridge);
    assert_eq!(cfg.relay_min_sources, 2);
}

#[test]
fn libp2p_helper_mode_effective_is_full() {
    let args = Libp2pArgs { libp2p_mode: Libp2pMode::Helper, ..Libp2pArgs::default() };

    let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
    assert_eq!(cfg.mode, AdapterMode::Full);
}

#[test]
fn libp2p_explicit_default_mode_and_role_are_respected() {
    let args = Libp2pArgs { libp2p_mode: Libp2pMode::Off, libp2p_role: Libp2pRole::Auto, ..Libp2pArgs::default() };

    let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
    assert_eq!(cfg.mode, AdapterMode::Off);
    assert_eq!(cfg.role, AdapterRole::Auto);
}

#[test]
fn libp2p_role_auto_resolution() {
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
mod relay_source_tests {
    use super::super::relay_source::AddressManagerRelaySource;
    use kaspa_addressmanager::AddressManager;
    use kaspa_consensus_core::config::{Config as ConsensusConfig, params::SIMNET_PARAMS};
    use kaspa_core::task::tick::TickService;
    use kaspa_database::create_temp_db;
    use kaspa_database::prelude::ConnBuilder;
    use kaspa_p2p_libp2p::relay_pool::RelayCandidateSource;
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
