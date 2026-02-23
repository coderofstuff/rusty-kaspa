use clap::ValueEnum;
use kaspa_p2p_libp2p::{Mode as AdapterMode, Role as AdapterRole};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Libp2pMode {
    #[default]
    Off,
    Full,
    /// Alias for full until a narrower helper-only mode is introduced.
    Helper,
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
            libp2p_mode: Libp2pMode::Off,
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
