use std::{net::SocketAddr, path::PathBuf};

/// Configuration for AutoNAT behaviour.
#[derive(Clone, Debug)]
pub struct AutoNatConfig {
    /// Enable AutoNAT client behaviour (address discovery).
    pub enable_client: bool,
    /// Enable AutoNAT server behaviour (help others discover addresses).
    pub enable_server: bool,
    /// Only allow AutoNAT server for publicly reachable nodes.
    pub server_only_if_public: bool,
    /// Maximum AutoNAT server requests per peer (rate limiting).
    pub max_server_requests_per_peer: usize,
    /// Confidence threshold for address confirmation (number of confirmations needed).
    pub confidence_threshold: usize,
}

impl Default for AutoNatConfig {
    fn default() -> Self {
        Self {
            enable_client: true,
            enable_server: true,
            server_only_if_public: false,
            max_server_requests_per_peer: 1,
            confidence_threshold: 3,
        }
    }
}

/// Runtime configuration for the libp2p adapter.
#[derive(Debug, Clone)]
pub struct Config {
    pub mode: Mode,
    pub role: Role,
    pub identity: Identity,
    pub helper_listen: Option<SocketAddr>,
    /// Socket addresses to bind libp2p listeners to.
    pub listen_addresses: Vec<SocketAddr>,
    pub relay_inbound_cap: Option<usize>,
    pub relay_inbound_unknown_cap: Option<usize>,
    pub libp2p_inbound_cap_private: usize,
    pub max_relays: usize,
    pub max_peers_per_relay: usize,
    pub relay_min_sources: usize,
    /// Optional list of relay reservation multiaddrs.
    pub reservations: Vec<String>,
    /// Optional list of relay candidate multiaddrs for auto selection.
    pub relay_candidates: Vec<String>,
    /// External multiaddrs to announce.
    pub external_multiaddrs: Vec<String>,
    /// Advertised socket addresses for non-libp2p awareness.
    pub advertise_addresses: Vec<SocketAddr>,
    /// Optional relay capacity to advertise via gossip.
    pub relay_advertise_capacity: Option<u32>,
    /// Optional relay TTL (ms) to advertise via gossip.
    pub relay_advertise_ttl_ms: Option<u64>,
    /// AutoNAT configuration.
    pub autonat: AutoNatConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Off,
            role: Role::Auto,
            identity: Identity::Ephemeral,
            helper_listen: None,
            listen_addresses: Vec::new(),
            relay_inbound_cap: None,
            relay_inbound_unknown_cap: None,
            libp2p_inbound_cap_private: 8,
            max_relays: 1,
            max_peers_per_relay: 1,
            relay_min_sources: 1,
            reservations: Vec::new(),
            relay_candidates: Vec::new(),
            external_multiaddrs: Vec::new(),
            advertise_addresses: Vec::new(),
            relay_advertise_capacity: None,
            relay_advertise_ttl_ms: None,
            autonat: AutoNatConfig::default(),
        }
    }
}

/// Identity source for the libp2p adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    Ephemeral,
    Persisted(PathBuf),
}

/// Builder for libp2p adapter configuration to centralize defaults and overrides.
#[derive(Debug, Clone, Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self { config: Config::default() }
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.config.mode = mode;
        self
    }

    pub fn role(mut self, role: Role) -> Self {
        self.config.role = role;
        self
    }

    pub fn identity(mut self, identity: Identity) -> Self {
        self.config.identity = identity;
        self
    }

    pub fn helper_listen(mut self, helper_listen: Option<SocketAddr>) -> Self {
        self.config.helper_listen = helper_listen;
        self
    }

    pub fn listen_addresses(mut self, listen_addresses: Vec<SocketAddr>) -> Self {
        self.config.listen_addresses = listen_addresses;
        self
    }

    pub fn relay_inbound_cap(mut self, cap: Option<usize>) -> Self {
        self.config.relay_inbound_cap = cap;
        self
    }

    pub fn relay_inbound_unknown_cap(mut self, cap: Option<usize>) -> Self {
        self.config.relay_inbound_unknown_cap = cap;
        self
    }

    pub fn libp2p_inbound_cap_private(mut self, cap: usize) -> Self {
        self.config.libp2p_inbound_cap_private = cap;
        self
    }

    pub fn max_relays(mut self, max_relays: usize) -> Self {
        self.config.max_relays = max_relays;
        self
    }

    pub fn max_peers_per_relay(mut self, max_peers_per_relay: usize) -> Self {
        self.config.max_peers_per_relay = max_peers_per_relay;
        self
    }

    pub fn relay_min_sources(mut self, relay_min_sources: usize) -> Self {
        self.config.relay_min_sources = relay_min_sources;
        self
    }

    pub fn reservations(mut self, reservations: Vec<String>) -> Self {
        self.config.reservations = reservations;
        self
    }

    pub fn relay_candidates(mut self, relay_candidates: Vec<String>) -> Self {
        self.config.relay_candidates = relay_candidates;
        self
    }

    pub fn external_multiaddrs(mut self, addrs: Vec<String>) -> Self {
        self.config.external_multiaddrs = addrs;
        self
    }

    pub fn advertise_addresses(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.config.advertise_addresses = addrs;
        self
    }

    pub fn relay_advertise_capacity(mut self, relay_advertise_capacity: Option<u32>) -> Self {
        self.config.relay_advertise_capacity = relay_advertise_capacity;
        self
    }

    pub fn relay_advertise_ttl_ms(mut self, relay_advertise_ttl_ms: Option<u64>) -> Self {
        self.config.relay_advertise_ttl_ms = relay_advertise_ttl_ms;
        self
    }

    pub fn autonat(mut self, autonat: AutoNatConfig) -> Self {
        self.config.autonat = autonat;
        self
    }

    pub fn build(self) -> Config {
        self.config
    }
}

/// Runtime mode for the libp2p adapter.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum Mode {
    #[default]
    Off,
    Full,
    /// Helper-only is currently an alias for full until a narrower mode is defined.
    Helper,
    Bridge,
}

impl Mode {
    pub fn effective(self) -> Self {
        match self {
            Mode::Helper => Mode::Full,
            Mode::Bridge => Mode::Bridge,
            other => other,
        }
    }

    pub fn is_enabled(self) -> bool {
        !matches!(self.effective(), Mode::Off)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum Role {
    Public,
    Private,
    #[default]
    Auto,
}
