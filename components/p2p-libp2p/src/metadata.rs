use kaspa_utils::networking::{IpAddress, PeerId};

/// How a connection reached us; used for accounting and relay budgeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathKind {
    Direct,
    Relay { relay_id: Option<String> },
    Unknown,
}

impl Default for PathKind {
    fn default() -> Self {
        PathKind::Unknown
    }
}

/// Capabilities advertised by the remote transport.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Capabilities {
    pub libp2p: bool,
}

/// Identity and path metadata attached to a transport.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportMetadata {
    pub peer_id: Option<PeerId>,
    pub reported_ip: Option<IpAddress>,
    pub path: PathKind,
    pub capabilities: Capabilities,
}

impl TransportMetadata {
    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn with_reported_ip(mut self, ip: IpAddress) -> Self {
        self.reported_ip = Some(ip);
        self
    }

    pub fn with_path(mut self, path: PathKind) -> Self {
        self.path = path;
        self
    }

    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
}
