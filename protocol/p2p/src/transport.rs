use crate::core::peer::PeerKey;
use crate::Router;
use kaspa_utils::networking::{IpAddress, NetAddress, PeerId};
use std::future::Future;
use std::sync::Arc;

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

/// Metadata attached to a transport connection. This is intentionally
/// transport-agnostic and filled in by the connector (TCP/libp2p/etc).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportMetadata {
    pub peer_id: Option<PeerId>,
    pub reported_ip: Option<IpAddress>,
    pub path: PathKind,
    pub capabilities: Capabilities,
}

/// Abstract transport connector used by the P2P connection handler.
///
/// This trait allows bridging different underlying transports (e.g., TCP, libp2p)
/// into the Router without coupling the handler to a specific transport.
pub trait TransportConnector: Send + Sync {
    type Error: Send + 'static;
    type Future<'a>: Future<Output = Result<(Arc<Router>, TransportMetadata, PeerKey), Self::Error>> + Send
    where
        Self: 'a;

    fn connect<'a>(&'a self, address: NetAddress) -> Self::Future<'a>;
}
