use crate::core::peer::PeerKey;
use crate::Router;
use kaspa_utils::networking::NetAddress;
use std::future::Future;
use std::sync::Arc;

/// Abstract transport connector used by the P2P connection handler.
///
/// This trait allows bridging different underlying transports (e.g., TCP, libp2p)
/// into the Router without coupling the handler to a specific transport.
pub trait TransportConnector: Send + Sync {
    type Error: Send + 'static;
    type Future<'a>: Future<Output = Result<(Arc<Router>, PeerKey), Self::Error>> + Send
    where
        Self: 'a;

    fn connect<'a>(&'a self, address: NetAddress) -> Self::Future<'a>;
}
