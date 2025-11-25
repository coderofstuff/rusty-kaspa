use crate::metadata::TransportMetadata;
use kaspa_utils::networking::NetAddress;
use std::future::Future;

/// Abstraction for establishing a transport connection.
///
/// This trait allows bridging different transports (TCP, libp2p relay/DCUtR, etc.)
/// into a common Router constructor without baking transport specifics into
/// the connection handler.
pub trait TransportConnector: Send + Sync {
    type Output;
    type Error;
    type Future<'a>: Future<Output = Result<(Self::Output, TransportMetadata), Self::Error>> + Send
    where
        Self: 'a;

    fn connect<'a>(&'a self, address: NetAddress) -> Self::Future<'a>;
}
