use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::{PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Libp2pError {
    #[error("libp2p connector not implemented yet")]
    NotImplemented,
    #[error("libp2p not enabled")]
    Disabled,
}

/// Placeholder libp2p transport connector. Will be expanded with real libp2p dial/listen logic.
#[derive(Clone)]
pub struct Libp2pConnector {
    pub config: Config,
}

impl Libp2pConnector {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl TransportConnector for Libp2pConnector {
    type Error = Libp2pError;
    type Future<'a> = BoxFuture<'a, Result<(Arc<Router>, TransportMetadata, PeerKey), Self::Error>>;

    fn connect<'a>(&'a self, _address: NetAddress) -> Self::Future<'a> {
        let metadata = TransportMetadata::default();
        // TODO: integrate real libp2p dial and wrap in Router.
        Box::pin(async move {
            let _ = metadata;
            Err(Libp2pError::NotImplemented)
        })
    }
}
