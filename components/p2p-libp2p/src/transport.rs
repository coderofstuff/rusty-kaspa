use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use log::warn;
use std::sync::Arc;
use std::sync::OnceLock;

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
        if !self.config.mode.is_enabled() {
            Box::pin(async move { Err(Libp2pError::Disabled) })
        } else {
            Box::pin(async move {
                let _ = metadata;
                Err(Libp2pError::NotImplemented)
            })
        }
    }
}

/// Outbound connector that prefers libp2p when enabled, otherwise falls back to TCP.
pub struct Libp2pOutboundConnector {
    config: Config,
    fallback: Arc<dyn OutboundConnector>,
}

impl Libp2pOutboundConnector {
    pub fn new(config: Config, fallback: Arc<dyn OutboundConnector>) -> Self {
        Self { config, fallback }
    }
}

impl OutboundConnector for Libp2pOutboundConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: CoreTransportMetadata,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        // Placeholder: fall back to TCP until libp2p dial is implemented.
        let mut metadata = metadata;
        metadata.capabilities.libp2p = true;
        metadata.path = kaspa_p2p_lib::PathKind::Unknown;
        if !self.config.mode.is_enabled() {
            return self.fallback.connect(address, metadata, handler);
        }

        static WARN_ONCE: OnceLock<()> = OnceLock::new();
        WARN_ONCE.get_or_init(|| warn!("libp2p mode enabled but libp2p connector is not implemented; falling back to TCP"));

        self.fallback.connect(address, metadata, handler)
    }
}
