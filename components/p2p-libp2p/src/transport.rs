use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use log::info;
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

impl Default for Libp2pConnector {
    fn default() -> Self {
        Self { config: Config::default() }
    }
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
        if !self.config.mode.is_enabled() {
            return Box::pin(async move { Err(Libp2pError::Disabled) });
        }

        // TODO: integrate real libp2p dial and wrap in Router, returning metadata and PeerKey.
        Box::pin(async move {
            info!("libp2p connector stub invoked; dial not implemented");
            let _ = metadata;
            Err(Libp2pError::NotImplemented)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::str::FromStr;

    #[test]
    fn libp2p_connect_disabled() {
        let connector = Libp2pConnector::default();
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }

    #[test]
    fn libp2p_connect_enabled_stubbed() {
        let mut cfg = Config::default();
        cfg.mode = crate::Mode::Full;
        let connector = Libp2pConnector::new(cfg);
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::NotImplemented)));
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
        let mut metadata = metadata;
        metadata.capabilities.libp2p = true;
        metadata.path = kaspa_p2p_lib::PathKind::Unknown;
        if !self.config.mode.is_enabled() {
            return self.fallback.connect(address, metadata, handler);
        }

        static WARN_ONCE: OnceLock<()> = OnceLock::new();
        WARN_ONCE.get_or_init(|| warn!("libp2p mode enabled but libp2p connector is not implemented; falling back to TCP"));

        Box::pin(async move {
            Err(ConnectionError::ProtocolError(kaspa_p2p_lib::common::ProtocolError::Other("libp2p outbound connector unimplemented")))
        })
    }
}
