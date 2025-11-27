use crate::transport::Libp2pStreamProvider;
use libp2p::Multiaddr;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct HelperApi {
    provider: Arc<dyn Libp2pStreamProvider>,
}

#[derive(Debug, thiserror::Error)]
pub enum HelperError {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("unknown action")]
    UnknownAction,
    #[error("dial failed: {0}")]
    DialFailed(String),
}

#[derive(Debug, Deserialize)]
struct HelperRequest {
    action: String,
    multiaddr: Option<String>,
}

impl HelperApi {
    pub fn new(provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self { provider }
    }

    pub async fn handle_json(&self, json: &str) -> Result<String, HelperError> {
        let req: HelperRequest = serde_json::from_str(json).map_err(|e| HelperError::Invalid(e.to_string()))?;
        match req.action.as_str() {
            "status" => Ok(r#"{"ok":true}"#.to_string()),
            "dial" => {
                let addr_str = req.multiaddr.ok_or_else(|| HelperError::Invalid("missing multiaddr".into()))?;
                let addr = Multiaddr::from_str(&addr_str).map_err(|e| HelperError::Invalid(e.to_string()))?;
                
                // Trigger the dial. We don't return the stream here, just success/failure of connection.
                let _ = self.provider.dial_multiaddr(addr).await.map_err(|e| HelperError::DialFailed(e.to_string()))?;
                
                Ok(r#"{"ok":true,"msg":"dial successful"}"#.to_string())
            }
            _ => Err(HelperError::UnknownAction),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TransportMetadata;
    use crate::transport::{BoxedLibp2pStream, StreamDirection};
    use crate::Libp2pError;
    use futures_util::future::BoxFuture;
    use kaspa_utils::networking::NetAddress;

    struct MockProvider;
    impl Libp2pStreamProvider for MockProvider {
        fn dial<'a>(&'a self, _: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async { Err(Libp2pError::Disabled) })
        }
        fn dial_multiaddr<'a>(&'a self, _: Multiaddr) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async { Err(Libp2pError::Disabled) }) // Mock implementation
        }
        fn listen<'a>(
            &'a self,
        ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async { Err(Libp2pError::Disabled) })
        }
        fn reserve<'a>(&'a self, _: Multiaddr) -> BoxFuture<'a, Result<(), Libp2pError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn helper_status_ok() {
        let api = HelperApi::new(Arc::new(MockProvider));
        let resp = api.handle_json(r#"{"action":"status"}"#).await.unwrap();
        assert!(resp.contains(r#""ok":true"#));
    }

    #[tokio::test]
    async fn helper_unknown_action_errors() {
        let api = HelperApi::new(Arc::new(MockProvider));
        let resp = api.handle_json(r#"{"action":"refresh"}"#).await;
        assert!(matches!(resp, Err(HelperError::UnknownAction)));
    }
}
