use crate::transport::Libp2pStreamProvider;
use libp2p::Multiaddr;
use serde::Deserialize;
use serde_json::json;
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

enum HelperAction {
    Status,
    Dial(Multiaddr),
}

impl HelperApi {
    pub fn new(provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self { provider }
    }

    pub async fn handle_json(&self, json: &str) -> Result<String, HelperError> {
        let req: HelperRequest = serde_json::from_str(json).map_err(|e| HelperError::Invalid(e.to_string()))?;
        let action = HelperAction::try_from(req)?;
        let resp = match action {
            HelperAction::Status => json!({ "ok": true }),
            HelperAction::Dial(addr) => {
                // Trigger the dial. We don't return the stream here, just success/failure of connection.
                self.provider.dial_multiaddr(addr).await.map_err(|e| HelperError::DialFailed(e.to_string()))?;
                json!({ "ok": true, "msg": "dial successful" })
            }
        };

        serde_json::to_string(&resp).map_err(|e| HelperError::Invalid(e.to_string()))
    }

    pub(crate) fn error_response(err: &HelperError) -> String {
        json!({ "ok": false, "error": err.to_string() }).to_string()
    }
}

impl TryFrom<HelperRequest> for HelperAction {
    type Error = HelperError;

    fn try_from(req: HelperRequest) -> Result<Self, Self::Error> {
        match req.action.as_str() {
            "status" => Ok(Self::Status),
            "dial" => {
                let addr_str = req.multiaddr.ok_or_else(|| HelperError::Invalid("missing multiaddr".into()))?;
                let addr = Multiaddr::from_str(&addr_str).map_err(|e| HelperError::Invalid(e.to_string()))?;
                Ok(Self::Dial(addr))
            }
            _ => Err(HelperError::UnknownAction),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TransportMetadata;
    use crate::transport::{BoxedLibp2pStream, ReservationHandle, StreamDirection};
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
        ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>>
        {
            Box::pin(async { Err(Libp2pError::Disabled) })
        }
        fn reserve<'a>(&'a self, _: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
            Box::pin(async { Ok(ReservationHandle::noop()) })
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
