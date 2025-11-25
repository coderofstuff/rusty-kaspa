use crate::{config::Config, transport::Libp2pError};

/// Placeholder libp2p service that will eventually own dial/listen/reservation logic.
#[derive(Clone)]
pub struct Libp2pService {
    config: Config,
}

impl Libp2pService {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Start the libp2p service. Currently returns `NotImplemented` when enabled.
    pub async fn start(&self) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }
        Err(Libp2pError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_disabled_returns_disabled() {
        let svc = Libp2pService::new(Config::default());
        let res = svc.start().await;
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }
}
