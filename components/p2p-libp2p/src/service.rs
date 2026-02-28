mod helper;
mod inbound;
mod meta_stream;
mod reservations;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::helper_api::HelperApi;
use crate::relay_auto::run_relay_auto_worker;
use crate::relay_pool::RelayCandidateSource;
use crate::reservations::ReservationManager;
use crate::transport::Libp2pStreamProvider;
use crate::{config::Config, transport::Libp2pError};

use helper::start_helper_listener;
use inbound::start_inbound_bridge;
use reservations::{ReservationState, reservation_worker};

use triggered::Listener;

const RESERVATION_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20 * 60);
const RESERVATION_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
const RESERVATION_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);

/// Placeholder libp2p service that will eventually own dial/listen/reservation logic.
#[derive(Clone)]
pub struct Libp2pService {
    config: Config,
    provider: Option<std::sync::Arc<dyn Libp2pStreamProvider>>,
    reservations: ReservationManager,
    relay_source: Option<Arc<dyn RelayCandidateSource>>,
    shutdown: Option<Listener>,
}

impl Libp2pService {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            provider: None,
            reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF),
            relay_source: None,
            shutdown: None,
        }
    }

    pub fn with_provider(config: Config, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self {
            config,
            provider: Some(provider),
            reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF),
            relay_source: None,
            shutdown: None,
        }
    }

    pub fn with_relay_source(mut self, relay_source: Arc<dyn RelayCandidateSource>) -> Self {
        self.relay_source = Some(relay_source);
        self
    }

    pub fn with_shutdown(mut self, shutdown: Listener) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    pub async fn shutdown(&self) {
        if let Some(provider) = &self.provider {
            provider.shutdown().await;
        }
    }

    pub async fn start(&self) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let provider = self.provider.as_ref().ok_or(Libp2pError::ProviderUnavailable)?.clone();

        if !self.config.reservations.is_empty() {
            let reservations = self.config.reservations.clone();
            let state = ReservationState::new(self.reservations.clone(), RESERVATION_REFRESH_INTERVAL);
            let provider_for_worker = provider.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move { reservation_worker(provider_for_worker, reservations, state, shutdown).await });
        }

        if self.config.reservations.is_empty()
            && let Some(relay_source) = self.relay_source.clone()
        {
            let provider_for_worker = provider.clone();
            let config = self.config.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move { run_relay_auto_worker(provider_for_worker, relay_source, config, shutdown).await });
        }

        if let Some(addr) = self.config.helper_listen {
            let api = HelperApi::new(provider.clone());
            start_helper_listener(addr, api, self.shutdown.clone()).await?;
        }

        Ok(())
    }

    /// Start an inbound listener using the provided stream provider and connection handler.
    /// This bridges libp2p streams into the tonic server via `serve_with_incoming`.
    pub async fn start_inbound(&self, handler: std::sync::Arc<kaspa_p2p_lib::ConnectionHandler>) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let provider = self.provider.as_ref().ok_or(Libp2pError::ProviderUnavailable)?.clone();
        start_inbound_bridge(provider, handler, self.shutdown.clone());
        Ok(())
    }
}
