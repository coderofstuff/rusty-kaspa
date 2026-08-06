use std::sync::Arc;

use kaspa_core::task::service::{AsyncService, AsyncServiceError, AsyncServiceFuture};
use kaspa_p2p_lib::{OutboundConnector, TcpConnector};
use kaspa_p2p_libp2p::{Config as AdapterConfig, Libp2pOutboundConnector, SwarmStreamProvider};
use kaspa_utils::triggers::SingleTrigger;
use tokio::sync::OnceCell;

pub struct Libp2pRuntime {
    pub outbound: Arc<dyn OutboundConnector>,
    pub peer_id: Option<String>,
    pub identity: Option<kaspa_p2p_libp2p::Libp2pIdentity>,
    pub(crate) init_service: Option<Arc<Libp2pInitService>>,
    pub(crate) provider_cell: Option<Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>>,
}

pub fn libp2p_runtime_from_config(config: &AdapterConfig) -> Libp2pRuntime {
    if config.mode.is_enabled() {
        match kaspa_p2p_libp2p::Libp2pIdentity::from_config(config) {
            Ok(identity) => {
                let peer_id = Some(identity.peer_id_string());
                let provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>> = Arc::new(OnceCell::new());
                let outbound = Arc::new(Libp2pOutboundConnector::with_provider_cell(
                    config.clone(),
                    Arc::new(TcpConnector),
                    provider_cell.clone(),
                ));
                let init_service = Some(Arc::new(Libp2pInitService {
                    config: config.clone(),
                    identity: identity.clone(),
                    provider_cell: provider_cell.clone(),
                    shutdown: SingleTrigger::new(),
                }));
                Libp2pRuntime { outbound, peer_id, identity: Some(identity), init_service, provider_cell: Some(provider_cell.clone()) }
            }
            Err(err) => {
                log::warn!("libp2p identity setup failed: {err}; falling back to TCP only");
                Libp2pRuntime {
                    outbound: Arc::new(TcpConnector),
                    peer_id: None,
                    identity: None,
                    init_service: None,
                    provider_cell: None,
                }
            }
        }
    } else {
        Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None, identity: None, init_service: None, provider_cell: None }
    }
}

pub(crate) struct Libp2pInitService {
    config: AdapterConfig,
    identity: kaspa_p2p_libp2p::Libp2pIdentity,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
    shutdown: SingleTrigger,
}

impl AsyncService for Libp2pInitService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-init"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            let handle = tokio::runtime::Handle::current();
            log::debug!("libp2p init: listen addresses {:?}", self.config.listen_addresses);
            let provider = match SwarmStreamProvider::with_handle(self.config.clone(), self.identity.clone(), handle) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("libp2p init failed to build provider: {e}");
                    return Err(AsyncServiceError::Service(e.to_string()));
                }
            };
            let _ = self.provider_cell.set(Arc::new(provider));
            log::debug!("libp2p init: provider ready");
            self.shutdown.listener.clone().await;
            Ok(())
        })
    }

    fn signal_exit(self: Arc<Self>) {
        self.shutdown.trigger.trigger();
    }

    fn stop(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if let Some(provider) = self.provider_cell.get() {
                provider.shutdown().await;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaspa_p2p_libp2p::{ConfigBuilder, Mode};
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn init_service_remains_supervised_until_shutdown() {
        let config = ConfigBuilder::new().mode(Mode::Bridge).listen_addresses(vec!["127.0.0.1:0".parse().unwrap()]).build();
        let runtime = libp2p_runtime_from_config(&config);
        let service = runtime.init_service.expect("enabled runtime has an init service");
        let provider_cell = runtime.provider_cell.expect("enabled runtime has a provider cell");

        let start_task = tokio::spawn(service.clone().start());
        timeout(Duration::from_secs(5), async {
            while provider_cell.get().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provider initialization timed out");

        assert!(!start_task.is_finished(), "successful initialization must not consume AsyncRuntime supervision");

        service.clone().signal_exit();
        timeout(Duration::from_secs(5), start_task)
            .await
            .expect("init service did not stop after shutdown")
            .expect("init service task panicked")
            .expect("init service returned an error");
        service.stop().await.expect("provider shutdown failed");
    }
}
