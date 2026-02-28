use std::sync::Arc;

use kaspa_connectionmanager::{Libp2pRoleConfig, set_libp2p_role_config};
use kaspa_core::task::service::{AsyncService, AsyncServiceError, AsyncServiceFuture};
use kaspa_p2p_flows::flow_context::FlowContext;
use kaspa_p2p_libp2p::relay_pool::{
    CompositeRelaySource, RelayCandidateSource, RelaySource, StaticRelaySource, relay_update_from_multiaddr_str,
};
use kaspa_p2p_libp2p::{Config as AdapterConfig, Role as AdapterRole};
use kaspa_utils::networking::{NET_ADDRESS_SERVICE_LIBP2P_RELAY, RelayRole};
use kaspa_utils::triggers::SingleTrigger;
use parking_lot::Mutex;
use tokio::sync::OnceCell;
use tokio::time::{Duration, sleep};

use super::config::DEFAULT_RELAY_CANDIDATE_TTL;
use super::relay_source::AddressManagerRelaySource;

fn apply_role_update(
    flow_context: &FlowContext,
    role: AdapterRole,
    relay_port: Option<u16>,
    relay_capacity: Option<u32>,
    relay_ttl_ms: Option<u64>,
    inbound_cap_private: usize,
    libp2p_peer_id: Option<String>,
    relay_hint: Option<String>,
) {
    let (services, relay_port, relay_role, relay_capacity, relay_ttl_ms, libp2p_peer_id, relay_hint) =
        if matches!(role, AdapterRole::Public) {
            (NET_ADDRESS_SERVICE_LIBP2P_RELAY, relay_port, Some(RelayRole::Public), relay_capacity, relay_ttl_ms, None, None)
        } else {
            (0, None, Some(RelayRole::Private), None, None, libp2p_peer_id, relay_hint)
        };
    flow_context.set_libp2p_advertisement(services, relay_port, relay_capacity, relay_ttl_ms, relay_role, libp2p_peer_id, relay_hint);
    let is_private = !matches!(role, AdapterRole::Public);
    set_libp2p_role_config(Libp2pRoleConfig { is_private, libp2p_inbound_cap_private: inbound_cap_private });
}

pub struct Libp2pNodeService {
    config: AdapterConfig,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
    flow_context: Arc<FlowContext>,
    libp2p_peer_id: Option<String>,
    shutdown: SingleTrigger,
}

impl Libp2pNodeService {
    pub fn new(
        config: AdapterConfig,
        provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
        flow_context: Arc<FlowContext>,
        libp2p_peer_id: Option<String>,
    ) -> Self {
        Self { config, provider_cell, flow_context, libp2p_peer_id, shutdown: SingleTrigger::new() }
    }
}

impl AsyncService for Libp2pNodeService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-node"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if !self.config.mode.is_enabled() {
                return Ok(());
            }
            log::info!("libp2p node service starting; waiting for provider and connection handler");

            let provider = loop {
                if let Some(provider) = self.provider_cell.get() {
                    log::info!("libp2p provider initialised; starting node service");
                    break provider.clone();
                }
                tokio::select! {
                    _ = self.shutdown.listener.clone() => {
                        return Err(AsyncServiceError::Service(
                            "libp2p node service stopped while waiting for provider initialization".to_owned(),
                        ));
                    }
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            };

            let relay_port = self.config.listen_addresses.first().map(|addr| addr.port());
            let relay_capacity = self.config.relay_advertise_capacity;
            let relay_ttl_ms = self.config.relay_advertise_ttl_ms;
            let inbound_cap_private = self.config.libp2p_inbound_cap_private;
            let libp2p_peer_id = self.libp2p_peer_id.clone();
            let current_role = Arc::new(Mutex::new(self.config.role));
            let current_hint: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

            apply_role_update(
                &self.flow_context,
                *current_role.lock(),
                relay_port,
                relay_capacity,
                relay_ttl_ms,
                inbound_cap_private,
                libp2p_peer_id.clone(),
                current_hint.lock().clone(),
            );

            if matches!(self.config.role, AdapterRole::Auto)
                && let Some(mut role_rx) = provider.role_updates()
            {
                let flow_context = self.flow_context.clone();
                let current_role = current_role.clone();
                let current_hint = current_hint.clone();
                let libp2p_peer_id = libp2p_peer_id.clone();
                tokio::spawn(async move {
                    let mut current = *role_rx.borrow();
                    loop {
                        if role_rx.changed().await.is_err() {
                            break;
                        }
                        let role = *role_rx.borrow();
                        if role == current {
                            continue;
                        }
                        current = role;
                        *current_role.lock() = role;
                        apply_role_update(
                            &flow_context,
                            role,
                            relay_port,
                            relay_capacity,
                            relay_ttl_ms,
                            inbound_cap_private,
                            libp2p_peer_id.clone(),
                            current_hint.lock().clone(),
                        );
                    }
                });
            }

            if let Some(mut hint_rx) = provider.relay_hint_updates() {
                let flow_context = self.flow_context.clone();
                let current_role = current_role.clone();
                let current_hint = current_hint.clone();
                let libp2p_peer_id = libp2p_peer_id.clone();
                let initial_hint = hint_rx.borrow().clone();
                if initial_hint.is_some() {
                    *current_hint.lock() = initial_hint.clone();
                    apply_role_update(
                        &flow_context,
                        *current_role.lock(),
                        relay_port,
                        relay_capacity,
                        relay_ttl_ms,
                        inbound_cap_private,
                        libp2p_peer_id.clone(),
                        initial_hint.clone(),
                    );
                }
                tokio::spawn(async move {
                    let mut current = initial_hint;
                    loop {
                        if hint_rx.changed().await.is_err() {
                            break;
                        }
                        let hint = hint_rx.borrow().clone();
                        if hint == current {
                            continue;
                        }
                        current = hint.clone();
                        *current_hint.lock() = hint.clone();
                        apply_role_update(
                            &flow_context,
                            *current_role.lock(),
                            relay_port,
                            relay_capacity,
                            relay_ttl_ms,
                            inbound_cap_private,
                            libp2p_peer_id.clone(),
                            hint,
                        );
                    }
                });
            }

            let handler = loop {
                if let Some(handler) = self.flow_context.connection_handler() {
                    log::info!("libp2p connection handler available; wiring inbound bridge");
                    break handler;
                }
                tokio::select! {
                    _ = self.shutdown.listener.clone() => {
                        return Err(AsyncServiceError::Service(
                            "libp2p node service stopped while waiting for connection handler".to_owned(),
                        ));
                    }
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            };

            let mut svc = kaspa_p2p_libp2p::Libp2pService::with_provider(self.config.clone(), provider)
                .with_shutdown(self.shutdown.listener.clone());
            if matches!(self.config.role, AdapterRole::Private | AdapterRole::Auto) {
                let mut sources: Vec<Arc<dyn RelayCandidateSource>> =
                    vec![Arc::new(AddressManagerRelaySource::new(self.flow_context.address_manager()))];
                if !self.config.relay_candidates.is_empty() {
                    let mut updates = Vec::new();
                    for candidate in &self.config.relay_candidates {
                        if let Some(update) =
                            relay_update_from_multiaddr_str(candidate, DEFAULT_RELAY_CANDIDATE_TTL, RelaySource::Config, None)
                        {
                            updates.push(update);
                        } else {
                            log::warn!("libp2p relay source: invalid relay candidate {}", candidate);
                        }
                    }
                    if !updates.is_empty() {
                        sources.push(Arc::new(StaticRelaySource::new(updates)));
                    }
                }
                let relay_source: Arc<dyn RelayCandidateSource> =
                    if sources.len() == 1 { sources[0].clone() } else { Arc::new(CompositeRelaySource::new(sources)) };
                svc = svc.with_relay_source(relay_source);
            }
            svc.start().await.map_err(|e| AsyncServiceError::Service(e.to_string()))?;
            if let Err(err) = svc.start_inbound(Arc::new(handler)).await {
                log::error!("libp2p inbound bridge failed to start: {err}");
                return Err(AsyncServiceError::Service(err.to_string()));
            }
            log::info!("libp2p node service started: bridging libp2p streams into Kaspa");

            self.shutdown.listener.clone().await;
            svc.shutdown().await;
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
