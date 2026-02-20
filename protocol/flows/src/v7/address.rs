use crate::{flow_context::FlowContext, flow_trait::Flow};
use itertools::Itertools;
use kaspa_addressmanager::NetAddress;
use kaspa_p2p_lib::{
    IncomingRoute, Router,
    common::ProtocolError,
    dequeue, dequeue_with_timeout, make_message,
    pb::{AddressesMessage, RequestAddressesMessage, kaspad_message::Payload},
};
use rand::seq::SliceRandom;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// The maximum number of addresses that are sent in a single kaspa Addresses message.
const MAX_ADDRESSES_SEND: usize = 1000;

/// The maximum number of addresses that can be received in a single kaspa Addresses response.
/// If a peer exceeds this value we consider it a protocol error.
const MAX_ADDRESSES_RECEIVE: usize = 2500;
/// Periodic refresh for address gossip to avoid startup race windows.
const ADDRESS_REFRESH_INTERVAL: Duration = Duration::from_secs(45);

pub struct ReceiveAddressesFlow {
    ctx: FlowContext,
    router: Arc<Router>,
    incoming_route: IncomingRoute,
}

#[async_trait::async_trait]
impl Flow for ReceiveAddressesFlow {
    fn router(&self) -> Option<Arc<Router>> {
        Some(self.router.clone())
    }

    async fn start(&mut self) -> Result<(), ProtocolError> {
        self.start_impl().await
    }
}

impl ReceiveAddressesFlow {
    pub fn new(ctx: FlowContext, router: Arc<Router>, incoming_route: IncomingRoute) -> Self {
        Self { ctx, router, incoming_route }
    }

    async fn start_impl(&mut self) -> Result<(), ProtocolError> {
        loop {
            self.router
                .enqueue(make_message!(
                    Payload::RequestAddresses,
                    RequestAddressesMessage { include_all_subnetworks: false, subnetwork_id: None }
                ))
                .await?;

            let msg = dequeue_with_timeout!(self.incoming_route, Payload::Addresses)?;
            let address_list: Vec<NetAddress> = msg.try_into()?;
            if address_list.len() > MAX_ADDRESSES_RECEIVE {
                return Err(ProtocolError::OtherOwned(format!(
                    "address count {} exceeded {}",
                    address_list.len(),
                    MAX_ADDRESSES_RECEIVE
                )));
            }
            {
                let mut amgr_lock = self.ctx.address_manager.lock();
                for addr in address_list {
                    amgr_lock.add_address(addr)
                }
            }
            sleep(ADDRESS_REFRESH_INTERVAL).await;
        }
    }
}

pub struct SendAddressesFlow {
    ctx: FlowContext,
    router: Arc<Router>,
    incoming_route: IncomingRoute,
}

#[async_trait::async_trait]
impl Flow for SendAddressesFlow {
    fn router(&self) -> Option<Arc<Router>> {
        Some(self.router.clone())
    }

    async fn start(&mut self) -> Result<(), ProtocolError> {
        self.start_impl().await
    }
}

impl SendAddressesFlow {
    pub fn new(ctx: FlowContext, router: Arc<Router>, incoming_route: IncomingRoute) -> Self {
        Self { ctx, router, incoming_route }
    }

    async fn start_impl(&mut self) -> Result<(), ProtocolError> {
        loop {
            dequeue!(self.incoming_route, Payload::RequestAddresses)?;
            let mut addresses = self.ctx.address_manager.lock().iterate_addresses().collect_vec();
            if let Some(local_address) = self.current_local_address() {
                addresses.push(local_address);
            }
            let unique_addresses = addresses.into_iter().unique().collect_vec();
            let address_list = unique_addresses
                .choose_multiple(&mut rand::thread_rng(), MAX_ADDRESSES_SEND)
                .map(|addr| addr.clone().into())
                .collect();
            self.router.enqueue(make_message!(Payload::Addresses, AddressesMessage { address_list })).await?;
        }
    }

    fn current_local_address(&self) -> Option<NetAddress> {
        let mut local = self.ctx.address_manager.lock().best_local_address()?;
        let (services, relay_port, relay_capacity, relay_ttl_ms, relay_role, libp2p_peer_id, relay_hint) =
            self.ctx.libp2p_advertisement();
        local.services |= services;
        if let Some(port) = relay_port {
            local.set_relay_port(Some(port));
        }
        if let Some(capacity) = relay_capacity {
            local.set_relay_capacity(Some(capacity));
        }
        if let Some(ttl_ms) = relay_ttl_ms {
            local.set_relay_ttl_ms(Some(ttl_ms));
        }
        if let Some(role) = relay_role {
            local.set_relay_role(Some(role));
        }
        if let Some(peer_id) = libp2p_peer_id {
            local.set_libp2p_peer_id(Some(peer_id));
        }
        if let Some(hint) = relay_hint {
            local.set_relay_circuit_hint(Some(hint));
        }
        Some(local)
    }
}
