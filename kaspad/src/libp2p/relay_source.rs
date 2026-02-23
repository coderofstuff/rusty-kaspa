use std::sync::Arc;

use futures_util::future::BoxFuture;
use kaspa_addressmanager::AddressManager;
use kaspa_p2p_libp2p::relay_pool::{RelayCandidateSource, RelayCandidateUpdate, RelaySource, relay_update_from_netaddr};
use kaspa_utils::networking::{NET_ADDRESS_SERVICE_LIBP2P_RELAY, RelayRole};
use parking_lot::Mutex;
use tokio::time::Duration;

use super::config::DEFAULT_RELAY_CANDIDATE_TTL;

pub(crate) struct AddressManagerRelaySource {
    address_manager: Arc<Mutex<AddressManager>>,
    ttl: Duration,
}

impl AddressManagerRelaySource {
    pub(crate) fn new(address_manager: Arc<Mutex<AddressManager>>) -> Self {
        Self { address_manager, ttl: DEFAULT_RELAY_CANDIDATE_TTL }
    }
}

impl RelayCandidateSource for AddressManagerRelaySource {
    fn fetch_candidates<'a>(&'a self) -> BoxFuture<'a, Vec<RelayCandidateUpdate>> {
        Box::pin(async move {
            let addresses = self.address_manager.lock().get_all_addresses();
            let mut updates = Vec::new();
            for addr in addresses {
                if !addr.has_services(NET_ADDRESS_SERVICE_LIBP2P_RELAY) {
                    continue;
                }
                if matches!(addr.relay_role, Some(RelayRole::Private)) {
                    continue;
                }
                let Some(relay_port) = addr.relay_port else {
                    continue;
                };
                let ttl = addr.relay_ttl_ms.map(Duration::from_millis).unwrap_or(self.ttl);
                let capacity = addr.relay_capacity.map(|cap| cap as usize);
                match relay_update_from_netaddr(addr, relay_port, ttl, RelaySource::AddressGossip, capacity) {
                    Ok(update) => updates.push(update),
                    Err(err) => log::debug!("libp2p relay source: invalid relay candidate: {err}"),
                }
            }
            updates
        })
    }
}
