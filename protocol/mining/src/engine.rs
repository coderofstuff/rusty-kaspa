use std::{ops::Deref, sync::Arc};

use kaspa_consensus_core::network::NetworkType::{Mainnet, Testnet};
use kaspa_consensusmanager::ConsensusSessionOwned;
use kaspa_p2p_flows::flow_context::FlowContext;

pub enum NearlySyncedFinder<'a> {
    BySession(&'a ConsensusSessionOwned),
    ByTimestampAndScore((u64, u64)),
}

#[derive(Clone)]
pub struct MiningRuleEngine {
    inner: Arc<MiningRuleEngineInner>,
}

pub struct MiningRuleEngineInner {
    flow_context: Arc<FlowContext>,
}

impl Deref for MiningRuleEngine {
    type Target = MiningRuleEngineInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl MiningRuleEngine {
    pub fn new(flow_context: Arc<FlowContext>) -> Self {
        Self { inner: Arc::new(MiningRuleEngineInner { flow_context }) }
    }

    pub async fn is_node_synced(&self, nearly_synced_finder: NearlySyncedFinder<'_>) -> bool {
        if self.flow_context.is_ibd_running() || !self.has_sufficient_peer_connectivity() {
            return false;
        }

        let is_nearly_synced = match nearly_synced_finder {
            NearlySyncedFinder::ByTimestampAndScore((sink_timestamp, sink_daa_score)) => {
                self.flow_context.config.is_nearly_synced(sink_timestamp, sink_daa_score)
            }
            NearlySyncedFinder::BySession(session) => session.async_is_nearly_synced().await,
        };

        is_nearly_synced
    }

    fn has_sufficient_peer_connectivity(&self) -> bool {
        // Other network types can be used in an isolated environment without peers
        !matches!(self.flow_context.config.net.network_type, Mainnet | Testnet) || self.flow_context.hub().has_peers()
    }
}
