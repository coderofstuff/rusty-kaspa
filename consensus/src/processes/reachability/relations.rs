use std::sync::Arc;

use itertools::Itertools;
use kaspa_consensus_core::{blockhash::BlockHashes, BlockHashSet};
use kaspa_database::prelude::{StoreError, StoreResult};
use kaspa_hashes::Hash;

use crate::model::{services::reachability::ReachabilityService, stores::relations::RelationsStoreReader};

#[derive(Clone)]
pub struct RelationsStoreInFutureOfRoot<T: RelationsStoreReader, U: ReachabilityService> {
    relations_store: T,
    reachability_service: U,
    root: Hash,
}

impl<T: RelationsStoreReader, U: ReachabilityService> RelationsStoreInFutureOfRoot<T, U> {
    pub fn new(relations_store: T, reachability_service: U, root: Hash) -> Self {
        RelationsStoreInFutureOfRoot { relations_store, reachability_service, root }
    }
}

impl<T: RelationsStoreReader, U: ReachabilityService> RelationsStoreReader for RelationsStoreInFutureOfRoot<T, U> {
    fn get_parents(&self, hash: Hash) -> Result<BlockHashes, kaspa_database::prelude::StoreError> {
        self.relations_store.get_parents(hash).map(|hashes| {
            Arc::new(hashes.iter().copied().filter(|h| self.reachability_service.is_dag_ancestor_of(self.root, *h)).collect_vec())
        })
    }

    fn get_children(&self, hash: Hash) -> StoreResult<kaspa_database::prelude::ReadLock<BlockHashSet>> {
        // We assume hash is in future of root
        assert!(self.reachability_service.is_dag_ancestor_of(self.root, hash));
        self.relations_store.get_children(hash)
    }

    fn has(&self, hash: Hash) -> Result<bool, StoreError> {
        if self.reachability_service.is_dag_ancestor_of(self.root, hash) {
            Ok(false)
        } else {
            self.relations_store.has(hash)
        }
    }

    fn counts(&self) -> Result<(usize, usize), kaspa_database::prelude::StoreError> {
        unimplemented!()
    }
}
