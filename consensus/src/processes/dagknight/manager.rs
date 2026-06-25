use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    sync::Arc,
};

use kaspa_consensus_core::{
    BlockHashMap, BlockHashSet, BlueWorkType, HashKTypeMap, HashMapCustomHasher, KType,
    blockhash::{self, BlockHashExtensions, BlockHashes},
};
use kaspa_database::prelude::{StoreError, StoreResultUnitExt};
use kaspa_hashes::Hash;

use crate::{
    model::{
        services::reachability::{MTReachabilityService, ReachabilityService},
        stores::{
            dagknight::{ColoringData, DagknightKey, DagknightStore, DagknightStoreReader},
            headers::HeaderStoreReader,
            reachability::ReachabilityStoreReader,
            relations::RelationsStoreReader,
        },
    },
    processes::{
        difficulty::calc_work,
        ghostdag::{
            mergeset::unordered_mergeset_without_selected_parent,
            ordering::SortableBlock,
            protocol::{ColoringOutput, ColoringState},
        },
        reachability::relations::FutureIntersectRelations,
    },
};

/// Chain block with attached coloring data.
/// Used internally by ConflictZoneManager to walk the VSPC chain during candidate checking.
struct ChainBlock {
    /// If `None`, signals being the new (virtual) block.
    hash: Option<Hash>,
    data: Arc<ColoringData>,
}

pub struct ConflictZoneManager<
    C: DagknightStore + DagknightStoreReader,
    O: HeaderStoreReader,
    D: RelationsStoreReader,
    R: ReachabilityStoreReader + Clone,
> {
    k: KType,
    root: Hash,
    free_search: bool,
    dagknight_store: Arc<C>,
    headers_store: Arc<O>,
    relations_store: FutureIntersectRelations<D, MTReachabilityService<R>>,
    reachability_service: MTReachabilityService<R>,
}

impl<C: DagknightStore + DagknightStoreReader, O: HeaderStoreReader, D: RelationsStoreReader, R: ReachabilityStoreReader + Clone>
    ConflictZoneManager<C, O, D, R>
{
    pub fn new(
        k: KType,
        root: Hash,
        dagknight_store: Arc<C>,
        headers_store: Arc<O>,
        relations_store: FutureIntersectRelations<D, MTReachabilityService<R>>,
        reachability_service: MTReachabilityService<R>,
    ) -> Self {
        Self { k, root, free_search: false, dagknight_store, headers_store, reachability_service, relations_store }
    }

    pub fn with_free_search(
        k: KType,
        root: Hash,
        dagknight_store: Arc<C>,
        headers_store: Arc<O>,
        relations_store: FutureIntersectRelations<D, MTReachabilityService<R>>,
        reachability_service: MTReachabilityService<R>,
        free_search: bool,
    ) -> Self {
        Self { k, root, free_search, dagknight_store, headers_store, reachability_service, relations_store }
    }

    pub fn has(&self, pov_hash: Hash) -> bool {
        let key = self.get_key(pov_hash);

        self.dagknight_store.has(key).unwrap()
    }

    pub fn insert(&self, pov_hash: Hash, cd: Arc<ColoringData>) -> Result<(), StoreError> {
        let key = self.get_key(pov_hash);

        self.dagknight_store.insert(key, cd)
    }

    fn get_key(&self, pov_hash: Hash) -> DagknightKey {
        DagknightKey::new(self.root, pov_hash, self.k, self.free_search)
    }

    pub fn get_blue_score(&self, pov_hash: Hash) -> Result<u64, StoreError> {
        let key = self.get_key(pov_hash);

        Ok(self.dagknight_store.get_data(key)?.blue_score)
    }

    pub fn get_blue_work(&self, pov_hash: Hash) -> Result<BlueWorkType, StoreError> {
        let key = self.get_key(pov_hash);

        Ok(self.dagknight_store.get_data(key)?.blue_work)
    }

    pub fn get_selected_parent(&self, pov_hash: Hash) -> Result<Hash, StoreError> {
        let key = self.get_key(pov_hash);

        Ok(self.dagknight_store.get_data(key)?.selected_parent)
    }

    pub fn get_blues_anticone_sizes(&self, pov_hash: Hash) -> Result<Arc<BlockHashMap<KType>>, StoreError> {
        let key = self.get_key(pov_hash);

        Ok(self.dagknight_store.get_data(key)?.blues_anticone_sizes.clone())
    }

    /// Returns the blues_anticone_work from the store for the given hash.
    /// Maps each blue block to the total work of blue blocks in its anticone (from POV of this block).
    pub fn get_blues_anticone_work(&self, pov_hash: Hash) -> Result<BlockHashMap<BlueWorkType>, StoreError> {
        let key = self.get_key(pov_hash);

        Ok(self.dagknight_store.get_data(key)?.blues_anticone_work.clone())
    }

    pub fn get_data(&self, pov_hash: Hash) -> Result<Arc<ColoringData>, StoreError> {
        let key = self.get_key(pov_hash);

        self.dagknight_store.get_data(key)
    }

    pub fn k_colouring(&self, parents: &[Hash], k: KType, custom_selected_parent: Option<Hash>) -> Arc<ColoringData> {
        assert!(!parents.is_empty(), "genesis must be added via a call to init");

        // Run the k_colouring parent selection algorithm
        let selected_parent = custom_selected_parent.unwrap_or(self.find_selected_parent(parents.iter().copied()));
        // Handle the special case of origin children first
        if selected_parent.is_origin() {
            // ORIGIN is always a single parent so both blue score and work should remain zero
            let cd = ColoringData::new_with_selected_parent(selected_parent, 1);
            return Arc::new(cd);
        }
        // Initialize new k_colouring block data with the selected parent
        let mut new_block_data = ColoringData::new_with_selected_parent(selected_parent, k);
        // Get the mergeset in consensus-agreed topological order (topological here means forward in time from blocks to children)
        let ordered_mergeset = self.ordered_mergeset_without_selected_parent(selected_parent, parents);

        let mut non_blue_candidates: Vec<Hash> = Vec::new();

        for blue_candidate in ordered_mergeset.iter().cloned() {
            let coloring = self.check_blue_candidate(Arc::new(new_block_data.clone()), blue_candidate, k);

            if let ColoringOutput::Blue(blue_anticone_size, blues_anticone_sizes) = coloring {
                let candidate_work = calc_work(self.headers_store.get_bits(blue_candidate).unwrap());
                let mut blues_anticone_work = BlockHashMap::with_capacity(blues_anticone_sizes.len());
                for (peer, _) in &blues_anticone_sizes {
                    let peer_work = calc_work(self.headers_store.get_bits(*peer).unwrap());
                    blues_anticone_work.insert(*peer, peer_work);
                }
                new_block_data.add_blue(
                    blue_candidate,
                    blue_anticone_size,
                    &blues_anticone_sizes,
                    &blues_anticone_work,
                    candidate_work,
                );
            } else {
                non_blue_candidates.push(blue_candidate);
            }
        }

        let nca = if selected_parent == self.root || self.free_search {
            self.root
        } else {
            self.reachability_service.get_next_chain_ancestor(selected_parent, self.root)
        };

        for hash in non_blue_candidates {
            if !self.free_search && self.reachability_service.is_chain_ancestor_of(nca, hash) {
                new_block_data.add_gray(hash);
            } else {
                new_block_data.add_red(hash);
            }
        }

        new_block_data.blue_score = self.get_blue_score(selected_parent).unwrap() + new_block_data.mergeset_blues.len() as u64;
        let added_blue_work: BlueWorkType =
            new_block_data.mergeset_blues.iter().cloned().map(|hash| calc_work(self.headers_store.get_bits(hash).unwrap())).sum();
        new_block_data.blue_work = self.get_blue_work(selected_parent).unwrap() + added_blue_work;

        let sp_data = self.get_data(selected_parent).unwrap();
        let mergeset_reds_work: BlueWorkType =
            new_block_data.mergeset_reds.iter().cloned().map(|h| calc_work(self.headers_store.get_bits(h).unwrap())).sum();

        new_block_data.past_red_work = sp_data.past_red_work + mergeset_reds_work;

        Arc::new(new_block_data)
    }

    fn check_blue_candidate_with_chain_block(
        &self,
        new_block_data: &ColoringData,
        chain_block: &ChainBlock,
        blue_candidate: Hash,
        candidate_blues_anticone_sizes: &mut BlockHashMap<KType>,
        candidate_blue_anticone_size: &mut KType,
        k: KType,
    ) -> ColoringState {
        // If blue_candidate is in the future of chain_block, it means
        // that all remaining blues are in the past of chain_block and thus
        // in the past of blue_candidate. In this case we know for sure that
        // the anticone of blue_candidate will not exceed K, and we can mark
        // it as blue.
        //
        // The new block is always in the future of blue_candidate, so there's
        // no point in checking it.

        // We check if chain_block is not the new block by checking if it has a hash.
        if let Some(hash) = chain_block.hash
            && self.reachability_service.is_dag_ancestor_of(hash, blue_candidate)
        {
            return ColoringState::Blue;
        }

        // Iterate over blue peers and check for k-cluster violations
        for &peer in chain_block.data.mergeset_blues.iter() {
            // Skip blocks that are in the past of blue_candidate (since they are not in its anticone)
            if self.reachability_service.is_dag_ancestor_of(peer, blue_candidate) {
                continue;
            }

            // Otherwise, peer must be in the anticone of blue_candidate, so we check for k limits.
            // Note that peer cannot be in the future of blue_candidate because we process the mergeset
            // in past-to-future topological order, so even if chain_block == new_block, an existing blue
            // cannot be in the future of a candidate blue

            let peer_blue_anticone_size = self.blue_anticone_size_and_work(peer, new_block_data).0;
            candidate_blues_anticone_sizes.insert(peer, peer_blue_anticone_size);

            *candidate_blue_anticone_size += 1;
            if *candidate_blue_anticone_size > k {
                // k-cluster violation: The candidate's blue anticone exceeded k
                return ColoringState::Red;
            }

            if peer_blue_anticone_size == k {
                // k-cluster violation: A block in candidate's blue anticone already
                // has k blue blocks in its own anticone
                return ColoringState::Red;
            }

            // This is a sanity check that validates that a blue
            // block's blue anticone is not already larger than K.
            assert!(peer_blue_anticone_size <= k, "found blue anticone larger than K");
            // [Crescendo]: this ^ is a valid assert since we are increasing k. Had we decreased k, this line would
            //              need to be removed and the condition above would need to be changed to >= k
        }

        ColoringState::Pending
    }

    /// Returns the blue anticone size and work of `block` from the worldview of `context`.
    /// Expects `block` to be in the blue set of `context`.
    /// Returns (anticone_size, anticone_work).
    fn blue_anticone_size_and_work(&self, block: Hash, context: &ColoringData) -> (KType, BlueWorkType) {
        let mut current_blues_anticone_sizes = HashKTypeMap::clone(&context.blues_anticone_sizes);
        let mut current_blues_anticone_work: BlockHashMap<BlueWorkType> = context.blues_anticone_work.clone();
        let mut current_selected_parent = context.selected_parent;
        loop {
            let size = current_blues_anticone_sizes.get(&block).copied();
            let work = current_blues_anticone_work.get(&block).copied().unwrap_or_default();
            if let Some(size) = size {
                return (size, work);
            }

            current_blues_anticone_sizes = self.get_blues_anticone_sizes(current_selected_parent).unwrap();
            current_blues_anticone_work = self.get_blues_anticone_work(current_selected_parent).unwrap();
            current_selected_parent = self.get_selected_parent(current_selected_parent).unwrap();
        }
    }

    fn check_blue_candidate(&self, new_block_data: Arc<ColoringData>, blue_candidate: Hash, k: KType) -> ColoringOutput {
        // The maximum length of new_block_data.mergeset_blues can be K+1 because
        // it contains the selected parent.
        if new_block_data.mergeset_blues.len() as KType == k + 1 {
            return ColoringOutput::Red;
        }

        let mut candidate_blues_anticone_sizes: BlockHashMap<KType> = BlockHashMap::with_capacity(k as usize);
        // Iterate over all blocks in the blue past of the new block that are not in the past
        // of blue_candidate, and check for each one of them if blue_candidate potentially
        // enlarges their blue anticone to be over K, or that they enlarge the blue anticone
        // of blue_candidate to be over K.
        let mut chain_block = ChainBlock { hash: None, data: new_block_data };
        let mut candidate_blue_anticone_size: KType = 0;

        loop {
            let state = self.check_blue_candidate_with_chain_block(
                &chain_block.data,
                &chain_block,
                blue_candidate,
                &mut candidate_blues_anticone_sizes,
                &mut candidate_blue_anticone_size,
                k,
            );

            match state {
                ColoringState::Blue => return ColoringOutput::Blue(candidate_blue_anticone_size, candidate_blues_anticone_sizes),
                ColoringState::Red => return ColoringOutput::Red,
                ColoringState::Pending => (), // continue looping
            }

            let sp = chain_block.data.selected_parent;
            chain_block = ChainBlock { hash: Some(sp), data: self.get_data(sp).unwrap() }
        }
    }

    fn sort_blocks(&self, blocks: impl IntoIterator<Item = Hash>) -> Vec<Hash> {
        let mut sorted_blocks: Vec<Hash> = blocks.into_iter().collect();
        sorted_blocks.sort_by_cached_key(|block| SortableBlock {
            hash: *block,
            // Sort by blue work as calculated within the zone. For blocks not within the zone (or not in agreement), we prefer them to be added later.
            // Using the header blue work will tend to order these blocks later.
            blue_work: self.get_blue_work(*block).unwrap_or(self.headers_store.get_header(*block).unwrap().blue_work),
        });
        sorted_blocks
    }

    pub fn ordered_mergeset_without_selected_parent(&self, selected_parent: Hash, parents: &[Hash]) -> Vec<Hash> {
        self.sort_blocks(self.unordered_mergeset_without_selected_parent(selected_parent, parents))
    }

    pub fn unordered_mergeset_without_selected_parent(&self, selected_parent: Hash, parents: &[Hash]) -> BlockHashSet {
        unordered_mergeset_without_selected_parent(&self.relations_store, &self.reachability_service, selected_parent, parents)
    }

    pub fn is_free_search(&self) -> bool {
        self.free_search
    }

    pub fn find_selected_parent(&self, parents: impl IntoIterator<Item = Hash>) -> Hash {
        let selected_parent = parents
            .into_iter()
            .filter_map(|parent| self.get_blue_work(parent).map(|blue_work| SortableBlock { hash: parent, blue_work }).ok())
            .max()
            .unwrap()
            .hash;

        if !self.free_search {
            assert!(
                self.reachability_service.is_chain_ancestor_of(self.root, selected_parent),
                "conflict genesis {} not a chain ancestor of selected parent {}",
                self.root,
                selected_parent
            );
        };

        selected_parent
    }

    pub fn init_root(&self) {
        if !self.has(self.root) {
            self.insert(
                self.root,
                Arc::new(ColoringData::new(
                    0,
                    Default::default(),
                    blockhash::ORIGIN,
                    BlockHashes::new(Vec::new()),
                    BlockHashes::new(Vec::new()),
                    BlockHashes::new(Vec::new()),
                    HashKTypeMap::new(BlockHashMap::new()),
                    BlockHashMap::new(),
                    BlueWorkType::ZERO, // past_red_work
                )),
            )
            .idempotent()
            .unwrap();
        }
    }

    pub fn find_last_known_tips(&self, tips: &[Hash]) -> (Vec<Hash>, BlockHashSet) {
        let mut visited = BlockHashSet::new();
        let mut queue: VecDeque<Hash> = VecDeque::from_iter(tips.iter().copied());

        let mut roots = vec![];

        while let Some(curr) = queue.pop_front() {
            if !visited.insert(curr) {
                continue;
            }

            if !self.free_search && !self.reachability_service.is_chain_ancestor_of(self.root, curr) {
                continue;
            }

            if self.has(curr) {
                roots.push(curr);
            } else {
                for parent in self.relations_store.get_parents(curr).unwrap().iter() {
                    queue.push_back(*parent);
                }
            }
        }

        (roots, visited)
    }

    // Calculates the rank of the subgroup over the region: <root, tips>
    // root = conflict genesis
    // subgroup = the current subgroup
    // tips = all tips in this conflict. part of which is the subgroup
    //
    // Returns the conflict zone manager which gives access to the coloring data of the conflict zone
    pub fn fill_zone_data(&self, tips: &[Hash]) -> BlockHashSet {
        self.init_root();

        let (last_known_tips, visited_subdag) = self.find_last_known_tips(tips);

        let mut topological_heap: BinaryHeap<_> = Default::default();

        last_known_tips.iter().for_each(|current_root| {
            topological_heap.push(Reverse(SortableBlock {
                hash: *current_root,
                blue_work: self.headers_store.get_header(*current_root).unwrap().blue_work,
            }));
        });

        let mut visited = BlockHashSet::new();

        loop {
            let Some(current) = topological_heap.pop() else {
                break;
            };
            let current_hash = current.0.hash;
            if !visited.insert(current_hash) {
                continue;
            }

            if !self.reachability_service.is_dag_ancestor_of_any(current_hash, &mut tips.iter().copied()) {
                continue;
            }

            if !self.has(current_hash) {
                let parents = &self.relations_store.get_parents(current_hash).unwrap();

                // For free_search, select from all parents; for committed search, only from agreeing parents
                let selected_parent = if self.free_search {
                    self.find_selected_parent(parents.iter().copied())
                } else {
                    let next_chain_ancestor_of_current = self.reachability_service.get_next_chain_ancestor(current_hash, self.root);
                    let agreeing_parents = parents
                        .iter()
                        .copied()
                        .filter(|&p| {
                            next_chain_ancestor_of_current == current_hash
                                || self.reachability_service.is_chain_ancestor_of(next_chain_ancestor_of_current, p)
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        !agreeing_parents.is_empty(),
                        "Expected at least one agreeing parent | current: {:#?} | parents: {:#?}",
                        current_hash,
                        parents
                    );
                    self.find_selected_parent(agreeing_parents.iter().copied())
                };

                let current_cd = self.k_colouring(parents, self.k, Some(selected_parent));

                self.insert(current_hash, current_cd).idempotent().unwrap();
            }

            for child in self.relations_store.get_children(current_hash).unwrap().read().iter().copied() {
                // For free_search, use DAG ancestry; for committed search, use chain ancestry
                let is_in_zone = if self.free_search {
                    self.reachability_service.try_is_dag_ancestor_of(self.root, child).unwrap_or(false)
                } else {
                    self.reachability_service.try_is_chain_ancestor_of(self.root, child).unwrap_or(false)
                };
                if !is_in_zone {
                    continue;
                }
                topological_heap
                    .push(Reverse(SortableBlock { hash: child, blue_work: self.headers_store.get_header(child).unwrap().blue_work }));
            }
        }

        visited_subdag
    }
    // END Copied from GD Manager
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::stores::{
        dagknight::{ColoringData, MemoryDagknightStore},
        headers::MemoryHeaderStore,
        reachability::MemoryReachabilityStore,
        relations::MemoryRelationsStore,
    };
    use crate::processes::reachability::tests::{DagBlock, DagBuilder};
    use kaspa_consensus_core::blockhash::ORIGIN;
    use kaspa_consensus_core::header::Header;
    use parking_lot::RwLock;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// Test that `find_last_known_tips` correctly uses chain ancestry (committed)
    /// vs DAG ancestry (free_search) when traversing back from tips.
    ///
    /// DAG structure:
    ///
    ///         A <= B <= D -- F
    ///          \       /
    ///            \  /
    ///        Z <- C <= E -- W
    ///         \    \   /
    ///           \   \ /
    ///            Y <= X
    ///
    /// Selected parents:
    /// - A: ORIGIN, B: A, D: B, Z: ORIGIN
    /// - C: A (agrees with A), E: C
    /// - Y: Z, X: Y (X does NOT agree with A - its chain goes X→Y→Z→ORIGIN)
    /// - F, W: tips (no selected parent yet)
    ///
    /// Parents:
    /// - A:[ORIGIN], B:[A], D:[B], F:[D,E]
    /// - Z:[ORIGIN], C:[A,Z], E:[C], W:[X,E]
    /// - Y:[Z], X:[Y,C]
    ///
    /// Chain ancestry from A: A→B→D, A→C→E
    /// X is NOT a chain ancestor of A
    ///
    /// Records filled for: A, B, C, D, E, Y, Z, X
    /// F and W are tips (no records)
    ///
    /// TEST with tips = [F, W]:
    /// - free_search=false: find_last_known_tips returns [D, E]
    ///   (F→D,E; W→X,E but X skipped since not chain ancestor)
    /// - free_search=true: find_last_known_tips returns [D, E, X]
    ///   (F→D,E; W→X,E; all are DAG ancestors)
    #[test]
    fn test_find_last_known_tips_uses_correct_ancestry_type() {
        let hash_a: Hash = 1_u64.into(); // root
        let hash_b: Hash = 2_u64.into();
        let hash_d: Hash = 3_u64.into();
        let hash_z: Hash = 4_u64.into();
        let hash_c: Hash = 5_u64.into();
        let hash_e: Hash = 6_u64.into();
        let hash_y: Hash = 7_u64.into();
        let hash_x: Hash = 8_u64.into();
        let hash_f: Hash = 9_u64.into();
        let hash_w: Hash = 10_u64.into();

        let dk_map = RefCell::new(HashMap::new());
        let dagknight_store = Arc::new(MemoryDagknightStore::new(dk_map.clone()));
        let headers_store = Arc::new(MemoryHeaderStore::new());
        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();

        // Build DAG
        {
            let mut builder = DagBuilder::new(&mut reachability, &mut relations);
            builder.init();
            builder.add_block(DagBlock::new(hash_a, vec![ORIGIN]));
            builder.add_block(DagBlock::new(hash_z, vec![ORIGIN]));
            builder.add_block_with_selected_parent(DagBlock::new(hash_b, vec![hash_a]), hash_a);
            builder.add_block_with_selected_parent(DagBlock::new(hash_c, vec![hash_a, hash_z]), hash_a);
            builder.add_block_with_selected_parent(DagBlock::new(hash_d, vec![hash_b]), hash_b);
            builder.add_block_with_selected_parent(DagBlock::new(hash_e, vec![hash_c]), hash_c);
            builder.add_block_with_selected_parent(DagBlock::new(hash_y, vec![hash_z]), hash_z);
            builder.add_block_with_selected_parent(DagBlock::new(hash_x, vec![hash_y, hash_c]), hash_y);
            builder.add_block(DagBlock::new(hash_f, vec![hash_d, hash_e])); // Tip
            builder.add_block(DagBlock::new(hash_w, vec![hash_x, hash_e])); // Tip

            // Insert headers with valid bits
            for (hash, parents) in [
                (hash_a, vec![]),
                (hash_b, vec![hash_a]),
                (hash_d, vec![hash_b]),
                (hash_z, vec![]),
                (hash_c, vec![hash_a, hash_z]),
                (hash_e, vec![hash_c]),
                (hash_y, vec![hash_z]),
                (hash_x, vec![hash_y, hash_c]),
                (hash_f, vec![hash_d, hash_e]),
                (hash_w, vec![hash_x, hash_e]),
            ] {
                let mut header = Header::from_precomputed_hash(hash, parents);
                header.bits = 0x207fffff;
                headers_store.insert(Arc::new(header));
            }
        }

        let reachability_service = MTReachabilityService::new(Arc::new(RwLock::new(reachability)));
        let relations_service = FutureIntersectRelations::new(relations.clone(), reachability_service.clone(), hash_a);

        // Create both managers sharing the same stores
        let manager_committed = ConflictZoneManager::new(
            0,
            hash_a,
            dagknight_store.clone(),
            headers_store.clone(),
            relations_service.clone(),
            reachability_service.clone(),
        );

        let manager_free = ConflictZoneManager::with_free_search(
            0,
            hash_a,
            dagknight_store.clone(),
            headers_store.clone(),
            relations_service,
            reachability_service,
            true,
        );

        // Initialize root and fill records for all blocks except tips F and W
        manager_committed.init_root();
        manager_free.init_root();

        // Fill records for non-tip blocks
        for (hash, selected_parent) in [
            (hash_b, hash_a),
            (hash_d, hash_b),
            (hash_z, ORIGIN),
            (hash_c, hash_a),
            (hash_e, hash_c),
            (hash_y, hash_z),
            (hash_x, hash_y),
        ] {
            let cd = Arc::new(ColoringData::new_with_selected_parent(selected_parent, 0));
            manager_committed.insert(hash, cd.clone()).unwrap();
            manager_free.insert(hash, cd).unwrap();
        }

        // Tips are F and W (no records yet)
        let tips = vec![hash_f, hash_w];

        let (roots_committed, _) = manager_committed.find_last_known_tips(&tips);
        let (roots_free, _) = manager_free.find_last_known_tips(&tips);

        assert_eq!(roots_committed.len(), 2, "Committed should find D, E");
        assert!(roots_committed.contains(&hash_d));
        assert!(roots_committed.contains(&hash_e));
        assert!(!roots_committed.contains(&hash_x), "X should not be in committed roots");

        assert_eq!(roots_free.len(), 3, "Free search should find D, E, X");
        assert!(roots_free.contains(&hash_d));
        assert!(roots_free.contains(&hash_e));
        assert!(roots_free.contains(&hash_x));
    }

    /// Test demonstrating the key difference between free_search and committed search.
    ///
    /// DAG structure:
    ///
    ///        A (conflict genesis)
    ///       / \
    ///      B   Z
    ///      |   |
    ///      C   Y
    ///      | \ |
    ///      D   X
    ///
    ///
    /// The tips are [D, X].
    ///
    /// TEST: When computing X's selected_parent during fill_zone_data:
    /// - In free_search=false (committed): selected_parent must be Y
    ///   (X only agrees with Y, not with C - they don't share a chain ancestor above A)
    /// - In free_search=true: selected_parent considers all parents [Y, C]
    ///   and selects based on blue work (or hash as tiebreaker). In this case, C wins
    #[test]
    fn test_free_search_considers_non_agreeing_parents() {
        use crate::processes::reachability::tests::{DagBlock, DagBuilder};

        let hash_a: Hash = 1_u64.into(); // conflict genesis
        let hash_b: Hash = 2_u64.into();
        let hash_c: Hash = 3_u64.into();
        let hash_d: Hash = 4_u64.into();
        let hash_z: Hash = 5_u64.into();
        let hash_y: Hash = 6_u64.into();
        let hash_x: Hash = 7_u64.into();

        let dk_map = RefCell::new(HashMap::new());
        let dagknight_store = Arc::new(MemoryDagknightStore::new(dk_map));

        let headers_store = Arc::new(MemoryHeaderStore::new());

        let mut reachability = MemoryReachabilityStore::new();
        let mut relations_store = MemoryRelationsStore::new();

        // Build DAG for committed search
        {
            let mut builder = DagBuilder::new(&mut reachability, &mut relations_store);
            builder.init();
            builder.add_block(DagBlock::new(hash_a, vec![ORIGIN]));
            builder.add_block_with_selected_parent(DagBlock::new(hash_b, vec![hash_a]), hash_a);
            builder.add_block_with_selected_parent(DagBlock::new(hash_c, vec![hash_b]), hash_b);
            builder.add_block_with_selected_parent(DagBlock::new(hash_d, vec![hash_c]), hash_c);
            builder.add_block_with_selected_parent(DagBlock::new(hash_z, vec![hash_a]), hash_a);
            builder.add_block_with_selected_parent(DagBlock::new(hash_y, vec![hash_z]), hash_z);
            builder.add_block_with_selected_parent(DagBlock::new(hash_x, vec![hash_y, hash_c]), hash_y);

            let insert_header_with_work =
                |hash: Hash, parents: Vec<Hash>, bits: u32, store: &Arc<MemoryHeaderStore>, blue_work: BlueWorkType| {
                    let mut header = Header::from_precomputed_hash(hash, parents);
                    header.bits = bits;
                    header.blue_work = blue_work;
                    store.insert(Arc::new(header));
                };

            insert_header_with_work(hash_a, vec![], 0x207fffff, &headers_store, 0.into());
            // Note the higher bits here to make this side have higher blue work, but not be the committed side
            insert_header_with_work(hash_b, vec![hash_a], 0x204fffff, &headers_store, 1.into());
            insert_header_with_work(hash_c, vec![hash_b], 0x207fffff, &headers_store, 3.into());
            insert_header_with_work(hash_d, vec![hash_b], 0x207fffff, &headers_store, 4.into());

            insert_header_with_work(hash_z, vec![hash_a], 0x207fffff, &headers_store, 1.into());
            insert_header_with_work(hash_y, vec![hash_z], 0x207fffff, &headers_store, 2.into());
            insert_header_with_work(hash_x, vec![hash_c, hash_y], 0x207fffff, &headers_store, 6.into());
        }

        let reachability_service = MTReachabilityService::new(Arc::new(RwLock::new(reachability)));
        let relations_service = FutureIntersectRelations::new(relations_store.clone(), reachability_service.clone(), hash_a);

        // Create committed manager (free_search = false)
        let manager_committed = ConflictZoneManager::new(
            0,
            hash_a,
            dagknight_store.clone(),
            headers_store.clone(),
            relations_service.clone(),
            reachability_service.clone(),
        );

        // Create free search manager (free_search = true)
        let manager_free = ConflictZoneManager::with_free_search(
            0,
            hash_a,
            dagknight_store,
            headers_store,
            relations_service,
            reachability_service,
            true,
        );
        assert!(manager_free.is_free_search(), "Manager should have free_search=true");

        // Pre-populate the store with blocks (simulating that they were already processed)
        // For committed search
        manager_committed.init_root();

        // Now fill zone data
        let tips = vec![hash_x, hash_d];
        manager_committed.fill_zone_data(&tips);
        manager_free.fill_zone_data(&tips);

        // Get X's selected parent from both managers
        let committed_sp = manager_committed.get_selected_parent(hash_x).unwrap();
        let free_sp = manager_free.get_selected_parent(hash_x).unwrap();

        assert_eq!(committed_sp, hash_y, "In committed search, X's selected parent must be Y (the only agreeing parent)");

        // In free search, X can select any parent and is expected to select C due to higher blue work (even if not agreeing)
        assert_eq!(
            free_sp, hash_c,
            "In free search, X's selected parent should be C (selected from all parents, wins by higher work)"
        );
    }
}
