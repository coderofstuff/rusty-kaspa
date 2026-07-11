use std::{
    cmp::{Ordering, Reverse},
    collections::{
        BTreeSet, BinaryHeap, HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
};

use kaspa_consensus_core::{BlockHashMap, BlockHashSet, KType};
use kaspa_core::info;
use kaspa_hashes::Hash;
use kaspa_math::{Uint192, int::SignedInteger};
use kaspa_utils::mem_size::MemSizeEstimator;

/// Signed work value (difference of two Uint192 work values).
type SignedWork = SignedInteger<Uint192>;

use crate::{
    model::{
        services::reachability::{MTReachabilityService, ReachabilityService},
        stores::{
            dagknight::{PastColoringData, PoppedBlue, UmcPersistedState, UmcPersistedTreeEntry},
            headers::HeaderStoreReader,
            reachability::ReachabilityStoreReader,
        },
    },
    processes::{difficulty::calc_work, ghostdag::ordering::SortableBlock},
};

// ============================================================================
// Cascade data structures
// ============================================================================

/// BTree entry in the cascade tree, ordered by floor value.
#[derive(Eq, Clone)]
pub struct CascadeTreeEntry {
    pub hash: Hash,
    pub floor: SignedWork,
}

impl CascadeTreeEntry {
    pub fn new(hash: Hash, floor: SignedWork) -> Self {
        Self { hash, floor }
    }
}

impl PartialEq for CascadeTreeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.floor == other.floor && self.hash == other.hash
    }
}

impl PartialOrd for CascadeTreeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CascadeTreeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.floor.partial_cmp(&other.floor).unwrap_or(Ordering::Equal).then_with(|| self.hash.cmp(&other.hash))
    }
}

/// BTree-based heap of blue blocks ordered by their floor value.
#[derive(Default)]
pub struct CascadeTree {
    btree: BTreeSet<CascadeTreeEntry>,
    rev_index: BlockHashMap<SignedWork>,

    pub past_blue_work: BlockHashMap<Uint192>,
    pub past_red_work: BlockHashMap<Uint192>,
    pub anticone_blue_work: BlockHashMap<Uint192>,
    pub arlb: BlockHashMap<Uint192>,
    pub last_red_index: BlockHashMap<usize>,
    pub red_index: usize,
}

impl CascadeTree {
    /// Insert a new block with its counters.
    /// Returns false if the block was already present.
    pub fn insert(
        &mut self,
        hash: Hash,
        past_blue_work: Uint192,
        past_red_work: Uint192,
        anticone_blue_work: Uint192,
        anticone_reds_lower_bound: Uint192,
    ) -> bool {
        match self.past_blue_work.entry(hash) {
            Occupied(_) => return false,
            Vacant(e) => e.insert(past_blue_work),
        };
        self.past_red_work.insert(hash, past_red_work).is_none().then_some(()).unwrap();
        self.anticone_blue_work.insert(hash, anticone_blue_work).is_none().then_some(()).unwrap();
        self.arlb.insert(hash, anticone_reds_lower_bound).is_none().then_some(()).unwrap();
        // Track the current red index so we know which reds are new when computing incremental ARlb
        self.last_red_index.insert(hash, self.red_index).is_none().then_some(()).unwrap();

        let floor = SignedWork::from(past_red_work) + SignedWork::from(anticone_reds_lower_bound)
            - SignedWork::from(past_blue_work)
            - SignedWork::from(anticone_blue_work);
        self.btree.insert(CascadeTreeEntry::new(hash, floor.clone())).then_some(()).unwrap();
        self.rev_index.insert(hash, floor).is_none().then_some(()).unwrap();

        true
    }

    /// Update `anticone_blue_work` of an existing block.
    #[allow(dead_code)]
    pub fn _update_anticone_blue_work(&mut self, hash: Hash, anticone_blue_work: Uint192) {
        let prev_floor = self.rev_index[&hash].clone();
        let prev_anticone_blue_work = self.anticone_blue_work.insert(hash, anticone_blue_work).unwrap();
        let new_floor = prev_floor - (SignedWork::from(anticone_blue_work) - SignedWork::from(prev_anticone_blue_work));
        self.btree.remove(&CascadeTreeEntry::new(hash, prev_floor)).then_some(()).unwrap();
        self.btree.insert(CascadeTreeEntry::new(hash, new_floor.clone())).then_some(()).unwrap();
        self.rev_index.insert(hash, new_floor);
    }

    /// Update `anticone_reds_lower_bound` of an existing block.
    /// Returns true if the new value results in a floor increase.
    pub fn update_anticone_reds_lower_bound(&mut self, hash: Hash, anticone_reds_lower_bound: Uint192) -> bool {
        let prev_floor = self.rev_index[&hash].clone();
        let prev_arlb = self.arlb.insert(hash, anticone_reds_lower_bound).unwrap();
        let new_floor = prev_floor + (SignedWork::from(anticone_reds_lower_bound) - SignedWork::from(prev_arlb));

        if new_floor > prev_floor {
            self.btree.remove(&CascadeTreeEntry::new(hash, prev_floor)).then_some(()).unwrap();
            self.btree.insert(CascadeTreeEntry::new(hash, new_floor.clone())).then_some(()).unwrap();
            self.rev_index.insert(hash, new_floor);
            true
        } else {
            false
        }
    }

    /// Peek at the minimum entry in the tree.
    pub fn peek_min(&self) -> Option<CascadeTreeEntry> {
        self.btree.first().cloned()
    }

    /// Pop the minimum entry in the tree with all its counters.
    pub fn pop_min_with_counters(&mut self) -> Option<(CascadeTreeEntry, Uint192, Uint192, Uint192, Uint192)> {
        let min_entry = self.peek_min()?;
        self.btree.remove(&min_entry);
        let _prev_floor = self.rev_index.remove(&min_entry.hash).unwrap();
        let past_blue_work = self.past_blue_work.remove(&min_entry.hash).unwrap();
        let past_red_work = self.past_red_work.remove(&min_entry.hash).unwrap();
        let anticone_blue_work = self.anticone_blue_work.remove(&min_entry.hash).unwrap();
        let arlb = self.arlb.remove(&min_entry.hash).unwrap();
        Some((min_entry, past_blue_work, past_red_work, anticone_blue_work, arlb))
    }

    pub fn has(&self, hash: Hash) -> bool {
        self.rev_index.contains_key(&hash)
    }

    /// Get the anticone_blue_work of an existing block.
    pub fn get_anticone_blue_work(&self, hash: &Hash) -> Option<Uint192> {
        self.anticone_blue_work.get(hash).copied()
    }

    /// Update `anticone_blue_work` of an existing blue block.
    pub fn update_anticone_blue_work(&mut self, hash: Hash, new_anticone_blue_work: Uint192) {
        let prev_floor = self.rev_index[&hash].clone();
        let prev_abw = self.anticone_blue_work.insert(hash, new_anticone_blue_work).unwrap();
        let new_floor = prev_floor - (SignedWork::from(new_anticone_blue_work) - SignedWork::from(prev_abw));

        self.btree.remove(&CascadeTreeEntry::new(hash, prev_floor));
        self.btree.insert(CascadeTreeEntry::new(hash, new_floor.clone()));
        self.rev_index.insert(hash, new_floor);
    }

    pub fn is_empty(&self) -> bool {
        self.btree.is_empty()
    }
}

/// Cascade data structure.
#[derive(Default)]
pub struct CascadeDast {
    pub red_set: Vec<Hash>,
    tree: CascadeTree,
    secondary_heap: BTreeSet<PoppedBlue>,
}

/// Context for traversing the DAG (reachability oracle).
pub struct TraversalContext<'a, T: ReachabilityStoreReader + ?Sized> {
    /// The reachability oracle
    oracle: &'a MTReachabilityService<T>,
}

impl<'a, T: ReachabilityStoreReader + ?Sized> TraversalContext<'a, T> {
    pub fn new(reachability: &'a MTReachabilityService<T>) -> Self {
        Self { oracle: reachability }
    }
}

/// Colouring of a block in the conflict zone.
#[derive(Debug)]
pub enum BlockColouring {
    Blue { anticone_blue_work: Uint192, past_blue_work: Uint192, past_red_work: Uint192 },
    Red,
}

// ============================================================================
// CascadeContext — orchestrates the cascade voting process
// ============================================================================

/// Cascade voting context.
pub struct CascadeContext<'a, T: ReachabilityStoreReader + ?Sized, H: HeaderStoreReader + ?Sized> {
    ctx: TraversalContext<'a, T>,
    headers_store: &'a H,
    dast: CascadeDast,
    seen_red_work: Uint192,
    threshold: SignedWork,
    cached_vote: bool,
    negative_blues: Uint192,
    conflict_genesis: Hash,
}

impl<'a, T: ReachabilityStoreReader + ?Sized, H: HeaderStoreReader + ?Sized> CascadeContext<'a, T, H> {
    pub fn new(conflict_genesis: Hash, ctx: TraversalContext<'a, T>, headers_store: &'a H, threshold: SignedWork) -> Self {
        let cached_vote = true; // The empty set is a d-UMC by definition
        Self {
            conflict_genesis,
            ctx,
            headers_store,
            dast: Default::default(),
            threshold,
            cached_vote,
            negative_blues: Uint192::ZERO,
            seen_red_work: Uint192::ZERO,
        }
    }

    /// Create a cascade context from persisted state.
    /// Restores the primary tree, secondary heap, and all counters.
    pub fn from_persisted(
        conflict_genesis: Hash,
        ctx: TraversalContext<'a, T>,
        headers_store: &'a H,
        threshold: SignedWork,
        persisted: &UmcPersistedState,
    ) -> Self {
        let tree = restore_tree(persisted);
        let secondary_heap = BTreeSet::from_iter(persisted.secondary_heap.iter().cloned());

        Self {
            conflict_genesis,
            ctx,
            headers_store,
            dast: CascadeDast { red_set: persisted.red_set.clone(), tree, secondary_heap },
            seen_red_work: persisted.seen_red_work,
            threshold,
            cached_vote: persisted.cached_vote,
            negative_blues: persisted.negative_blues,
        }
    }

    /// Insert a new block into the cascade context.
    /// Returns whether the resulting blue cluster *contains* a subset of blocks which is
    /// a d-UMC (via incremental cascade voting).
    pub fn insert(&mut self, hash: Hash, colouring: BlockColouring) -> bool {
        if let BlockColouring::Blue { anticone_blue_work, past_blue_work, past_red_work } = colouring {
            // Blocks are inserted into the cascade context in topological order.
            let anticone_red_lower_bound_work = self.seen_red_work.saturating_sub(past_red_work);

            // future is empty, no need to subtract 1
            self.dast
                .tree
                .insert(hash, past_blue_work, past_red_work, anticone_blue_work, anticone_red_lower_bound_work)
                .then_some(())
                .unwrap();

            if self.cached_vote && hash != self.conflict_genesis {
                // A blue block preserves the positive vote
                return true;
            }
        } else {
            self.seen_red_work = self.seen_red_work + calc_work(self.headers_store.get_bits(hash).unwrap());
            // Track red insertion order and increment the tree's red index
            self.dast.red_set.push(hash);
            self.dast.tree.red_index += 1;

            if !self.try_promote_from_secondary(hash) && !self.cached_vote {
                // Red preserves negative vote — but don't short-circuit if any negatives were revived
                return true;
            }
        }

        self.cached_vote = self.vote();
        self.cached_vote
    }

    fn peek_min(&self) -> Option<CascadeTreeEntry> {
        self.dast.tree.peek_min()
    }

    fn is_empty(&self) -> bool {
        self.dast.tree.is_empty()
    }

    /// Run the cascade voting loop.
    /// Returns true if the blue cluster contains a d-UMC.
    ///
    /// A strict greater-than check is used since we require strict majority.
    pub fn vote(&mut self) -> bool {
        loop {
            let Some(min_entry) = self.peek_min() else {
                return false;
            };

            // negative_blues tracks the total work of blues that have been popped as negative.
            // As negative blues are found, their work is added to the threshold required to
            // cover majority.
            if min_entry.floor > self.threshold + SignedWork::from(self.negative_blues) {
                return true;
            }

            // Incrementally update ARlb: only scan reds that are new since this blue was last evaluated
            let last_index = *self.dast.tree.last_red_index.get(&min_entry.hash).unwrap();
            let current_index = self.dast.tree.red_index;

            // Start from the blue's stored ARlb and add contributions from new reds in its anticone
            let mut new_arlb = *self.dast.tree.arlb.get(&min_entry.hash).unwrap();
            for &red in &self.dast.red_set[last_index..current_index] {
                if self.ctx.oracle.is_dag_ancestor_of(red, min_entry.hash) || self.ctx.oracle.is_dag_ancestor_of(min_entry.hash, red) {
                    // Red is in blue's past or future, not in anticone
                    continue;
                }
                // Red is in blue's anticone — add its work
                new_arlb = new_arlb + calc_work(self.headers_store.get_bits(red).unwrap_or(0x207fffff));
            }

            if self.dast.tree.update_anticone_reds_lower_bound(min_entry.hash, new_arlb) {
                // Floor improved — re-check from the top
                continue;
            }

            // Update last_red_index to current index since we've now processed all new reds
            self.dast.tree.last_red_index.insert(min_entry.hash, current_index);

            // Result is a negative blue — pop it and store in secondary heap
            let (entry, past_blue_work, past_red_work, anticone_blue_work, arlb) = self.dast.tree.pop_min_with_counters().unwrap();
            self.negative_blues = self.negative_blues + calc_work(self.headers_store.get_bits(entry.hash).unwrap());

            self.dast.secondary_heap.insert(PoppedBlue {
                hash: entry.hash,
                floor: entry.floor,
                past_blue_work,
                past_red_work,
                anticone_blue_work,
                arlb,
                last_red_index: current_index,
            });

            if self.is_empty() {
                return false;
            }

            // iteration += 1;
        }
    }

    /// Check secondary heap for blues whose floor has improved past the threshold.
    /// Promotes recovered blues back to the primary tree and decrements negative_blues.
    /// Returns true if any blues were promoted.
    pub(super) fn try_promote_from_secondary(&mut self, _found_red: Hash) -> bool {
        let mut any_promoted = false;

        loop {
            if self.dast.secondary_heap.is_empty() {
                break;
            }

            let current_index = self.dast.tree.red_index;
            let mut recovered = Vec::new();

            for pb in self.dast.secondary_heap.iter() {
                // Incrementally update ARlb: scan reds new since this blue was last evaluated
                let mut current_arlb = pb.arlb;
                for &red in &self.dast.red_set[pb.last_red_index..current_index] {
                    if self.ctx.oracle.is_dag_ancestor_of(red, pb.hash) || self.ctx.oracle.is_dag_ancestor_of(pb.hash, red) {
                        // Red is in blue's past or future, not in anticone
                        continue;
                    }
                    // Red is in blue's anticone — add its work
                    current_arlb = current_arlb + calc_work(self.headers_store.get_bits(red).unwrap_or(0x207fffff));
                }

                let current_anticone_blue_work = pb.anticone_blue_work;

                let new_floor = SignedWork::from(pb.past_red_work) + SignedWork::from(current_arlb)
                    - SignedWork::from(pb.past_blue_work)
                    - SignedWork::from(current_anticone_blue_work);

                if new_floor >= self.threshold + SignedWork::from(self.negative_blues) {
                    recovered.push((
                        pb.hash,
                        pb.past_blue_work,
                        pb.past_red_work,
                        current_anticone_blue_work,
                        current_arlb,
                        current_index,
                    ));
                }
            }

            if recovered.is_empty() {
                break;
            }

            for (hash, past_blue_work, past_red_work, anticone_blue_work, arlb, last_red_index) in recovered {
                self.dast.tree.insert(hash, past_blue_work, past_red_work, anticone_blue_work, arlb);
                // Restore the last_red_index for the promoted blue so incremental updates continue correctly
                self.dast.tree.last_red_index.insert(hash, last_red_index);
                self.negative_blues = self.negative_blues - calc_work(self.headers_store.get_bits(hash).unwrap());
                self.dast.secondary_heap.retain(|pb| pb.hash != hash);
            }

            any_promoted = true;
        }

        any_promoted
    }

    /// Extract current state for persistence.
    /// The `tip_set_hash` is populated by the caller for staleness detection.
    pub(super) fn extract_state(&self, tip_set_hash: Hash) -> UmcPersistedState {
        let tree_entries: Vec<UmcPersistedTreeEntry> = self
            .dast
            .tree
            .rev_index
            .iter()
            .map(|(&hash, floor)| UmcPersistedTreeEntry {
                hash,
                floor: floor.clone(),
                past_blue_work: *self.dast.tree.past_blue_work.get(&hash).unwrap(),
                past_red_work: *self.dast.tree.past_red_work.get(&hash).unwrap(),
                anticone_blue_work: *self.dast.tree.anticone_blue_work.get(&hash).unwrap(),
                arlb: *self.dast.tree.arlb.get(&hash).unwrap(),
                last_red_index: *self.dast.tree.last_red_index.get(&hash).unwrap(),
            })
            .collect();

        let secondary_heap: Vec<PoppedBlue> = self.dast.secondary_heap.iter().cloned().collect();

        UmcPersistedState {
            tree_entries,
            red_index: self.dast.tree.red_index,
            red_set: self.dast.red_set.clone(),
            secondary_heap,
            seen_red_work: self.seen_red_work,
            negative_blues: self.negative_blues,
            cached_vote: self.cached_vote,
            tip_set_hash,
        }
    }
}

// ============================================================================
// UmcVoter — the main entry point for UMC cascade voting
// ============================================================================

/// Input data for UMC cascade voting.
#[derive(Clone)]
pub struct UmcVoterInput {
    pub conflict_genesis: Hash,
    pub k: KType,
    pub next_chain_ancestor: Hash,
    pub blue_set: BlockHashSet,
    pub red_set: Vec<Hash>,
    pub blue_work: Uint192,
    pub red_work: Uint192,
    pub deficit: Uint192,
    pub deficit_work_basis: Uint192,
    pub virtual_coloring_data_map: HashMap<Hash, PastColoringData>,
}

/// UMC cascade voter.
pub struct UmcVoter<'a, T, H>
where
    T: ReachabilityStoreReader + ?Sized,
    H: HeaderStoreReader + ?Sized,
{
    reachability: &'a MTReachabilityService<T>,
    headers_store: &'a H,
}

impl<'a, T, H> UmcVoter<'a, T, H>
where
    T: ReachabilityStoreReader + ?Sized,
    H: HeaderStoreReader + ?Sized,
{
    /// Create a new UmcVoter.
    pub fn new(reachability: &'a MTReachabilityService<T>, headers_store: &'a H) -> Self {
        Self { reachability, headers_store }
    }

    // ------------------------------------------------------------------
    // Cascade Voting
    // ------------------------------------------------------------------

    pub fn run_cascade(&self, input: &UmcVoterInput) -> bool {
        // Deficit and threshold
        let deficit = Uint192::from_u64(input.k.isqrt() as u64) * input.deficit_work_basis;
        // threshold = total_red_work - total_blue_work - deficit
        let threshold_work = SignedWork::from(input.red_work) - SignedWork::from(input.blue_work) - SignedWork::from(deficit);

        let traversal_ctx = TraversalContext::new(self.reachability);

        let mut cascade_ctx = CascadeContext::new(input.conflict_genesis, traversal_ctx, self.headers_store, threshold_work);

        // Build topological heap: blues and reds in reverse topological order (grays are skipped)
        let mut topological_heap: BinaryHeap<Reverse<SortableBlock>> = BinaryHeap::new();

        // Insert blues
        for &hash in input.blue_set.iter() {
            if hash != input.conflict_genesis {
                let header = self.headers_store.get_header(hash).expect("header must exist");
                topological_heap.push(Reverse(SortableBlock { hash, blue_work: header.blue_work }));
            }
        }

        // Insert reds
        for &hash in input.red_set.iter() {
            let header = self.headers_store.get_header(hash).expect("header must exist");
            topological_heap.push(Reverse(SortableBlock { hash, blue_work: header.blue_work }));
        }

        // Insert conflict genesis
        topological_heap.push(Reverse(SortableBlock { hash: input.conflict_genesis, blue_work: Uint192::ZERO }));

        // Process in topological order
        while let Some(Reverse(SortableBlock { hash, .. })) = topological_heap.pop() {
            let coloring = if input.blue_set.contains(&hash) {
                let counters = input.virtual_coloring_data_map.get(&hash).cloned().unwrap_or_default();

                let past_blue_work = counters.past_blue_work;
                let past_red_work = counters.past_red_work;
                let anticone_blue_work = counters.anticone_blue_work;

                BlockColouring::Blue { anticone_blue_work, past_blue_work, past_red_work }
            } else {
                BlockColouring::Red
            };

            cascade_ctx.insert(hash, coloring);
        }

        cascade_ctx.vote()
    }

    /// Run cascade voting incrementally, restoring from persisted state when available.
    ///
    /// Returns `(vote_result, final_state, was_restored)` where:
    /// - `vote_result`: whether the blue cluster contains a d-UMC
    /// - `final_state`: the cascade state after processing (for persistence)
    /// - `was_restored`: true if state was restored from persistence, false if computed from scratch
    pub fn run_cascade_incremental(
        &self,
        input: &UmcVoterInput,
        persisted: Option<UmcPersistedState>,
    ) -> (bool, UmcPersistedState, bool) {
        // info!("incremental UMC triggered | cg: {} | k: {} | nca: {}", input.conflict_genesis, input.k, input.next_chain_ancestor);
        // Deficit and threshold
        let deficit = Uint192::from_u64(input.k.isqrt() as u64) * input.deficit_work_basis;
        let threshold_work = SignedWork::from(input.red_work) - SignedWork::from(input.blue_work) - SignedWork::from(deficit);

        let traversal_ctx = TraversalContext::new(self.reachability);

        // Determine whether we can restore from persisted state
        let mut was_restored = false;
        let persisted_blues: BlockHashSet = if let Some(ref persisted) = persisted {
            let persisted_blue_hashes: BlockHashSet =
                persisted.tree_entries.iter().map(|e| e.hash).chain(persisted.secondary_heap.iter().map(|p| p.hash)).collect();

            if persisted_blue_hashes.is_subset(&input.blue_set) {
                // Persisted blues are still valid — mark as restorable
                was_restored = true;
                persisted_blue_hashes
            } else {
                // Zone shrank or changed — fall back to from-scratch
                BlockHashSet::default()
            }
        } else {
            BlockHashSet::default()
        };

        let mut cascade_ctx = if was_restored {
            CascadeContext::from_persisted(
                input.conflict_genesis,
                traversal_ctx,
                self.headers_store,
                threshold_work,
                persisted.as_ref().unwrap(),
            )
        } else {
            // No persisted state or stale — from scratch
            CascadeContext::new(input.conflict_genesis, traversal_ctx, self.headers_store, threshold_work)
        };

        // Determine which blues/reds are new (not already in cascade_ctx's tree)
        let new_blues: Vec<Hash> = input.blue_set.iter().filter(|h| !persisted_blues.contains(h)).copied().collect();

        let new_reds: Vec<Hash> = if let Some(ref persisted) = persisted {
            input.red_set.iter().filter(|h| !persisted.red_set.contains(*h)).copied().collect()
        } else {
            input.red_set.clone()
        };

        if was_restored && new_blues.is_empty() && new_reds.is_empty() {
            // Zone unchanged — use cached vote if available, otherwise run vote()
            if cascade_ctx.cached_vote {
                let result = cascade_ctx.cached_vote;
                let state = cascade_ctx.extract_state(Hash::default());
                return (result, state, was_restored);
            }
            // Vote was negative, need to re-check
            let result = cascade_ctx.vote();
            let state = cascade_ctx.extract_state(Hash::default());
            return (result, state, was_restored);
        }

        // Process new blues in topological order
        let mut topological_heap: BinaryHeap<Reverse<SortableBlock>> = BinaryHeap::new();

        for &hash in new_blues.iter() {
            if hash != input.conflict_genesis {
                let header = self.headers_store.get_header(hash).expect("header must exist");
                topological_heap.push(Reverse(SortableBlock { hash, blue_work: header.blue_work }));
            }
        }

        // Process new reds — insert them directly (no topological ordering needed for reds)
        for &red_hash in new_reds.iter() {
            cascade_ctx.seen_red_work = cascade_ctx.seen_red_work + calc_work(self.headers_store.get_bits(red_hash).unwrap());
            cascade_ctx.dast.red_set.push(red_hash);
            cascade_ctx.dast.tree.red_index += 1;

            if !cascade_ctx.try_promote_from_secondary(red_hash) && !cascade_ctx.cached_vote {
                // Red preserves negative vote — but don't short-circuit if any negatives were revived
                let result = true;
                let state = cascade_ctx.extract_state(Hash::default());
                return (result, state, was_restored);
            }
        }

        // Insert new blues in topological order
        while let Some(Reverse(SortableBlock { hash, .. })) = topological_heap.pop() {
            let counters = input.virtual_coloring_data_map.get(&hash).cloned().unwrap_or_default();
            let past_blue_work = counters.past_blue_work;
            let past_red_work = counters.past_red_work;
            let anticone_blue_work = counters.anticone_blue_work;

            cascade_ctx.insert(hash, BlockColouring::Blue { anticone_blue_work, past_blue_work, past_red_work });
        }

        let result = cascade_ctx.cached_vote;
        let state = cascade_ctx.extract_state(Hash::default());
        (result, state, was_restored)
    }
}

impl MemSizeEstimator for UmcVoterInput {
    fn estimate_mem_bytes(&self) -> usize {
        size_of::<Self>() + self.blue_set.len() * std::mem::size_of::<Hash>() + self.red_set.len() * std::mem::size_of::<Hash>()
    }
}

/// Restore a `CascadeTree` from persisted state.
fn restore_tree(persisted: &UmcPersistedState) -> CascadeTree {
    // use kaspa_math::Uint192;
    // use kaspa_math::int::SignedInteger;

    // type SignedWork = SignedInteger<Uint192>;

    let mut tree = CascadeTree { red_index: persisted.red_index, ..Default::default() };

    for entry in &persisted.tree_entries {
        tree.past_blue_work.insert(entry.hash, entry.past_blue_work);
        tree.past_red_work.insert(entry.hash, entry.past_red_work);
        tree.anticone_blue_work.insert(entry.hash, entry.anticone_blue_work);
        tree.arlb.insert(entry.hash, entry.arlb);
        tree.last_red_index.insert(entry.hash, entry.last_red_index);

        let floor = entry.floor.clone();
        tree.btree.insert(CascadeTreeEntry::new(entry.hash, floor.clone()));
        tree.rev_index.insert(entry.hash, floor);
    }

    tree
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::stores::dagknight::UmcPersistenceKey;

    #[test]
    fn test_persistence_key_construction() {
        let cg: Hash = 0xAA_u64.into();
        let nca: Hash = 0xBB_u64.into();
        let k: KType = 5;

        let key_committed = UmcPersistenceKey::new(cg, k, nca, false);
        let key_free = UmcPersistenceKey::new(cg, k, nca, true);

        // Same fields except free_search → different keys
        assert_ne!(key_committed.as_ref(), key_free.as_ref());
        assert_ne!(key_committed, key_free);

        // Verify free_search flag is the last byte
        assert_eq!(*key_committed.as_ref().last().unwrap(), 0u8);
        assert_eq!(*key_free.as_ref().last().unwrap(), 1u8);
    }

    #[test]
    fn test_persistence_key_different_k() {
        let cg: Hash = 0xAA_u64.into();
        let nca: Hash = 0xBB_u64.into();

        let key_k1 = UmcPersistenceKey::new(cg, 1, nca, false);
        let key_k2 = UmcPersistenceKey::new(cg, 2, nca, false);

        assert_ne!(key_k1, key_k2);
        assert_ne!(key_k1.as_ref(), key_k2.as_ref());
    }

    #[test]
    fn test_persistence_key_different_nca() {
        let cg: Hash = 0xAA_u64.into();
        let nca1: Hash = 0xBB_u64.into();
        let nca2: Hash = 0xCC_u64.into();

        let key1 = UmcPersistenceKey::new(cg, 5, nca1, false);
        let key2 = UmcPersistenceKey::new(cg, 5, nca2, false);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_persistence_key_same_fields_equal() {
        let cg: Hash = 0xAA_u64.into();
        let nca: Hash = 0xBB_u64.into();

        let key1 = UmcPersistenceKey::new(cg, 5, nca, false);
        let key2 = UmcPersistenceKey::new(cg, 5, nca, false);

        assert_eq!(key1, key2);
        assert_eq!(key1.as_ref(), key2.as_ref());
    }

    #[test]
    fn test_persisted_state_roundtrip() {
        let hash1: Hash = 0x01_u64.into();
        let hash2: Hash = 0x02_u64.into();

        let state = UmcPersistedState {
            tree_entries: vec![
                UmcPersistedTreeEntry {
                    hash: hash1,
                    floor: SignedWork::from(Uint192::from_u64(100)),
                    past_blue_work: Uint192::from_u64(50),
                    past_red_work: Uint192::from_u64(30),
                    anticone_blue_work: Uint192::from_u64(10),
                    arlb: Uint192::from_u64(20),
                    last_red_index: 0,
                },
                UmcPersistedTreeEntry {
                    hash: hash2,
                    floor: SignedWork::from(Uint192::from_u64(80)),
                    past_blue_work: Uint192::from_u64(40),
                    past_red_work: Uint192::from_u64(25),
                    anticone_blue_work: Uint192::from_u64(5),
                    arlb: Uint192::from_u64(15),
                    last_red_index: 1,
                },
            ],
            red_index: 2,
            red_set: vec![0x10_u64.into(), 0x20_u64.into()],
            secondary_heap: vec![PoppedBlue {
                hash: 0x30_u64.into(),
                floor: SignedWork::from(Uint192::from_u64(50)),
                past_blue_work: Uint192::from_u64(25),
                past_red_work: Uint192::from_u64(15),
                anticone_blue_work: Uint192::from_u64(5),
                arlb: Uint192::from_u64(10),
                last_red_index: 1,
            }],
            seen_red_work: Uint192::from_u64(100),
            negative_blues: Uint192::from_u64(30),
            cached_vote: true,
            tip_set_hash: Hash::default(),
        };

        // Clone as a proxy for serialization roundtrip
        let restored = state.clone();
        assert_eq!(state, restored);
        assert_eq!(state.tree_entries.len(), 2);
        assert_eq!(state.red_set.len(), 2);
        assert_eq!(state.secondary_heap.len(), 1);
    }

    #[test]
    fn test_restore_tree_from_persisted_state() {
        let hash1: Hash = 0x01_u64.into();
        let hash2: Hash = 0x02_u64.into();

        let state = UmcPersistedState {
            tree_entries: vec![
                UmcPersistedTreeEntry {
                    hash: hash1,
                    floor: SignedWork::from(Uint192::from_u64(100)),
                    past_blue_work: Uint192::from_u64(50),
                    past_red_work: Uint192::from_u64(30),
                    anticone_blue_work: Uint192::from_u64(10),
                    arlb: Uint192::from_u64(20),
                    last_red_index: 0,
                },
                UmcPersistedTreeEntry {
                    hash: hash2,
                    floor: SignedWork::from(Uint192::from_u64(80)),
                    past_blue_work: Uint192::from_u64(40),
                    past_red_work: Uint192::from_u64(25),
                    anticone_blue_work: Uint192::from_u64(5),
                    arlb: Uint192::from_u64(15),
                    last_red_index: 1,
                },
            ],
            red_index: 2,
            red_set: vec![],
            secondary_heap: vec![],
            seen_red_work: Uint192::ZERO,
            negative_blues: Uint192::ZERO,
            cached_vote: true,
            tip_set_hash: Hash::default(),
        };

        let tree = restore_tree(&state);

        // Verify tree was restored with correct entries
        assert!(!tree.is_empty());
        assert_eq!(tree.red_index, 2);

        // Verify hash1 is in tree
        assert!(tree.has(hash1));
        assert_eq!(*tree.past_blue_work.get(&hash1).unwrap(), Uint192::from_u64(50));
        assert_eq!(*tree.past_red_work.get(&hash1).unwrap(), Uint192::from_u64(30));
        assert_eq!(*tree.anticone_blue_work.get(&hash1).unwrap(), Uint192::from_u64(10));
        assert_eq!(*tree.arlb.get(&hash1).unwrap(), Uint192::from_u64(20));
        assert_eq!(*tree.last_red_index.get(&hash1).unwrap(), 0);

        // Verify hash2 is in tree
        assert!(tree.has(hash2));
        assert_eq!(*tree.past_blue_work.get(&hash2).unwrap(), Uint192::from_u64(40));

        // Verify ordering: hash2 (floor=80) should be min, hash1 (floor=100) should be next
        let min_entry = tree.peek_min().unwrap();
        assert_eq!(min_entry.hash, hash2, "hash2 should have lower floor (80 < 100)");
    }

    #[test]
    fn test_restore_tree_negative_floor() {
        // Test with negative floor values (common in cascade voting)
        let hash1: Hash = 0x01_u64.into();

        // Create a negative floor: past_red(10) + arlb(5) - past_blue(100) - anticone_blue(10) = -95
        let floor = SignedWork::from(Uint192::from_u64(10)) + SignedWork::from(Uint192::from_u64(5))
            - SignedWork::from(Uint192::from_u64(100))
            - SignedWork::from(Uint192::from_u64(10));

        let state = UmcPersistedState {
            tree_entries: vec![UmcPersistedTreeEntry {
                hash: hash1,
                floor,
                past_blue_work: Uint192::from_u64(100),
                past_red_work: Uint192::from_u64(10),
                anticone_blue_work: Uint192::from_u64(10),
                arlb: Uint192::from_u64(5),
                last_red_index: 0,
            }],
            red_index: 0,
            red_set: vec![],
            secondary_heap: vec![],
            seen_red_work: Uint192::ZERO,
            negative_blues: Uint192::ZERO,
            cached_vote: false,
            tip_set_hash: Hash::default(),
        };

        let tree = restore_tree(&state);
        assert!(tree.has(hash1));

        let min_entry = tree.peek_min().unwrap();
        assert_eq!(min_entry.hash, hash1);
    }

    #[test]
    fn test_cascade_tree_insert_then_restore_consistency() {
        // Build a tree via insert(), then verify restore produces equivalent state
        let hash1: Hash = 0x01_u64.into();
        let hash2: Hash = 0x02_u64.into();

        let mut tree = CascadeTree::default();
        tree.insert(hash1, Uint192::from_u64(50), Uint192::from_u64(30), Uint192::from_u64(10), Uint192::from_u64(20));
        tree.insert(hash2, Uint192::from_u64(40), Uint192::from_u64(25), Uint192::from_u64(5), Uint192::from_u64(15));
        tree.red_index = 2;

        // Build persisted state from the tree
        let persisted = UmcPersistedState {
            tree_entries: vec![
                UmcPersistedTreeEntry {
                    hash: hash1,
                    floor: tree.rev_index[&hash1].clone(),
                    past_blue_work: *tree.past_blue_work.get(&hash1).unwrap(),
                    past_red_work: *tree.past_red_work.get(&hash1).unwrap(),
                    anticone_blue_work: *tree.anticone_blue_work.get(&hash1).unwrap(),
                    arlb: *tree.arlb.get(&hash1).unwrap(),
                    last_red_index: *tree.last_red_index.get(&hash1).unwrap(),
                },
                UmcPersistedTreeEntry {
                    hash: hash2,
                    floor: tree.rev_index[&hash2].clone(),
                    past_blue_work: *tree.past_blue_work.get(&hash2).unwrap(),
                    past_red_work: *tree.past_red_work.get(&hash2).unwrap(),
                    anticone_blue_work: *tree.anticone_blue_work.get(&hash2).unwrap(),
                    arlb: *tree.arlb.get(&hash2).unwrap(),
                    last_red_index: *tree.last_red_index.get(&hash2).unwrap(),
                },
            ],
            red_index: 2,
            red_set: vec![],
            secondary_heap: vec![],
            seen_red_work: Uint192::ZERO,
            negative_blues: Uint192::ZERO,
            cached_vote: true,
            tip_set_hash: Hash::default(),
        };

        // Restore and verify equivalence
        let restored = restore_tree(&persisted);

        assert_eq!(tree.red_index, restored.red_index);
        assert!(restored.has(hash1));
        assert!(restored.has(hash2));

        // Verify floors match
        assert_eq!(tree.rev_index[&hash1], restored.rev_index[&hash1]);
        assert_eq!(tree.rev_index[&hash2], restored.rev_index[&hash2]);

        // Verify min entry is the same
        let orig_min = tree.peek_min().unwrap();
        let restored_min = restored.peek_min().unwrap();
        assert_eq!(orig_min.hash, restored_min.hash);
        assert_eq!(orig_min.floor, restored_min.floor);
    }

    /// Integration test: verifies incremental UMC voting correctly restores from persisted state
    /// and produces the same result as from-scratch computation.
    ///
    /// Scenario: same conflict genesis, same K value, but a few blocks added between runs.
    /// The test verifies that:
    /// 1. First run computes from scratch (was_restored=false)
    /// 2. Second run with additional blocks restores from persisted state (was_restored=true)
    /// 3. Both incremental and from-scratch produce the same vote result
    #[test]
    fn test_incremental_umc_voting_restores_from_persisted_state() {
        use super::super::super::difficulty::calc_work;
        use super::super::super::reachability::tests::{DagBlock, DagBuilder};
        use crate::model::stores::{
            dagknight::{MemoryUmcPersistenceStore, UmcPersistenceKey, UmcPersistenceStore, UmcPersistenceStoreReader},
            headers::MemoryHeaderStore,
            reachability::MemoryReachabilityStore,
            relations::MemoryRelationsStore,
        };
        use kaspa_consensus_core::blockhash::ORIGIN;
        use kaspa_consensus_core::header::Header;
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        // Build a simple DAG with a conflict:
        //
        //        G (genesis)
        //       / \
        //      B1  R1
        //      |   |
        //      B2  R2
        //      |   |
        //      B3  R3
        //
        // Blues: G, B1, B2, B3 (chain)
        // Reds: R1, R2, R3 (parallel chain)
        //
        // First run: blues = {G, B1}, reds = {R1}
        // Second run: blues = {G, B1, B2, B3}, reds = {R1, R2, R3}
        //
        // Same conflict genesis (G), same K, same NCA

        // Hash assignments
        let genesis: Hash = 1_u64.into();
        let b1: Hash = 2_u64.into();
        let b2: Hash = 3_u64.into();
        let b3: Hash = 4_u64.into();
        let r1: Hash = 5_u64.into();
        let r2: Hash = 6_u64.into();
        let r3: Hash = 7_u64.into();

        let bits = 0x207fffff;
        let work_per_block = calc_work(bits);

        // Set up stores
        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();
        let headers_store = Arc::new(MemoryHeaderStore::new());

        // Build DAG
        {
            let mut builder = DagBuilder::new(&mut reachability, &mut relations);
            builder.init();
            builder.add_block(DagBlock::new(genesis, vec![ORIGIN]));
            builder.add_block(DagBlock::new(b1, vec![genesis]));
            builder.add_block(DagBlock::new(b2, vec![b1]));
            builder.add_block(DagBlock::new(b3, vec![b2]));
            builder.add_block(DagBlock::new(r1, vec![genesis]));
            builder.add_block(DagBlock::new(r2, vec![r1]));
            builder.add_block(DagBlock::new(r3, vec![r2]));

            // Insert headers
            for (hash, parents) in [
                (genesis, vec![]),
                (b1, vec![genesis]),
                (b2, vec![b1]),
                (b3, vec![b2]),
                (r1, vec![genesis]),
                (r2, vec![r1]),
                (r3, vec![r2]),
            ] {
                let mut header = Header::from_precomputed_hash(hash, parents);
                header.bits = bits;
                headers_store.insert(Arc::new(header));
            }
        }

        let reachability_service = MTReachabilityService::new(Arc::new(RwLock::new(reachability)));
        let voter = UmcVoter::new(&reachability_service, &*headers_store);
        let persistence_store = MemoryUmcPersistenceStore::default();

        let k: KType = 4;
        let next_chain_ancestor: Hash = 100_u64.into(); // NCA is above the zone
        let deficit_work_basis = work_per_block;
        let deficit = Uint192::from_u64(k.isqrt() as u64) * deficit_work_basis;

        // ------------------------------------------------------------------
        // FIRST RUN: blues = {G, B1}, reds = {R1}
        // ------------------------------------------------------------------
        let blue_work_1 = work_per_block * 2u64; // G + B1
        let red_work_1 = work_per_block; // R1

        let blue_set_1: BlockHashSet = [genesis, b1].iter().copied().collect();
        let red_set_1 = vec![r1];

        let mut virtual_coloring_data_map_1 = HashMap::new();
        virtual_coloring_data_map_1.insert(genesis, PastColoringData::default());
        virtual_coloring_data_map_1.insert(
            b1,
            PastColoringData {
                past_blue_work: work_per_block, // G
                past_red_work: Uint192::ZERO,
                anticone_blue_work: Uint192::ZERO,
            },
        );

        let input_1 = UmcVoterInput {
            conflict_genesis: genesis,
            k,
            next_chain_ancestor,
            blue_set: blue_set_1.clone(),
            red_set: red_set_1.clone(),
            blue_work: blue_work_1,
            red_work: red_work_1,
            deficit,
            deficit_work_basis,
            virtual_coloring_data_map: virtual_coloring_data_map_1,
        };

        // First run: no persisted state → from scratch
        let (_vote_1, state_1, was_restored_1) = voter.run_cascade_incremental(&input_1, None);

        assert!(!was_restored_1, "First run should compute from scratch (was_restored=false)");

        // Persist the state
        let key_1 = UmcPersistenceKey::new(genesis, k, next_chain_ancestor, false);
        persistence_store.insert(key_1.clone(), state_1.clone()).unwrap();

        // Verify state was persisted
        let retrieved = persistence_store.get(key_1.clone()).unwrap().expect("state should be persisted");
        assert_eq!(retrieved, state_1);

        // ------------------------------------------------------------------
        // SECOND RUN: blues = {G, B1, B2, B3}, reds = {R1, R2, R3}
        // ------------------------------------------------------------------
        let blue_work_2 = work_per_block * 4u64; // G + B1 + B2 + B3
        let red_work_2 = work_per_block * 3u64; // R1 + R2 + R3

        let blue_set_2: BlockHashSet = [genesis, b1, b2, b3].iter().copied().collect();
        let red_set_2 = vec![r1, r2, r3];

        let mut virtual_coloring_data_map_2 = HashMap::new();
        virtual_coloring_data_map_2.insert(genesis, PastColoringData::default());
        virtual_coloring_data_map_2.insert(
            b1,
            PastColoringData { past_blue_work: work_per_block, past_red_work: Uint192::ZERO, anticone_blue_work: Uint192::ZERO },
        );
        virtual_coloring_data_map_2.insert(
            b2,
            PastColoringData {
                past_blue_work: work_per_block * 2u64, // G + B1
                past_red_work: Uint192::ZERO,
                anticone_blue_work: Uint192::ZERO,
            },
        );
        virtual_coloring_data_map_2.insert(
            b3,
            PastColoringData {
                past_blue_work: work_per_block * 3u64, // G + B1 + B2
                past_red_work: Uint192::ZERO,
                anticone_blue_work: Uint192::ZERO,
            },
        );

        let input_2 = UmcVoterInput {
            conflict_genesis: genesis,
            k,
            next_chain_ancestor,
            blue_set: blue_set_2.clone(),
            red_set: red_set_2.clone(),
            blue_work: blue_work_2,
            red_work: red_work_2,
            deficit,
            deficit_work_basis,
            virtual_coloring_data_map: virtual_coloring_data_map_2,
        };

        // Second run: with persisted state → should restore and process deltas
        let key_2 = UmcPersistenceKey::new(genesis, k, next_chain_ancestor, false);
        let persisted_for_2 = persistence_store.get(key_2.clone()).unwrap();

        let (vote_2_incremental, state_2_incremental, was_restored_2) = voter.run_cascade_incremental(&input_2, persisted_for_2);

        assert!(was_restored_2, "Second run should restore from persisted state (was_restored=true)");

        // Persist updated state
        persistence_store.insert(key_2.clone(), state_2_incremental).unwrap();

        // ------------------------------------------------------------------
        // VERIFY: incremental result matches from-scratch computation
        // ------------------------------------------------------------------
        let vote_2_from_scratch = voter.run_cascade(&input_2);

        assert_eq!(vote_2_incremental, vote_2_from_scratch, "Incremental result must match from-scratch result");

        // ------------------------------------------------------------------
        // THIRD RUN: same input as second, verify cached vote is used
        // ------------------------------------------------------------------
        let persisted_for_3 = persistence_store.get(key_2.clone()).unwrap();
        let (vote_3, _state_3, was_restored_3) = voter.run_cascade_incremental(&input_2, persisted_for_3);

        assert!(was_restored_3, "Third run should also restore from persisted state");
        assert_eq!(vote_3, vote_2_from_scratch, "Cached vote should match");
    }
}
