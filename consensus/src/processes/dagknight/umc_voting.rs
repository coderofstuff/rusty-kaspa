use std::{
    cmp::{Ordering, Reverse},
    collections::{
        BTreeSet, BinaryHeap, HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
};

use kaspa_consensus_core::{BlockHashMap, BlockHashSet, KType};
use kaspa_hashes::Hash;
use kaspa_math::{Uint192, int::SignedInteger};
use kaspa_utils::mem_size::MemSizeEstimator;

/// Signed work value (difference of two Uint192 work values).
type SignedWork = SignedInteger<Uint192>;

use crate::{
    model::{
        services::reachability::{MTReachabilityService, ReachabilityService},
        stores::{dagknight::PastColoringData, headers::HeaderStoreReader, reachability::ReachabilityStoreReader},
    },
    processes::{difficulty::calc_work, ghostdag::ordering::SortableBlock},
};

// ============================================================================
// Cascade data structures
// ============================================================================

/// A blue block that was popped from the primary tree as "negative".
#[derive(Eq, Clone)]
pub struct PoppedBlue {
    pub hash: Hash,
    pub floor: SignedWork,
    pub past_blue_work: Uint192,
    pub past_red_work: Uint192,
    pub anticone_blue_work: Uint192,
    pub arlb: Uint192,
    pub last_red_index: usize,
}

impl PartialEq for PoppedBlue {
    fn eq(&self, other: &Self) -> bool {
        self.floor == other.floor && self.hash == other.hash
    }
}

impl PartialOrd for PoppedBlue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PoppedBlue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.floor.partial_cmp(&other.floor).unwrap_or(Ordering::Equal).then_with(|| self.hash.cmp(&other.hash))
    }
}

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
    fn try_promote_from_secondary(&mut self, _found_red: Hash) -> bool {
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
}

impl MemSizeEstimator for UmcVoterInput {
    fn estimate_mem_bytes(&self) -> usize {
        size_of::<Self>() + self.blue_set.len() * std::mem::size_of::<Hash>() + self.red_set.len() * std::mem::size_of::<Hash>()
    }
}
