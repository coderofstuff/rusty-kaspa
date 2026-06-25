use std::{
    cmp::Ordering,
    collections::{
        BTreeSet, BinaryHeap, HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    sync::Arc,
};

use kaspa_consensus_core::{BlockHashMap, BlockHashSet, KType};
use kaspa_hashes::Hash;
use kaspa_math::{Uint192, int::SignedInteger};
use kaspa_utils::mem_size::MemSizeEstimator;
use parking_lot::RwLock;

/// Signed work value (difference of two Uint192 work values).
type SignedWork = SignedInteger<Uint192>;

use crate::{
    model::{
        services::reachability::{MTReachabilityService, ReachabilityService},
        stores::{
            dagknight::PastColoringData, headers::HeaderStoreReader, reachability::ReachabilityStoreReader,
            relations::RelationsStoreReader,
        },
    },
    processes::{difficulty::calc_work, ghostdag::ordering::SortableBlock},
};

// ============================================================================
// Cascade data structures
// ============================================================================

/// A blue block that was popped from the primary tree as "negative".
/// Stored in the secondary heap so it can be promoted back when its
/// floor improves (e.g., when new reds increase its ARlb).
#[derive(Eq, Clone)]
pub struct PoppedBlue {
    pub hash: Hash,
    pub floor: SignedWork,
    /// Counters preserved to allow floor recalculation
    pub past_blue_work: Uint192,
    pub past_red_work: Uint192,
    pub anticone_blue_work: Uint192,
    /// Anticone reds lower bound — updated when reds arrive
    pub arlb: Uint192,
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

#[allow(dead_code)]
impl PoppedBlue {
    /// Recalculate floor from preserved counters.
    pub fn recalc_floor(&self) -> SignedWork {
        SignedWork::from(self.past_red_work) + SignedWork::from(self.arlb)
            - SignedWork::from(self.past_blue_work)
            - SignedWork::from(self.anticone_blue_work)
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
/// Maintains exact per-block counters for floor computation.
#[derive(Default)]
pub struct CascadeTree {
    btree: BTreeSet<CascadeTreeEntry>,
    rev_index: BlockHashMap<SignedWork>,

    // Exact counters (work-based)
    pub past_blue_work: BlockHashMap<Uint192>,
    pub past_red_work: BlockHashMap<Uint192>,
    pub anticone_blue_work: BlockHashMap<Uint192>,

    /// Anticone reds lower bound (work)
    pub arlb: BlockHashMap<Uint192>,
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

    /// Pop the minimum entry in the tree (legacy, discards counters).
    #[allow(dead_code)]
    pub fn pop_min(&mut self) -> Option<(CascadeTreeEntry, SignedWork)> {
        let (entry, _pb, _pr, _ab, _arlb) = self.pop_min_with_counters()?;
        let floor = entry.floor;
        Some((entry, floor))
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

/// Cascade data structure — contains sets and heaps for the cascade voting process.
#[derive(Default)]
pub struct CascadeDast {
    /// All blocks in the zone (as of this processing point)
    pub g: BlockHashSet,

    /// Blue set
    pub blueset: BlockHashSet,

    /// Gray set
    pub grayset: BlockHashSet,

    /// B-tree ordered by floor values
    tree: CascadeTree,

    /// Secondary heap of blues popped as "negative".
    /// When reds arrive, these blues' ARlb may improve, allowing
    /// some to be promoted back to the primary tree.
    secondary_heap: BTreeSet<PoppedBlue>,
}

/// Context for traversing the DAG (reachability + relations oracles).
pub struct TraversalContext<'a, T: ReachabilityStoreReader + ?Sized, S: RelationsStoreReader + ?Sized> {
    /// The reachability oracle
    oracle: &'a MTReachabilityService<T>,
    /// The relations oracle (local DAG area)
    _relations: &'a Arc<RwLock<S>>,
}

impl<'a, T: ReachabilityStoreReader + ?Sized, S: RelationsStoreReader + ?Sized> TraversalContext<'a, T, S> {
    pub fn new(reachability: &'a MTReachabilityService<T>, _relations: &'a Arc<RwLock<S>>) -> Self {
        Self { oracle: reachability, _relations }
    }
}

/// Colouring of a block in the conflict zone.
#[derive(Debug)]
pub enum BlockColouring {
    Blue { anticone_blue_work: Uint192, past_blue_work: Uint192 },
    Red,
    Gray,
}

// ============================================================================
// CascadeContext — orchestrates the cascade voting process
// ============================================================================

/// Cascade voting context. Maintains the vote state and processes blocks
/// in topological order to determine if the blue cluster has a d-UMC.
pub struct CascadeContext<'a, T: ReachabilityStoreReader + ?Sized, S: RelationsStoreReader + ?Sized, H: HeaderStoreReader + ?Sized> {
    /// Traversal context (reachability + relations oracles)
    ctx: TraversalContext<'a, T, S>,

    /// Headers store for work lookups
    headers_store: &'a H,

    /// Cascade data structure
    dast: CascadeDast,

    /// Observed work so far
    seen_red_work: Uint192,

    /// The allowed deficit (normalized by work basis)
    threshold: SignedWork,

    /// Cached result of cascade voting
    cached_vote: bool,

    /// Count of negative blues (in block-count units)
    negative_blues: i64,

    /// Conflict genesis
    conflict_genesis: Hash,
}

impl<'a, T: ReachabilityStoreReader + ?Sized, S: RelationsStoreReader + ?Sized, H: HeaderStoreReader + ?Sized>
    CascadeContext<'a, T, S, H>
{
    pub fn new(conflict_genesis: Hash, ctx: TraversalContext<'a, T, S>, headers_store: &'a H, threshold: SignedWork) -> Self {
        let cached_vote = true; // The empty set is a d-UMC by definition
        Self {
            conflict_genesis,
            ctx,
            headers_store,
            dast: Default::default(),
            threshold,
            cached_vote,
            negative_blues: 0,
            seen_red_work: Uint192::ZERO,
        }
    }

    /// Insert a new block into the cascade context.
    /// Returns whether the resulting blue cluster *contains* a subset of blocks which is
    /// a d-UMC (via incremental cascade voting).
    pub fn insert(&mut self, hash: Hash, colouring: BlockColouring) -> bool {
        // println!("\t↳ inserting: {} | colouring: {:?}", hash.to_le_u64()[3], colouring);
        self.dast.g.insert(hash).then_some(()).unwrap();

        if let BlockColouring::Blue { anticone_blue_work, past_blue_work } = colouring {
            self.dast.blueset.insert(hash).then_some(()).unwrap();

            // Blocks are inserted into the cascade context in topological order.
            // This means that every red block seen so far is an anti-future of this blue
            let antifuture_red_work_lb = self.seen_red_work;

            // TODO[DK]: Cleanup DAST so past_red_work and anticone_reds_lower_bound is consolidated
            // future is empty, no need to subtract 1
            self.dast
                .tree
                .insert(hash, past_blue_work, Uint192::ZERO, anticone_blue_work, antifuture_red_work_lb)
                .then_some(())
                .unwrap();

            if self.cached_vote && hash != self.conflict_genesis {
                // A blue block preserves the positive vote
                return true;
            }
        } else if let BlockColouring::Gray = colouring {
            self.dast.grayset.insert(hash).then_some(()).unwrap();
        } else {
            self.seen_red_work = self.seen_red_work + calc_work(self.headers_store.get_bits(hash).unwrap());
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

    fn is_genesis_in_heap(&self) -> bool {
        self.dast.tree.has(self.conflict_genesis)
    }

    /// Run the cascade voting loop.
    /// Returns true if the blue cluster contains a d-UMC.
    pub fn vote(&mut self) -> bool {
        // let mut iteration = 0;
        loop {
            if !self.is_genesis_in_heap() {
                // println!(
                //     "\t\t\t↳ iteration: {} | genesis not in heap | threshold: {} | negative_blues: {} | sufficient: false",
                //     iteration, self.threshold, self.negative_blues
                // );
                return false;
            }

            let Some(min_entry) = self.peek_min() else {
                return false;
            };

            // println!(
            //     "\t\t\t↳ iteration: {} | hash: {} | floor: {} | threshold: {} | negative_blues: {} | sufficient: {}",
            //     iteration,
            //     min_entry.hash.to_le_u64()[3],
            //     min_entry.floor,
            //     self.threshold,
            //     self.negative_blues,
            //     min_entry.floor > self.threshold + SignedWork::from(Uint192::from_u64(self.negative_blues as u64))
            // );
            // TODO[DK]: Should equal be allowed. For now, no, since we're claiming "majority"
            if min_entry.floor > self.threshold + SignedWork::from(Uint192::from_u64(self.negative_blues as u64)) {
                return true;
            }

            // Calculate exact ARlb: count non-gray reds in the anticone of the minimum-floor entry
            let arlb = self
                .dast
                .g
                .difference(&self.dast.blueset)
                .filter(|&&red| {
                    !self.dast.grayset.contains(&red)
                        && !self.ctx.oracle.is_dag_ancestor_of(red, min_entry.hash)
                        && !self.ctx.oracle.is_dag_ancestor_of(min_entry.hash, red)
                })
                .map(|&r| self.headers_store.get_bits(r).unwrap_or(0x207fffff))
                .map(calc_work)
                .fold(Uint192::ZERO, |acc, w| acc + w);

            if self.dast.tree.update_anticone_reds_lower_bound(min_entry.hash, arlb) {
                // Floor improved — re-check from the top
                continue;
            }

            // Result is a negative blue — pop it and store in secondary heap
            let (entry, past_blue_work, past_red_work, anticone_blue_work, arlb) = self.dast.tree.pop_min_with_counters().unwrap();
            self.negative_blues += 1;

            self.dast.secondary_heap.insert(PoppedBlue {
                hash: entry.hash,
                floor: entry.floor,
                past_blue_work,
                past_red_work,
                anticone_blue_work,
                arlb,
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
    fn try_promote_from_secondary(&mut self, found_red: Hash) -> bool {
        let mut any_promoted = false;

        loop {
            if self.dast.secondary_heap.is_empty() {
                break;
            }

            let mut recovered = Vec::new();

            for pb in self.dast.secondary_heap.iter() {
                if self.ctx.oracle.is_dag_ancestor_of(found_red, pb.hash) || self.ctx.oracle.is_dag_ancestor_of(pb.hash, found_red) {
                    // this new red is not in the anticone of this blue, skip
                    continue;
                }

                // The current red is in the anticone of the popped blue.
                // It was not counted in the PB's arlb yet.
                let current_arlb = pb.arlb + calc_work(self.headers_store.get_bits(found_red).unwrap_or(0x207fffff));

                // TODO[DK]: Should we accurately calculate anticone blues at this time?
                let current_anticone_blue_work = pb.anticone_blue_work;

                let new_floor = SignedWork::from(pb.past_red_work) + SignedWork::from(current_arlb)
                    - SignedWork::from(pb.past_blue_work)
                    - SignedWork::from(current_anticone_blue_work);

                if new_floor >= self.threshold + SignedWork::from(Uint192::from_u64(self.negative_blues as u64)) {
                    recovered.push((pb.hash, pb.past_blue_work, pb.past_red_work, current_anticone_blue_work, current_arlb));
                }
            }

            if recovered.is_empty() {
                break;
            }

            for (hash, past_blue_work, past_red_work, anticone_blue_work, arlb) in recovered {
                self.dast.tree.insert(hash, past_blue_work, past_red_work, anticone_blue_work, arlb);
                self.negative_blues -= 1;
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
/// Contains all zone data collected from the VSPC chain walk, so that
/// the voter itself is self-contained and testable.
#[derive(Clone)]
pub struct UmcVoterInput {
    /// The conflict genesis block
    pub conflict_genesis: Hash,

    /// The k parameter for the k-cluster
    pub k: KType,

    /// Next chain ancestor of the winning subgroup
    pub next_chain_ancestor: Hash,

    /// Set of blue blocks in the zone
    pub blue_set: BlockHashSet,

    /// Set of red blocks (adversarial) in the zone
    pub red_set: BlockHashSet,

    /// Total work of blue blocks
    pub blue_work: Uint192,

    /// Total work of non-gray red blocks
    pub red_work: Uint192,

    /// Deficit: isqrt(k) * work(conflict_genesis)
    pub deficit: Uint192,

    /// Work of a single block at the conflict genesis difficulty
    pub deficit_work_basis: Uint192,

    /// virtual coloring data map
    pub virtual_coloring_data_map: HashMap<Hash, PastColoringData>,
}

/// UMC cascade voter.
///
/// Collects zone data from the VSPC chain walk, computes per-block counters,
/// and runs the cascade voting algorithm to determine if the blue cluster
/// contains a d-UMC.
///
/// Type parameters:
/// - `T`: reachability store reader type
/// - `S`: relations store reader type
/// - `H`: header store reader type
pub struct UmcVoter<'a, T, S, H>
where
    T: ReachabilityStoreReader + ?Sized,
    S: RelationsStoreReader + ?Sized,
    H: HeaderStoreReader + ?Sized,
{
    /// Reachability oracle for ancestry checks
    reachability: &'a MTReachabilityService<T>,

    /// Relations store for parent lookups
    relations: &'a Arc<RwLock<S>>,

    /// Headers store for work lookups
    headers_store: &'a H,
}

impl<'a, T, S, H> UmcVoter<'a, T, S, H>
where
    T: ReachabilityStoreReader + ?Sized,
    S: RelationsStoreReader + ?Sized,
    H: HeaderStoreReader + ?Sized,
{
    /// Create a new UmcVoter.
    pub fn new(reachability: &'a MTReachabilityService<T>, relations: &'a Arc<RwLock<S>>, headers_store: &'a H) -> Self {
        Self { reachability, relations, headers_store }
    }

    // ------------------------------------------------------------------
    // Cascade Voting
    // ------------------------------------------------------------------

    pub fn run_cascade(&self, input: &UmcVoterInput) -> bool {
        // Deficit and threshold
        let deficit = Uint192::from_u64(input.k.isqrt() as u64) * input.deficit_work_basis;
        // threshold = total_red_work - total_blue_work - deficit
        // any blue blocks whose past_blues - past_reds are at or above this threshold have a positive vote
        let threshold_work = SignedWork::from(input.red_work) - SignedWork::from(input.blue_work) - SignedWork::from(deficit);

        let traversal_ctx = TraversalContext::new(self.reachability, self.relations);

        let mut cascade_ctx = CascadeContext::new(input.conflict_genesis, traversal_ctx, self.headers_store, threshold_work);

        // TODO[DK]: Clean these up
        // Build topological heap: blues and reds in reverse topological order (grays are skipped)
        let mut topological_heap: BinaryHeap<SortableBlock> = BinaryHeap::new();

        // Insert blues
        for &hash in input.blue_set.iter() {
            if hash != input.conflict_genesis {
                let header = self.headers_store.get_header(hash).expect("header must exist");
                topological_heap.push(SortableBlock { hash, blue_work: header.blue_work });
            }
        }

        // Insert reds
        for &hash in input.red_set.iter() {
            let header = self.headers_store.get_header(hash).expect("header must exist");
            topological_heap.push(SortableBlock { hash, blue_work: header.blue_work });
        }

        // Insert conflict genesis
        topological_heap.push(SortableBlock { hash: input.conflict_genesis, blue_work: Uint192::ZERO });

        // Process in topological order
        while let Some(SortableBlock { hash, .. }) = topological_heap.pop() {
            let coloring = if input.blue_set.contains(&hash) {
                let counters = input.virtual_coloring_data_map.get(&hash).cloned().unwrap_or_default();

                let past_blue_work = counters.past_blue_work;
                let anticone_blue_work = counters.anticone_blue_work;

                BlockColouring::Blue { anticone_blue_work, past_blue_work }
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
