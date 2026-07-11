use std::{
    cell::Cell,
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use dashmap::DashMap;
use itertools::Itertools;
use kaspa_consensus_core::KType;
use kaspa_core::{debug, time::Stopwatch, trace};
use kaspa_hashes::Hash;
use kaspa_math::Uint192;

use parking_lot::RwLock;

use crate::{
    model::{
        services::reachability::{MTReachabilityService, ReachabilityService},
        stores::{
            dagknight::{
                ColoringData, DagknightStore, DagknightStoreReader, PastColoringData, UmcPersistenceKey,
                UmcPersistenceStats, UmcPersistenceStore,
            },
            headers::HeaderStoreReader,
            reachability::ReachabilityStoreReader,
            relations::RelationsStoreReader,
        },
    },
    processes::{
        dagknight::{
            GroupMetadata,
            manager::ConflictZoneManager,
            rank_search::RankSearcher,
            tie_breaking::{DagknightTieBreaker, TieBreakInput, TieBreaker},
            umc_voting::{UmcVoter, UmcVoterInput},
        },
        difficulty::calc_work,
        ghostdag::ordering::SortableBlock,
        reachability::relations::FutureIntersectRelations,
    },
};

/// TODO[DK]: If writes here are moved out or batched, revisit this
/// Global lock map for conflict genesis level locking
/// Maps conflict genesis hashes to their respective locks
static CONFLICT_LOCKS: OnceLock<DashMap<Hash, Arc<RwLock<()>>>> = OnceLock::new();

fn get_conflict_locks() -> &'static DashMap<Hash, Arc<RwLock<()>>> {
    CONFLICT_LOCKS.get_or_init(DashMap::new)
}

/// Cleans up unused locks from the global lock map.
/// A lock is considered unused if its Arc strong count is 1 (only the map holds a reference).
/// This should be called periodically or opportunistically to prevent unbounded growth.
pub fn cleanup_conflict_locks() {
    let locks = get_conflict_locks();
    let mut to_remove = Vec::new();

    // First pass: identify locks with no external references
    for entry in locks.iter() {
        let hash = *entry.key();
        let arc = entry.value();
        // If strong count is 1, only the DashMap holds a reference
        if Arc::strong_count(arc) == 1 {
            to_remove.push(hash);
        }
    }

    // Second pass: remove identified locks
    // Note: by the time we remove, another thread might have acquired the lock,
    // so we double-check the count before removing
    let mut removed_count = 0;
    for hash in to_remove {
        if let Some(entry) = locks.get(&hash)
            && Arc::strong_count(entry.value()) == 1
        {
            drop(entry); // Release the reference before removing
            locks.remove(&hash);
            removed_count += 1;
        }
    }

    if removed_count > 0 {
        trace!("Cleaned up {} unused conflict locks, {} remaining", removed_count, locks.len());
    }
}

/// A struct encapsulating the logic and algorithms of the DAGKNIGHT protocol
#[derive(Clone)]
pub struct DagknightExecutor<
    C: DagknightStore + DagknightStoreReader,
    O: HeaderStoreReader + 'static,
    D: RelationsStoreReader + Clone,
    R: ReachabilityStoreReader + Clone,
> {
    pub genesis_hash: Hash,
    pub dagknight_store: Arc<C>,
    pub headers_store: Arc<O>,
    pub relations_store: Arc<RwLock<D>>,
    pub reachability_service: MTReachabilityService<R>,
    pub umc_persistence_store: Option<Arc<dyn UmcPersistenceStore + Send + Sync>>,
    /// Optional stats tracker for incremental UMC persistence effort savings.
    pub umc_persistence_stats: Option<Arc<UmcPersistenceStats>>,
}

#[derive(Clone)]
pub struct DagknightData {
    pub selected_parent: Hash,               // The selected parent for this call
    pub conflict_ordered_parents: Vec<Hash>, // The rest of the parents, ordered by conflict hierarchy (parents from latest/topmost conflicts first)
}

impl<
    C: DagknightStore + DagknightStoreReader,
    O: HeaderStoreReader + 'static,
    D: RelationsStoreReader + Clone,
    R: ReachabilityStoreReader + Clone,
> DagknightExecutor<C, O, D, R>
{
    pub fn dagknight(&self, parents: &[Hash]) -> DagknightData {
        /*
            input: a set of block parents
            output: the selected parent + incremental metadata

            Algo scheme:
                Run DK from the bottom up per conflict, for each conflict search through k and find the minimal
                committed k-cluster which confirms to UMC cascade voting with parameter d=sqrt(k)

            High-level tasks/challenges:
                1. Incremental k-colouring -- known from GD
                2. Iterating through conflicts -- requires finding the common chain-ancestor which
                   is a simple operation, though it might require optimizing with an indexed chain
                   (and using logarithmic step searches)
                3. Representatives (alternatively: gray blocks)
                4. Tie-breaking rule
                5. Cascade voting -- requires most thought for making incremental
        */

        // g = find LCCA
        let mut conflict_genesis = self.common_chain_ancestor(parents);
        let mut curr_subgroup = Arc::new(parents.to_vec());
        let mut conflict_ordered_parents = vec![];
        debug!("conflict_genesis: {:#?}", conflict_genesis);

        while curr_subgroup.len() > 1 {
            let agreement_grouping: HashMap<Hash, Arc<Vec<Hash>>> = curr_subgroup
                .iter()
                .copied()
                .into_group_map_by(|&parent| self.reachability_service.get_next_chain_ancestor(parent, conflict_genesis))
                .into_iter()
                .map(|(k, v)| (k, Arc::new(v)))
                .collect();

            // Shortcut condition to avoid doing unnecessary work
            if agreement_grouping.len() == 1 {
                // There is exactly one group, we don't rank anymore.
                let (_, subgroup) = agreement_grouping.iter().next().unwrap();
                curr_subgroup = subgroup.clone();
                let next_conflict_genesis = self.common_chain_ancestor(&curr_subgroup);
                assert_ne!(
                    next_conflict_genesis, conflict_genesis,
                    "Expected the conflict genesis to change after skipping a level of the conflict hierarchy but got {}",
                    conflict_genesis
                );
                conflict_genesis = next_conflict_genesis;
                continue;
            }

            // Pick a "winner" among these subgroups
            // The incremental check (per-block UMC per the paper) is authoritative.
            // The basic check (aggregate work-based) serves as a secondary validation.
            let ((winning_conflict_genesis, winning_subgroup), winning_k) = {
                let best_groups = self.rank(conflict_genesis, &agreement_grouping, &curr_subgroup, true);
                let winning_k = best_groups[0].k;
                let final_winner = if best_groups.len() > 1 {
                    self.tie_breaking(conflict_genesis, &curr_subgroup, &best_groups)
                } else {
                    let single_winner = best_groups.into_iter().next().expect("best_groups should be non-empty after filtering");
                    (single_winner.conflict_genesis, single_winner.subgroup)
                };
                (final_winner, winning_k)
            };

            // Sanity check: basic (aggregate) and incremental (per-block) should agree
            // When they disagree, the incremental check (per-block UMC) is authoritative per the DK paper
            let sanity_check = {
                let best_groups_basic = self.rank(conflict_genesis, &agreement_grouping, &curr_subgroup, false);
                let basic_winning_k = best_groups_basic[0].k;
                let final_winner_basic = if best_groups_basic.len() > 1 {
                    self.tie_breaking(conflict_genesis, &curr_subgroup, &best_groups_basic)
                } else {
                    let single = best_groups_basic.into_iter().next().expect("best_groups should be non-empty after filtering");
                    (single.conflict_genesis, single.subgroup)
                };
                (final_winner_basic, basic_winning_k)
            };

            assert!(
                winning_subgroup == sanity_check.0.1,
                "UMC basic and incremental differ for conflict_genesis {}, basic_winner {:?}, incremental_winner {:?} | k_basic = {} | k_incremental = {} | using incremental",
                conflict_genesis.to_le_u64()[3],
                sanity_check.0.1.iter().map(|h| h.to_le_u64()[3]).collect_vec(),
                winning_subgroup.iter().map(|h| h.to_le_u64()[3]).collect_vec(),
                sanity_check.1,
                winning_k
            );

            // Add the non-winners to the ordered parents
            agreement_grouping.iter().for_each(|(&conflict_genesis, subgroup)| {
                // TODO[DK]: Asserting here that order of the non-winning parents within a conflict hierarchy doesn't matter
                if conflict_genesis != winning_conflict_genesis {
                    conflict_ordered_parents.extend(subgroup.as_ref().iter().copied());
                }
            });

            curr_subgroup = winning_subgroup;
            // Skip to the top-most new common chain ancestor:
            conflict_genesis = self.common_chain_ancestor(&curr_subgroup);
        }
        assert_eq!(1, curr_subgroup.len(), "Expected dagknight to have only a single parent at the end");

        conflict_ordered_parents.reverse();

        debug!("dk::sp: {} | conflict_ordered_parents: {:?}", curr_subgroup[0], conflict_ordered_parents);

        // Opportunistically cleanup unused locks after processing
        cleanup_conflict_locks();

        DagknightData { selected_parent: curr_subgroup[0], conflict_ordered_parents }
    }

    fn common_chain_ancestor(&self, parents: &[Hash]) -> Hash {
        /*
           Notes:
               - ignore parents not agreeing on the pruning point as a chain block
               - optimize for shortest path
               - optimize with index
        */

        let start = parents[0];

        if start == self.genesis_hash {
            return self.genesis_hash;
        }

        for cb in self.reachability_service.default_backward_chain_iterator(start).skip(1) {
            if self.reachability_service.is_chain_ancestor_of_all(cb, &parents[1..]) {
                return cb;
            }
        }

        panic!("")
    }

    fn umc_cascade_voting(
        &self,
        conflict_genesis: Hash,
        _subgroup: &[Hash],
        virtual_cd: Arc<ColoringData>,
        k: KType,
        _conflict_zone_manager: &ConflictZoneManager<C, O, D, R>,
    ) -> bool {
        /*
            inputs: G, U, d
            output: does U have a subset U' s.t. U' is d-UMC of G
                    where d-UMC means that each block in U' is majority covered by U' (up to d)

            sketch 1:
                maintain the blue/total past sizes and blue/total anticone sizes for each blue block
            problems:
                1. keeping the anticone size can be costly (a single attacker block with a huge anticone would dirty many entries)
                2. challenging to reason about blue blocks which can be treated as red (U / U')
                3. plus does not benefit from the above


        */
        let deficit_work_basis = calc_work(self.headers_store.get_bits(conflict_genesis).unwrap());
        let deficit = Uint192::from_u64(k.isqrt() as u64) * deficit_work_basis;

        virtual_cd.blue_work + deficit > virtual_cd.past_red_work
    }

    /// Incremental UMC cascade voting using UmcVoter with floor-based heap.
    ///
    /// For each blue block B, the floor is a lower bound on its score:
    ///   floor(B) = past_red_work(B) + arlb(B) - effective_past_blue(B) - anticone_blue_work(B)
    /// where effective_past_blue = past_blue_work(B) + work(B) (absorbs self-term into floor).
    ///
    /// The check is: floor(B) >= -deficit' where deficit' = total_blue_work + deficit - total_non_gray_red_work.
    /// This is equivalent to: future_blue_work(B) + deficit >= future_red_work(B).
    ///
    /// Gray reds (chain ancestors of the winning subgroup's next chain ancestor) are excluded.
    ///
    /// Delegates to [UmcVoter] in the `umc_voting` module.
    ///
    /// If persistence store is available, attempts to restore from persisted state and
    /// only processes new blocks (delta computation).
    fn incremental_umc_voting(
        &self,
        conflict_genesis: Hash,
        k: KType,
        conflict_zone_manager: &ConflictZoneManager<C, O, D, R>,
        subgroup: &[Hash],
        virtual_cd: &ColoringData,
    ) -> bool {
        // Compute the next chain ancestor of the subgroup
        let next_chain_ancestor = self.reachability_service.get_next_chain_ancestor(subgroup[0], conflict_genesis);

        // Walk VSPC chain exactly like the basic check to collect zone data
        let mut curr_cd: Arc<ColoringData> = Arc::new(virtual_cd.clone());

        let mut blue_set = kaspa_consensus_core::BlockHashSet::default();
        let mut red_set: Vec<Hash> = Vec::new();

        let blue_work = virtual_cd.blue_work;
        let red_work = virtual_cd.past_red_work;

        // Deficit is sqrt(k) * work(conflict_genesis) in work units
        let deficit_work_basis = calc_work(self.headers_store.get_bits(conflict_genesis).unwrap());
        let deficit = Uint192::from_u64(k.isqrt() as u64) * deficit_work_basis;

        if blue_work + deficit <= red_work {
            // Short-circuit if we know that root doesn't have majority coverage:
            return false;
        }

        // TODO[DK]: Move all this below to umc_voting
        let mut virtual_coloring_data_map = HashMap::<Hash, PastColoringData>::new();
        let mut chain_blocks: Vec<super::umc_voting::ChainBlockData> = Vec::new();

        let blue_anticone_map = self.extract_blue_anticone_map(conflict_genesis, &conflict_zone_manager, Arc::new(virtual_cd.clone()));

        loop {
            if curr_cd.selected_parent == conflict_genesis {
                break;
            }

            // Collect chain block data for incremental traversal
            let cb_data = super::umc_voting::ChainBlockData {
                hash: curr_cd.selected_parent,
                mergeset_blues: curr_cd.mergeset_blues.iter().copied().collect(),
                mergeset_reds: curr_cd.mergeset_reds.iter().copied().collect(),
            };
            chain_blocks.push(cb_data);

            // Process mergeset blues: collect into blue set and compute past counters
            for &mbb in curr_cd.mergeset_blues.iter().filter(|&&b| self.reachability_service.is_dag_ancestor_of(conflict_genesis, b)) {
                blue_set.insert(mbb);

                let (past_blue_work, past_red_work) = if curr_cd.selected_parent == mbb {
                    let sp_cd = conflict_zone_manager.get_data(curr_cd.selected_parent).unwrap();
                    (sp_cd.blue_work, sp_cd.past_red_work)
                } else {
                    let anticone_blue_work = curr_cd.blues_anticone_work.get(&mbb).copied().unwrap();

                    let future_blue_work: Uint192 = curr_cd
                        .mergeset_blues
                        .iter()
                        .filter(|&&c| c != mbb && self.reachability_service.is_dag_ancestor_of(mbb, c))
                        .map(|&c| calc_work(self.headers_store.get_bits(c).unwrap()))
                        .sum();

                    let self_work = calc_work(self.headers_store.get_bits(mbb).unwrap());
                    let past_blue_work = curr_cd.blue_work - anticone_blue_work - future_blue_work - self_work;

                    // past_red_work: subtract only future reds from curr_cd.past_red_work.
                    // The overestimate in past_red_work is exactly offset by the corresponding
                    // underestimate in arlb, so the floor remains accurate.
                    let future_red_work: Uint192 = curr_cd
                        .mergeset_reds
                        .iter()
                        .filter(|&&r| self.reachability_service.is_dag_ancestor_of(mbb, r))
                        .map(|&r| calc_work(self.headers_store.get_bits(r).unwrap()))
                        .sum();

                    let past_red_work_upper_bound = curr_cd.past_red_work - future_red_work;

                    (past_blue_work, past_red_work_upper_bound)
                };

                let mut coloring_data = PastColoringData::default();
                coloring_data.past_blue_work = past_blue_work;
                coloring_data.past_red_work = past_red_work;

                if let Some(&anticone_work) = blue_anticone_map.get(&mbb) {
                    coloring_data.anticone_blue_work = anticone_work;
                }

                virtual_coloring_data_map.insert(mbb, coloring_data);
            }

            curr_cd.mergeset_reds.iter().for_each(|&b| {
                if !self.reachability_service.is_chain_ancestor_of(next_chain_ancestor, b) {
                    red_set.push(b);
                }
            });

            curr_cd = conflict_zone_manager.get_data(curr_cd.selected_parent).unwrap();
        }

        blue_set.insert(conflict_genesis);
        virtual_coloring_data_map.insert(conflict_genesis, PastColoringData::default());

        let input = UmcVoterInput {
            conflict_genesis,
            k,
            next_chain_ancestor,
            blue_set: blue_set.clone(),
            red_set: red_set.clone(),
            blue_work,
            red_work,
            deficit,
            deficit_work_basis,
            virtual_coloring_data_map,
            chain_blocks,
        };

        // Try incremental voting with persistence
        let voter = UmcVoter::new(&self.reachability_service, &*self.headers_store);

        if let Some(store) = &self.umc_persistence_store {
            // Attempt to load persisted state
            let key = UmcPersistenceKey::new(conflict_genesis, k, next_chain_ancestor, false);
            let persisted = match store.get(key.clone()) {
                Ok(Some(state)) => Some(state),
                Ok(None) | Err(_) => None, // Fallback to from-scratch on any error
            };

            // Compute zone size for stats (before voting)
            let zone_blocks = input.blue_set.len() + input.red_set.len();
            let persisted_blocks = persisted
                .as_ref()
                .map(|s| s.tree_entries.len() + s.secondary_heap.len())
                .unwrap_or(0);

            let (result, new_state, was_restored) = voter.run_cascade_incremental(&input, persisted);

            // DEBUG: assert incremental matches full run — remove when confident
            let full_result = voter.run_cascade(&input);
            assert!(
                result == full_result,
                "Incremental ({}) differs from full run ({}) for conflict_genesis {} | k={} | was_restored={}",
                result,
                full_result,
                conflict_genesis,
                k,
                was_restored,
            );

            // Record stats
            if let Some(stats) = &self.umc_persistence_stats {
                stats.record(was_restored, persisted_blocks, zone_blocks);
            }

            // Persist updated state
            if let Err(e) = store.insert(key, new_state) {
                trace!("Failed to persist UMC state: {:?}", e);
            }

            result
        } else {
            // No persistence store — fall back to from-scratch
            voter.run_cascade(&input)
        }
    }

    /// Tie-breaking rule in case of multiple winning subgroups with the same rank value.
    fn tie_breaking(&self, conflict_genesis: Hash, all_tips: &[Hash], subgroups: &[GroupMetadata]) -> (Hash, Arc<Vec<Hash>>) {
        debug!("Winning groups had rank k = {}", subgroups[0].k);
        let mutual_k = subgroups[0].k;

        let winning_index = DagknightTieBreaker::new(
            self.dagknight_store.clone(),
            self.headers_store.clone(),
            self.relations_store.clone(),
            self.reachability_service.clone(),
        )
        .tie_break(&TieBreakInput { conflict_genesis, all_tips, subgroups, k: mutual_k });

        let winning_conflict_genesis = subgroups[winning_index].conflict_genesis;
        let winning_subgroup = subgroups[winning_index].subgroup.clone();

        (winning_conflict_genesis, winning_subgroup)
    }

    /// Follows the Calculate-Rank algorithm in the DK paper
    ///
    /// Currently returns both the Rank and a selected parent (deviates from the paper) since the tie breaking logic
    /// in the caller is simply using blue_work + hash to break ties between subgroups.
    ///
    /// Returns an array of winning subgroups with their metadata
    fn rank(
        &self,
        conflict_genesis: Hash,
        agreeing_subgroups: &HashMap<Hash, Arc<Vec<Hash>>>,
        all_tips: &[Hash],
        use_incremental: bool,
    ) -> Vec<GroupMetadata> {
        let mut group_map = Cell::new(agreeing_subgroups.clone());
        let best_groups_cell = Cell::new(vec![]);
        let evaluate = |k: KType| -> Option<()> {
            let (filtered_groups_kv, best_groups): (HashMap<_, _>, Vec<GroupMetadata>) = group_map
                .get_mut()
                .iter()
                .filter_map(|(curr_conflict_genesis, subgroup)| {
                    // `subgroup` is an `&Arc<Vec<Hash>>` here; pass a `&[Hash]` to the colouring function
                    self.select_parent_from_k_colouring(conflict_genesis, subgroup.as_ref(), &all_tips, k, use_incremental).map(
                        |selected_parent| {
                            (
                                (*curr_conflict_genesis, subgroup.clone()),
                                GroupMetadata {
                                    conflict_genesis: *curr_conflict_genesis,
                                    subgroup: subgroup.clone(),
                                    k,
                                    selected_parent,
                                },
                            )
                        },
                    )
                })
                .unzip();

            if filtered_groups_kv.is_empty() {
                None
            } else {
                group_map.swap(&Cell::new(filtered_groups_kv));
                best_groups_cell.swap(&Cell::new(best_groups));
                Some(())
            }
        };

        let _search_result = RankSearcher::search(evaluate);
        // let (best_k) = search_result.map(|r| (r.k, r.result)).unwrap();
        best_groups_cell.take()
    }

    /// Applies a coloring to the conflict zone, and determines if the
    /// coloring represents a majority over "g" only (as opposed to full UMC)
    /// TODO[DK]: Implement full UMC cascade voting after coloring
    fn select_parent_from_k_colouring(
        &self,
        conflict_genesis: Hash,
        subgroup: &[Hash],
        all_tips: &[Hash],
        k_to_check: KType,
        use_incremental: bool,
    ) -> Option<SortableBlock> {
        let reachability_service = self.reachability_service.clone();
        let relations_store = self.relations_store.read();
        let relations_service = FutureIntersectRelations::new(relations_store.clone(), reachability_service.clone(), conflict_genesis);
        let conflict_zone_manager = ConflictZoneManager::new(
            k_to_check,
            conflict_genesis,
            self.dagknight_store.clone(),
            self.headers_store.clone(),
            relations_service,
            reachability_service.clone(),
        );

        // Acquire a lock for this conflict_genesis to prevent concurrent writes
        let locks = get_conflict_locks();
        let lock_arc = locks.entry(conflict_genesis).or_insert_with(|| Arc::new(RwLock::new(()))).clone();
        let _lock = lock_arc.write();

        conflict_zone_manager.fill_zone_data(all_tips);

        // selected a parent in this subgroup => Conditioned upon virtual agreeing with this subgroup
        let subgroup_virtual_sp = conflict_zone_manager.find_selected_parent(subgroup.iter().copied());
        let virtual_cd = conflict_zone_manager.k_colouring(all_tips, k_to_check, Some(subgroup_virtual_sp));

        let umc_result = if !use_incremental {
            self.umc_cascade_voting(conflict_genesis, subgroup, virtual_cd.clone(), k_to_check, &conflict_zone_manager)
        } else {
            let stopwatch = Stopwatch::new("incremental_umc_voting");
            let incremental_umc =
                self.incremental_umc_voting(conflict_genesis, k_to_check, &conflict_zone_manager, subgroup, &virtual_cd);
            if stopwatch.elapsed() > std::time::Duration::from_millis(500) {
                debug!(
                    "UMC voting took {:#?} for conflict_genesis {}, subgroup size {}, k = {}",
                    stopwatch.elapsed(),
                    conflict_genesis,
                    subgroup.len(),
                    k_to_check
                );
            }

            incremental_umc
        };

        if umc_result {
            Some(SortableBlock {
                hash: subgroup_virtual_sp,
                blue_work: self.headers_store.get_header(subgroup_virtual_sp).unwrap().blue_work,
            })
        } else {
            None
        }
    }

    fn extract_blue_anticone_map(
        &self,
        conflict_genesis: Hash,
        czm: &ConflictZoneManager<C, O, D, R>,
        pov_cd: Arc<ColoringData>,
    ) -> HashMap<Hash, Uint192> {
        let mut anticone_map = HashMap::new();
        let mut curr_cd = pov_cd;

        loop {
            if curr_cd.selected_parent == conflict_genesis {
                break;
            }

            for (&hash, &work) in &curr_cd.blues_anticone_work {
                anticone_map.entry(hash).or_insert(work);
            }

            curr_cd = czm.get_data(curr_cd.selected_parent).unwrap();
        }

        anticone_map
    }
}

#[derive(Clone)]
pub struct DagPlan {
    genesis: u64,
    blocks: Vec<(u64, Vec<u64>)>, // All blocks other than genesis
}

impl DagPlan {
    /// Returns all block ids other than genesis
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.blocks.iter().map(|(i, _)| *i)
    }

    pub fn genesis(&self) -> u64 {
        self.genesis
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::{cell::RefCell, fs::File};

    use kaspa_consensus_core::blockhash::ORIGIN;
    use kaspa_consensus_core::header::Header;
    use kaspa_consensus_core::{BlockHashSet, HashMapCustomHasher};
    use parking_lot::lock_api::RwLock;

    use super::*;
    use crate::model::stores::ghostdag::{GhostdagStore, GhostdagStoreReader};
    use crate::model::stores::headers::MemoryHeaderStore;
    use crate::processes::ghostdag::protocol::GhostdagManager;
    use crate::processes::reachability::tests::r#gen::generate_complex_dag;
    use crate::{
        model::stores::{
            dagknight::MemoryDagknightStore, ghostdag::MemoryGhostdagStore, reachability::MemoryReachabilityStore,
            relations::MemoryRelationsStore,
        },
        processes::reachability::tests::{DagBlock, DagBuilder},
        test_helpers::generate_dot_with_chain,
    };

    #[test]
    fn test_cascade() {
        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();

        // Build the DAG
        {
            let plan = DagPlan {
                genesis: 1,
                blocks: vec![
                    (2, vec![1]),
                    (3, vec![1]),
                    (4, vec![2, 3]),
                    (5, vec![4]),
                    (6, vec![1]),
                    (7, vec![5, 6]),
                    (8, vec![1]),
                    (9, vec![1]),
                    (10, vec![7, 8, 9]),
                    (11, vec![1]),
                    (12, vec![11, 10]),
                ],
            };
            let mut builder = DagBuilder::new(&mut reachability, &mut relations);
            builder.init().add_block(DagBlock::genesis(plan.genesis.into()));
            for (block, parents) in plan.blocks.iter() {
                builder.add_block(DagBlock::new((*block).into(), parents.iter().map(|&i| i.into()).collect()));
            }
        }
    }

    /// This is the main body of the test.
    /// 1. It sets up the necessary stores
    /// 2. Reads the DagPlan
    /// 3. Runs DK over the blocks on it, fills the global GD store with the results
    /// 4. Generates a DOT file over that GD store showing the SPC and blocks colored
    ///    according to the global GD store
    #[allow(clippy::arc_with_non_send_sync)]
    fn run_dagknight_test(k_max: KType, plan: DagPlan, base_name: &str) {
        let genesis_hash = plan.genesis.into();

        let dk_map = RefCell::new(HashMap::new());

        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();
        // Global GD store. To be used for global coloring:
        let coloring_ghostdag_store = Arc::new(MemoryGhostdagStore::new());
        let headers_store = Arc::new(MemoryHeaderStore::new());
        let coloring_gd_manager = GhostdagManager::new(
            genesis_hash,
            k_max,
            coloring_ghostdag_store.clone(),
            relations.clone(),
            headers_store.clone(),
            reachability.clone(),
        );

        coloring_ghostdag_store.insert(genesis_hash, Arc::new(coloring_gd_manager.genesis_ghostdag_data())).unwrap();

        // Global GD store. To be used for topology:
        let topology_ghostdag_store = Arc::new(MemoryGhostdagStore::new());

        let topology_gd_manager = GhostdagManager::new(
            genesis_hash,
            k_max,
            topology_ghostdag_store.clone(),
            relations.clone(),
            headers_store.clone(),
            reachability.clone(),
        );

        topology_ghostdag_store.insert(genesis_hash, Arc::new(topology_gd_manager.genesis_ghostdag_data())).unwrap();

        let dagknight_store = Arc::new(MemoryDagknightStore::new(dk_map));

        let dk_executor = DagknightExecutor {
            genesis_hash,
            dagknight_store: dagknight_store.clone(),
            headers_store: headers_store.clone(),
            reachability_service: MTReachabilityService::new(Arc::new(RwLock::new(reachability.clone()))),
            relations_store: Arc::new(RwLock::new(relations.clone())),
            umc_persistence_store: None,
            umc_persistence_stats: None,
        };
        let mut builder = DagBuilder::new(&mut reachability, &mut relations);
        builder.init();
        let genesis = DagBlock::new(genesis_hash, vec![ORIGIN]);
        builder.add_block(genesis.clone());

        let mut tips = BlockHashSet::new();
        tips.insert(genesis.hash);

        let mut genesis_header = Header::from_precomputed_hash(genesis_hash, vec![]);
        genesis_header.bits = 0x207fffff;
        headers_store.insert(Arc::new(genesis_header));

        for block_data in &plan.blocks {
            let block_id: u64 = block_data.0;
            let block_hash = block_id.into();
            tips.insert(block_hash);

            let parent_hashes: Vec<Hash> = block_data.1.iter().map(|&a| Hash::from_u64_word(a)).collect();

            parent_hashes.iter().for_each(|ph| {
                tips.remove(ph);
            });

            let new_block = DagBlock::new(block_hash, parent_hashes.clone());

            // Pure GD for blue_work:
            let topology_gd_data = topology_gd_manager.ghostdag(&new_block.parents);

            let DagknightData { selected_parent, .. } = dk_executor.dagknight(&new_block.parents);

            // Maintain global coloring based on DK megachain selected parent:
            let gd_data = coloring_gd_manager.incremental_coloring(&new_block.parents, selected_parent);

            builder.add_block_with_selected_parent(new_block, selected_parent);

            let mut curr_header = Header::from_precomputed_hash(block_hash, parent_hashes);
            curr_header.bits = 0x207fffff;
            curr_header.daa_score = gd_data.blue_score;
            curr_header.blue_score = gd_data.blue_score;
            curr_header.blue_work = topology_gd_data.blue_work;

            topology_ghostdag_store.insert(block_hash, Arc::new(topology_gd_data)).unwrap();
            coloring_ghostdag_store.insert(block_hash, Arc::new(gd_data)).unwrap();

            headers_store.insert(Arc::new(curr_header));
        }

        let tip_hashes = tips.iter().copied().collect_vec();
        let virtual_hash = Hash::from_u64_word(plan.blocks.last().unwrap().0 + 1);
        let virtual_block = DagBlock::new(virtual_hash, tip_hashes.clone());
        let DagknightData { selected_parent, .. } = dk_executor.dagknight(&virtual_block.parents.clone());
        // let selected_parent = dk_data.selected_parent;
        let gd_data = coloring_gd_manager.incremental_coloring(&tip_hashes, selected_parent);
        println!("virtual_block: {} | sp: {}", virtual_block.hash, selected_parent);
        builder.add_block_with_selected_parent(virtual_block, selected_parent);
        coloring_ghostdag_store.insert(virtual_hash, Arc::new(gd_data)).unwrap();

        // let blues = BlockHashSet::new();
        let mut reds = BlockHashSet::new();

        // Collect chain nodes during VSPC traversal
        let mut chain_nodes = BlockHashSet::new();
        let mut curr = virtual_hash;
        chain_nodes.insert(curr);

        while curr != genesis.hash {
            let mergeset_reds = coloring_ghostdag_store.get_mergeset_reds(curr).unwrap();
            mergeset_reds.iter().for_each(|mrr| {
                reds.insert(*mrr);
            });

            let chain_parent = reachability.get_chain_parent(curr);
            println!("{} <- {}", chain_parent.to_le_u64()[3], curr.to_le_u64()[3]);
            chain_nodes.insert(chain_parent);
            curr = chain_parent;
        }

        // Generate DOT file with chain nodes as double circles
        let mut all_blocks = vec![(plan.genesis, vec![])];
        all_blocks.extend(plan.blocks.clone());
        all_blocks.push((virtual_hash.to_le_u64()[3], tips.iter().map(|h| h.to_le_u64()[3]).collect_vec()));
        generate_dot_with_chain(&all_blocks, &chain_nodes, reds, base_name).expect("Failed to generate DOT file");
    }

    #[test]
    fn test_dag_dk_sample() {
        let plan = DagPlan {
            genesis: 1,
            blocks: vec![
                (2, vec![1]),
                (3, vec![2]),
                (4, vec![3]),
                (5, vec![4]),
                (6, vec![5]),
                (7, vec![6]),
                (8, vec![7]),
                (9, vec![7]),
                (10, vec![8, 9]),
                (11, vec![10]),
                (12, vec![1]),
                (13, vec![12]),
                (14, vec![13]),
                (15, vec![14]),
                (16, vec![15]),
                (17, vec![6, 16]),
            ],
        };

        run_dagknight_test(0, plan, "dag_bps_whitepaper_sample");
    }

    #[test]
    fn test_dag_from_json() {
        // Test the Task 0 implementation here
        let json_filename = "dag_bps_2.json";
        let file = File::open(json_filename).expect("Unable to open JSON file");
        let json_data: serde_json::Value = serde_json::from_reader(file).expect("Unable to parse JSON");

        let genesis = json_data["genesis"].as_u64().expect("Genesis is not a number");
        let blocks = json_data["blocks"].as_array().expect("Blocks is not an array");

        // Construct DagPlan from JSON data
        let dag_plan = DagPlan {
            genesis,
            blocks: blocks
                .iter()
                .map(|block| {
                    let id = block["id"].as_u64().unwrap();
                    let parents = block["parents"].as_array().unwrap().iter().map(|p| p.as_u64().unwrap()).collect();
                    (id, parents)
                })
                .chain(vec![(60, vec![1]), (61, vec![1]), (62, vec![60, 61]), (63, vec![60, 61]), (70, vec![50, 51, 63])])
                .collect(),
        };

        // print the data
        println!("Genesis: {}", dag_plan.genesis);
        println!("Blocks: {}", dag_plan.blocks.len());

        // Sample here is 2BPS. K = 31
        run_dagknight_test(31, dag_plan, "dag_bps_2");
    }

    #[test]
    fn test_complex_dag() {
        let (genesis, mut blocks) = generate_complex_dag(0.1, 10.0, 50);
        let (_, attacker_blocks) = generate_complex_dag(0.1, 10.0, 40);

        // Make the attacker blocks still point to the original genesis and adjust their labels
        let mut attacker_blocks = attacker_blocks
            .iter()
            .map(|(block, parents)| {
                let block = if *block == genesis { *block } else { block + 100 };
                let parents = parents.iter().map(|&p| if p == genesis { p } else { p + 100 }).collect_vec();

                (block, parents)
            })
            .collect_vec();

        blocks.append(&mut attacker_blocks);

        let plan = DagPlan { genesis, blocks };

        run_dagknight_test(5, plan, "dag_complex");
    }

    #[test]
    fn test_monitonicity_simple() {
        // SETUP:
        let genesis_hash = 1.into();

        let dk_map = RefCell::new(HashMap::new());

        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();

        let headers_store = Arc::new(MemoryHeaderStore::new());
        let mut genesis_header = Header::from_precomputed_hash(genesis_hash, vec![]);
        genesis_header.bits = 0x207fffff;
        headers_store.insert(Arc::new(genesis_header));
        // Global GD store. To be used for topology:
        let topology_ghostdag_store = Arc::new(MemoryGhostdagStore::new());

        let topology_gd_manager = GhostdagManager::new(
            genesis_hash,
            5,
            topology_ghostdag_store.clone(),
            relations.clone(),
            headers_store.clone(),
            reachability.clone(),
        );

        topology_ghostdag_store.insert(genesis_hash, Arc::new(topology_gd_manager.genesis_ghostdag_data())).unwrap();

        let dagknight_store = Arc::new(MemoryDagknightStore::new(dk_map));

        let dk_executor = DagknightExecutor {
            genesis_hash,
            dagknight_store: dagknight_store.clone(),
            headers_store: headers_store.clone(),
            reachability_service: MTReachabilityService::new(Arc::new(RwLock::new(reachability.clone()))),
            relations_store: Arc::new(RwLock::new(relations.clone())),
            umc_persistence_store: None,
            umc_persistence_stats: None,
        };
        let mut builder = DagBuilder::new(&mut reachability, &mut relations);
        builder.init();
        let genesis = DagBlock::new(genesis_hash, vec![ORIGIN]);
        builder.add_block(genesis.clone());

        // Add blocks 2 and 3 and insert headers/ghostdag entries.
        // We'll use a small helper closure to reduce repetition when adding a block and its header.
        let mut add_block_with_header = |id: u64, parents: Vec<Hash>| {
            let current_hash = id.into();
            let DagknightData { selected_parent, .. } = dk_executor.dagknight(&parents);
            builder.add_block_with_selected_parent(DagBlock::new(current_hash, parents.clone()), selected_parent);
            let gd = topology_gd_manager.ghostdag(&parents);

            let mut header = Header::from_precomputed_hash(current_hash, parents);
            header.bits = 0x207fffff;
            header.daa_score = gd.blue_score;
            header.blue_score = gd.blue_score;
            header.blue_work = gd.blue_work;
            headers_store.insert(Arc::new(header));
            topology_ghostdag_store.insert(current_hash, Arc::new(gd)).unwrap();

            current_hash
        };

        // TEST BEGINS HERE:
        // This test follows the example described in the DK paper section 2.6.6
        //     1
        //    ↙ ↘
        //   2   3
        //   |   |\ \ \ \
        //   ↓   ↓ ↓ ↓ ↓ ↓
        //   9   4 5 6 7 8
        //
        let hash_of_2 = add_block_with_header(2, vec![genesis_hash]);
        let hash_of_3 = add_block_with_header(3, vec![genesis_hash]);

        let DagknightData { selected_parent: virtual_sp, .. } = dk_executor.dagknight(&[hash_of_2, hash_of_3]);
        println!("virtual sp: {}", virtual_sp);

        let other_tip = if hash_of_2 == virtual_sp { hash_of_3 } else { hash_of_2 };
        let mut tips = vec![];

        // Raise the rank of the selected tip of previos selected parent by pointing multiple blocks to it
        for i in 4..9 {
            let current_hash = add_block_with_header(i, vec![virtual_sp]);
            tips.push(current_hash);
        }

        // Add just one tip to previously unselected parent
        let hash_of_9 = add_block_with_header(9, vec![other_tip]);
        tips.push(hash_of_9);

        let DagknightData { selected_parent: new_sp_virtual, .. } = dk_executor.dagknight(&tips);
        println!("new virtual sp: {}", new_sp_virtual);

        assert!(
            reachability.is_chain_ancestor_of(virtual_sp, new_sp_virtual),
            "The selected parent chain changed after attacker raised the rank of previously selected tip"
        )
    }

    #[test]
    fn test_parent_ordering_stability() {
        let genesis_hash = Hash::from_u64_word(1);
        let mut reachability = MemoryReachabilityStore::new();
        let mut relations = MemoryRelationsStore::new();
        let headers_store = Arc::new(MemoryHeaderStore::new());

        let dk_map = RefCell::new(HashMap::new());

        let dagknight_store = Arc::new(MemoryDagknightStore::new(dk_map));

        let dk_executor = DagknightExecutor {
            genesis_hash,
            dagknight_store: dagknight_store.clone(),
            headers_store: headers_store.clone(),
            reachability_service: MTReachabilityService::new(Arc::new(RwLock::new(reachability.clone()))),
            relations_store: Arc::new(RwLock::new(relations.clone())),
            umc_persistence_store: None,
            umc_persistence_stats: None,
        };

        let mut builder = DagBuilder::new(&mut reachability, &mut relations);
        builder.init();
        let mut add_block = |hash, parents: Vec<Hash>, blue_work, bits, blue_score, daa_score, selected_parent: Hash| -> Hash {
            let mut header = Header::from_precomputed_hash(hash, parents.clone());
            header.bits = bits;
            header.blue_work = blue_work;
            header.blue_score = blue_score;
            header.daa_score = daa_score;
            headers_store.insert(Arc::new(header));

            builder.add_block_with_selected_parent(DagBlock::new(hash, parents.clone()), selected_parent);
            hash
        };

        let json_filename = "test_parent_ordering_stability.json";
        let file = File::open(json_filename).expect("Unable to open JSON file");
        let json_data: serde_json::Value = serde_json::from_reader(file).expect("Unable to parse JSON");

        let tips: Vec<Hash> = json_data["tips"].as_array().unwrap().iter().map(|t| prefixed_hash(t.as_str().unwrap())).collect();

        let blocks = json_data["blocks"].as_array().expect("Blocks is not an array");

        let test_blocks: Vec<(Hash, Vec<Hash>, Uint192, u32, u64, u64, Hash)> = blocks
            .iter()
            .map(|block| {
                let id = prefixed_hash(block["id"].as_str().unwrap());
                let parents: Vec<Hash> = if block["parents"].as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    vec![ORIGIN]
                } else {
                    block["parents"].as_array().unwrap().iter().map(|p| prefixed_hash(p.as_str().unwrap())).collect()
                };
                let blue_work = Uint192::from_u64(block["blue_work"].as_str().unwrap().parse::<u64>().unwrap());
                let bits = u32::from_str_radix(block["bits"].as_str().unwrap(), 16).unwrap();
                let blue_score = block["blue_score"].as_u64().unwrap();
                let daa_score = block["daa_score"].as_u64().unwrap();
                let selected_parent = if block["selected_parent"].is_null() {
                    ORIGIN
                } else {
                    prefixed_hash(block["selected_parent"].as_str().unwrap())
                };
                (id, parents, blue_work, bits, blue_score, daa_score, selected_parent)
            })
            .collect();

        let mut test_blocks = test_blocks;

        test_blocks.sort_by_key(|(_, _, blue_work, _, _, _, _)| *blue_work);

        for (hash, parents, blue_work, bits, blue_score, daa_score, selected_parent) in &test_blocks {
            add_block(*hash, parents.clone(), *blue_work, *bits, *blue_score, *daa_score, *selected_parent);
        }

        let mut parents = tips.clone();
        let base_result = dk_executor.dagknight(&parents);

        parents.sort();
        let sorted_result = dk_executor.dagknight(&parents);

        assert_eq!(
            base_result.selected_parent, sorted_result.selected_parent,
            "Selected parent must be the same regardless of parent order"
        );
    }

    fn prefixed_hash(s: &str) -> Hash {
        let mut hex = [b'0'; 64];
        hex[..s.len()].copy_from_slice(s.as_bytes());
        Hash::from_str(std::str::from_utf8(&hex).unwrap()).expect("Invalid hash string")
    }
}
