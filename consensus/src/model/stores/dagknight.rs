use std::{cell::RefCell, collections::HashMap, sync::Arc};

use kaspa_consensus_core::{BlockHashMap, BlueWorkType, HashMapCustomHasher, KType, blockhash::BlockHashes};
use kaspa_database::{
    prelude::{DbKey, StoreError},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;
use kaspa_utils::mem_size::MemSizeEstimator;
use serde::{Deserialize, Serialize};

use kaspa_consensus_core::HashKTypeMap;

/// Map from blue block hash to the total work of blue blocks in its anticone (from POV of current block).
pub type HashBlueWorkMap = BlockHashMap<BlueWorkType>;

use kaspa_database::prelude::{BatchDbWriter, CachePolicy, CachedDbAccess, DB};
use rocksdb::WriteBatch;

/// ColoringData used by the DAGKnight conflict zone manager.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ColoringData {
    pub blue_score: u64,
    pub blue_work: BlueWorkType,
    pub selected_parent: Hash,
    pub mergeset_blues: BlockHashes,
    pub mergeset_reds: BlockHashes,
    /// Gray blocks: mergeset members that are chain ancestors of the NCA.
    pub mergeset_grays: BlockHashes,
    pub blues_anticone_sizes: HashKTypeMap,
    /// Parallel to `blues_anticone_sizes`: maps each blue block to the total work of
    /// blue blocks in its anticone from the POV of the current block.
    pub blues_anticone_work: BlockHashMap<BlueWorkType>,
    /// Total work of non-gray red blocks in this block's past within the conflict zone.
    /// Computed incrementally: past_red_work(B) = past_red_work(SP(B)) + work(mergeset_reds)
    pub past_red_work: BlueWorkType,
}

/// Data computed for a block during the VSPC walk for UMC cascade voting.
/// Contains the precise work counters needed for floor calculation.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct PastColoringData {
    pub past_blue_work: BlueWorkType,
    pub past_red_work: BlueWorkType,
    pub anticone_blue_work: BlueWorkType,
}

impl MemSizeEstimator for ColoringData {
    fn estimate_mem_bytes(&self) -> usize {
        let mut bytes = size_of::<Self>();
        bytes += (self.mergeset_blues.len() + self.mergeset_reds.len() + self.mergeset_grays.len()) * size_of::<Hash>();
        bytes += self.blues_anticone_sizes.len() * size_of::<(Hash, KType)>();
        bytes
    }
}

impl ColoringData {
    /// Creates a new ColoringData with all fields explicitly set.
    pub fn new(
        blue_score: u64,
        blue_work: BlueWorkType,
        selected_parent: Hash,
        mergeset_blues: BlockHashes,
        mergeset_reds: BlockHashes,
        mergeset_grays: BlockHashes,
        blues_anticone_sizes: HashKTypeMap,
        blues_anticone_work: BlockHashMap<BlueWorkType>,
        past_red_work: BlueWorkType,
    ) -> Self {
        Self {
            blue_score,
            blue_work,
            selected_parent,
            mergeset_blues,
            mergeset_reds,
            mergeset_grays,
            blues_anticone_sizes,
            blues_anticone_work,
            past_red_work,
        }
    }

    /// Creates a new ColoringData initialized with just the selected parent.
    /// Seeds the anticone maps with SP → 0.
    pub fn new_with_selected_parent(selected_parent: Hash, _k: KType) -> Self {
        let mut mergeset_blues: Vec<Hash> = Vec::with_capacity((_k + 1) as usize);
        let mut blues_anticone_sizes: BlockHashMap<KType> = BlockHashMap::with_capacity(_k as usize);
        let mut blues_anticone_work: BlockHashMap<BlueWorkType> = BlockHashMap::with_capacity(_k as usize);
        mergeset_blues.push(selected_parent);
        blues_anticone_sizes.insert(selected_parent, 0);
        blues_anticone_work.insert(selected_parent, BlueWorkType::ZERO);

        Self {
            blue_score: Default::default(),
            blue_work: Default::default(),
            selected_parent,
            mergeset_blues: BlockHashes::new(mergeset_blues),
            mergeset_reds: Default::default(),
            mergeset_grays: Default::default(),
            blues_anticone_sizes: HashKTypeMap::new(blues_anticone_sizes),
            blues_anticone_work,
            past_red_work: BlueWorkType::ZERO,
        }
    }

    /// Returns the total mergeset size (blues + reds + grays).
    pub fn mergeset_size(&self) -> usize {
        self.mergeset_blues.len() + self.mergeset_reds.len() + self.mergeset_grays.len()
    }

    /// Adds a gray block to the mergeset.
    pub fn add_gray(&mut self, block: Hash) {
        BlockHashes::make_mut(&mut self.mergeset_grays).push(block);
    }

    /// Adds a blue block to the mergeset, tracking both anticone sizes and work.
    pub fn add_blue(
        &mut self,
        block: Hash,
        blue_anticone_size: KType,
        block_blues_anticone_sizes: &BlockHashMap<KType>,
        block_blues_anticone_work: &BlockHashMap<BlueWorkType>,
        candidate_work: BlueWorkType,
    ) {
        BlockHashes::make_mut(&mut self.mergeset_blues).push(block);

        let blues_anticone_sizes = HashKTypeMap::make_mut(&mut self.blues_anticone_sizes);
        blues_anticone_sizes.insert(block, blue_anticone_size);
        for (blue, size) in block_blues_anticone_sizes {
            blues_anticone_sizes.insert(*blue, size + 1);
        }

        let candidate_anticone_work: BlueWorkType = block_blues_anticone_work.values().copied().sum();

        let blues_anticone_work = &mut self.blues_anticone_work;
        blues_anticone_work.insert(block, candidate_anticone_work);

        for &peer in block_blues_anticone_sizes.keys() {
            let entry = blues_anticone_work.entry(peer).or_insert(BlueWorkType::ZERO);
            *entry = *entry + candidate_work;
        }
    }

    /// Adds a red block to the mergeset.
    pub fn add_red(&mut self, block: Hash) {
        BlockHashes::make_mut(&mut self.mergeset_reds).push(block);
    }
}

pub struct MemoryDagknightStore {
    dk_map: RefCell<HashMap<DagknightKey, Arc<ColoringData>>>,
}

pub trait DagknightStoreReader {
    fn get_selected_parent(&self, dk_key: DagknightKey) -> Result<Hash, StoreError>;
    fn get_data(&self, dk_key: DagknightKey) -> Result<Arc<ColoringData>, StoreError>;
    fn has(&self, dk_key: DagknightKey) -> Result<bool, StoreError>;
}

#[derive(Clone)]
pub struct DagknightKey {
    pub pov_hash: Hash,
    pub root_hash: Hash,
    pub k: KType,
    pub free_search: bool,
    // Precomputed bytes in order: root_hash || k(u16 BE) || pov_hash || free_search
    bytes: [u8; kaspa_hashes::HASH_SIZE * 2 + 3],
}

impl DagknightKey {
    pub fn new(root_hash: Hash, pov_hash: Hash, k: KType, free_search: bool) -> Self {
        // Layout must match DB-level expectations where `k` is encoded as a u16
        // (two bytes). Allocate enough space: root_hash + k(2) + pov_hash + free_search(1).
        let mut bytes = [0u8; kaspa_hashes::HASH_SIZE * 2 + 3];
        let hash_size = kaspa_hashes::HASH_SIZE;
        bytes[..hash_size].copy_from_slice(root_hash.as_ref());

        // Encode k as big-endian u16 to match other code paths that construct
        // DB keys using two bytes for k.
        let k_be = k.to_be_bytes();
        bytes[hash_size] = k_be[0];
        bytes[hash_size + 1] = k_be[1];

        bytes[(hash_size + 2)..(hash_size + 2 + hash_size)].copy_from_slice(pov_hash.as_ref());
        bytes[(2 * hash_size) + 2] = if free_search { 1 } else { 0 };

        Self { pov_hash, root_hash, k, free_search, bytes }
    }
}

impl std::fmt::Display for DagknightKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.bytes)
    }
}

impl AsRef<[u8]> for DagknightKey {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Eq for DagknightKey {}

impl std::hash::Hash for DagknightKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based on the logical key fields
        self.root_hash.hash(state);
        self.k.hash(state);
        self.pov_hash.hash(state);
        self.free_search.hash(state);
    }
}

impl PartialEq for DagknightKey {
    fn eq(&self, other: &Self) -> bool {
        self.pov_hash == other.pov_hash
            && self.k == other.k
            && self.root_hash == other.root_hash
            && self.free_search == other.free_search
    }
}

pub trait DagknightStore {
    fn insert(&self, key: DagknightKey, dk_data: Arc<ColoringData>) -> Result<(), StoreError>;
    fn delete(&self, key: DagknightKey) -> Result<(), StoreError>;
    fn delete_rooted_range(&self, batch: &mut WriteBatch, hash: Hash) -> Result<u32, StoreError>;
}

impl MemoryDagknightStore {
    pub fn new(dk_map: RefCell<HashMap<DagknightKey, Arc<ColoringData>>>) -> Self {
        Self { dk_map }
    }
}

impl DagknightStoreReader for MemoryDagknightStore {
    fn get_selected_parent(&self, dk_key: DagknightKey) -> Result<Hash, StoreError> {
        Ok(self.get_data(dk_key)?.selected_parent)
    }

    fn get_data(&self, key: DagknightKey) -> Result<Arc<ColoringData>, StoreError> {
        if let Some(pov_block_dk_data) = self.dk_map.borrow().get(&key) {
            Ok(pov_block_dk_data.clone())
        } else {
            Err(StoreError::KeyNotFound(DbKey::new(DatabaseStorePrefixes::DagKnight.as_ref(), key)))
        }
    }

    fn has(&self, dk_key: DagknightKey) -> Result<bool, StoreError> {
        Ok(self.dk_map.borrow().contains_key(&dk_key))
    }
}

impl DagknightStore for MemoryDagknightStore {
    fn insert(&self, key: DagknightKey, dk_data: Arc<ColoringData>) -> Result<(), StoreError> {
        self.dk_map.borrow_mut().insert(key, dk_data);

        Ok(())
    }

    fn delete(&self, key: DagknightKey) -> Result<(), StoreError> {
        self.dk_map.borrow_mut().remove(&key);

        Ok(())
    }

    fn delete_rooted_range(&self, _batch: &mut WriteBatch, _hash: Hash) -> Result<u32, StoreError> {
        unimplemented!()
    }
}

/// A DB + cache implementation of `DagknightStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbDagknightStore {
    db: Arc<DB>,
    access: CachedDbAccess<DagknightKey, Arc<ColoringData>>,
}

impl DbDagknightStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        let prefix = DatabaseStorePrefixes::DagKnight.as_ref().to_vec();
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, prefix) }
    }

    pub fn insert_batch(&self, batch: &mut WriteBatch, key: DagknightKey, data: Arc<ColoringData>) -> Result<(), StoreError> {
        if self.access.has(key.clone())? {
            return Err(StoreError::KeyAlreadyExists(key.to_string()));
        }
        self.access.write(BatchDbWriter::new(batch), key, data)?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, key: DagknightKey) -> Result<(), StoreError> {
        self.access.delete(BatchDbWriter::new(batch), key)
    }
}

impl DagknightStoreReader for DbDagknightStore {
    fn get_selected_parent(&self, dk_key: DagknightKey) -> Result<Hash, StoreError> {
        Ok(self.get_data(dk_key)?.selected_parent)
    }

    fn get_data(&self, dk_key: DagknightKey) -> Result<Arc<ColoringData>, StoreError> {
        self.access.read(dk_key)
    }

    fn has(&self, dk_key: DagknightKey) -> Result<bool, StoreError> {
        self.access.has(dk_key)
    }
}

impl DagknightStore for DbDagknightStore {
    fn insert(&self, key: DagknightKey, dk_data: Arc<ColoringData>) -> Result<(), StoreError> {
        if self.access.has(key.clone())? {
            return Err(StoreError::KeyAlreadyExists(key.to_string()));
        }
        let mut batch = WriteBatch::default();
        self.access.write(BatchDbWriter::new(&mut batch), key, dk_data)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn delete(&self, key: DagknightKey) -> Result<(), StoreError> {
        let mut batch = WriteBatch::default();
        self.access.delete(BatchDbWriter::new(&mut batch), key)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn delete_rooted_range(&self, batch: &mut WriteBatch, hash: Hash) -> Result<u32, StoreError> {
        // delete records that have a prefix rooted at this DK store key + hash
        let root_bytes_prefix = {
            let mut bytes = Vec::with_capacity(kaspa_hashes::HASH_SIZE + 1);
            bytes.extend(DatabaseStorePrefixes::DagKnight.as_ref());
            bytes.extend_from_slice(hash.as_ref());
            bytes
        };
        let start_conflict_genesis_bytes = {
            let mut bytes = Vec::with_capacity(kaspa_hashes::HASH_SIZE + 2);
            bytes.extend_from_slice(&root_bytes_prefix);
            bytes.push(0); // k = 0 u16 first byte
            bytes.push(0); // k = 0 u16 second byte
            bytes
        };
        let end_conflict_genesis_bytes = {
            let mut bytes = Vec::with_capacity(kaspa_hashes::HASH_SIZE + 2);
            bytes.extend_from_slice(&root_bytes_prefix);
            // TODO[DK]: This range check misses entries where k = u16::MAX. However, we don't expect k to reach that value anyway
            // in practice so we don't expect records to exist here as well. In the DK implementation, k may be clamped to max out
            // lower than k = u16::MAX
            bytes.push(0xFF); // k = 0xFFFF u16 first byte
            bytes.push(0xFF); // k = 0xFFFF u16 second byte
            bytes
        };
        // TODO[DK]: count keys in range. Possibly would be removed.
        let mut count = 0;
        let mut iterator = self.db.raw_iterator();
        iterator.seek(&start_conflict_genesis_bytes);
        while iterator.valid() {
            let key = iterator.key();
            if key.unwrap() >= end_conflict_genesis_bytes.as_slice() {
                break;
            }
            count += 1;
            iterator.next();
        }
        // Perform the range delete
        batch.delete_range(start_conflict_genesis_bytes, end_conflict_genesis_bytes);
        Ok(count)
    }
}

// ============================================================================
// UMC Persistence Store — stores cascade voting state for incremental UMC
// ============================================================================

use kaspa_math::{Uint192, int::SignedInteger};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Signed work value (difference of two Uint192 work values).
type SignedWork = SignedInteger<Uint192>;

/// Accumulated statistics for incremental UMC persistence.
/// Diagnostics about a single incremental UMC evaluation, used for stats tracking.
#[derive(Debug, Default)]
pub struct UmcFallbackDiagnostics {
    /// Whether persisted state was successfully restored (no staleness)
    pub was_restored: bool,
    /// Whether persisted state was recovered (blues stale, reds OK — repaired incrementally)
    pub was_recovered: bool,
    /// Number of blocks already in persisted heap (tree + secondary), 0 if not restored/recovered
    pub persisted_blocks: usize,
    /// Number of persisted blues no longer in current blue set (0 if restored or no persisted state)
    pub stale_blues: usize,
    /// Number of persisted reds no longer in current red set (0 if restored or no persisted state)
    pub stale_reds: usize,
    /// Total persisted blues when fallback occurred (0 if not applicable)
    pub persisted_blues_on_fallback: usize,
    /// Total persisted reds when fallback occurred (0 if not applicable)
    pub persisted_reds_on_fallback: usize,
}

/// Tracks how much effort is saved by restoring from persisted cascade state.
///
/// **Effort saved** is measured as:
/// ```text
/// blocks_skipped = blocks already in persisted heap (tree + secondary heap)
/// total_blocks   = blue_set.len() + red_set.len()  (current conflict zone size)
/// effort_saved   = blocks_skipped / total_blocks
/// ```
///
/// This ratio shows what fraction of zone blocks were *not* re-processed because
/// they were already in the cascade heap from a previous evaluation.
///
/// Additionally tracks **staleness** on fallback: when persisted state is rejected
/// because the zone changed, records how many persisted blocks were stale (no longer
/// in the current zone) to understand how often persistence is useful.
#[derive(Default)]
pub struct UmcPersistenceStats {
    /// Total number of incremental UMC evaluations
    total_calls: AtomicU64,
    /// Number of evaluations that restored from persisted state
    restored_calls: AtomicU64,
    /// Number of evaluations where persisted state existed but was rejected (fallback)
    fallback_calls: AtomicU64,
    /// Fallbacks where blues were valid but reds had stale entries (only reds stale)
    fallback_blue_ok_red_stale: AtomicU64,
    /// Fallbacks where reds were valid but blues had stale entries (only blues stale)
    fallback_blue_stale_red_ok: AtomicU64,
    /// Fallbacks where both blues and reds had stale entries
    fallback_both_stale: AtomicU64,
    /// Total blocks in zone across all calls (blues + reds)
    total_blocks_in_zone: AtomicU64,
    /// Total blocks already in persisted heap across restored calls (skipped)
    total_blocks_skipped: AtomicU64,
    /// Total stale blues across all fallback calls
    total_stale_blues_on_fallback: AtomicU64,
    /// Total stale reds across all fallback calls
    total_stale_reds_on_fallback: AtomicU64,
    /// Total persisted blues when fallback occurred
    total_persisted_blues_on_fallback: AtomicU64,
    /// Total persisted reds when fallback occurred
    total_persisted_reds_on_fallback: AtomicU64,
}

impl UmcPersistenceStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the result of a single incremental UMC evaluation.
    pub fn record(&self, diag: &UmcFallbackDiagnostics, zone_blocks: usize) {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        self.total_blocks_in_zone.fetch_add(zone_blocks as u64, Ordering::Relaxed);

        if diag.was_restored || diag.was_recovered {
            // Both restored and recovered count as successful (skipped processing persisted blocks)
            self.restored_calls.fetch_add(1, Ordering::Relaxed);
            self.total_blocks_skipped.fetch_add(diag.persisted_blocks as u64, Ordering::Relaxed);
        } else if diag.stale_blues > 0 || diag.stale_reds > 0 {
            // Had persisted state but fell back due to staleness
            self.fallback_calls.fetch_add(1, Ordering::Relaxed);
            self.total_stale_blues_on_fallback.fetch_add(diag.stale_blues as u64, Ordering::Relaxed);
            self.total_stale_reds_on_fallback.fetch_add(diag.stale_reds as u64, Ordering::Relaxed);
            self.total_persisted_blues_on_fallback.fetch_add(diag.persisted_blues_on_fallback as u64, Ordering::Relaxed);
            self.total_persisted_reds_on_fallback.fetch_add(diag.persisted_reds_on_fallback as u64, Ordering::Relaxed);

            // Categorize by staleness type
            let blues_stale = diag.stale_blues > 0;
            let reds_stale = diag.stale_reds > 0;
            if blues_stale && reds_stale {
                self.fallback_both_stale.fetch_add(1, Ordering::Relaxed);
            } else if blues_stale {
                self.fallback_blue_stale_red_ok.fetch_add(1, Ordering::Relaxed);
            } else {
                self.fallback_blue_ok_red_stale.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.total_calls.store(0, Ordering::Relaxed);
        self.restored_calls.store(0, Ordering::Relaxed);
        self.fallback_calls.store(0, Ordering::Relaxed);
        self.fallback_blue_ok_red_stale.store(0, Ordering::Relaxed);
        self.fallback_blue_stale_red_ok.store(0, Ordering::Relaxed);
        self.fallback_both_stale.store(0, Ordering::Relaxed);
        self.total_blocks_in_zone.store(0, Ordering::Relaxed);
        self.total_blocks_skipped.store(0, Ordering::Relaxed);
        self.total_stale_blues_on_fallback.store(0, Ordering::Relaxed);
        self.total_stale_reds_on_fallback.store(0, Ordering::Relaxed);
        self.total_persisted_blues_on_fallback.store(0, Ordering::Relaxed);
        self.total_persisted_reds_on_fallback.store(0, Ordering::Relaxed);
    }

    /// Returns a formatted summary string.
    pub fn snapshot(&self) -> String {
        let total = self.total_calls.load(Ordering::Relaxed);
        let restored = self.restored_calls.load(Ordering::Relaxed);
        let fallbacks = self.fallback_calls.load(Ordering::Relaxed);
        let fb_blue_ok_red_stale = self.fallback_blue_ok_red_stale.load(Ordering::Relaxed);
        let fb_blue_stale_red_ok = self.fallback_blue_stale_red_ok.load(Ordering::Relaxed);
        let fb_both_stale = self.fallback_both_stale.load(Ordering::Relaxed);
        let in_zone = self.total_blocks_in_zone.load(Ordering::Relaxed);
        let skipped = self.total_blocks_skipped.load(Ordering::Relaxed);
        let stale_blues = self.total_stale_blues_on_fallback.load(Ordering::Relaxed);
        let stale_reds = self.total_stale_reds_on_fallback.load(Ordering::Relaxed);
        let persisted_blues_fb = self.total_persisted_blues_on_fallback.load(Ordering::Relaxed);
        let persisted_reds_fb = self.total_persisted_reds_on_fallback.load(Ordering::Relaxed);

        let effort_saved_pct = if in_zone > 0 {
            (skipped as f64 / in_zone as f64) * 100.0
        } else {
            0.0
        };

        let avg_skipped_per_call = if total > 0 {
            skipped as f64 / total as f64
        } else {
            0.0
        };

        let restored_pct = if total > 0 {
            (restored as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        // Staleness diagnostics on fallback
        let avg_stale_blues = if fallbacks > 0 {
            stale_blues as f64 / fallbacks as f64
        } else {
            0.0
        };
        let avg_stale_reds = if fallbacks > 0 {
            stale_reds as f64 / fallbacks as f64
        } else {
            0.0
        };
        let avg_persisted_blues_fb = if fallbacks > 0 {
            persisted_blues_fb as f64 / fallbacks as f64
        } else {
            0.0
        };
        let avg_persisted_reds_fb = if fallbacks > 0 {
            persisted_reds_fb as f64 / fallbacks as f64
        } else {
            0.0
        };
        let stale_pct_blues = if persisted_blues_fb > 0 {
            stale_blues as f64 / persisted_blues_fb as f64 * 100.0
        } else {
            0.0
        };
        let stale_pct_reds = if persisted_reds_fb > 0 {
            stale_reds as f64 / persisted_reds_fb as f64 * 100.0
        } else {
            0.0
        };

        // Fallback category percentages
        let fb_red_stale_pct = if fallbacks > 0 {
            fb_blue_ok_red_stale as f64 / fallbacks as f64 * 100.0
        } else {
            0.0
        };
        let fb_blue_stale_pct = if fallbacks > 0 {
            fb_blue_stale_red_ok as f64 / fallbacks as f64 * 100.0
        } else {
            0.0
        };
        let fb_both_pct = if fallbacks > 0 {
            fb_both_stale as f64 / fallbacks as f64 * 100.0
        } else {
            0.0
        };

        format!(
            "UMC Persistence Stats: calls={}, restored={} ({:.1}%), \
             total_zone_blocks={}, skipped={}, effort_saved={:.1}%, avg_skipped_per_call={:.1} | \
             fallbacks={}, categories=[blue_ok_red_stale={} ({:.1}%), blue_stale_red_ok={} ({:.1}%), both_stale={} ({:.1}%)], \
             avg_stale_blues={:.1} ({:.1}% of persisted), avg_stale_reds={:.1} ({:.1}% of persisted), \
             avg_persisted_on_fallback=blues={:.1}, reds={:.1}",
            total, restored, restored_pct, in_zone, skipped, effort_saved_pct, avg_skipped_per_call,
            fallbacks, fb_blue_ok_red_stale, fb_red_stale_pct, fb_blue_stale_red_ok, fb_blue_stale_pct, fb_both_stale, fb_both_pct,
            avg_stale_blues, stale_pct_blues, avg_stale_reds, stale_pct_reds,
            avg_persisted_blues_fb, avg_persisted_reds_fb,
        )
    }
}

/// A blue block that was popped from the primary tree as "negative".
#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl Eq for PoppedBlue {}

impl PartialOrd for PoppedBlue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PoppedBlue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.floor.partial_cmp(&other.floor).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.hash.cmp(&other.hash))
    }
}

/// Serialized entry in the cascade tree (primary heap).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UmcPersistedTreeEntry {
    pub hash: Hash,
    pub floor: SignedWork,
    pub past_blue_work: Uint192,
    pub past_red_work: Uint192,
    pub anticone_blue_work: Uint192,
    pub arlb: Uint192,
    pub last_red_index: usize,
}

/// Full persisted state of the cascade voting process.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UmcPersistedState {
    /// Entries in the primary cascade tree (btree heap)
    pub tree_entries: Vec<UmcPersistedTreeEntry>,
    /// Current red index in the tree
    pub red_index: usize,
    /// Red blocks in insertion order
    pub red_set: Vec<Hash>,
    /// Blues popped to secondary heap
    pub secondary_heap: Vec<PoppedBlue>,
    /// Running total of red work seen
    pub seen_red_work: Uint192,
    /// Accumulated work of negative blues
    pub negative_blues: Uint192,
    /// Cached vote result
    pub cached_vote: bool,
    /// Hash of the tip-set at time of persistence, used for stronger staleness detection.
    /// If the current tip-set hash differs from this value, the persisted state is stale.
    pub tip_set_hash: Hash,
}

impl MemSizeEstimator for UmcPersistedState {
    fn estimate_mem_bytes(&self) -> usize {
        size_of::<Self>()
            + self.tree_entries.len() * size_of::<UmcPersistedTreeEntry>()
            + self.red_set.len() * size_of::<Hash>()
            + self.secondary_heap.len() * size_of::<PoppedBlue>()
            + size_of::<Hash>()
    }
}

/// Persistence key for UMC cascade state.
/// Identifies the cascade voting state for a specific conflict zone POV.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UmcPersistenceKey {
    pub conflict_genesis: Hash,
    pub k: KType,
    pub next_chain_ancestor: Hash,
    pub free_search: bool,
    /// Precomputed bytes: conflict_genesis || k(u16 BE) || next_chain_ancestor || free_search
    bytes: [u8; kaspa_hashes::HASH_SIZE * 2 + 3],
}

impl UmcPersistenceKey {
    /// Create a new persistence key.
    pub fn new(conflict_genesis: Hash, k: KType, next_chain_ancestor: Hash, free_search: bool) -> Self {
        const HASH_SIZE: usize = kaspa_hashes::HASH_SIZE;
        let mut bytes = [0u8; HASH_SIZE * 2 + 3];

        bytes[..HASH_SIZE].copy_from_slice(conflict_genesis.as_ref());

        let k_be = k.to_be_bytes();
        bytes[HASH_SIZE] = k_be[0];
        bytes[HASH_SIZE + 1] = k_be[1];

        bytes[(HASH_SIZE + 2)..(HASH_SIZE + 2 + HASH_SIZE)].copy_from_slice(next_chain_ancestor.as_ref());
        bytes[(2 * HASH_SIZE) + 2] = if free_search { 1 } else { 0 };

        Self { conflict_genesis, k, next_chain_ancestor, free_search, bytes }
    }
}

impl AsRef<[u8]> for UmcPersistenceKey {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Display for UmcPersistenceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.bytes)
    }
}

/// Reader trait for UMC persistence store.
pub trait UmcPersistenceStoreReader {
    /// Get persisted state by key.
    fn get(&self, key: UmcPersistenceKey) -> Result<Option<UmcPersistedState>, StoreError>;

    /// Check if key exists.
    fn has(&self, key: UmcPersistenceKey) -> Result<bool, StoreError>;
}

/// Writer trait for UMC persistence store.
pub trait UmcPersistenceStore: UmcPersistenceStoreReader {
    /// Insert or update persisted state.
    fn insert(&self, key: UmcPersistenceKey, state: UmcPersistedState) -> Result<(), StoreError>;

    /// Delete persisted state by key.
    fn delete(&self, key: UmcPersistenceKey) -> Result<(), StoreError>;

    /// Delete all persisted states for a given conflict genesis.
    /// Used for pruning: when a conflict zone is pruned, all associated UMC states should be removed.
    /// Returns the number of entries deleted.
    fn prune_by_conflict_genesis(&self, conflict_genesis: Hash) -> Result<u32, StoreError>;
}

/// In-memory implementation of UMC persistence store.
pub struct MemoryUmcPersistenceStore {
    map: RwLock<HashMap<UmcPersistenceKey, UmcPersistedState>>,
}

impl Default for MemoryUmcPersistenceStore {
    fn default() -> Self {
        Self { map: RwLock::new(HashMap::new()) }
    }
}

impl UmcPersistenceStoreReader for MemoryUmcPersistenceStore {
    fn get(&self, key: UmcPersistenceKey) -> Result<Option<UmcPersistedState>, StoreError> {
        Ok(self.map.read().get(&key).cloned())
    }

    fn has(&self, key: UmcPersistenceKey) -> Result<bool, StoreError> {
        Ok(self.map.read().contains_key(&key))
    }
}

impl UmcPersistenceStore for MemoryUmcPersistenceStore {
    fn insert(&self, key: UmcPersistenceKey, state: UmcPersistedState) -> Result<(), StoreError> {
        self.map.write().insert(key, state);
        Ok(())
    }

    fn delete(&self, key: UmcPersistenceKey) -> Result<(), StoreError> {
        self.map.write().remove(&key);
        Ok(())
    }

    fn prune_by_conflict_genesis(&self, conflict_genesis: Hash) -> Result<u32, StoreError> {
        let mut map = self.map.write();
        let keys_to_remove: Vec<UmcPersistenceKey> = map
            .iter()
            .filter(|(key, _)| key.conflict_genesis == conflict_genesis)
            .map(|(key, _)| key.clone())
            .collect();
        let count = keys_to_remove.len() as u32;
        for key in keys_to_remove {
            map.remove(&key);
        }
        Ok(count)
    }
}

/// DB-backed implementation of UMC persistence store.
#[derive(Clone)]
pub struct DbUmcPersistenceStore {
    db: Arc<DB>,
    access: CachedDbAccess<UmcPersistenceKey, UmcPersistedState>,
}

impl DbUmcPersistenceStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        // Use DagKnight prefix + 1 for UMC state (70 + 1 = 71)
        let prefix = vec![DatabaseStorePrefixes::DagKnight as u8 + 1];
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, prefix) }
    }
}

impl UmcPersistenceStoreReader for DbUmcPersistenceStore {
    fn get(&self, key: UmcPersistenceKey) -> Result<Option<UmcPersistedState>, StoreError> {
        match self.access.read(key) {
            Ok(state) => Ok(Some(state)),
            Err(StoreError::KeyNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn has(&self, key: UmcPersistenceKey) -> Result<bool, StoreError> {
        self.access.has(key)
    }
}

impl UmcPersistenceStore for DbUmcPersistenceStore {
    fn insert(&self, key: UmcPersistenceKey, state: UmcPersistedState) -> Result<(), StoreError> {
        let mut batch = WriteBatch::default();
        self.access.write(BatchDbWriter::new(&mut batch), key, state)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn delete(&self, key: UmcPersistenceKey) -> Result<(), StoreError> {
        let mut batch = WriteBatch::default();
        self.access.delete(BatchDbWriter::new(&mut batch), key)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn prune_by_conflict_genesis(&self, conflict_genesis: Hash) -> Result<u32, StoreError> {
        // UMC persistence key layout: conflict_genesis || k(u16 BE) || next_chain_ancestor || free_search
        // We need to delete all keys with this conflict_genesis prefix.
        let prefix = DatabaseStorePrefixes::DagKnight as u8 + 1; // UMC state prefix
        let mut start_bytes = Vec::with_capacity(kaspa_hashes::HASH_SIZE + 3);
        start_bytes.push(prefix);
        start_bytes.extend_from_slice(conflict_genesis.as_ref());

        // End bytes: same prefix + conflict_genesis with last byte incremented
        // This covers all possible k, NCA, and free_search combinations
        let mut end_bytes = Vec::with_capacity(kaspa_hashes::HASH_SIZE + 3);
        end_bytes.push(prefix);
        end_bytes.extend_from_slice(conflict_genesis.as_ref());
        // Increment last byte of conflict_genesis to get the end of the range
        // If the last byte is 0xFF, the range naturally extends to the next prefix
        let end_hash = {
            let mut h = [0u8; kaspa_hashes::HASH_SIZE];
            h.copy_from_slice(conflict_genesis.as_ref());
            // Add 1 to the hash bytes (big-endian increment)
            for i in (0..h.len()).rev() {
                if h[i] == 0xFF {
                    h[i] = 0;
                } else {
                    h[i] += 1;
                    break;
                }
            }
            h
        };
        end_bytes.extend_from_slice(&end_hash);

        let mut batch = WriteBatch::default();
        batch.delete_range(start_bytes, end_bytes);
        self.db.write(batch)?;

        // Count deleted entries (approximate — we can't easily count after range delete)
        // Return 0 as a placeholder; in practice the caller doesn't need the count
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_umc_persistence_key_construction() {
        let cg: Hash = 0xAA_u64.into();
        let nca: Hash = 0xBB_u64.into();
        let k: KType = 5;

        let key_committed = UmcPersistenceKey::new(cg, k, nca, false);
        let key_free = UmcPersistenceKey::new(cg, k, nca, true);

        assert_ne!(key_committed.as_ref(), key_free.as_ref());
        assert_eq!(*key_committed.as_ref().last().unwrap(), 0u8);
        assert_eq!(*key_free.as_ref().last().unwrap(), 1u8);
    }

    #[test]
    fn test_memory_umc_persistence_store_roundtrip() {
        let store = MemoryUmcPersistenceStore::default();
        let key = UmcPersistenceKey::new(0xAA_u64.into(), 5, 0xBB_u64.into(), false);
        let state = UmcPersistedState {
            tree_entries: vec![UmcPersistedTreeEntry {
                hash: 0x01_u64.into(),
                floor: SignedWork::from(Uint192::from_u64(100)),
                past_blue_work: Uint192::from_u64(50),
                past_red_work: Uint192::from_u64(30),
                anticone_blue_work: Uint192::from_u64(10),
                arlb: Uint192::from_u64(20),
                last_red_index: 0,
            }],
            red_index: 1,
            red_set: vec![0x10_u64.into()],
            secondary_heap: vec![],
            seen_red_work: Uint192::from_u64(100),
            negative_blues: Uint192::ZERO,
            cached_vote: true,
            tip_set_hash: Hash::default(),
        };

        // Insert
        store.insert(key.clone(), state.clone()).unwrap();

        // Verify has
        assert!(store.has(key.clone()).unwrap());

        // Retrieve
        let retrieved = store.get(key.clone()).unwrap().expect("state should exist");
        assert_eq!(retrieved, state);

        // Delete
        store.delete(key.clone()).unwrap();
        assert!(!store.has(key).unwrap());
    }

    #[test]
    fn test_memory_umc_persistence_store_overwrite() {
        let store = MemoryUmcPersistenceStore::default();
        let key = UmcPersistenceKey::new(0xAA_u64.into(), 5, 0xBB_u64.into(), false);

        let state1 = UmcPersistedState { cached_vote: true, tip_set_hash: Hash::default(), ..Default::default() };
        let state2 = UmcPersistedState { cached_vote: false, tip_set_hash: Hash::default(), ..Default::default() };

        store.insert(key.clone(), state1).unwrap();
        assert!(store.get(key.clone()).unwrap().unwrap().cached_vote);

        store.insert(key.clone(), state2).unwrap();
        assert!(!store.get(key).unwrap().unwrap().cached_vote);
    }

    #[test]
    fn test_db_umc_persistence_store_roundtrip() {
        use kaspa_database::prelude::{CachePolicy, ConnBuilder};

        let (_lifetime, db) = kaspa_database::create_temp_db!(ConnBuilder::default().with_files_limit(10));
        let store = DbUmcPersistenceStore::new(db, CachePolicy::Count(16));

        let key = UmcPersistenceKey::new(0xAA_u64.into(), 5, 0xBB_u64.into(), false);
        let state = UmcPersistedState {
            tree_entries: vec![UmcPersistedTreeEntry {
                hash: 0x01_u64.into(),
                floor: SignedWork::from(Uint192::from_u64(100)),
                past_blue_work: Uint192::from_u64(50),
                past_red_work: Uint192::from_u64(30),
                anticone_blue_work: Uint192::from_u64(10),
                arlb: Uint192::from_u64(20),
                last_red_index: 0,
            }],
            red_index: 1,
            red_set: vec![0x10_u64.into()],
            secondary_heap: vec![],
            seen_red_work: Uint192::from_u64(100),
            negative_blues: Uint192::ZERO,
            cached_vote: true,
            tip_set_hash: Hash::default(),
        };

        store.insert(key.clone(), state.clone()).unwrap();
        assert!(store.has(key.clone()).unwrap());

        let retrieved = store.get(key.clone()).unwrap().expect("state should exist");
        assert_eq!(retrieved, state);

        store.delete(key.clone()).unwrap();
        assert!(!store.has(key).unwrap());
    }

    #[test]
    fn test_dagknight_key_encodes_free_search_flag() {
        use kaspa_hashes::Hash;

        let root: Hash = 0xAA_u64.into();
        let pov: Hash = 0xBB_u64.into();
        let k: KType = 5;

        let key_committed = DagknightKey::new(root, pov, k, false);
        let key_free = DagknightKey::new(root, pov, k, true);

        // Keys with different free_search values must be distinct
        assert_ne!(key_committed.as_ref(), key_free.as_ref());

        // Verify the free_search flag is encoded as the last byte
        assert_eq!(key_committed.as_ref().last().unwrap(), &0u8);
        assert_eq!(key_free.as_ref().last().unwrap(), &1u8);
    }

    #[test]
    fn test_dagknight_key_encodes_k() {
        use crate::model::stores::dagknight::DagknightKey;
        use kaspa_hashes::Hash;

        let root: Hash = 0xAA_u64.into();
        let pov: Hash = 0xBB_u64.into();
        let k1: KType = 0x0001;
        let k2: KType = 0x0101; // 2nd byte is the same as above

        let key1 = DagknightKey::new(root, pov, k1, false);
        let key2 = DagknightKey::new(root, pov, k2, false);

        // The DB key bytes must differ when k differs. This captures the previous
        // bug where `k` was encoded incorrectly and keys collided across k values.
        println!("key1 bytes: {:?}", key1.as_ref());
        println!("key2 bytes: {:?}", key2.as_ref());
        assert_ne!(key1.as_ref(), key2.as_ref(), "DagknightKey DB bytes must encode k uniquely");

        // Also assert that the differing two-byte `k` slot differs (sanity check on layout)
        let hash_size = kaspa_hashes::HASH_SIZE;
        // k is encoded as two bytes after root_hash
        assert_ne!(
            &key1.as_ref()[hash_size..hash_size + 2],
            &key2.as_ref()[hash_size..hash_size + 2],
            "k slot (two bytes) must differ for different k values"
        );
    }

    #[test]
    fn test_db_dagknight_store_isolates_by_k() {
        use crate::model::stores::dagknight::{ColoringData, DbDagknightStore};
        use kaspa_database::prelude::CachePolicy;
        use kaspa_database::prelude::ConnBuilder;
        use kaspa_hashes::Hash;
        use std::sync::Arc;

        // Create a temporary RocksDB
        let (_lifetime, db) = kaspa_database::create_temp_db!(ConnBuilder::default().with_files_limit(10));

        let store = DbDagknightStore::new(db.clone(), CachePolicy::Count(16));

        let root: Hash = 0xAA_u64.into();
        let pov: Hash = 0xBB_u64.into();

        let k1 = 0x0001;
        let k2 = 0x0101; // 2nd byte is the same as above

        // Create two distinct ColoringData values
        let cd1 = ColoringData::new(
            10,
            Default::default(),
            Hash::from_u64_word(1),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            BlueWorkType::ZERO, // past_red_work
        );
        let cd2 = ColoringData::new(
            20,
            Default::default(),
            Hash::from_u64_word(2),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            BlueWorkType::ZERO, // past_red_work
        );

        let key1 = DagknightKey::new(root, pov, k1, false);
        let key2 = DagknightKey::new(root, pov, k2, false);

        // Insert both into the DB-backed store
        store.insert(key1.clone(), Arc::new(cd1)).expect("insert k1");
        store.insert(key2.clone(), Arc::new(cd2)).expect("insert k2");

        // Read them back and verify isolation
        let read1 = store.get_data(key1).expect("read k1");
        let read2 = store.get_data(key2).expect("read k2");

        assert_eq!(read1.blue_score, 10);
        assert_eq!(read2.blue_score, 20);
    }
}
