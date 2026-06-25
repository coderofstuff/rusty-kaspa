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
use kaspa_math::Uint192;

/// Map from blue block hash to the total work of blue blocks in its anticone (from POV of current block).
pub type HashBlueWorkMap = BlockHashMap<BlueWorkType>;

use crate::model::stores::ghostdag::GhostdagData;
use kaspa_database::prelude::{BatchDbWriter, CachePolicy, CachedDbAccess, DB};
use rocksdb::WriteBatch;

/// Extended coloring data used by the DAGKnight conflict zone manager.
///
/// Contains all fields of [GhostdagData] plus incrementally computed counters
/// that eliminate O(n) scans during UMC cascade voting. All past counters are
/// accumulated during k-coloring:
///
/// ```text
/// past_count(B)     = past_count(SP(B))     + |mergeset_blues(B)| + |mergeset_reds(B)|
/// past_grays(B)     = past_grays(SP(B))     + |mergeset_reds(B) ∩ grays|
/// past_gray_work(B) = past_gray_work(SP(B)) + work(mergeset_reds(B) ∩ grays)
/// ```
///
/// A red in `mergeset_reds` is classified as "gray" if it is a chain ancestor
/// of the current block's selected parent (i.e., it agrees with the selected
/// parent's chain). This matches the gray classification used during UMC voting.
///
/// `blue_work` already exists and represents the cumulative blue work in B's past.
/// Together, these pre-computed counters enable O(1) counter lookup during cascade
/// voting instead of O(n) zone scans.
///
/// `blues_anticone_work` is parallel to `blues_anticone_sizes`: for each blue block
/// it tracks the total work of blue blocks in its anticone from the POV of this block.
/// Unlike sizes, work increments by the actual work of each new blue rather than +1.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ColoringData {
    pub blue_score: u64,
    pub blue_work: BlueWorkType,
    pub selected_parent: Hash,
    pub mergeset_blues: BlockHashes,
    pub mergeset_reds: BlockHashes,
    pub blues_anticone_sizes: HashKTypeMap,
    /// Parallel to `blues_anticone_sizes`: maps each blue block to the total work of
    /// blue blocks in its anticone from the POV of the current block.
    pub blues_anticone_work: BlockHashMap<BlueWorkType>,
    /// Number of blocks in this block's past that are within the conflict zone.
    /// Computed incrementally during k-coloring.
    pub past_count: u64,
    /// Number of gray blocks (mergeset_reds that are chain ancestors of the selected parent)
    /// in this block's past. Computed incrementally:
    /// past_grays(B) = past_grays(SP(B)) + |mergeset_reds(B) ∩ chain_ancestors_of(SP)|
    pub past_grays: u64,
    /// Total work of gray blocks in this block's past. Computed incrementally:
    /// past_gray_work(B) = past_gray_work(SP(B)) + work(mergeset_reds(B) ∩ chain_ancestors_of(SP))
    pub past_gray_work: Uint192,
    pub past_reds: u64,
    pub past_red_work: Uint192,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct PastColoringData {
    pub past_count: u64,
    pub past_blues: u64,
    pub past_blue_work: BlueWorkType,
    pub past_grays: u64,
    pub past_gray_work: BlueWorkType,
    pub past_reds: u64,
    pub past_red_work: BlueWorkType,
    pub anticone_blue_work: BlueWorkType,
    pub anticone_red_work_lower_bound: BlueWorkType,
}

impl MemSizeEstimator for ColoringData {
    fn estimate_mem_bytes(&self) -> usize {
        let mut bytes = size_of::<Self>();
        bytes += (self.mergeset_blues.len() + self.mergeset_reds.len()) * size_of::<Hash>();
        bytes += self.blues_anticone_sizes.len() * size_of::<(Hash, KType)>();
        bytes
    }
}

impl From<GhostdagData> for ColoringData {
    /// Converts GhostdagData into ColoringData, setting past counters to 0.
    fn from(gd: GhostdagData) -> Self {
        Self {
            blue_score: gd.blue_score,
            blue_work: gd.blue_work,
            selected_parent: gd.selected_parent,
            mergeset_blues: gd.mergeset_blues,
            mergeset_reds: gd.mergeset_reds,
            blues_anticone_sizes: gd.blues_anticone_sizes,
            blues_anticone_work: BlockHashMap::new(),
            past_count: 0,
            past_grays: 0,
            past_gray_work: Uint192::ZERO,
            past_reds: 0,
            past_red_work: Uint192::ZERO,
        }
    }
}

impl From<&GhostdagData> for ColoringData {
    /// Converts a reference to GhostdagData into a new ColoringData,
    /// cloning all fields and setting past counters to 0.
    fn from(gd: &GhostdagData) -> Self {
        Self {
            blue_score: gd.blue_score,
            blue_work: gd.blue_work,
            selected_parent: gd.selected_parent,
            mergeset_blues: gd.mergeset_blues.clone(),
            mergeset_reds: gd.mergeset_reds.clone(),
            blues_anticone_sizes: gd.blues_anticone_sizes.clone(),
            blues_anticone_work: BlockHashMap::new(),
            past_count: 0,
            past_grays: 0,
            past_gray_work: Uint192::ZERO,
            past_reds: 0,
            past_red_work: Uint192::ZERO,
        }
    }
}

impl From<ColoringData> for GhostdagData {
    /// Converts ColoringData back into GhostdagData, dropping past counters.
    fn from(cd: ColoringData) -> Self {
        Self {
            blue_score: cd.blue_score,
            blue_work: cd.blue_work,
            selected_parent: cd.selected_parent,
            mergeset_blues: cd.mergeset_blues,
            mergeset_reds: cd.mergeset_reds,
            blues_anticone_sizes: cd.blues_anticone_sizes,
        }
    }
}

impl From<&ColoringData> for GhostdagData {
    /// Converts a reference to ColoringData into a new GhostdagData,
    /// cloning all shared fields and dropping past counters.
    fn from(cd: &ColoringData) -> Self {
        Self {
            blue_score: cd.blue_score,
            blue_work: cd.blue_work,
            selected_parent: cd.selected_parent,
            mergeset_blues: cd.mergeset_blues.clone(),
            mergeset_reds: cd.mergeset_reds.clone(),
            blues_anticone_sizes: cd.blues_anticone_sizes.clone(),
        }
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
        blues_anticone_sizes: HashKTypeMap,
        blues_anticone_work: BlockHashMap<BlueWorkType>,
        past_count: u64,
        past_grays: u64,
        past_gray_work: Uint192,
        past_reds: u64,
        past_red_work: Uint192,
    ) -> Self {
        Self {
            blue_score,
            blue_work,
            selected_parent,
            mergeset_blues,
            mergeset_reds,
            blues_anticone_sizes,
            blues_anticone_work,
            past_count,
            past_grays,
            past_gray_work,
            past_reds,
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
            blues_anticone_sizes: HashKTypeMap::new(blues_anticone_sizes),
            blues_anticone_work,
            past_count: 0,
            past_grays: 0,
            past_gray_work: Uint192::ZERO,
            past_reds: 0,
            past_red_work: Uint192::ZERO,
        }
    }

    /// Returns the total mergeset size (blues + reds).
    pub fn mergeset_size(&self) -> usize {
        self.mergeset_blues.len() + self.mergeset_reds.len()
    }

    /// Adds a blue block to the mergeset, tracking both anticone sizes and work.
    ///
    /// `block_blues_anticone_sizes` maps each peer in the candidate's anticone to
    /// that peer's anticone size. `block_blues_anticone_work` maps each peer to
    /// its own work (from header bits). `candidate_work` is the candidate's own
    /// work (from header bits).
    ///
    /// The candidate's own anticone work = sum of work of all peers already in its
    /// anticone (from check_blue_candidate's VSPC walk). Each existing peer's work
    /// entry is then incremented by the candidate's own work.
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

        // Candidate's own anticone work = sum of work of all blues already in its anticone
        let candidate_anticone_work: BlueWorkType =
            block_blues_anticone_work.values().copied().sum();

        let blues_anticone_work = &mut self.blues_anticone_work;
        blues_anticone_work.insert(block, candidate_anticone_work);

        // Increment each existing peer's work entry by the candidate's own work
        for &peer in block_blues_anticone_sizes.keys() {
            let entry = blues_anticone_work.entry(peer).or_insert(BlueWorkType::ZERO);
            *entry = *entry + candidate_work;
        }
    }

    /// Adds a red block to the mergeset.
    pub fn add_red(&mut self, block: Hash) {
        BlockHashes::make_mut(&mut self.mergeset_reds).push(block);
    }

    /// Finalizes blue_score, blue_work, and past_count in a single call.
    pub fn finalize_score_work_and_past_count(&mut self, blue_score: u64, blue_work: BlueWorkType, past_count: u64) {
        self.blue_score = blue_score;
        self.blue_work = blue_work;
        self.past_count = past_count;
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

impl ToString for DagknightKey {
    fn to_string(&self) -> String {
        format!("{:?}", &self.bytes)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            5,             // past_count
            0,             // past_grays
            Uint192::ZERO, // past_gray_work
            0,             // past_reds
            Uint192::ZERO, // past_red_work
        );
        let cd2 = ColoringData::new(
            20,
            Default::default(),
            Hash::from_u64_word(2),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            8,             // past_count
            0,             // past_grays
            Uint192::ZERO, // past_gray_work
            0,             // past_reds
            Uint192::ZERO, // past_red_work
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
        assert_eq!(read1.past_count, 5);
        assert_eq!(read2.past_count, 8);
    }
}
