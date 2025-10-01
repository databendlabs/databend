// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FixedKey;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::KeyAccessor;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;
use databend_common_hashtable::StringRawEntry;
use databend_common_hashtable::STRING_EARLY_SIZE;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use ethnum::U256;
use parking_lot::RwLock;

use super::merge_into_hash_join_optimization::MergeIntoState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildState;
use crate::pipelines::processors::transforms::hash_join::transform_hash_join_build::HashTableType;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;
use crate::sql::IndexType;

pub struct SerializerHashJoinHashTable {
    probed_rows: AtomicUsize,
    matched_probe_rows: AtomicUsize,
    pub(crate) hash_table: BinaryHashJoinHashMap,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct SingleBinaryHashJoinHashTable {
    probed_rows: AtomicUsize,
    matched_probe_rows: AtomicUsize,
    pub(crate) hash_table: BinaryHashJoinHashMap,
    pub(crate) hash_method: HashMethodSingleBinary,
}

pub struct FixedKeyHashJoinHashTable<T: HashtableKeyable + FixedKey> {
    probed_rows: AtomicUsize,
    matched_probe_rows: AtomicUsize,
    pub(crate) hash_table: HashJoinHashMap<T>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashJoinHashTable {
    Null,
    Serializer(SerializerHashJoinHashTable),
    SingleBinary(SingleBinaryHashJoinHashTable),
    KeysU8(FixedKeyHashJoinHashTable<u8>),
    KeysU16(FixedKeyHashJoinHashTable<u16>),
    KeysU32(FixedKeyHashJoinHashTable<u32>),
    KeysU64(FixedKeyHashJoinHashTable<u64>),
    KeysU128(FixedKeyHashJoinHashTable<u128>),
    KeysU256(FixedKeyHashJoinHashTable<U256>),
}

/// Define some shared states for hash join build and probe.
/// It will like a bridge to connect build and probe.
/// Such as build side will pass hash table to probe side by it
pub struct HashJoinState {
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: SyncUnsafeCell<HashJoinHashTable>,
    /// After HashTable is built, send message to notify all probe processors.
    /// There are three types of messages:
    /// 1. FirstRound: it is the first time the hash table is constructed.
    /// 2. Restored: the hash table is restored from the spilled data.
    /// 3. Empty: the hash table is empty.
    pub(crate) build_watcher: Sender<HashTableType>,
    /// A dummy receiver to make build done watcher channel open
    pub(crate) _build_done_dummy_receiver: Receiver<HashTableType>,
    /// Some description of hash join. Such as join type, join keys, etc.
    pub(crate) hash_join_desc: HashJoinDesc,
    /// Interrupt the build phase or probe phase.
    pub(crate) interrupt: AtomicBool,
    /// If there is no data in build side, maybe we can fast return.
    pub(crate) fast_return: AtomicBool,
    /// Use the column of probe side to construct build side column.
    /// (probe index, (is probe column nullable, is build column nullable))
    pub(crate) probe_to_build: Vec<(usize, (bool, bool))>,
    pub(crate) build_schema: DataSchemaRef,
    /// `BuildState` contains all data used in probe phase.
    pub(crate) build_state: SyncUnsafeCell<BuildState>,

    /// Spill related states.
    /// It record whether spill has happened.
    pub(crate) is_spill_happened: AtomicBool,
    /// Spilled partition set, it contains all spilled partition sets from all build processors.
    pub(crate) spilled_partitions: RwLock<HashSet<usize>>,
    /// Spill partition bits, it is used to calculate the number of partitions.
    pub(crate) spill_partition_bits: usize,
    /// Spill buffer size threshold.
    pub(crate) spill_buffer_threshold: usize,
    /// The next partition id to be restored.
    pub(crate) partition_id: AtomicUsize,
    /// Whether need next round, if it is true, restore data from spilled data and start next round.
    pub(crate) need_next_round: AtomicBool,
    /// Send message to notify all build processors to next round.
    /// Initial message is false, send true to wake up all build processors.
    pub(crate) continue_build_watcher: Sender<bool>,
    /// A dummy receiver to make continue build watcher channel open
    pub(crate) _continue_build_dummy_receiver: Receiver<bool>,

    pub(crate) merge_into_state: Option<SyncUnsafeCell<MergeIntoState>>,

    /// Build side cache info.
    /// A HashMap for mapping the column indexes to the BlockEntry indexes in DataBlock.
    pub(crate) column_map: HashMap<usize, usize>,
    // The index of the next cache block to be read.
    pub(crate) next_cache_block_index: AtomicUsize,
}

impl HashJoinState {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        mut build_schema: DataSchemaRef,
        build_projections: &ColumnSet,
        hash_join_desc: HashJoinDesc,
        probe_to_build: &[(usize, (bool, bool))],
        merge_into_is_distributed: bool,
        enable_merge_into_optimization: bool,
        build_side_cache_info: Option<(usize, HashMap<IndexType, usize>)>,
    ) -> Result<Arc<HashJoinState>> {
        if matches!(
            hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) {
            build_schema = build_schema_wrap_nullable(&build_schema);
        };
        let (build_watcher, _build_done_dummy_receiver) = watch::channel(HashTableType::UnFinished);
        let (continue_build_watcher, _continue_build_dummy_receiver) = watch::channel(false);

        let settings = ctx.get_settings();
        let spill_partition_bits = settings.get_join_spilling_partition_bits()?;
        let spill_buffer_threshold = settings.get_join_spilling_buffer_threshold_per_proc()?;

        let column_map = if let Some((_, column_map)) = build_side_cache_info {
            column_map
        } else {
            HashMap::new()
        };

        let mut projected_build_fields = vec![];
        for (i, field) in build_schema.fields().iter().enumerate() {
            if build_projections.contains(&i) {
                projected_build_fields.push(field.clone());
            }
        }
        let build_schema = DataSchemaRefExt::create(projected_build_fields);
        Ok(Arc::new(HashJoinState {
            hash_table: SyncUnsafeCell::new(HashJoinHashTable::Null),
            build_watcher,
            _build_done_dummy_receiver,
            hash_join_desc,
            interrupt: AtomicBool::new(false),
            fast_return: Default::default(),
            probe_to_build: probe_to_build.to_vec(),
            build_schema,
            build_state: SyncUnsafeCell::new(BuildState::new()),
            spilled_partitions: Default::default(),
            continue_build_watcher,
            _continue_build_dummy_receiver,
            partition_id: AtomicUsize::new(0),
            need_next_round: AtomicBool::new(false),
            is_spill_happened: AtomicBool::new(false),
            spill_partition_bits,
            spill_buffer_threshold,
            merge_into_state: match enable_merge_into_optimization {
                false => None,
                true => Some(MergeIntoState::create_merge_into_state(
                    merge_into_is_distributed,
                )),
            },
            column_map,
            next_cache_block_index: AtomicUsize::new(0),
        }))
    }

    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
    }

    /// Used by hash join probe processors, wait for build phase finished.
    #[async_backtrace::framed]
    pub async fn wait_build_notify(&self) -> Result<HashTableType> {
        let mut rx = self.build_watcher.subscribe();
        if *rx.borrow() != HashTableType::UnFinished {
            return Ok(*rx.borrow());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("build_watcher's sender is dropped"))?;
        let hash_table_type = *rx.borrow();
        Ok(hash_table_type)
    }

    pub fn join_type(&self) -> JoinType {
        self.hash_join_desc.join_type.clone()
    }

    pub fn need_outer_scan(&self) -> bool {
        matches!(
            self.hash_join_desc.join_type,
            JoinType::Full
                | JoinType::Right
                | JoinType::RightSingle
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub fn need_mark_scan(&self) -> bool {
        matches!(self.hash_join_desc.join_type, JoinType::LeftMark)
    }

    pub fn need_final_scan(&self) -> bool {
        self.need_outer_scan() || self.need_mark_scan()
    }

    pub fn add_spilled_partitions(&self, partitions: &HashSet<usize>) {
        let mut spilled_partitions = self.spilled_partitions.write();
        spilled_partitions.extend(partitions);
    }

    #[async_backtrace::framed]
    pub(crate) async fn wait_probe_notify(&self) -> Result<()> {
        let mut rx = self.continue_build_watcher.subscribe();
        if *rx.borrow() {
            return Ok(());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("continue_build_watcher's sender is dropped"))?;
        debug_assert!(*rx.borrow());
        Ok(())
    }

    // Reset the state for next round run.
    // It's only called when spill is enable.
    pub(crate) fn reset(&self) {
        let build_state = unsafe { &mut *self.build_state.get() };
        build_state.generation_state.chunks.clear();
        build_state.generation_state.build_num_rows = 0;
        build_state.generation_state.build_columns.clear();
        build_state.generation_state.build_columns_data_type.clear();
        if self.need_outer_scan() {
            build_state.outer_scan_map.clear();
        }
        if self.need_mark_scan() {
            build_state.mark_scan_map.clear();
        }
        build_state.generation_state.is_build_projected = true;
    }

    pub fn num_build_chunks(&self) -> usize {
        let build_state = unsafe { &*self.build_state.get() };
        build_state.generation_state.chunks.len()
    }

    pub fn get_cached_columns(&self, column_index: usize) -> Vec<BlockEntry> {
        let index = self.column_map.get(&column_index).unwrap();
        let build_state = unsafe { &*self.build_state.get() };
        let columns = build_state
            .generation_state
            .chunks
            .iter()
            .map(|data_block| data_block.get_by_offset(*index).clone())
            .collect::<Vec<_>>();
        columns
    }

    pub fn get_cached_num_rows(&self) -> Vec<usize> {
        let build_state = unsafe { &*self.build_state.get() };
        let num_rows = build_state
            .generation_state
            .chunks
            .iter()
            .map(|data_block| data_block.num_rows())
            .collect::<Vec<_>>();
        num_rows
    }

    pub fn next_cache_block_index(&self) -> usize {
        self.next_cache_block_index.fetch_add(1, Ordering::AcqRel)
    }

    pub fn gather(
        &self,
        row_ptrs: &[RowPtr],
        build_columns: &[ColumnVec],
        build_columns_data_type: &[DataType],
        num_rows: &usize,
    ) -> Result<DataBlock> {
        if *num_rows != 0 {
            let data_block = DataBlock::take_column_vec(
                build_columns,
                build_columns_data_type,
                row_ptrs,
                row_ptrs.len(),
            );
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.build_schema.clone()))
        }
    }
}

impl<T: HashtableKeyable + FixedKey> FixedKeyHashJoinHashTable<T> {
    pub fn new(hash_table: HashJoinHashMap<T>, hash_method: HashMethodFixedKeys<T>) -> Self {
        FixedKeyHashJoinHashTable::<T> {
            hash_table,
            hash_method,
            probed_rows: Default::default(),
            matched_probe_rows: Default::default(),
        }
    }

    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;
        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let entry_size = std::mem::size_of::<RawEntry<T>>();
        arena.reserve(num_rows * entry_size);

        let mut raw_entry_ptr =
            unsafe { std::mem::transmute::<*mut u8, *mut RawEntry<T>>(arena.as_mut_ptr()) };

        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            // # Safety
            // The memory address of `raw_entry_ptr` is valid.
            unsafe {
                *raw_entry_ptr = RawEntry {
                    row_ptr,
                    key: *key,
                    next: 0,
                }
            }

            self.hash_table.insert(*key, raw_entry_ptr);
            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
        }

        Ok(())
    }

    pub fn probe_keys(
        &self,
        keys: DataBlock,
        valids: Option<Bitmap>,
    ) -> Result<Box<dyn ProbeStream>> {
        let num_rows = keys.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let enable_early_filtering = match self.probed_rows.load(Ordering::Relaxed) {
            0 => false,
            probed_rows => {
                let matched_probe_rows = self.matched_probe_rows.load(Ordering::Relaxed) as f64;
                matched_probe_rows / (probed_rows as f64) < 0.8
            }
        };

        self.probed_rows.fetch_add(
            match &valids {
                None => num_rows,
                Some(valids) => valids.len() - valids.null_count(),
            },
            Ordering::Relaxed,
        );

        match enable_early_filtering {
            true => {
                let mut selection = vec![0; num_rows];

                match self.hash_table.early_filtering_matched_probe(
                    &mut hashes,
                    valids,
                    &mut selection,
                ) {
                    0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                    _ => Ok(FixedKeysProbeStream::create(hashes, keys)),
                }
            }
            false => match self.hash_table.probe(&mut hashes, valids) {
                0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                _ => Ok(FixedKeysProbeStream::create(hashes, keys)),
            },
        }
    }
}

impl SerializerHashJoinHashTable {
    pub fn new(
        hash_table: BinaryHashJoinHashMap,
        hash_method: HashMethodSerializer,
    ) -> SerializerHashJoinHashTable {
        SerializerHashJoinHashTable {
            hash_table,
            hash_method,
            probed_rows: AtomicUsize::new(0),
            matched_probe_rows: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;
        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let space_size = match &keys_state {
            // safe to unwrap(): offset.len() >= 1.
            KeysState::Column(Column::Bitmap(col)) => col.data().len(),
            KeysState::Column(Column::Binary(col)) => col.data().len(),
            KeysState::Column(Column::Variant(col)) => col.data().len(),
            KeysState::Column(Column::String(col)) => col.total_bytes_len(),
            _ => unreachable!(),
        };

        static ENTRY_SIZE: usize = std::mem::size_of::<StringRawEntry>();
        arena.reserve(num_rows * ENTRY_SIZE + space_size);

        let (mut raw_entry_ptr, mut string_local_space_ptr) = unsafe {
            (
                std::mem::transmute::<*mut u8, *mut StringRawEntry>(arena.as_mut_ptr()),
                arena.as_mut_ptr().add(num_rows * ENTRY_SIZE),
            )
        };

        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            // # Safety
            // The memory address of `raw_entry_ptr` is valid.
            // string_offset + key.len() <= space_size.
            unsafe {
                (*raw_entry_ptr).row_ptr = row_ptr;
                (*raw_entry_ptr).length = key.len() as u32;
                (*raw_entry_ptr).next = 0;
                (*raw_entry_ptr).key = string_local_space_ptr;
                // The size of `early` is 4.
                std::ptr::copy_nonoverlapping(
                    key.as_ptr(),
                    (*raw_entry_ptr).early.as_mut_ptr(),
                    std::cmp::min(STRING_EARLY_SIZE, key.len()),
                );
                std::ptr::copy_nonoverlapping(key.as_ptr(), string_local_space_ptr, key.len());
                string_local_space_ptr = string_local_space_ptr.add(key.len());
            }

            self.hash_table.insert(key, raw_entry_ptr);
            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
        }

        Ok(())
    }

    pub fn probe_keys(
        &self,
        keys: DataBlock,
        valids: Option<Bitmap>,
    ) -> Result<Box<dyn ProbeStream>> {
        let num_rows = keys.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let enable_early_filtering = match self.probed_rows.load(Ordering::Relaxed) {
            0 => false,
            probed_rows => {
                let matched_probe_rows = self.matched_probe_rows.load(Ordering::Relaxed) as f64;
                matched_probe_rows / (probed_rows as f64) < 0.8
            }
        };

        self.probed_rows.fetch_add(
            match &valids {
                None => keys.len(),
                Some(valids) => valids.len() - valids.null_count(),
            },
            Ordering::Relaxed,
        );

        match enable_early_filtering {
            true => {
                let mut selection = vec![0; keys.len()];

                match self.hash_table.early_filtering_matched_probe(
                    &mut hashes,
                    valids,
                    &mut selection,
                ) {
                    0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                    _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
                }
            }
            false => match self.hash_table.probe(&mut hashes, valids) {
                0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
            },
        }
    }
}

impl SingleBinaryHashJoinHashTable {
    pub fn new(
        hash_table: BinaryHashJoinHashMap,
        hash_method: HashMethodSingleBinary,
    ) -> SingleBinaryHashJoinHashTable {
        SingleBinaryHashJoinHashTable {
            hash_table,
            hash_method,
            probed_rows: AtomicUsize::new(0),
            matched_probe_rows: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, keys: DataBlock, chunk: usize, arena: &mut Vec<u8>) -> Result<()> {
        let num_rows = keys.num_rows();
        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = self.hash_method.build_keys_state(keys, num_rows)?;
        let build_keys_iter = self.hash_method.build_keys_iter(&keys_state)?;

        let space_size = match &keys_state {
            // safe to unwrap(): offset.len() >= 1.
            KeysState::Column(Column::Bitmap(col)) => col.data().len(),
            KeysState::Column(Column::Binary(col)) => col.data().len(),
            KeysState::Column(Column::Variant(col)) => col.data().len(),
            KeysState::Column(Column::String(col)) => col.total_bytes_len(),
            _ => unreachable!(),
        };

        static ENTRY_SIZE: usize = std::mem::size_of::<StringRawEntry>();
        arena.reserve(num_rows * ENTRY_SIZE + space_size);

        let (mut raw_entry_ptr, mut string_local_space_ptr) = unsafe {
            (
                std::mem::transmute::<*mut u8, *mut StringRawEntry>(arena.as_mut_ptr()),
                arena.as_mut_ptr().add(num_rows * ENTRY_SIZE),
            )
        };

        for (row_index, key) in build_keys_iter.enumerate() {
            let row_ptr = RowPtr {
                chunk_index: chunk as u32,
                row_index: row_index as u32,
            };

            // # Safety
            // The memory address of `raw_entry_ptr` is valid.
            // string_offset + key.len() <= space_size.
            unsafe {
                (*raw_entry_ptr).row_ptr = row_ptr;
                (*raw_entry_ptr).length = key.len() as u32;
                (*raw_entry_ptr).next = 0;
                (*raw_entry_ptr).key = string_local_space_ptr;
                // The size of `early` is 4.
                std::ptr::copy_nonoverlapping(
                    key.as_ptr(),
                    (*raw_entry_ptr).early.as_mut_ptr(),
                    std::cmp::min(STRING_EARLY_SIZE, key.len()),
                );
                std::ptr::copy_nonoverlapping(key.as_ptr(), string_local_space_ptr, key.len());
                string_local_space_ptr = string_local_space_ptr.add(key.len());
            }

            self.hash_table.insert(key, raw_entry_ptr);
            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
        }

        Ok(())
    }

    pub fn probe_keys(
        &self,
        keys: DataBlock,
        valids: Option<Bitmap>,
    ) -> Result<Box<dyn ProbeStream>> {
        let num_rows = keys.num_rows();
        let hash_method = &self.hash_method;
        let mut hashes = Vec::with_capacity(num_rows);

        let keys = ProjectedBlock::from(keys.columns());
        let keys_state = hash_method.build_keys_state(keys, num_rows)?;
        hash_method.build_keys_hashes(&keys_state, &mut hashes);
        let keys = hash_method.build_keys_accessor(keys_state.clone())?;

        let enable_early_filtering = match self.probed_rows.load(Ordering::Relaxed) {
            0 => false,
            probed_rows => {
                let matched_probe_rows = self.matched_probe_rows.load(Ordering::Relaxed) as f64;
                matched_probe_rows / (probed_rows as f64) < 0.8
            }
        };

        self.probed_rows.fetch_add(
            match &valids {
                None => keys.len(),
                Some(valids) => valids.len() - valids.null_count(),
            },
            Ordering::Relaxed,
        );

        match enable_early_filtering {
            true => {
                let mut selection = vec![0; keys.len()];

                match self.hash_table.early_filtering_matched_probe(
                    &mut hashes,
                    valids,
                    &mut selection,
                ) {
                    0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                    _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
                }
            }
            false => match self.hash_table.probe(&mut hashes, valids) {
                0 => Ok(AllUnmatchedProbeStream::create(hashes.len())),
                _ => Ok(BinaryKeyProbeStream::create(hashes, keys)),
            },
        }
    }
}

pub struct ProbeKeysResult {
    pub unmatched: Vec<usize>,
    pub matched_probe: Vec<u64>,
    pub matched_build: Vec<RowPtr>,
}

impl ProbeKeysResult {
    pub fn empty() -> ProbeKeysResult {
        ProbeKeysResult::new(vec![], vec![], vec![])
    }

    pub fn new(
        unmatched: Vec<usize>,
        matched_probe: Vec<u64>,
        matched_build: Vec<RowPtr>,
    ) -> ProbeKeysResult {
        assert_eq!(matched_build.len(), matched_probe.len());

        ProbeKeysResult {
            unmatched,
            matched_probe,
            matched_build,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.matched_build.is_empty() && self.unmatched.is_empty()
    }

    pub fn is_all_unmatched(&self) -> bool {
        self.matched_build.is_empty() && !self.unmatched.is_empty()
    }

    pub fn all_unmatched(unmatched: Vec<usize>) -> ProbeKeysResult {
        ProbeKeysResult::new(unmatched, vec![], vec![])
    }
}

pub trait ProbeStream {
    fn next(&mut self, max_rows: usize) -> Result<ProbeKeysResult>;
}

pub struct AllUnmatchedProbeStream {
    idx: usize,
    size: usize,
}

impl AllUnmatchedProbeStream {
    pub fn create(size: usize) -> Box<dyn ProbeStream> {
        Box::new(AllUnmatchedProbeStream { idx: 0, size })
    }
}

impl ProbeStream for AllUnmatchedProbeStream {
    fn next(&mut self, max_rows: usize) -> Result<ProbeKeysResult> {
        if self.idx >= self.size {
            return Ok(ProbeKeysResult::empty());
        }

        let res = std::cmp::min(self.size - self.idx, max_rows);
        let res = (self.idx..self.idx + res).collect::<Vec<_>>();
        self.idx += res.len();
        Ok(ProbeKeysResult::all_unmatched(res))
    }
}

pub struct FixedKeysProbeStream<Key: FixedKey + HashtableKeyable> {
    key_idx: usize,
    pointers: Vec<u64>,
    keys: Box<(dyn KeyAccessor<Key = Key>)>,
    probe_entry_ptr: u64,
}

impl<Key: FixedKey + HashtableKeyable> FixedKeysProbeStream<Key> {
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = Key>>,
    ) -> Box<dyn ProbeStream> {
        Box::new(FixedKeysProbeStream {
            keys,
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
        })
    }
}

impl<Key: FixedKey + HashtableKeyable> ProbeStream for FixedKeysProbeStream<Key> {
    fn next(&mut self, max_rows: usize) -> Result<ProbeKeysResult> {
        unsafe {
            let mut matched_build = Vec::with_capacity(max_rows);
            let mut matched_probe = Vec::with_capacity(max_rows);
            let mut unmatched = Vec::with_capacity(max_rows);

            while self.key_idx < self.keys.len() {
                std::hint::assert_unchecked(unmatched.len() <= unmatched.capacity());
                std::hint::assert_unchecked(matched_probe.len() == matched_build.len());
                std::hint::assert_unchecked(matched_build.len() <= matched_build.capacity());
                std::hint::assert_unchecked(matched_probe.len() <= matched_probe.capacity());

                if matched_probe.len() == max_rows {
                    break;
                }

                if self.probe_entry_ptr == 0 {
                    self.probe_entry_ptr = *self.pointers.get_unchecked(self.key_idx);

                    if self.probe_entry_ptr == 0 {
                        unmatched.push(self.key_idx);
                        self.key_idx += 1;
                        continue;
                    }
                }

                let key = self.keys.key_unchecked(self.key_idx);

                while self.probe_entry_ptr != 0 {
                    let raw_entry = &*(self.probe_entry_ptr as *mut RawEntry<Key>);

                    if key == &raw_entry.key {
                        let row_ptr = raw_entry.row_ptr;
                        matched_probe.push(self.key_idx as u64);
                        matched_build.push(row_ptr);

                        if matched_probe.len() == max_rows {
                            self.probe_entry_ptr = raw_entry.next;

                            if self.probe_entry_ptr == 0 {
                                self.key_idx += 1;
                            }

                            return Ok(ProbeKeysResult::new(
                                unmatched,
                                matched_probe,
                                matched_build,
                            ));
                        }
                    }

                    self.probe_entry_ptr = raw_entry.next;
                }

                self.key_idx += 1;
            }

            Ok(ProbeKeysResult::new(
                unmatched,
                matched_probe,
                matched_build,
            ))
        }
    }
}

struct BinaryKeyProbeStream {
    key_idx: usize,
    pointers: Vec<u64>,
    keys: Box<(dyn KeyAccessor<Key = [u8]>)>,
    probe_entry_ptr: u64,
}

impl BinaryKeyProbeStream {
    pub fn create(
        pointers: Vec<u64>,
        keys: Box<dyn KeyAccessor<Key = [u8]>>,
    ) -> Box<dyn ProbeStream> {
        Box::new(BinaryKeyProbeStream {
            keys,
            pointers,
            key_idx: 0,
            probe_entry_ptr: 0,
        })
    }
}

impl ProbeStream for BinaryKeyProbeStream {
    fn next(&mut self, max_rows: usize) -> Result<ProbeKeysResult> {
        unsafe {
            let mut matched_build = Vec::with_capacity(max_rows);
            let mut matched_probe = Vec::with_capacity(max_rows);
            let mut unmatched = Vec::with_capacity(max_rows);

            while self.key_idx < self.keys.len() {
                std::hint::assert_unchecked(unmatched.len() <= unmatched.capacity());
                std::hint::assert_unchecked(matched_probe.len() == matched_build.len());
                std::hint::assert_unchecked(matched_build.len() <= matched_build.capacity());
                std::hint::assert_unchecked(matched_probe.len() <= matched_probe.capacity());

                if matched_probe.len() == max_rows {
                    break;
                }

                if self.probe_entry_ptr == 0 {
                    self.probe_entry_ptr = *self.pointers.get_unchecked(self.key_idx);

                    if self.probe_entry_ptr == 0 {
                        unmatched.push(self.key_idx);
                        self.key_idx += 1;
                        continue;
                    }
                }

                let key = self.keys.key_unchecked(self.key_idx);

                while self.probe_entry_ptr != 0 {
                    let raw_entry = &*(self.probe_entry_ptr as *mut StringRawEntry);
                    // Compare `early` and the length of the string, the size of `early` is 4.
                    let min_len = std::cmp::min(STRING_EARLY_SIZE, key.len());

                    if raw_entry.length as usize == key.len()
                        && key[0..min_len] == raw_entry.early[0..min_len]
                    {
                        let key_ref = std::slice::from_raw_parts(
                            raw_entry.key as *const u8,
                            raw_entry.length as usize,
                        );
                        if key == key_ref {
                            let row_ptr = raw_entry.row_ptr;
                            matched_probe.push(self.key_idx as u64);
                            matched_build.push(row_ptr);

                            if matched_probe.len() == max_rows {
                                self.probe_entry_ptr = raw_entry.next;

                                if self.probe_entry_ptr == 0 {
                                    self.key_idx += 1;
                                }

                                return Ok(ProbeKeysResult::new(
                                    unmatched,
                                    matched_probe,
                                    matched_build,
                                ));
                            }
                        }
                    }

                    self.probe_entry_ptr = raw_entry.next;
                }

                self.key_idx += 1;
            }

            Ok(ProbeKeysResult::new(
                unmatched,
                matched_probe,
                matched_build,
            ))
        }
    }
}
