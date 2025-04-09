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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnRef;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::KeysState;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;
use databend_common_hashtable::StringRawEntry;
use databend_common_hashtable::STRING_EARLY_SIZE;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use ethnum::U256;
use itertools::Itertools;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

use super::desc::RuntimeFilterDesc;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::transform_hash_join_build::HashTableType;
use crate::pipelines::processors::transforms::hash_join::util::dedup_build_key_column;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method;
use crate::pipelines::processors::transforms::hash_join::util::inlist_filter;
use crate::pipelines::processors::transforms::hash_join::util::min_max_filter;
use crate::pipelines::processors::transforms::hash_join::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::HashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;

pub(crate) const INLIST_RUNTIME_FILTER_THRESHOLD: usize = 1024;

/// Define some shared states for all hash join build threads.
pub struct HashJoinBuildState {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    /// `hash_join_state` is shared by `HashJoinBuild` and `HashJoinProbe`
    pub(crate) hash_join_state: Arc<HashJoinState>,
    /// The counters will be increased by 1 when a new hash join build processor is created.
    /// After the processor finished Collect/Finalize/NextRound step, it will be decreased by 1.
    /// When the counter is 0, it means all processors have finished their work.
    pub(crate) collect_counter: AtomicUsize,
    pub(crate) finalize_counter: AtomicUsize,
    pub(crate) next_round_counter: AtomicUsize,
    /// The barrier is used to synchronize build side processors.
    pub(crate) barrier: Barrier,
    // When build side input data is coming, will put it into chunks.
    // To make the size of each chunk suitable, it's better to define a threshold to the size of each chunk.
    // Before putting the input data into `Chunk`, we will add them to buffer of `RowSpace`
    // After buffer's size hits the threshold, we will flush the buffer to `Chunk`.
    pub(crate) chunk_size_limit: usize,
    /// Hash method for hash join keys.
    pub(crate) method: HashMethodKind,
    /// The size of each entry in HashTable.
    pub(crate) entry_size: AtomicUsize,
    pub(crate) raw_entry_spaces: Mutex<Vec<Vec<u8>>>,
    /// `build_projections` only contains the columns from upstream required columns
    /// and columns from other_condition which are in build schema.
    pub(crate) build_projections: ColumnSet,
    pub(crate) build_worker_num: AtomicU32,
    /// Tasks for building hash table.
    pub(crate) build_hash_table_tasks: RwLock<VecDeque<usize>>,
    pub(crate) mutex: Mutex<()>,

    /// Spill related states.
    pub(crate) memory_settings: MemorySettings,
}

impl HashJoinBuildState {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<QueryContext>,
        func_ctx: FunctionContext,
        build_keys: &[RemoteExpr],
        build_projections: &ColumnSet,
        hash_join_state: Arc<HashJoinState>,
        num_threads: usize,
    ) -> Result<Arc<HashJoinBuildState>> {
        let hash_key_types = build_keys
            .iter()
            .zip(&hash_join_state.hash_join_desc.is_null_equal)
            .map(|(expr, is_null_equal)| {
                let expr = expr.as_expr(&BUILTIN_FUNCTIONS);
                if *is_null_equal {
                    expr.data_type().clone()
                } else {
                    expr.data_type().remove_nullable()
                }
            })
            .collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types)?;

        let settings = ctx.get_settings();
        let chunk_size_limit = settings.get_max_block_size()? as usize * 16;
        let memory_settings = MemorySettings::from_join_settings(&ctx)?;

        Ok(Arc::new(Self {
            ctx: ctx.clone(),
            func_ctx,
            hash_join_state,
            collect_counter: AtomicUsize::new(0),
            finalize_counter: AtomicUsize::new(0),
            next_round_counter: AtomicUsize::new(0),
            barrier: Barrier::new(num_threads),
            chunk_size_limit,
            method,
            entry_size: Default::default(),
            raw_entry_spaces: Default::default(),
            build_projections: build_projections.clone(),
            build_worker_num: Default::default(),
            build_hash_table_tasks: Default::default(),
            mutex: Default::default(),
            memory_settings,
        }))
    }

    /// Add input `DataBlock` to `hash_join_state.row_space`.
    pub fn build(&self, input: DataBlock) -> Result<()> {
        let mut buffer = self.hash_join_state.row_space.buffer.write();

        let input_rows = input.num_rows();
        // We have acquired the lock, so we can use Ordering::Relaxed here.
        let old_size = self
            .hash_join_state
            .row_space
            .buffer_row_size
            .fetch_add(input_rows, Ordering::Relaxed);
        buffer.push(input);

        if old_size + input_rows < self.chunk_size_limit {
            return Ok(());
        }

        let data_block = DataBlock::concat(buffer.as_slice())?;
        buffer.clear();
        drop(buffer);

        // We have acquired the lock, so we can use Ordering::Relaxed here.
        self.hash_join_state
            .row_space
            .buffer_row_size
            .store(0, Ordering::Relaxed);

        self.add_build_block(data_block)
    }

    // Add `data_block` for build table to `row_space`
    pub(crate) fn add_build_block(&self, data_block: DataBlock) -> Result<()> {
        let block_outer_scan_map = if self.hash_join_state.need_outer_scan()
            || matches!(
                self.hash_join_state.hash_join_desc.single_to_inner,
                Some(JoinType::RightSingle)
            ) {
            vec![false; data_block.num_rows()]
        } else {
            vec![]
        };

        let block_mark_scan_map = if self.hash_join_state.need_mark_scan() {
            vec![MARKER_KIND_FALSE; data_block.num_rows()]
        } else {
            vec![]
        };

        {
            // Acquire lock in current scope
            let _lock = self.mutex.lock();
            let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
            if self.hash_join_state.need_outer_scan()
                || matches!(
                    self.hash_join_state.hash_join_desc.single_to_inner,
                    Some(JoinType::RightSingle)
                )
            {
                build_state.outer_scan_map.push(block_outer_scan_map);
            }
            if self.hash_join_state.need_mark_scan() {
                build_state.mark_scan_map.push(block_mark_scan_map);
            }

            build_state.generation_state.build_num_rows += data_block.num_rows();
            build_state.generation_state.chunks.push(data_block);

            self.merge_into_try_add_chunk_offset(build_state);
        }
        Ok(())
    }

    /// Attach to state: `collect_counter` and `finalize_counter`.
    pub fn build_attach(&self) {
        self.build_worker_num.fetch_add(1, Ordering::AcqRel);
        self.collect_counter.fetch_add(1, Ordering::AcqRel);
        self.finalize_counter.fetch_add(1, Ordering::AcqRel);
        self.next_round_counter.fetch_add(1, Ordering::AcqRel);
    }

    /// Detach to state: `collect_counter`,
    /// create finalize task and initialize the hash table.
    pub(crate) fn collect_done(&self) -> Result<()> {
        let old_count = self.collect_counter.fetch_sub(1, Ordering::AcqRel);
        if old_count == 1 {
            {
                let mut buffer = self.hash_join_state.row_space.buffer.write();
                if !buffer.is_empty() {
                    let data_block = DataBlock::concat(&buffer)?;
                    self.add_build_block(data_block)?;
                    buffer.clear();
                }
            }

            // Get the number of rows of the build side.
            let build_num_rows = unsafe {
                (*self.hash_join_state.build_state.get())
                    .generation_state
                    .build_num_rows
            };

            // If the build side is empty and there is no spilled data, perform fast path for hash join.
            if build_num_rows == 0
                && !matches!(
                    self.hash_join_state.hash_join_desc.join_type,
                    JoinType::LeftMark | JoinType::RightMark
                )
                && !self
                    .hash_join_state
                    .is_spill_happened
                    .load(Ordering::Acquire)
            {
                self.hash_join_state
                    .fast_return
                    .store(true, Ordering::Release);
                self.hash_join_state
                    .build_watcher
                    .send(HashTableType::Empty)
                    .map_err(|_| ErrorCode::TokioError("build_watcher channel is closed"))?;
                self.set_bloom_filter_ready(false)?;
                return Ok(());
            }

            if self.hash_join_state.hash_join_desc.join_type == JoinType::Cross {
                return Ok(());
            }

            let build_chunks = unsafe {
                (*self.hash_join_state.build_state.get())
                    .generation_state
                    .chunks
                    .clone()
            };

            // If spilling happened, skip adding runtime filter, because probe data is ready and spilled.
            if self.hash_join_state.spilled_partitions.read().is_empty() {
                self.add_runtime_filter(&build_chunks, build_num_rows)?;
            } else {
                self.set_bloom_filter_ready(false)?;
            }

            // Divide the finalize phase into multiple tasks.
            self.generate_finalize_task()?;

            // Create a fixed size hash table.
            let (hash_join_hash_table, entry_size) = match self.method.clone() {
                HashMethodKind::Serializer(_) => (
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSerializer::default(),
                    }),
                    std::mem::size_of::<StringRawEntry>(),
                ),
                HashMethodKind::SingleBinary(_) => (
                    HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSingleBinary::default(),
                    }),
                    std::mem::size_of::<StringRawEntry>(),
                ),
                HashMethodKind::KeysU8(hash_method) => (
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<u8>>(),
                ),
                HashMethodKind::KeysU16(hash_method) => (
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<u16>>(),
                ),
                HashMethodKind::KeysU32(hash_method) => (
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<u32>>(),
                ),
                HashMethodKind::KeysU64(hash_method) => (
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<u64>>(),
                ),
                HashMethodKind::KeysU128(hash_method) => (
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<u128>>(),
                ),
                HashMethodKind::KeysU256(hash_method) => (
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    }),
                    std::mem::size_of::<RawEntry<U256>>(),
                ),
            };
            self.entry_size.store(entry_size, Ordering::Release);
            let hash_table = unsafe { &mut *self.hash_join_state.hash_table.get() };
            *hash_table = hash_join_hash_table;
            self.merge_into_try_generate_matched_memory();
        }
        Ok(())
    }

    /// Divide the finalize phase into multiple tasks.
    pub fn generate_finalize_task(&self) -> Result<()> {
        let task_num = unsafe { &*self.hash_join_state.build_state.get() }
            .generation_state
            .chunks
            .len();
        if task_num == 0 {
            return Ok(());
        }
        let tasks = (0..task_num).collect_vec();
        *self.build_hash_table_tasks.write() = tasks.into();
        Ok(())
    }

    /// Get the finalize task and using the `chunks` in `hash_join_state.row_space` to build hash table in parallel.
    pub(crate) fn finalize(&self, task: usize) -> Result<()> {
        let entry_size = self.entry_size.load(Ordering::Acquire);
        let mut local_raw_entry_spaces: Vec<Vec<u8>> = Vec::new();
        let hashtable = unsafe { &mut *self.hash_join_state.hash_table.get() };
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };

        macro_rules! insert_key {
            ($table: expr, $method: expr, $chunk: expr, $build_keys: expr, $valids: expr, $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, $t: ty,) => {{
                let keys_state = $method.build_keys_state($build_keys, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let valid_num = match &$valids {
                    Some(valids) => valids.len() - valids.null_count(),
                    None => $chunk.num_rows(),
                };
                let mut local_space: Vec<u8> = Vec::with_capacity(valid_num * entry_size);
                let mut raw_entry_ptr = unsafe {
                    std::mem::transmute::<*mut u8, *mut RawEntry<$t>>(local_space.as_mut_ptr())
                };

                match $valids {
                    Some(valids) => {
                        for (row_index, (key, valid)) in
                            build_keys_iter.zip(valids.iter()).enumerate()
                        {
                            if !valid {
                                continue;
                            }
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
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
                            $table.insert(*key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                    None => {
                        for (row_index, key) in build_keys_iter.enumerate() {
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
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
                            $table.insert(*key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                }

                local_raw_entry_spaces.push(local_space);
            }};
        }

        macro_rules! insert_binary_key {
            ($table: expr, $method: expr, $chunk: expr, $build_keys: expr, $valids: expr, $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, ) => {{
                let keys_state = $method.build_keys_state($build_keys, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let space_size = match &keys_state {
                    // safe to unwrap(): offset.len() >= 1.
                    KeysState::Column(Column::String(col)) => col.total_bytes_len(),
                    KeysState::Column(
                        Column::Binary(col) | Column::Variant(col) | Column::Bitmap(col),
                    ) => col.data().len(),
                    _ => unreachable!(),
                };
                let valid_num = match &$valids {
                    Some(valids) => valids.len() - valids.null_count(),
                    None => $chunk.num_rows(),
                };
                let mut entry_local_space: Vec<u8> = Vec::with_capacity(valid_num * entry_size);
                let mut string_local_space: Vec<u8> = Vec::with_capacity(space_size as usize);
                let mut raw_entry_ptr = unsafe {
                    std::mem::transmute::<*mut u8, *mut StringRawEntry>(
                        entry_local_space.as_mut_ptr(),
                    )
                };
                let mut string_local_space_ptr = string_local_space.as_mut_ptr();

                match $valids {
                    Some(valids) => {
                        for (row_index, (key, valid)) in
                            build_keys_iter.zip(valids.iter()).enumerate()
                        {
                            if !valid {
                                continue;
                            }
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
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
                                std::ptr::copy_nonoverlapping(
                                    key.as_ptr(),
                                    string_local_space_ptr,
                                    key.len(),
                                );
                                string_local_space_ptr = string_local_space_ptr.add(key.len());
                            }

                            $table.insert(key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                    None => {
                        for (row_index, key) in build_keys_iter.enumerate() {
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
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
                                std::ptr::copy_nonoverlapping(
                                    key.as_ptr(),
                                    string_local_space_ptr,
                                    key.len(),
                                );
                                string_local_space_ptr = string_local_space_ptr.add(key.len());
                            }

                            $table.insert(key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                }

                local_raw_entry_spaces.push(entry_local_space);
                local_raw_entry_spaces.push(string_local_space);
            }};
        }

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let chunk_index = task;
        let chunk = &mut build_state.generation_state.chunks[chunk_index];

        let mut _has_null = false;
        let mut _nullable_chunk = None;
        let evaluator = if matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) {
            let validity = Bitmap::new_constant(true, chunk.num_rows());
            let nullable_columns = chunk
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, chunk.num_rows(), &validity))
                .collect::<Vec<_>>();
            _nullable_chunk = Some(DataBlock::new(nullable_columns, chunk.num_rows()));
            Evaluator::new(
                _nullable_chunk.as_ref().unwrap(),
                &self.func_ctx,
                &BUILTIN_FUNCTIONS,
            )
        } else {
            Evaluator::new(chunk, &self.func_ctx, &BUILTIN_FUNCTIONS)
        };
        let build_keys = &self.hash_join_state.hash_join_desc.build_keys;
        let mut keys_columns = build_keys
            .iter()
            .map(|expr| {
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(expr.data_type(), chunk.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        let column_nums = chunk.num_columns();
        let mut block_entries = Vec::with_capacity(self.build_projections.len());
        for index in 0..column_nums {
            if !self.build_projections.contains(&index) {
                continue;
            }
            block_entries.push(chunk.get_by_offset(index).clone());
        }
        if block_entries.is_empty() {
            build_state.generation_state.is_build_projected = false;
        }
        *chunk = DataBlock::new(block_entries, chunk.num_rows());

        let is_null_equal = &self.hash_join_state.hash_join_desc.is_null_equal;
        let may_null = build_keys.iter().any(|expr| {
            let ty = expr.data_type();
            ty.is_nullable() || ty.is_null()
        });
        let valids = if !may_null {
            None
        } else {
            let valids = keys_columns
                .iter()
                .zip(is_null_equal.iter().copied())
                .filter(|(_, is_null_equal)| !is_null_equal)
                .map(|(col, _)| col.validity())
                .try_fold(None, |valids, (is_all_null, tmp_valids)| {
                    if is_all_null {
                        ControlFlow::Break(Some(Bitmap::new_constant(false, chunk.num_rows())))
                    } else {
                        ControlFlow::Continue(and_validities(valids, tmp_valids.cloned()))
                    }
                });
            match valids {
                ControlFlow::Continue(Some(valids)) | ControlFlow::Break(Some(valids)) => {
                    if valids.null_count() == valids.len() {
                        return Ok(());
                    }
                    if valids.null_count() != 0 {
                        Some(valids)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };

        match self.hash_join_state.hash_join_desc.join_type {
            JoinType::LeftMark => {
                let markers = &mut build_state.mark_scan_map[chunk_index];
                self.hash_join_state.init_markers(
                    (&keys_columns).into(),
                    chunk.num_rows(),
                    markers,
                );
            }
            JoinType::RightMark => {
                if !_has_null && !keys_columns.is_empty() {
                    if let Some(validity) = keys_columns[0].validity().1 {
                        if validity.null_count() > 0 {
                            _has_null = true;
                            let mut has_null_ref = self
                                .hash_join_state
                                .hash_join_desc
                                .marker_join_desc
                                .has_null
                                .write();
                            *has_null_ref = true;
                        }
                    }
                }
            }
            _ => {}
        };

        keys_columns
            .iter_mut()
            .zip(is_null_equal.iter().copied())
            .filter(|(col, is_null_equal)| !is_null_equal && col.as_nullable().is_some())
            .for_each(|(col, _)| *col = col.remove_nullable());
        let build_keys = (&keys_columns).into();

        match hashtable {
            HashJoinHashTable::Serializer(table) => insert_binary_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
            },
            HashJoinHashTable::SingleBinary(table) => insert_binary_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
            },
            HashJoinHashTable::KeysU8(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u8,
            },
            HashJoinHashTable::KeysU16(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u16,
            },
            HashJoinHashTable::KeysU32(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u32,
            },
            HashJoinHashTable::KeysU64(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u64,
            },
            HashJoinHashTable::KeysU128(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u128,
            },
            HashJoinHashTable::KeysU256(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, U256,
            },
            HashJoinHashTable::Null => {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the hash table is uninitialized.",
                ));
            }
        }

        {
            let mut raw_entry_spaces = self.raw_entry_spaces.lock();
            raw_entry_spaces.extend(local_raw_entry_spaces);
        }
        Ok(())
    }

    /// Get one build hash table task.
    pub fn finalize_task(&self) -> Option<usize> {
        let mut tasks = self.build_hash_table_tasks.write();
        tasks.pop_front()
    }

    // Build `BuildBlockGenerationState`.
    fn build_generation_state(&self) {
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let build_num_rows = build_state.generation_state.build_num_rows;
        info!("finish build hash table with {} rows", build_num_rows);

        let data_blocks = &mut build_state.generation_state.chunks;
        if !data_blocks.is_empty()
            && self.hash_join_state.hash_join_desc.join_type != JoinType::Cross
        {
            let num_columns = data_blocks[0].num_columns();
            let columns_data_type: Vec<DataType> = (0..num_columns)
                .map(|index| data_blocks[0].get_by_offset(index).data_type.clone())
                .collect();
            let columns: Vec<ColumnVec> = (0..num_columns)
                .map(|index| {
                    let columns = data_blocks
                        .iter()
                        .map(|block| (block.get_by_offset(index), block.num_rows()))
                        .collect_vec();
                    let full_columns: Vec<Column> = columns
                        .iter()
                        .map(|(entry, rows)| match &entry.value {
                            Value::Scalar(s) => {
                                let builder =
                                    ColumnBuilder::repeat(&s.as_ref(), *rows, &entry.data_type);
                                builder.build()
                            }
                            Value::Column(c) => c.clone(),
                        })
                        .collect();
                    Column::take_downcast_column_vec(&full_columns, columns[0].0.data_type.clone())
                })
                .collect();
            build_state.generation_state.build_columns_data_type = columns_data_type;
            build_state.generation_state.build_columns = columns;
        }
    }

    /// Detach to state: `finalize_counter`.
    pub(crate) fn finalize_done(&self, hash_table_type: HashTableType) -> Result<()> {
        if self.finalize_counter.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.build_generation_state();
            if self.hash_join_state.need_next_round.load(Ordering::Acquire) {
                let partition_id = if self.join_type() != JoinType::Cross {
                    // If build side has spilled data, we need to wait build side to next round.
                    // Set partition id to `HashJoinState`
                    let mut spill_partitions = self.hash_join_state.spilled_partitions.write();
                    let partition_id = spill_partitions.iter().next().cloned().unwrap();
                    spill_partitions.remove(&partition_id);
                    partition_id
                } else {
                    0
                };
                // The next partition to read.
                self.hash_join_state
                    .partition_id
                    .store(partition_id, Ordering::Release);
            }
            self.hash_join_state
                .build_watcher
                .send(hash_table_type)
                .map_err(|_| ErrorCode::TokioError("build_done_watcher channel is closed"))?;
        }
        Ok(())
    }

    pub fn add_runtime_filter_ready(&self) {
        if self.ctx.get_cluster().is_empty() {
            return;
        }

        let mut wait_runtime_filter_table_indexes = HashSet::new();
        for rf in self.runtime_filter_desc() {
            wait_runtime_filter_table_indexes.insert(rf.scan_id);
        }

        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let runtime_filter_ready = &mut build_state.runtime_filter_ready;
        for table_index in wait_runtime_filter_table_indexes.into_iter() {
            let ready = Arc::new(RuntimeFilterReady::default());
            runtime_filter_ready.push(ready.clone());
            self.ctx.set_runtime_filter_ready(table_index, ready);
        }
    }

    pub fn set_bloom_filter_ready(&self, ready: bool) -> Result<()> {
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        for runtime_filter_ready in build_state.runtime_filter_ready.iter() {
            runtime_filter_ready
                .runtime_filter_watcher
                .send(Some(ready))
                .map_err(|_| ErrorCode::TokioError("watcher channel is closed"))?;
        }
        Ok(())
    }

    fn add_runtime_filter(&self, build_chunks: &[DataBlock], build_num_rows: usize) -> Result<()> {
        let mut bloom_filter_ready = false;
        for rf in self.runtime_filter_desc() {
            let mut runtime_filter = RuntimeFilterInfo::default();
            if rf.enable_inlist_runtime_filter && build_num_rows < INLIST_RUNTIME_FILTER_THRESHOLD {
                self.inlist_runtime_filter(
                    &mut runtime_filter,
                    build_chunks,
                    &rf.build_key,
                    &rf.probe_key,
                )?;
            }
            if rf.enable_bloom_runtime_filter {
                self.bloom_runtime_filter(
                    build_chunks,
                    &mut runtime_filter,
                    &rf.build_key,
                    &rf.probe_key,
                )?;
            }
            if rf.enable_min_max_runtime_filter {
                self.min_max_runtime_filter(
                    build_chunks,
                    &mut runtime_filter,
                    &rf.build_key,
                    &rf.probe_key,
                )?;
            }
            if !runtime_filter.is_empty() {
                bloom_filter_ready |= !runtime_filter.is_blooms_empty();
                self.ctx.set_runtime_filter((rf.scan_id, runtime_filter));
            }
        }
        self.set_bloom_filter_ready(bloom_filter_ready)?;
        Ok(())
    }

    fn bloom_runtime_filter(
        &self,
        data_blocks: &[DataBlock],
        runtime_filter: &mut RuntimeFilterInfo,
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if !build_key.data_type().remove_nullable().is_number()
            && !build_key.data_type().remove_nullable().is_string()
        {
            return Ok(());
        }
        let Expr::ColumnRef(ColumnRef { id, .. }) = probe_key else {
            return Ok(());
        };

        let mut columns = Vec::with_capacity(data_blocks.len());
        for block in data_blocks.iter() {
            if block.num_columns() == 0 {
                continue;
            }
            let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(build_key)?
                .convert_to_full_column(build_key.data_type(), block.num_rows());
            columns.push(column);
        }
        if columns.is_empty() {
            return Ok(());
        }
        let build_key_column = Column::concat_columns(columns.into_iter())?;
        // Generate bloom filter using build column
        let data_type = build_key.data_type();
        let num_rows = build_key_column.len();
        let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()])?;
        let mut hashes = HashSet::with_capacity(num_rows);
        let key_columns = &[build_key_column];
        hash_by_method(&method, key_columns.into(), num_rows, &mut hashes)?;
        let mut hashes_vec = Vec::with_capacity(num_rows);
        hashes.into_iter().for_each(|hash| {
            hashes_vec.push(hash);
        });
        let filter = BinaryFuse16::try_from(&hashes_vec)?;
        runtime_filter.add_bloom((id.to_string(), filter));
        Ok(())
    }

    fn inlist_runtime_filter(
        &self,
        runtime_filter: &mut RuntimeFilterInfo,
        data_blocks: &[DataBlock],
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if let Some(distinct_build_column) =
            dedup_build_key_column(&self.func_ctx, data_blocks, build_key)?
        {
            if let Some(filter) = inlist_filter(probe_key, distinct_build_column.clone())? {
                info!("inlist_filter: {:?}", filter.sql_display());
                runtime_filter.add_inlist(filter);
            }
        }
        Ok(())
    }

    fn min_max_runtime_filter(
        &self,
        data_blocks: &[DataBlock],
        runtime_filter: &mut RuntimeFilterInfo,
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if !build_key.runtime_filter_supported_types() {
            return Ok(());
        }
        if let Expr::ColumnRef(_) = probe_key {
            let mut columns = Vec::with_capacity(data_blocks.len());
            for block in data_blocks.iter() {
                if block.num_columns() == 0 {
                    continue;
                }
                let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(build_key)?
                    .convert_to_full_column(build_key.data_type(), block.num_rows());
                columns.push(column);
            }
            if columns.is_empty() {
                return Ok(());
            }
            let build_key_column = Column::concat_columns(columns.into_iter())?;
            if build_key_column.len() == 0 {
                return Ok(());
            }
            // Generate min max filter using build column
            let min_max = build_key_column.remove_nullable().domain();
            let min_max_filter = match min_max {
                Domain::Number(domain) => match domain {
                    NumberDomain::UInt8(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt16(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int8(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int16(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Float32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Float64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                },
                Domain::String(domain) => {
                    let min = Scalar::String(domain.min);
                    let max = Scalar::String(domain.max.unwrap());
                    min_max_filter(min, max, probe_key)?
                }
                Domain::Date(date_domain) => {
                    let min = Scalar::Date(date_domain.min);
                    let max = Scalar::Date(date_domain.max);
                    min_max_filter(min, max, probe_key)?
                }
                _ => unreachable!(),
            };
            if let Some(min_max_filter) = min_max_filter {
                info!("min_max_filter: {:?}", min_max_filter.sql_display());
                runtime_filter.add_min_max(min_max_filter);
            }
        }
        Ok(())
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.hash_join_state.hash_join_desc.join_type.clone()
    }

    fn runtime_filter_desc(&self) -> &[RuntimeFilterDesc] {
        &self.hash_join_state.hash_join_desc.runtime_filter.filters
    }

    /// only used for test
    pub fn get_enable_bloom_runtime_filter(&self) -> bool {
        self.hash_join_state
            .hash_join_desc
            .runtime_filter
            .filters
            .iter()
            .any(|rf| rf.enable_bloom_runtime_filter)
    }

    /// only used for test
    pub fn get_enable_min_max_runtime_filter(&self) -> bool {
        self.hash_join_state
            .hash_join_desc
            .runtime_filter
            .filters
            .iter()
            .any(|rf| rf.enable_min_max_runtime_filter)
    }
}
