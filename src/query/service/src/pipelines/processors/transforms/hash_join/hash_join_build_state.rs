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

use std::collections::VecDeque;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::ColumnVec;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_expression::HashMethodSerializer;
use common_expression::HashMethodSingleString;
use common_expression::KeysState;
use common_expression::RemoteExpr;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::HashJoinHashMap;
use common_hashtable::RawEntry;
use common_hashtable::RowPtr;
use common_hashtable::StringHashJoinHashMap;
use common_hashtable::StringRawEntry;
use common_hashtable::STRING_EARLY_SIZE;
use common_sql::plans::JoinType;
use common_sql::ColumnSet;
use ethnum::U256;
use itertools::Itertools;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::hash_join::common::set_validity;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::hash_join_state::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::hash_join_state::HashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::hash_join_state::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::hash_join_state::SingleStringHashJoinHashTable;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;

/// Define some shared states for all hash join build threads.
pub struct HashJoinBuildState {
    pub(crate) ctx: Arc<QueryContext>,
    /// `hash_join_state` is shared by `HashJoinBuild` and `HashJoinProbe`
    pub(crate) hash_join_state: Arc<HashJoinState>,
    /// Processors count
    pub(crate) _processor_count: usize,
    /// When build side input data is coming, we will put it into `RowSpace`'s chunks.
    /// To make the size of each chunk suitable, it's better to define a threshold to the size of each chunk.
    /// Before putting the input data into `Chunk`, we will add them to buffer of `RowSpace`
    /// After buffer's size hits the threshold, we will flush the buffer to `Chunk`.
    pub(crate) chunk_size_limit: Arc<usize>,
    /// Wait util all processors finish row space build, then go to next phase.
    pub(crate) barrier: RwLock<Barrier>,
    /// It will be increased by 1 when a new hash join build processor is created.
    /// After the processor put its input data into `RowSpace`, it will be decreased by 1.
    /// The processor will wait other processors to finish their work before starting to build hash table.
    /// When the counter is 0, it means all hash join build processors have input their data to `RowSpace`.
    pub(crate) row_space_builders: Mutex<usize>,
    /// Hash method for hash join keys.
    pub(crate) method: Arc<HashMethodKind>,
    /// The size of each entry in HashTable.
    pub(crate) entry_size: Arc<AtomicUsize>,
    pub(crate) raw_entry_spaces: Mutex<Vec<Vec<u8>>>,
    /// `build_projections` only contains the columns from upstream required columns
    /// and columns from other_condition which are in build schema.
    pub(crate) build_projections: Arc<ColumnSet>,
    /// FIXME: the field can be removed later
    pub(crate) build_worker_num: Arc<AtomicU32>,
    /// Tasks for building hash table.
    pub(crate) build_hash_table_tasks: Arc<RwLock<VecDeque<(usize, usize)>>>,
}

impl HashJoinBuildState {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        build_keys: &[RemoteExpr],
        build_projections: &ColumnSet,
        hash_join_state: Arc<HashJoinState>,
        processor_count: usize,
        barrier: Barrier,
    ) -> Result<Arc<HashJoinBuildState>> {
        let hash_key_types = build_keys
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone())
            .collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types, false)?;
        Ok(Arc::new(Self {
            ctx: ctx.clone(),
            hash_join_state,
            _processor_count: processor_count,
            chunk_size_limit: Arc::new(ctx.get_settings().get_max_block_size()? as usize * 16),
            barrier: RwLock::new(barrier),
            row_space_builders: Default::default(),
            method: Arc::new(method),
            entry_size: Arc::new(Default::default()),
            raw_entry_spaces: Default::default(),
            build_projections: Arc::new(build_projections.clone()),
            build_worker_num: Arc::new(Default::default()),
            build_hash_table_tasks: Arc::new(Default::default()),
        }))
    }

    /// Add input `DataBlock` to `hash_join_state.row_space`.
    pub fn build(&self, input: DataBlock) -> Result<()> {
        let mut buffer = self.hash_join_state.row_space.buffer.write();
        let mut buffer_row_size = self.hash_join_state.row_space.buffer_row_size.write();
        *buffer_row_size += input.num_rows();
        buffer.push(input);
        if *buffer_row_size < *self.chunk_size_limit {
            Ok(())
        } else {
            let data_block = DataBlock::concat(buffer.as_slice())?;
            buffer.clear();
            *buffer_row_size = 0;
            drop(buffer);
            drop(buffer_row_size);
            self.add_build_block(data_block)
        }
    }

    // Add `data_block` for build table to `row_space`
    pub(crate) fn add_build_block(&self, data_block: DataBlock) -> Result<()> {
        let mut data_block = data_block;
        if matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) {
            let mut validity = MutableBitmap::new();
            validity.extend_constant(data_block.num_rows(), true);
            let validity: Bitmap = validity.into();

            let nullable_columns = data_block
                .columns()
                .iter()
                .map(|c| set_validity(c, validity.len(), &validity))
                .collect::<Vec<_>>();
            data_block = DataBlock::new(nullable_columns, data_block.num_rows());
        }

        let block_outer_scan_map = if self.hash_join_state.need_outer_scan() {
            vec![false; data_block.num_rows()]
        } else {
            vec![]
        };

        let block_mark_scan_map = if self.hash_join_state.need_outer_scan() {
            vec![MARKER_KIND_FALSE; data_block.num_rows()]
        } else {
            vec![]
        };

        {
            // Acquire write lock in current scope
            let _write_lock = self.hash_join_state.row_space.write_lock.write();
            if self.hash_join_state.need_outer_scan() {
                let outer_scan_map = unsafe { &mut *self.hash_join_state.outer_scan_map.get() };
                outer_scan_map.push(block_outer_scan_map);
            }
            if self.hash_join_state.need_mark_scan() {
                let mark_scan_map = unsafe { &mut *self.hash_join_state.mark_scan_map.get() };
                mark_scan_map.push(block_mark_scan_map);
            }
            let build_num_rows = unsafe { &mut *self.hash_join_state.build_num_rows.get() };
            *build_num_rows += data_block.num_rows();
            let chunks = unsafe { &mut *self.hash_join_state.chunks.get() };
            chunks.push(data_block);
        }
        Ok(())
    }

    /// Attach to state: `row_space_builders` and `hash_table_builders`.
    pub fn build_attach(&self) {
        let mut count = self.row_space_builders.lock();
        *count += 1;
        let mut count = self.hash_join_state.hash_table_builders.lock();
        *count += 1;
        self.build_worker_num.fetch_add(1, Ordering::Relaxed);
    }

    /// Detach to state: `row_space_builders`,
    /// create finalize task and initialize the hash table.
    pub(crate) fn row_space_build_done(&self) -> Result<()> {
        let mut count = self.row_space_builders.lock();
        *count -= 1;
        if *count == 0 {
            // Need to reset `final_probe_done` before processor into `WaitProbe`
            {
                let mut final_probe_done = self.hash_join_state.final_probe_done.lock();
                if *final_probe_done {
                    *final_probe_done = false;
                }
            }
            {
                let mut buffer = self.hash_join_state.row_space.buffer.write();
                if !buffer.is_empty() {
                    let data_block = DataBlock::concat(&buffer)?;
                    self.add_build_block(data_block)?;
                    buffer.clear();
                }
            }

            // Get the number of rows of the build side.
            let build_num_rows = unsafe { *self.hash_join_state.build_num_rows.get() };

            if self.hash_join_state.hash_join_desc.join_type == JoinType::Cross {
                return Ok(());
            }

            // Divide the finalize phase into multiple tasks.
            self.generate_finalize_task()?;

            // Fast path for hash join
            if build_num_rows == 0
                && !matches!(
                    self.hash_join_state.hash_join_desc.join_type,
                    JoinType::LeftMark | JoinType::RightMark
                )
                && self.ctx.get_cluster().is_empty()
                && !self.ctx.get_settings().get_enable_join_spill()?
            {
                {
                    let mut fast_return = self.hash_join_state.fast_return.write();
                    *fast_return = true;
                }
                let mut build_done = self.hash_join_state.build_done.lock();
                *build_done = true;
                self.hash_join_state.build_done_notify.notify_waiters();
                return Ok(());
            }

            // Create a fixed size hash table.
            let hashjoin_hashtable = match (*self.method).clone() {
                HashMethodKind::Serializer(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                        hash_table: StringHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSerializer::default(),
                    })
                }
                HashMethodKind::SingleString(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::SingleString(SingleStringHashJoinHashTable {
                        hash_table: StringHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSingleString::default(),
                    })
                }
                HashMethodKind::KeysU8(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u8>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU16(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u16>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU32(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u32>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU64(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u64>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU128(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u128>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU256(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<U256>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::DictionarySerializer(_) => unimplemented!(),
            };
            let hashtable = unsafe { &mut *self.hash_join_state.hash_table.get() };
            *hashtable = hashjoin_hashtable;
        }
        Ok(())
    }

    /// Divide the finalize phase into multiple tasks.
    pub fn generate_finalize_task(&self) -> Result<()> {
        let chunks_len = unsafe { &*self.hash_join_state.chunks.get() }.len();
        if chunks_len == 0 {
            return Ok(());
        }

        let worker_num = self.build_worker_num.load(Ordering::Relaxed) as usize;
        let (task_size, task_num) = if chunks_len >= worker_num {
            (chunks_len / worker_num, worker_num)
        } else {
            (1, chunks_len)
        };

        let mut build_hash_table_tasks = self.build_hash_table_tasks.write();
        for idx in 0..task_num - 1 {
            let task = (idx * task_size, (idx + 1) * task_size);
            build_hash_table_tasks.push_back(task);
        }
        let last_task = ((task_num - 1) * task_size, chunks_len);
        build_hash_table_tasks.push_back(last_task);

        Ok(())
    }

    /// Get the finalize task and using the `chunks` in `hash_join_state.row_space` to build hash table in parallel.
    pub(crate) fn finalize(&self, task: (usize, usize)) -> Result<()> {
        let entry_size = self.entry_size.load(Ordering::Relaxed);
        let mut local_raw_entry_spaces: Vec<Vec<u8>> = Vec::new();
        let hashtable = unsafe { &mut *self.hash_join_state.hash_table.get() };
        let mark_scan_map = unsafe { &mut *self.hash_join_state.mark_scan_map.get() };

        macro_rules! insert_key {
            ($table: expr, $method: expr, $chunk: expr, $columns: expr,  $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, $t: ty,) => {{
                let keys_state = $method.build_keys_state(&$columns, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let mut local_space: Vec<u8> = Vec::with_capacity($chunk.num_rows() * entry_size);
                let local_space_ptr = local_space.as_mut_ptr();

                local_raw_entry_spaces.push(local_space);

                let mut offset = 0;
                for (row_index, key) in build_keys_iter.enumerate().take($chunk.num_rows()) {
                    // # Safety
                    // offset + entry_size <= $chunk.num_rows() * entry_size.
                    let raw_entry_ptr = unsafe {
                        std::mem::transmute::<*mut u8, *mut RawEntry<$t>>(
                            local_space_ptr.add(offset),
                        )
                    };
                    offset += entry_size;

                    let row_ptr = RowPtr {
                        chunk_index: $chunk_index,
                        row_index: row_index as u32,
                    };

                    // # Safety
                    // The memory address of `raw_entry_ptr` is valid.
                    unsafe {
                        (*raw_entry_ptr).row_ptr = row_ptr;
                        (*raw_entry_ptr).key = *key;
                        (*raw_entry_ptr).next = 0;
                    }

                    $table.insert(*key, raw_entry_ptr);
                }
            }};
        }

        macro_rules! insert_string_key {
            ($table: expr, $method: expr, $chunk: expr, $columns: expr,  $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, ) => {{
                let keys_state = $method.build_keys_state(&$columns, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let space_size = match &keys_state {
                    KeysState::Column(Column::String(col)) => col.offsets().last(),
                    // The function `build_keys_state` of both HashMethodSerializer and HashMethodSingleString
                    // must return `KeysState::Column(Column::String)`.
                    _ => unreachable!(),
                };
                let mut entry_local_space: Vec<u8> =
                    Vec::with_capacity($chunk.num_rows() * entry_size);
                // safe to unwrap(): offset.len() >= 1.
                let mut string_local_space: Vec<u8> =
                    Vec::with_capacity(*space_size.unwrap() as usize);
                let entry_local_space_ptr = entry_local_space.as_mut_ptr();
                let string_local_space_ptr = string_local_space.as_mut_ptr();

                local_raw_entry_spaces.push(entry_local_space);
                local_raw_entry_spaces.push(string_local_space);

                let mut entry_offset = 0;
                let mut string_offset = 0;
                for (row_index, key) in build_keys_iter.enumerate().take($chunk.num_rows()) {
                    // # Safety
                    // entry_offset + entry_size <= $chunk.num_rows() * entry_size.
                    let raw_entry_ptr = unsafe {
                        std::mem::transmute::<*mut u8, *mut StringRawEntry>(
                            entry_local_space_ptr.add(entry_offset),
                        )
                    };
                    entry_offset += entry_size;

                    let row_ptr = RowPtr {
                        chunk_index: $chunk_index,
                        row_index: row_index as u32,
                    };

                    // # Safety
                    // The memory address of `raw_entry_ptr` is valid.
                    // string_offset + key.len() <= space_size.
                    unsafe {
                        let dst = string_local_space_ptr.add(string_offset);
                        (*raw_entry_ptr).row_ptr = row_ptr;
                        (*raw_entry_ptr).length = key.len() as u32;
                        (*raw_entry_ptr).next = 0;
                        (*raw_entry_ptr).key = dst;
                        // The size of `early` is 4.
                        std::ptr::copy_nonoverlapping(
                            key.as_ptr(),
                            (*raw_entry_ptr).early.as_mut_ptr(),
                            std::cmp::min(STRING_EARLY_SIZE, key.len()),
                        );
                        std::ptr::copy_nonoverlapping(key.as_ptr(), dst, key.len());
                    }
                    string_offset += key.len();

                    $table.insert(key, raw_entry_ptr);
                }
            }};
        }

        let func_ctx = self.ctx.get_function_context()?;
        let chunks = unsafe { &mut *self.hash_join_state.chunks.get() };
        let mut has_null = false;
        let interrupt = self.hash_join_state.interrupt.clone();
        for chunk_index in task.0..task.1 {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let chunk = &mut chunks[chunk_index];

            let evaluator = Evaluator::new(chunk, &func_ctx, &BUILTIN_FUNCTIONS);
            let columns: Vec<(Column, DataType)> = self
                .hash_join_state
                .hash_join_desc
                .build_keys
                .iter()
                .map(|expr| {
                    let return_type = expr.data_type();
                    Ok((
                        evaluator
                            .run(expr)?
                            .convert_to_full_column(return_type, chunk.num_rows()),
                        return_type.clone(),
                    ))
                })
                .collect::<Result<_>>()?;

            let column_nums = chunk.num_columns();
            let mut block_entries = Vec::with_capacity(self.build_projections.len());
            for index in 0..column_nums {
                if !self.build_projections.contains(&index) {
                    continue;
                }
                block_entries.push(chunk.get_by_offset(index).clone());
            }
            if block_entries.is_empty() {
                self.hash_join_state
                    .is_build_projected
                    .store(false, Ordering::SeqCst);
            }
            *chunk = DataBlock::new(block_entries, chunk.num_rows());

            match self.hash_join_state.hash_join_desc.join_type {
                JoinType::LeftMark => {
                    let markers = &mut mark_scan_map[chunk_index];
                    self.hash_join_state
                        .init_markers(&columns, chunk.num_rows(), markers);
                }
                JoinType::RightMark => {
                    if !has_null && !columns.is_empty() {
                        if let Some(validity) = columns[0].0.validity().1 {
                            if validity.unset_bits() > 0 {
                                has_null = true;
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

            match hashtable {
                HashJoinHashTable::Serializer(table) => insert_string_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
                },
                HashJoinHashTable::SingleString(table) => insert_string_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
                },
                HashJoinHashTable::KeysU8(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u8,
                },
                HashJoinHashTable::KeysU16(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u16,
                },
                HashJoinHashTable::KeysU32(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u32,
                },
                HashJoinHashTable::KeysU64(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u64,
                },
                HashJoinHashTable::KeysU128(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u128,
                },
                HashJoinHashTable::KeysU256(table) => insert_key! {
                  &mut table.hash_table, &table.hash_method, chunk, columns, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, U256,
                },
                HashJoinHashTable::Null => {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the hash table is uninitialized.",
                    ));
                }
            }
        }

        {
            let mut raw_entry_spaces = self.raw_entry_spaces.lock();
            raw_entry_spaces.extend(local_raw_entry_spaces);
        }
        Ok(())
    }

    /// Get one build hash table task.
    pub fn finalize_task(&self) -> Option<(usize, usize)> {
        let mut tasks = self.build_hash_table_tasks.write();
        tasks.pop_front()
    }

    /// Detach to state: `hash_table_builders`.
    pub(crate) fn build_done(&self) -> Result<()> {
        let mut count = self.hash_join_state.hash_table_builders.lock();
        *count -= 1;
        if *count == 0 {
            info!("finish build hash table with {} rows", unsafe {
                *self.hash_join_state.build_num_rows.get()
            });
            let data_blocks = unsafe { &mut *self.hash_join_state.chunks.get() };
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
                        Column::take_downcast_column_vec(
                            &full_columns,
                            columns[0].0.data_type.clone(),
                        )
                    })
                    .collect();
                let build_columns_data_type =
                    unsafe { &mut *self.hash_join_state.build_columns_data_type.get() };
                let build_columns = unsafe { &mut *self.hash_join_state.build_columns.get() };
                *build_columns_data_type = columns_data_type;
                *build_columns = columns;
            }
            let mut build_done = self.hash_join_state.build_done.lock();
            *build_done = true;
            self.hash_join_state.build_done_notify.notify_waiters();
        }
        Ok(())
    }
}
