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

use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_expression::HashMethodSerializer;
use common_expression::HashMethodSingleString;
use common_expression::KeysState;
use common_hashtable::HashJoinHashMap;
use common_hashtable::MarkerKind;
use common_hashtable::RawEntry;
use common_hashtable::RowPtr;
use common_hashtable::StringHashJoinHashMap;
use common_hashtable::StringRawEntry;
use common_hashtable::STRING_EARLY_SIZE;
use ethnum::U256;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::JoinState;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::HashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::SingleStringHashJoinHashTable;
use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::processors::JoinHashTable;
use crate::sessions::TableContext;
use crate::sql::planner::plans::JoinType;

#[async_trait::async_trait]
impl HashJoinState for JoinHashTable {
    fn build(&self, input: DataBlock) -> Result<()> {
        let data_block_size_limit = self.ctx.get_settings().get_max_block_size()? * 16;
        let mut buffer = self.row_space.buffer.write().unwrap();
        buffer.push(input);
        let buffer_row_size = buffer.iter().fold(0, |acc, x| acc + x.num_rows());
        if buffer_row_size < data_block_size_limit as usize {
            Ok(())
        } else {
            let data_block = DataBlock::concat(buffer.as_slice())?;
            buffer.clear();
            drop(buffer);
            self.add_build_block(data_block)
        }
    }

    fn probe(&self, input: &DataBlock, probe_state: &mut ProbeState) -> Result<Vec<DataBlock>> {
        match self.hash_join_desc.join_type {
            JoinType::Inner
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::Left
            | JoinType::LeftMark
            | JoinType::RightMark
            | JoinType::Single
            | JoinType::Right
            | JoinType::Full => self.probe_join(input, probe_state),
            JoinType::Cross => self.probe_cross_join(input, probe_state),
        }
    }

    fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
    }

    fn join_state(&self) -> &JoinState {
        &self.hash_join_desc.join_state
    }

    fn attach(&self) -> Result<()> {
        let mut count = self.build_count.lock().unwrap();
        *count += 1;
        let mut count = self.finalize_count.lock().unwrap();
        *count += 1;
        self.worker_num.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn build_end(&self) -> Result<()> {
        let mut count = self.build_count.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            // Divide the finalize phase into multiple tasks.
            self.divide_finalize_task()?;

            // Get the number of rows of the build side.
            let chunks = self.row_space.chunks.read().unwrap();
            let mut row_num = 0;
            for chunk in chunks.iter() {
                row_num += chunk.num_rows();
            }

            // Create a fixed size hash table.
            let hashjoin_hashtable = match (*self.method).clone() {
                HashMethodKind::Serializer(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                        hash_table: StringHashJoinHashMap::with_build_row_num(row_num),
                        hash_method: HashMethodSerializer::default(),
                    })
                }
                HashMethodKind::SingleString(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::SingleString(SingleStringHashJoinHashTable {
                        hash_table: StringHashJoinHashMap::with_build_row_num(row_num),
                        hash_method: HashMethodSingleString::default(),
                    })
                }
                HashMethodKind::KeysU8(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u8>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u8>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU16(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u16>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u16>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU32(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u32>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u32>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU64(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u64>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u64>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU128(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u128>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u128>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU256(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<U256>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<U256>::with_build_row_num(row_num),
                        hash_method,
                    })
                }
                HashMethodKind::DictionarySerializer(_) => unimplemented!(),
            };
            let hashtable = unsafe { &mut *self.hash_table.get() };
            *hashtable = hashjoin_hashtable;

            let mut is_built = self.is_built.lock().unwrap();
            *is_built = true;
            self.built_notify.notify_waiters();
            Ok(())
        } else {
            Ok(())
        }
    }

    fn divide_finalize_task(&self) -> Result<()> {
        {
            let buffer = self.row_space.buffer.write().unwrap();
            if !buffer.is_empty() {
                let data_block = DataBlock::concat(&buffer)?;
                self.add_build_block(data_block)?;
            }
        }

        let chunks = self.row_space.chunks.read().unwrap();
        let chunks_len = chunks.len();
        if chunks_len == 0 {
            self.unfinished_task_num.store(0, Ordering::Relaxed);
            return Ok(());
        }

        let task_num = self.worker_num.load(Ordering::Relaxed) as usize;
        let (task_size, task_num) = if chunks_len >= task_num {
            (chunks_len / task_num, task_num)
        } else {
            (1, chunks_len)
        };

        let mut finalize_tasks = self.finalize_tasks.write();
        for idx in 0..task_num - 1 {
            let task = (idx * task_size, (idx + 1) * task_size);
            finalize_tasks.push(task);
        }
        let last_task = ((task_num - 1) * task_size, chunks_len);
        finalize_tasks.push(last_task);

        self.unfinished_task_num
            .store(task_num as i32, Ordering::Relaxed);

        Ok(())
    }

    fn finalize(&self) -> Result<bool> {
        // Get task
        let task_idx = self.unfinished_task_num.fetch_sub(1, Ordering::SeqCst) - 1;
        if task_idx < 0 {
            return Ok(false);
        }
        let task = {
            let finalize_tasks = self.finalize_tasks.read();
            finalize_tasks[task_idx as usize]
        };

        let entry_size = self.entry_size.load(Ordering::Relaxed);
        let mut local_raw_entry_spaces: Vec<Vec<u8>> = Vec::new();
        let hashtable = unsafe { &mut *self.hash_table.get() };

        macro_rules! insert_key {
            ($table: expr, $markers: expr, $method: expr, $chunk: expr, $columns: expr,  $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, $t: ty,) => {{
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
                        row_index,
                        marker: $markers[row_index],
                    };

                    if self.hash_join_desc.join_type == JoinType::LeftMark {
                        let mut self_row_ptrs = self.row_ptrs.write();
                        self_row_ptrs.push(row_ptr.clone());
                    }

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
            ($table: expr, $markers: expr, $method: expr, $chunk: expr, $columns: expr,  $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, ) => {{
                let keys_state = $method.build_keys_state(&$columns, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let space_size = match &keys_state {
                    KeysState::Column(Column::String(col)) => col.offsets.last(),
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
                        row_index,
                        marker: $markers[row_index],
                    };

                    if self.hash_join_desc.join_type == JoinType::LeftMark {
                        let mut self_row_ptrs = self.row_ptrs.write();
                        self_row_ptrs.push(row_ptr.clone());
                    }

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

        let interrupt = self.interrupt.clone();
        let chunks = self.row_space.chunks.read().unwrap();
        let mut has_null = false;
        for chunk_index in task.0..task.1 {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let chunk = &chunks[chunk_index];
            let columns = &chunk.cols;
            let markers = match self.hash_join_desc.join_type {
                JoinType::LeftMark => Self::init_markers(&chunk.cols, chunk.num_rows())
                    .iter()
                    .map(|x| Some(*x))
                    .collect(),
                JoinType::RightMark => {
                    if !has_null && !chunk.cols.is_empty() {
                        if let Some(validity) = chunk.cols[0].0.validity().1 {
                            if validity.unset_bits() > 0 {
                                has_null = true;
                                let mut has_null_ref =
                                    self.hash_join_desc.marker_join_desc.has_null.write();
                                *has_null_ref = true;
                            }
                        }
                    }
                    vec![None; chunk.num_rows()]
                }
                _ => {
                    vec![None; chunk.num_rows()]
                }
            };

            match hashtable {
                HashJoinHashTable::Serializer(table) => insert_string_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces,
                },
                HashJoinHashTable::SingleString(table) => insert_string_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces,
                },
                HashJoinHashTable::KeysU8(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk,columns, chunk_index, entry_size, &mut local_raw_entry_spaces, u8,
                },
                HashJoinHashTable::KeysU16(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk,columns, chunk_index, entry_size, &mut local_raw_entry_spaces, u16,
                },
                HashJoinHashTable::KeysU32(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces, u32,
                },
                HashJoinHashTable::KeysU64(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces, u64,
                },
                HashJoinHashTable::KeysU128(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces, u128,
                },
                HashJoinHashTable::KeysU256(table) => insert_key! {
                  &mut table.hash_table, &markers, &table.hash_method, chunk, columns, chunk_index, entry_size, &mut local_raw_entry_spaces, U256,
                },
                HashJoinHashTable::Null => {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the hash table is uninitialized.",
                    ));
                }
            }
        }

        {
            let mut raw_entry_spaces = self.raw_entry_spaces.lock().unwrap();
            raw_entry_spaces.extend(local_raw_entry_spaces);
        }
        Ok(true)
    }

    fn finalize_end(&self) -> Result<()> {
        let mut count = self.finalize_count.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            let mut is_finalized = self.is_finalized.lock().unwrap();
            *is_finalized = true;
            self.finalized_notify.notify_waiters();
            Ok(())
        } else {
            Ok(())
        }
    }

    #[async_backtrace::framed]
    async fn wait_build_finish(&self) -> Result<()> {
        let notified = {
            let built_guard = self.is_built.lock().unwrap();

            match *built_guard {
                true => None,
                false => Some(self.built_notify.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn wait_finalize_finish(&self) -> Result<()> {
        let notified = {
            let finalized_guard = self.is_finalized.lock().unwrap();

            match *finalized_guard {
                true => None,
                false => Some(self.finalized_notify.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }

        Ok(())
    }

    fn mark_join_blocks(&self) -> Result<Vec<DataBlock>> {
        let data_blocks = self.row_space.chunks.read().unwrap();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let row_ptrs = self.row_ptrs.read();
        let has_null = self.hash_join_desc.marker_join_desc.has_null.read();

        let markers = row_ptrs.iter().map(|r| r.marker.unwrap()).collect();
        let marker_block = self.create_marker_block(*has_null, markers)?;
        let build_block = self.row_space.gather(&row_ptrs, &data_blocks, &num_rows)?;
        Ok(vec![self.merge_eq_block(&marker_block, &build_block)?])
    }

    fn right_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        let mut row_state = self.row_state_for_right_join()?;
        let unmatched_build_indexes = self.find_unmatched_build_indexes(&row_state)?;

        // Don't need process non-equi conditions for full join in the method
        // Because non-equi conditions have been processed in left probe join
        if self.hash_join_desc.join_type == JoinType::Full {
            let null_block = self.null_blocks_for_right_join(&unmatched_build_indexes)?;
            return Ok(vec![DataBlock::concat(&[blocks, &[null_block]].concat())?]);
        }

        let rest_block = self.rest_block()?;
        let input_block = DataBlock::concat(&[blocks, &[rest_block]].concat())?;

        if unmatched_build_indexes.is_empty() && self.hash_join_desc.other_predicate.is_none() {
            if input_block.is_empty() {
                return Ok(vec![]);
            }
            return Ok(vec![input_block]);
        }

        if self.hash_join_desc.other_predicate.is_none() {
            let null_block = self.null_blocks_for_right_join(&unmatched_build_indexes)?;
            if input_block.is_empty() {
                return Ok(vec![null_block]);
            }
            return Ok(vec![DataBlock::concat(&[input_block, null_block])?]);
        }

        if input_block.is_empty() {
            let null_block = self.null_blocks_for_right_join(&unmatched_build_indexes)?;
            return Ok(vec![null_block]);
        }

        let (bm, all_true, all_false) = self.get_other_filters(
            &input_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        if all_true {
            let null_block = self.null_blocks_for_right_join(&unmatched_build_indexes)?;
            return Ok(vec![DataBlock::concat(&[input_block, null_block])?]);
        }

        let num_rows = input_block.num_rows();
        let validity = match (bm, all_false) {
            (Some(b), _) => b,
            (None, true) => Bitmap::new_zeroed(num_rows),
            // must be one of above
            _ => unreachable!(),
        };
        let probe_column_len = self.probe_schema.fields().len();
        let probe_columns = input_block.columns()[0..probe_column_len]
            .iter()
            .map(|c| Self::set_validity(c, num_rows, &validity))
            .collect::<Vec<_>>();
        let probe_block = DataBlock::new(probe_columns, num_rows);
        let build_block =
            DataBlock::new(input_block.columns()[probe_column_len..].to_vec(), num_rows);
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        // If build_indexes size will greater build table size, we need filter the redundant rows for build side.
        let mut bm = validity.into_mut().right().unwrap();
        self.filter_rows_for_right_join(&mut bm, &mut row_state);
        let filtered_block = DataBlock::filter_with_bitmap(merged_block, &bm.into())?;

        // Concat null blocks
        let null_block = self.null_blocks_for_right_join(&unmatched_build_indexes)?;
        Ok(vec![DataBlock::concat(&[filtered_block, null_block])?])
    }

    fn right_semi_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        let data_blocks = self.row_space.chunks.read().unwrap();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let mut row_state = self.row_state_for_right_join()?;
        let unmatched_build_indexes = self.find_unmatched_build_indexes(&row_state)?;
        let unmatched_build_block =
            self.row_space
                .gather(&unmatched_build_indexes, &data_blocks, &num_rows)?;
        // Fast path for right anti join with non-equi conditions
        if self.hash_join_desc.other_predicate.is_none()
            && self.hash_join_desc.join_type == JoinType::RightAnti
        {
            return Ok(vec![unmatched_build_block]);
        }

        let rest_block = self.rest_block()?;
        let input_block = DataBlock::concat(&[blocks, &[rest_block]].concat())?;

        if input_block.is_empty() {
            if JoinType::RightAnti == self.hash_join_desc.join_type {
                return Ok(vec![unmatched_build_block]);
            }
            return Ok(vec![]);
        }

        let probe_fields_len = self.probe_schema.fields().len();
        let build_columns = input_block.columns()[probe_fields_len..].to_vec();
        let build_block = DataBlock::new(build_columns, input_block.num_rows());

        // Fast path for right semi join with non-equi conditions
        if self.hash_join_desc.other_predicate.is_none()
            && self.hash_join_desc.join_type == JoinType::RightSemi
        {
            let mut bm = MutableBitmap::new();
            bm.extend_constant(build_block.num_rows(), true);
            let filtered_block =
                self.filter_rows_for_right_semi_join(&mut bm, build_block, &mut row_state)?;
            return Ok(vec![filtered_block]);
        }

        // Right anti/semi join with non-equi conditions
        let (bm, all_true, all_false) = self.get_other_filters(
            &input_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        // Fast path for all non-equi conditions are true
        if all_true {
            return if self.hash_join_desc.join_type == JoinType::RightSemi {
                let mut bm = MutableBitmap::new();
                bm.extend_constant(build_block.num_rows(), true);
                let filtered_block =
                    self.filter_rows_for_right_semi_join(&mut bm, build_block, &mut row_state)?;
                return Ok(vec![filtered_block]);
            } else {
                let unmatched_build_indexes = self.find_unmatched_build_indexes(&row_state)?;
                let unmatched_build_block =
                    self.row_space
                        .gather(&unmatched_build_indexes, &data_blocks, &num_rows)?;
                Ok(vec![unmatched_build_block])
            };
        }

        // Fast path for all non-equi conditions are false
        if all_false {
            return if self.hash_join_desc.join_type == JoinType::RightSemi {
                Ok(vec![DataBlock::empty_with_schema(
                    self.row_space.data_schema.clone(),
                )])
            } else {
                Ok(self.row_space.datablocks())
            };
        }

        let mut bm = bm.unwrap().into_mut().right().unwrap();

        // Right semi join with non-equi conditions
        if self.hash_join_desc.join_type == JoinType::RightSemi {
            let filtered_block =
                self.filter_rows_for_right_semi_join(&mut bm, build_block, &mut row_state)?;
            return Ok(vec![filtered_block]);
        }

        // Right anti join with non-equi conditions
        {
            let build_indexes = self.hash_join_desc.join_state.build_indexes.read();
            for (idx, row_ptr) in build_indexes.iter().enumerate() {
                if !bm.get(idx) {
                    row_state[row_ptr.chunk_index][row_ptr.row_index] -= 1;
                }
            }
        }
        let unmatched_build_indexes = self.find_unmatched_build_indexes(&row_state)?;
        let unmatched_build_block =
            self.row_space
                .gather(&unmatched_build_indexes, &data_blocks, &num_rows)?;
        Ok(vec![unmatched_build_block])
    }

    fn left_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        // Get rest blocks
        let mut input_blocks = blocks.to_vec();
        let rest_block = self.rest_block()?;
        if rest_block.is_empty() {
            return Ok(input_blocks);
        }
        input_blocks.push(rest_block);
        Ok(input_blocks)
    }
}

impl JoinHashTable {
    pub(crate) fn filter_rows_for_right_join(
        &self,
        bm: &mut MutableBitmap,
        row_state: &mut [Vec<usize>],
    ) {
        let build_indexes = self.hash_join_desc.join_state.build_indexes.read();
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row.chunk_index][row.row_index] == 1_usize {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state[row.chunk_index][row.row_index] -= 1;
            }
        }
    }

    pub(crate) fn filter_rows_for_right_semi_join(
        &self,
        bm: &mut MutableBitmap,
        input: DataBlock,
        row_state: &mut [Vec<usize>],
    ) -> Result<DataBlock> {
        let build_indexes = self.hash_join_desc.join_state.build_indexes.read();
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row.chunk_index][row.row_index] > 1_usize && !bm.get(index) {
                row_state[row.chunk_index][row.row_index] -= 1;
            }
        }
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row.chunk_index][row.row_index] > 1_usize && bm.get(index) {
                bm.set(index, false);
                row_state[row.chunk_index][row.row_index] -= 1;
            }
        }
        DataBlock::filter_with_bitmap(input, &bm.clone().into())
    }

    pub(crate) fn non_equi_conditions_for_left_join(
        &self,
        input_blocks: &[DataBlock],
        probe_indexes_vec: &[Vec<(u32, u32)>],
        row_state: &mut [u32],
    ) -> Result<Vec<DataBlock>> {
        let mut output_blocks = Vec::with_capacity(input_blocks.len());
        let mut begin = 0;
        let probe_side_len = self.probe_schema.fields().len();
        for (idx, input_block) in input_blocks.iter().enumerate() {
            if self.interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            // Process non-equi conditions
            let (bm, all_true, all_false) = self.get_other_filters(
                input_block,
                self.hash_join_desc.other_predicate.as_ref().unwrap(),
            )?;

            if all_true {
                output_blocks.push(input_block.clone());
                continue;
            }

            let num_rows = input_block.num_rows();
            let validity = match (bm, all_false) {
                (Some(b), _) => b,
                (None, true) => Bitmap::new_zeroed(num_rows),
                // must be one of above
                _ => unreachable!(),
            };

            // probed_block contains probe side and build side.
            let nullable_columns = input_block.columns()[probe_side_len..]
                .iter()
                .map(|c| Self::set_validity(c, num_rows, &validity))
                .collect::<Vec<_>>();

            let nullable_build_block = DataBlock::new(nullable_columns.clone(), num_rows);

            let probe_block =
                DataBlock::new(input_block.columns()[0..probe_side_len].to_vec(), num_rows);

            let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;
            let mut bm = validity.into_mut().right().unwrap();
            if self.hash_join_desc.join_type == JoinType::Full {
                let mut build_indexes = self.hash_join_desc.join_state.build_indexes.write();
                let build_indexes = &mut build_indexes[begin..(begin + bm.len())];
                begin += bm.len();
                for (idx, build_index) in build_indexes.iter_mut().enumerate() {
                    if !bm.get(idx) {
                        build_index.marker = Some(MarkerKind::False);
                    }
                }
            }
            self.fill_null_for_left_join(&mut bm, &probe_indexes_vec[idx], row_state);

            output_blocks.push(DataBlock::filter_with_bitmap(merged_block, &bm.into())?);
        }
        Ok(output_blocks)
    }
}
