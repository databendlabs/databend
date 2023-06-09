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
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_expression::HashMethodSerializer;
use common_expression::HashMethodSingleString;
use common_expression::KeysState;
use common_expression::Scalar;
use common_expression::Value;
use common_hashtable::HashJoinHashMap;
use common_hashtable::RawEntry;
use common_hashtable::RowPtr;
use common_hashtable::StringHashJoinHashMap;
use common_hashtable::StringRawEntry;
use common_hashtable::STRING_EARLY_SIZE;
use ethnum::U256;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::JoinState;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
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
        let mut buffer = self.row_space.buffer.write();
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
        let mut count = self.build_count.lock();
        *count += 1;
        let mut count = self.finalize_count.lock();
        *count += 1;
        let mut count = self.probe_count.lock();
        *count += 1;
        self.worker_num.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn build_done(&self) -> Result<()> {
        let mut count = self.build_count.lock();
        *count -= 1;
        if *count == 0 {
            // Divide the finalize phase into multiple tasks.
            self.generate_finalize_task()?;

            // Get the number of rows of the build side.
            let chunks = self.row_space.chunks.read();
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

            let mut build_done = self.build_done.lock();
            *build_done = true;
            self.build_done_notify.notify_waiters();
        }
        Ok(())
    }

    fn generate_finalize_task(&self) -> Result<()> {
        {
            let buffer = self.row_space.buffer.write();
            if !buffer.is_empty() {
                let data_block = DataBlock::concat(&buffer)?;
                self.add_build_block(data_block)?;
            }
        }

        let chunks = self.row_space.chunks.read();
        let chunks_len = chunks.len();
        if chunks_len == 0 {
            return Ok(());
        }

        let worker_num = self.worker_num.load(Ordering::Relaxed) as usize;
        let (task_size, task_num) = if chunks_len >= worker_num {
            (chunks_len / worker_num, worker_num)
        } else {
            (1, chunks_len)
        };

        let mut finalize_tasks = self.finalize_tasks.write();
        for idx in 0..task_num - 1 {
            let task = (idx * task_size, (idx + 1) * task_size);
            finalize_tasks.push_back(task);
        }
        let last_task = ((task_num - 1) * task_size, chunks_len);
        finalize_tasks.push_back(last_task);

        Ok(())
    }

    fn finalize(&self, task: (usize, usize)) -> Result<()> {
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
        let chunks = self.row_space.chunks.read();
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
            let mut raw_entry_spaces = self.raw_entry_spaces.lock();
            raw_entry_spaces.extend(local_raw_entry_spaces);
        }
        Ok(())
    }

    fn finalize_task(&self) -> Option<(usize, usize)> {
        let mut tasks = self.finalize_tasks.write();
        tasks.pop_front()
    }

    fn finalize_done(&self) -> Result<()> {
        let mut count = self.finalize_count.lock();
        *count -= 1;
        if *count == 0 {
            let mut finalize_done = self.finalize_done.lock();
            *finalize_done = true;
            self.finalize_done_notify.notify_waiters();
        }
        Ok(())
    }

    fn probe_done(&self) -> Result<()> {
        let mut count = self.probe_count.lock();
        *count -= 1;
        if *count == 0 {
            // Divide the outer scan phase into multiple tasks.
            self.generate_outer_scan_task()?;

            let mut probe_done = self.probe_done.lock();
            *probe_done = true;
            self.probe_done_notify.notify_waiters();
        }
        Ok(())
    }

    fn need_outer_scan(&self) -> bool {
        matches!(
            self.hash_join_desc.join_type,
            JoinType::Full | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn generate_outer_scan_task(&self) -> Result<()> {
        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };
        let bitmap_num = outer_scan_bitmap.len();
        if bitmap_num == 0 {
            return Ok(());
        }
        let mut outer_scan_tasks = self.outer_scan_tasks.write();
        for idx in 0..bitmap_num {
            let task = (idx, idx + 1);
            outer_scan_tasks.push_back(task);
        }
        Ok(())
    }

    fn outer_scan_task(&self) -> Option<(usize, usize)> {
        let mut tasks = self.outer_scan_tasks.write();
        tasks.pop_front()
    }

    fn outer_scan(&self, task: (usize, usize), state: &mut ProbeState) -> Result<Vec<DataBlock>> {
        match &self.hash_join_desc.join_type {
            JoinType::Right | JoinType::Full => self.outer_scan_right_and_full_join(task, state),
            JoinType::RightSemi => self.outer_scan_right_semi_join(task, state),
            JoinType::RightAnti => self.outer_scan_right_anti_join(task, state),
            _ => unreachable!(),
        }
    }

    fn outer_scan_right_and_full_join(
        &self,
        task: (usize, usize),
        state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let true_validity = &state.true_validity;
        let build_indexes = &mut state.build_indexes;
        let mut build_indexes_occupied = 0;
        let mut result_blocks = vec![];

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let total_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };
        let interrupt = self.interrupt.clone();
        let mut chunk_index = task.0;
        while chunk_index < task.1 {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            let bitmap = &outer_scan_bitmap[chunk_index];
            let bitmap_len = bitmap.len();
            let mut row_index = 0;
            while row_index < bitmap_len {
                while row_index < bitmap_len && build_indexes_occupied < JOIN_MAX_BLOCK_SIZE {
                    if !bitmap.get(row_index) {
                        build_indexes[build_indexes_occupied].chunk_index = chunk_index;
                        build_indexes[build_indexes_occupied].row_index = row_index;
                        build_indexes_occupied += 1;
                    }
                    row_index += 1;
                }
                let mut unmatched_build_block = self.row_space.gather(
                    &build_indexes[0..build_indexes_occupied],
                    &data_blocks,
                    &total_num_rows,
                )?;

                if self.hash_join_desc.join_type == JoinType::Full {
                    let num_rows = unmatched_build_block.num_rows();
                    let nullable_unmatched_build_columns = if num_rows == JOIN_MAX_BLOCK_SIZE {
                        unmatched_build_block
                            .columns()
                            .iter()
                            .map(|c| Self::set_validity(c, num_rows, true_validity))
                            .collect::<Vec<_>>()
                    } else {
                        let mut validity = MutableBitmap::new();
                        validity.extend_constant(num_rows, true);
                        let validity: Bitmap = validity.into();
                        unmatched_build_block
                            .columns()
                            .iter()
                            .map(|c| Self::set_validity(c, num_rows, &validity))
                            .collect::<Vec<_>>()
                    };
                    unmatched_build_block =
                        DataBlock::new(nullable_unmatched_build_columns, num_rows);
                };
                // Create null chunk for unmatched rows in probe side
                let null_probe_block = DataBlock::new(
                    self.probe_schema
                        .fields()
                        .iter()
                        .map(|df| {
                            BlockEntry::new(df.data_type().clone(), Value::Scalar(Scalar::Null))
                        })
                        .collect(),
                    build_indexes_occupied,
                );
                result_blocks.push(self.merge_eq_block(&unmatched_build_block, &null_probe_block)?);
                build_indexes_occupied = 0;
            }
            chunk_index += 1;
        }
        Ok(result_blocks)
    }

    fn outer_scan_right_semi_join(
        &self,
        task: (usize, usize),
        state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_indexes = &mut state.build_indexes;
        let mut build_indexes_occupied = 0;
        let mut result_blocks = vec![];

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let total_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };
        let interrupt = self.interrupt.clone();
        let mut chunk_index = task.0;
        while chunk_index < task.1 {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            let bitmap = &outer_scan_bitmap[chunk_index];
            let bitmap_len = bitmap.len();
            let mut row_index = 0;
            while row_index < bitmap_len {
                while row_index < bitmap_len && build_indexes_occupied < JOIN_MAX_BLOCK_SIZE {
                    if bitmap.get(row_index) {
                        build_indexes[build_indexes_occupied].chunk_index = chunk_index;
                        build_indexes[build_indexes_occupied].row_index = row_index;
                        build_indexes_occupied += 1;
                    }
                    row_index += 1;
                }
                result_blocks.push(self.row_space.gather(
                    &build_indexes[0..build_indexes_occupied],
                    &data_blocks,
                    &total_num_rows,
                )?);
                build_indexes_occupied = 0;
            }
            chunk_index += 1;
        }
        // for bl in result_blocks.iter() {
        //     dbg!("bl = {:?}", &bl);
        // }
        Ok(result_blocks)
    }

    fn outer_scan_right_anti_join(
        &self,
        task: (usize, usize),
        state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_indexes = &mut state.build_indexes;
        let mut build_indexes_occupied = 0;
        let mut result_blocks = vec![];

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let total_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };
        let interrupt = self.interrupt.clone();
        let mut chunk_index = task.0;
        while chunk_index < task.1 {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            let bitmap = &outer_scan_bitmap[chunk_index];
            let bitmap_len = bitmap.len();
            let mut row_index = 0;
            while row_index < bitmap_len {
                while row_index < bitmap_len && build_indexes_occupied < JOIN_MAX_BLOCK_SIZE {
                    if !bitmap.get(row_index) {
                        build_indexes[build_indexes_occupied].chunk_index = chunk_index;
                        build_indexes[build_indexes_occupied].row_index = row_index;
                        build_indexes_occupied += 1;
                    }
                    row_index += 1;
                }
                result_blocks.push(self.row_space.gather(
                    &build_indexes[0..build_indexes_occupied],
                    &data_blocks,
                    &total_num_rows,
                )?);
                build_indexes_occupied = 0;
            }
            chunk_index += 1;
        }
        Ok(result_blocks)
    }

    #[async_backtrace::framed]
    async fn wait_build_finish(&self) -> Result<()> {
        let notified = {
            let built_guard = self.build_done.lock();

            match *built_guard {
                true => None,
                false => Some(self.build_done_notify.notified()),
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
            let finalized_guard = self.finalize_done.lock();

            match *finalized_guard {
                true => None,
                false => Some(self.finalize_done_notify.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn wait_probe_finish(&self) -> Result<()> {
        let notified = {
            let finalized_guard = self.probe_done.lock();

            match *finalized_guard {
                true => None,
                false => Some(self.probe_done_notify.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }
        Ok(())
    }

    fn mark_join_blocks(&self) -> Result<Vec<DataBlock>> {
        let data_blocks = self.row_space.chunks.read();
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
}
