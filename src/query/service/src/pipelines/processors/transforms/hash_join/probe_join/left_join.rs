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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::KeyAccessor;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::sql::plans::JoinType;

impl HashJoinProbeState {
    pub(crate) fn left_join<'a, H: HashJoinHashtableLike, const LEFT_SINGLE: bool>(
        &self,
        input: &DataBlock,
        keys: Box<(dyn KeyAccessor<Key = H::Key>)>,
        hash_table: &H,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Probe states.
        let input_rows = input.num_rows();
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();
        // Safe to unwrap.
        let probe_unmatched_indexes = probe_state.probe_unmatched_indexes.as_mut().unwrap();

        // Build states.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let outer_scan_map = &mut build_state.outer_scan_map;

        // Results.
        let mut matched_idx = 0;
        let mut unmatched_idx = 0;
        let mut result_blocks = vec![];

        if probe_state.probe_with_selection {
            unmatched_idx = probe_state.probe_unmatched_indexes_count;
            let selection = &probe_state.selection.as_slice()[0..probe_state.selection_count];
            for idx in selection.iter() {
                let key = unsafe { keys.key_unchecked(*idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(*idx as usize) };
                // Probe hash table and fill `build_indexes`.
                let (match_count, mut incomplete_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count > 0 {
                    if LEFT_SINGLE && match_count > 1 {
                        return Err(ErrorCode::Internal(
                            "Scalar subquery can't return more than one row",
                        ));
                    }

                    for _ in 0..match_count {
                        unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = *idx };
                        matched_idx += 1;
                    }
                } else {
                    unsafe { *probe_unmatched_indexes.get_unchecked_mut(unmatched_idx) = *idx };
                    unmatched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_or_full_join_block(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        outer_scan_map,
                        &mut result_blocks,
                        None,
                        None,
                        None,
                    )?;
                    (matched_idx, incomplete_ptr) = self.fill_left_outer_states::<_, LEFT_SINGLE>(
                        hash_table,
                        key,
                        incomplete_ptr,
                        *idx,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                        false,
                        None,
                        None,
                    )?;
                }
            }
        } else {
            // Probe hash table and generate data blocks.
            for idx in 0..input_rows {
                let key = unsafe { keys.key_unchecked(idx) };
                let ptr = unsafe { *pointers.get_unchecked(idx) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut incomplete_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count > 0 {
                    if LEFT_SINGLE && match_count > 1 {
                        return Err(ErrorCode::Internal(
                            "Scalar subquery can't return more than one row",
                        ));
                    }

                    for _ in 0..match_count {
                        unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32 };
                        matched_idx += 1;
                    }
                } else {
                    unsafe {
                        *probe_unmatched_indexes.get_unchecked_mut(unmatched_idx) = idx as u32
                    };
                    unmatched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_or_full_join_block(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        outer_scan_map,
                        &mut result_blocks,
                        None,
                        None,
                        None,
                    )?;
                    (matched_idx, incomplete_ptr) = self.fill_left_outer_states::<_, LEFT_SINGLE>(
                        hash_table,
                        key,
                        incomplete_ptr,
                        idx as u32,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                        false,
                        None,
                        None,
                    )?;
                }
            }
        }

        if matched_idx > 0 {
            self.process_left_or_full_join_block(
                matched_idx,
                input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                outer_scan_map,
                &mut result_blocks,
                None,
                None,
                None,
            )?;
        }

        if unmatched_idx > 0 {
            result_blocks.push(self.process_left_or_full_join_null_block(
                unmatched_idx,
                input,
                probe_unmatched_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
            )?);
        }

        Ok(result_blocks)
    }

    pub(crate) fn left_join_with_conjunct<'a, H: HashJoinHashtableLike, const LEFT_SINGLE: bool>(
        &self,
        input: &DataBlock,
        keys: Box<(dyn KeyAccessor<Key = H::Key>)>,
        hash_table: &H,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Probe states.
        let input_rows = input.num_rows();
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();
        // The row_state is used to record whether a row in probe input is matched.
        // Safe to unwrap.
        let row_state = probe_state.row_state.as_mut().unwrap();
        // The row_state_indexes[idx] = i records the row_state[i] has been increased 1 by the idx,
        // if idx is filtered by other conditions, we will set row_state[idx] = row_state[idx] - 1.
        // Safe to unwrap.
        let row_state_indexes = probe_state.row_state_indexes.as_mut().unwrap();

        // Build states.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let outer_scan_map = &mut build_state.outer_scan_map;
        let filter_executor = probe_state.filter_executor.as_mut().unwrap();

        // Results.
        let mut matched_idx = 0;
        let mut result_blocks = vec![];

        // Probe hash table and generate data blocks.
        if probe_state.probe_with_selection {
            let selection = &probe_state.selection.as_slice()[0..probe_state.selection_count];
            for idx in selection.iter() {
                let key = unsafe { keys.key_unchecked(*idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(*idx as usize) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut incomplete_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count > 0 {
                    if LEFT_SINGLE && match_count > 1 {
                        return Err(ErrorCode::Internal(
                            "Scalar subquery can't return more than one row",
                        ));
                    }

                    unsafe {
                        *row_state.get_unchecked_mut(*idx as usize) += match_count;
                        for _ in 0..match_count {
                            *row_state_indexes.get_unchecked_mut(matched_idx) = *idx as usize;
                            *probe_indexes.get_unchecked_mut(matched_idx) = *idx;
                            matched_idx += 1;
                        }
                    }
                }

                while matched_idx == max_block_size {
                    self.process_left_or_full_join_block(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        outer_scan_map,
                        &mut result_blocks,
                        Some(filter_executor),
                        Some(row_state),
                        Some(row_state_indexes),
                    )?;
                    (matched_idx, incomplete_ptr) = self.fill_left_outer_states::<_, LEFT_SINGLE>(
                        hash_table,
                        key,
                        incomplete_ptr,
                        *idx,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                        true,
                        Some(row_state),
                        Some(row_state_indexes),
                    )?;
                }
            }
        } else {
            for idx in 0..input_rows {
                let key = unsafe { keys.key_unchecked(idx) };
                let ptr = unsafe { *pointers.get_unchecked(idx) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut incomplete_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count > 0 {
                    if LEFT_SINGLE && match_count > 1 {
                        return Err(ErrorCode::Internal(
                            "Scalar subquery can't return more than one row",
                        ));
                    }

                    unsafe {
                        *row_state.get_unchecked_mut(idx) += match_count;
                        for _ in 0..match_count {
                            *row_state_indexes.get_unchecked_mut(matched_idx) = idx;
                            *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32;
                            matched_idx += 1;
                        }
                    }
                }

                while matched_idx == max_block_size {
                    self.process_left_or_full_join_block(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        outer_scan_map,
                        &mut result_blocks,
                        Some(filter_executor),
                        Some(row_state),
                        Some(row_state_indexes),
                    )?;
                    (matched_idx, incomplete_ptr) = self.fill_left_outer_states::<_, LEFT_SINGLE>(
                        hash_table,
                        key,
                        incomplete_ptr,
                        idx as u32,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                        true,
                        Some(row_state),
                        Some(row_state_indexes),
                    )?;
                }
            }
        }

        if matched_idx > 0 {
            self.process_left_or_full_join_block(
                matched_idx,
                input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                outer_scan_map,
                &mut result_blocks,
                Some(filter_executor),
                Some(row_state),
                Some(row_state_indexes),
            )?;
        }

        let mut unmatched_idx = 0;
        for (idx, state) in row_state[0..input_rows].iter_mut().enumerate() {
            if *state == 0 {
                unsafe { *probe_indexes.get_unchecked_mut(unmatched_idx) = idx as u32 };
                unmatched_idx += 1;
            } else {
                // reset to zero.
                *state = 0;
            }
        }

        if unmatched_idx > 0 {
            result_blocks.push(self.process_left_or_full_join_null_block(
                unmatched_idx,
                input,
                probe_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
            )?);
        }

        Ok(result_blocks)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_or_full_join_null_block(
        &self,
        unmatched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
    ) -> Result<DataBlock> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_block = if probe_state.is_probe_projected {
            let mut probe_block = DataBlock::take(
                input,
                &probe_indexes[0..unmatched_idx],
                &mut probe_state.string_items_buf,
            )?;
            // For full join, wrap nullable for probe block
            if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                let nullable_probe_columns = probe_block
                    .columns()
                    .iter()
                    .map(|c| wrap_true_validity(c, unmatched_idx, &probe_state.true_validity))
                    .collect::<Vec<_>>();
                probe_block = DataBlock::new(nullable_probe_columns, unmatched_idx);
            }
            Some(probe_block)
        } else {
            None
        };
        let build_block = if build_state.is_build_projected {
            let null_build_block = DataBlock::new(
                self.hash_join_state
                    .row_space
                    .build_schema
                    .fields()
                    .iter()
                    .map(|df| BlockEntry {
                        data_type: df.data_type().clone(),
                        value: Value::Scalar(Scalar::Null),
                    })
                    .collect(),
                unmatched_idx,
            );
            Some(null_build_block)
        } else {
            None
        };

        Ok(self.merge_eq_block(probe_block, build_block, unmatched_idx))
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_or_full_join_block(
        &self,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        outer_scan_map: &mut [Vec<bool>],
        result_blocks: &mut Vec<DataBlock>,
        filter_executor: Option<&mut FilterExecutor>,
        row_state: Option<&mut Vec<usize>>,
        row_state_indexes: Option<&mut Vec<usize>>,
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_block = if probe_state.is_probe_projected {
            let mut probe_block = DataBlock::take(
                input,
                &probe_indexes[0..matched_idx],
                &mut probe_state.string_items_buf,
            )?;
            // For full join, wrap nullable for probe block
            if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                let nullable_probe_columns = probe_block
                    .columns()
                    .iter()
                    .map(|c| wrap_true_validity(c, matched_idx, &probe_state.true_validity))
                    .collect::<Vec<_>>();
                probe_block = DataBlock::new(nullable_probe_columns, matched_idx);
            }
            Some(probe_block)
        } else {
            None
        };
        let build_block = if build_state.is_build_projected {
            let build_block = self.hash_join_state.row_space.gather(
                &build_indexes[0..matched_idx],
                &build_state.build_columns,
                &build_state.build_columns_data_type,
                &build_state.build_num_rows,
                &mut probe_state.string_items_buf,
            )?;
            // For left or full join, wrap nullable for build block.
            let nullable_columns = if build_state.build_num_rows == 0 {
                build_block
                    .columns()
                    .iter()
                    .map(|c| BlockEntry {
                        value: Value::Scalar(Scalar::Null),
                        data_type: c.data_type.wrap_nullable(),
                    })
                    .collect::<Vec<_>>()
            } else {
                build_block
                    .columns()
                    .iter()
                    .map(|c| wrap_true_validity(c, matched_idx, &probe_state.true_validity))
                    .collect::<Vec<_>>()
            };
            Some(DataBlock::new(nullable_columns, matched_idx))
        } else {
            None
        };

        let result_block = self.merge_eq_block(probe_block, build_block, matched_idx);

        if filter_executor.is_none() {
            result_blocks.push(result_block);
            if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                for row_ptr in build_indexes[0..matched_idx].iter() {
                    unsafe {
                        *outer_scan_map
                            .get_unchecked_mut(row_ptr.chunk_index as usize)
                            .get_unchecked_mut(row_ptr.row_index as usize) = true;
                    };
                }
            }
            self.merge_into_check_and_set_matched(build_indexes, matched_idx, None)?;
            return Ok(());
        }

        // Safe to unwrap.
        let row_state = row_state.unwrap();
        let row_state_indexes = row_state_indexes.unwrap();
        let filter_executor = filter_executor.unwrap();

        let (result_block, selection, all_true, all_false) =
            self.get_other_predicate_result_block(filter_executor, result_block)?;

        if all_true {
            result_blocks.push(result_block);
            if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                for row_ptr in build_indexes[0..matched_idx].iter() {
                    unsafe {
                        *outer_scan_map
                            .get_unchecked_mut(row_ptr.chunk_index as usize)
                            .get_unchecked_mut(row_ptr.row_index as usize) = true
                    };
                }
            }
            self.merge_into_check_and_set_matched(build_indexes, matched_idx, Some(selection))?;
        } else if all_false {
            for idx in 0..matched_idx {
                unsafe {
                    *row_state.get_unchecked_mut(*row_state_indexes.get_unchecked(idx)) -= 1;
                };
            }
        } else {
            result_blocks.push(result_block);
            let mut count = 0;
            if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                for idx in selection {
                    unsafe {
                        while count < *idx {
                            *row_state.get_unchecked_mut(
                                *row_state_indexes.get_unchecked(count as usize),
                            ) -= 1;
                            count += 1;
                        }
                        let row_ptr = build_indexes.get_unchecked(*idx as usize);
                        *outer_scan_map
                            .get_unchecked_mut(row_ptr.chunk_index as usize)
                            .get_unchecked_mut(row_ptr.row_index as usize) = true;
                        count += 1;
                    }
                }
            } else {
                self.merge_into_check_and_set_matched(build_indexes, matched_idx, Some(selection))?;
                for idx in selection {
                    while count < *idx {
                        unsafe {
                            *row_state.get_unchecked_mut(
                                *row_state_indexes.get_unchecked(count as usize),
                            ) -= 1
                        };
                        count += 1;
                    }
                    count += 1;
                }
            }
            while (count as usize) < matched_idx {
                unsafe {
                    *row_state
                        .get_unchecked_mut(*row_state_indexes.get_unchecked(count as usize)) -= 1
                };
                count += 1;
            }
        }

        Ok(())
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    fn fill_left_outer_states<'a, H: HashJoinHashtableLike, const LEFT_SINGLE: bool>(
        &self,
        hash_table: &H,
        key: &H::Key,
        incomplete_ptr: u64,
        idx: u32,
        probe_indexes: &mut [u32],
        build_indexes_ptr: *mut RowPtr,
        max_block_size: usize,
        with_conjunct: bool,
        row_state: Option<&mut Vec<usize>>,
        row_state_indexes: Option<&mut Vec<usize>>,
    ) -> Result<(usize, u64)>
    where
        H::Key: 'a,
    {
        let (match_count, ptr) =
            hash_table.next_probe(key, incomplete_ptr, build_indexes_ptr, 0, max_block_size);
        if match_count == 0 {
            return Ok((0, 0));
        }

        if LEFT_SINGLE {
            return Err(ErrorCode::Internal(
                "Scalar subquery can't return more than one row",
            ));
        }

        if !with_conjunct {
            for i in 0..match_count {
                unsafe { *probe_indexes.get_unchecked_mut(i) = idx };
            }
        } else {
            let row_state = row_state.unwrap();
            let row_state_indexes = row_state_indexes.unwrap();
            unsafe {
                *row_state.get_unchecked_mut(idx as usize) += match_count;
                for i in 0..match_count {
                    *row_state_indexes.get_unchecked_mut(i) = idx as usize;
                    *probe_indexes.get_unchecked_mut(i) = idx;
                }
            }
        }

        Ok((match_count, ptr))
    }
}
