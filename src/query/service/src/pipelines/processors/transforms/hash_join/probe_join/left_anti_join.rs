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

use databend_common_base::hints::assume;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::KeyAccessor;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;

impl HashJoinProbeState {
    pub(crate) fn left_anti_join<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Process States.
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let pointers = probe_state.hashes.as_slice();

        // Probe hash table and generate data blocks.
        let (probe_indexes, count) = if probe_state.probe_with_selection {
            // Safe to unwrap.
            let probe_unmatched_indexes = probe_state.probe_unmatched_indexes.as_mut().unwrap();
            let selection = &probe_state.selection.as_slice()[0..probe_state.selection_count];
            for idx in selection.iter() {
                let key = unsafe { keys.key_unchecked(*idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(*idx as usize) };
                if !hash_table.next_contains(key, ptr) {
                    assume(probe_unmatched_indexes.len() < probe_unmatched_indexes.capacity());
                    probe_unmatched_indexes.push(*idx);
                }
            }
            let unmatched_count = probe_unmatched_indexes.len();
            (probe_unmatched_indexes, unmatched_count)
        } else {
            let mutable_indexes = &mut probe_state.mutable_indexes;
            let probe_indexes = &mut mutable_indexes.probe_indexes;
            let mut unmatched_idx = 0;
            for idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(idx) };
                let ptr = unsafe { *pointers.get_unchecked(idx) };
                if !hash_table.next_contains(key, ptr) {
                    unsafe { *probe_indexes.get_unchecked_mut(unmatched_idx) = idx as u32 };
                    unmatched_idx += 1;
                }
            }
            (probe_indexes, unmatched_idx)
        };

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        let result_block = DataBlock::take(&process_state.input, &probe_indexes[0..count])?;

        if probe_state.probe_with_selection {
            probe_indexes.clear();
        }

        probe_state.process_state = None;

        if result_block.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![result_block])
        }
    }

    pub(crate) fn left_anti_join_with_conjunct<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Process States.
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };

        // For anti join, it defaults to false.
        let mut row_state = vec![false; process_state.input.num_rows()];
        let filter_executor = probe_state.filter_executor.as_mut().unwrap();

        // Results.
        let mut matched_idx = 0;
        let mut result_blocks = vec![];

        // Probe hash table and generate data blocks.
        if probe_state.probe_with_selection {
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) };
                let key = unsafe { keys.key_unchecked(key_idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx as usize) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count == 0 {
                    continue;
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_anti_join_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        &mut row_state,
                        filter_executor,
                    )?;
                    (matched_idx, next_ptr) = self.fill_probe_and_build_indexes::<_, false>(
                        hash_table,
                        key,
                        next_ptr,
                        key_idx,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                    )?;
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

                if match_count == 0 {
                    continue;
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_anti_join_block(
                        matched_idx,
                        &process_state.input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        &mut row_state,
                        filter_executor,
                    )?;
                    (matched_idx, next_ptr) = self.fill_probe_and_build_indexes::<_, false>(
                        hash_table,
                        key,
                        next_ptr,
                        key_idx as u32,
                        probe_indexes,
                        build_indexes_ptr,
                        max_block_size,
                    )?;
                }
            }
        }

        if matched_idx > 0 {
            self.process_left_anti_join_block(
                matched_idx,
                &process_state.input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                &mut row_state,
                filter_executor,
            )?;
        }

        // Find all unmatched indexes and generate the result `DataBlock`.
        let mut unmatched_idx = 0;
        for (i, state) in row_state.iter().enumerate() {
            if !*state {
                unsafe { *probe_indexes.get_unchecked_mut(unmatched_idx) = i as u32 };
                unmatched_idx += 1;
            }
        }
        if unmatched_idx > 0 {
            result_blocks.push(DataBlock::take(
                &process_state.input,
                &probe_indexes[0..unmatched_idx],
            )?);
        }

        probe_state.process_state = None;

        Ok(result_blocks)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_anti_join_block(
        &self,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        row_state: &mut [bool],
        filter_executor: &mut FilterExecutor,
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        let probe_block = if probe_state.is_probe_projected {
            Some(DataBlock::take(input, &probe_indexes[0..matched_idx])?)
        } else {
            None
        };
        let build_block = if build_state.is_build_projected {
            Some(self.hash_join_state.gather(
                &build_indexes[0..matched_idx],
                &build_state.build_columns,
                &build_state.build_columns_data_type,
                &build_state.build_num_rows,
            )?)
        } else {
            None
        };

        let result_block = self.merge_eq_block(probe_block.clone(), build_block, matched_idx);
        self.update_row_state(
            &result_block,
            &probe_indexes[0..matched_idx],
            row_state,
            filter_executor,
        )?;

        Ok(())
    }
}
