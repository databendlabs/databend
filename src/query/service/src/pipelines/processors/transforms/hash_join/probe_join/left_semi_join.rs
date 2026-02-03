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
    pub(crate) fn left_semi_join<'a, H: HashJoinHashtableLike>(
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
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let pointers = probe_state.hashes.as_slice();

        // Results.
        let mut matched_idx = 0;
        let mut result_blocks = vec![];

        // Probe hash table and generate data blocks.
        if probe_state.probe_with_selection {
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) } as usize;
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };
                if hash_table.next_contains(key, ptr) {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };
                if hash_table.next_contains(key, ptr) {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }
            }
        }

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        if matched_idx > 0 {
            result_blocks.push(DataBlock::take(
                &process_state.input,
                &probe_indexes[0..matched_idx],
            )?);
        }

        probe_state.process_state = None;

        Ok(result_blocks)
    }

    pub(crate) fn left_semi_join_with_conjunct<'a, H: HashJoinHashtableLike>(
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

        // For semi join, it defaults to false.
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
                    self.process_left_semi_join_block(
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

                // Probe hash table and fill build_indexes.
                let (match_count, mut next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                // Fill probe_indexes.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    self.process_left_semi_join_block(
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
            self.process_left_semi_join_block(
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

        // Find all matched indexes and generate the result `DataBlock`.
        matched_idx = 0;
        for (i, state) in row_state.iter().enumerate() {
            if *state {
                unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = i as u32 };
                matched_idx += 1;
            }
        }
        if matched_idx > 0 {
            result_blocks.push(DataBlock::take(
                &process_state.input,
                &probe_indexes[0..matched_idx],
            )?);
        }

        probe_state.process_state = None;

        Ok(result_blocks)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_left_semi_join_block(
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

    #[inline]
    pub(crate) fn update_row_state(
        &self,
        result_block: &DataBlock,
        probe_indexes: &[u32],
        row_state: &mut [bool],
        filter_executor: &mut FilterExecutor,
    ) -> Result<()> {
        let (selection, all_true, all_false) =
            self.get_other_predicate_selection(filter_executor, result_block)?;
        if all_true {
            for index in probe_indexes.iter() {
                unsafe { *row_state.get_unchecked_mut(*index as usize) = true };
            }
        } else if !all_false {
            for i in selection {
                let index = unsafe { *probe_indexes.get_unchecked(*i as usize) };
                unsafe { *row_state.get_unchecked_mut(index as usize) = true };
            }
        }
        Ok(())
    }
}
