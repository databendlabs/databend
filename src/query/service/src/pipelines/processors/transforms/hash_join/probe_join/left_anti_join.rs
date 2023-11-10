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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::KeyAccessor;
use common_hashtable::HashJoinHashtableLike;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn left_anti_join<'a, H: HashJoinHashtableLike>(
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
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let pointers = probe_state.hashes.as_slice();

        // Results.
        let mut matched_idx = 0;
        let mut result_blocks = vec![];

        // Probe hash table and generate data blocks.
        for idx in 0..input.num_rows() {
            let key = unsafe { keys.key_unchecked(idx) };
            let ptr = unsafe { *pointers.get_unchecked(idx) };
            if !hash_table.next_contains(key, ptr) {
                unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32 };
                matched_idx += 1;
            }
        }

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        if matched_idx > 0 {
            result_blocks.push(DataBlock::take(
                input,
                &probe_indexes[0..matched_idx],
                &mut probe_state.generation_state.string_items_buf,
            )?);
        }

        Ok(result_blocks)
    }

    pub(crate) fn left_anti_join_with_conjunct<'a, H: HashJoinHashtableLike>(
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
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_slice();
        // Safe to unwrap.
        let probe_unmatched_indexes = probe_state.probe_unmatched_indexes.as_mut().unwrap();

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };

        // For semi join, it defaults to all.
        let mut row_state = vec![0_u32; input.num_rows()];
        let other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap();

        // Results.
        let mut matched_idx = 0;
        let mut unmatched_idx = 0;
        let mut result_blocks = vec![];

        // Probe hash table and generate data blocks.
        for idx in 0..input.num_rows() {
            let key = unsafe { keys.key_unchecked(idx) };
            let ptr = unsafe { *pointers.get_unchecked(idx) };

            // Probe hash table and fill `build_indexes`.
            let (mut match_count, mut incomplete_ptr) =
                hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);

            if match_count == 0 {
                unsafe { *probe_unmatched_indexes.get_unchecked_mut(unmatched_idx) = idx as u32 };
                unmatched_idx += 1;
                continue;
            }

            unsafe { *row_state.get_unchecked_mut(idx) += match_count as u32 };

            // Fill `probe_indexes`.
            for _ in 0..match_count {
                unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32 };
                matched_idx += 1;
            }

            while matched_idx == max_block_size {
                self.process_left_anti_join_block(
                    matched_idx,
                    input,
                    probe_indexes,
                    build_indexes,
                    &mut probe_state.generation_state,
                    &build_state.generation_state,
                    other_predicate,
                    &mut row_state,
                    &mut result_blocks,
                )?;
                matched_idx = 0;
                (match_count, incomplete_ptr) = hash_table.next_probe(
                    key,
                    incomplete_ptr,
                    build_indexes_ptr,
                    matched_idx,
                    max_block_size,
                );
                unsafe { *row_state.get_unchecked_mut(idx) += match_count as u32 };
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32 };
                    matched_idx += 1;
                }
            }
        }

        if unmatched_idx > 0 {
            result_blocks.push(DataBlock::take(
                input,
                &probe_unmatched_indexes[0..unmatched_idx],
                &mut probe_state.generation_state.string_items_buf,
            )?);
        }

        if matched_idx > 0 {
            self.process_left_anti_join_block(
                matched_idx,
                input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                other_predicate,
                &mut row_state,
                &mut result_blocks,
            )?;
            return Ok(result_blocks);
        }

        Ok(result_blocks)
    }

    // keep the index of the negative state
    // bitmap: [1, 1, 1] with row_state [3, 0], probe_index: [(0, 3)] => [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [0, 1, 1] -> [0, 0, 1] -> [0, 0, 0]
    // row_state will be [3, 0] -> [3, 0] -> [3, 0] -> [3, 0]
    pub(crate) fn fill_null_for_anti_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexes: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexes.iter().enumerate() {
            unsafe {
                if *row_state.get_unchecked(*row as usize) == 0 {
                    // if state is not matched, anti result will take one
                    bm.set_unchecked(index, true);
                } else if *row_state.get_unchecked(*row as usize) == 1 {
                    // if state has just one, anti reverse the result
                    *row_state.get_unchecked_mut(*row as usize) -= 1;
                    bm.set_unchecked(index, !bm.get(index))
                } else if !bm.get(index) {
                    *row_state.get_unchecked_mut(*row as usize) -= 1;
                } else {
                    bm.set_unchecked(index, false);
                }
            }
        }
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
        other_predicate: &Expr,
        row_state: &mut [u32],
        result_blocks: &mut Vec<DataBlock>,
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let probe_block = if probe_state.is_probe_projected {
            Some(DataBlock::take(
                input,
                &probe_indexes[0..matched_idx],
                &mut probe_state.string_items_buf,
            )?)
        } else {
            None
        };
        let build_block = if build_state.is_build_projected {
            Some(self.hash_join_state.row_space.gather(
                &build_indexes[0..matched_idx],
                &build_state.build_columns,
                &build_state.build_columns_data_type,
                &build_state.build_num_rows,
                &mut probe_state.string_items_buf,
            )?)
        } else {
            None
        };

        let result_block = self.merge_eq_block(probe_block.clone(), build_block, matched_idx);

        let mut bm = match self.get_other_filters(&result_block, other_predicate, &self.func_ctx)? {
            (Some(b), _, _) => b.make_mut(),
            (_, true, _) => MutableBitmap::from_len_set(matched_idx),
            (_, _, true) => MutableBitmap::from_len_zeroed(matched_idx),
            _ => unreachable!(),
        };

        self.fill_null_for_anti_join(&mut bm, &probe_indexes[0..matched_idx], row_state);

        if let Some(probe_block) = probe_block {
            let result_block = DataBlock::filter_with_bitmap(probe_block, &bm.into())?;
            if !result_block.is_empty() {
                result_blocks.push(result_block);
            }
        }

        Ok(())
    }
}
