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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::KeyAccessor;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::RowPtr;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;

use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn inner_join<
        'a,
        H: HashJoinHashtableLike,
        const FROM_LEFT_SINGLE: bool,
        const FROM_RIGHT_SINGLE: bool,
    >(
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

        // Build states.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let outer_scan_map = &mut build_state.outer_scan_map;
        let mut right_single_scan_map = if FROM_RIGHT_SINGLE {
            outer_scan_map
                .iter_mut()
                .map(|sp| unsafe {
                    std::mem::transmute::<*mut bool, *mut AtomicBool>(sp.as_mut_ptr())
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

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
                if match_count == 0 {
                    continue;
                }

                if FROM_LEFT_SINGLE && match_count > 0 {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = *idx };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    result_blocks.push(self.process_inner_join_block::<FROM_RIGHT_SINGLE>(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        &mut right_single_scan_map,
                    )?);
                    (matched_idx, incomplete_ptr) = self
                        .fill_probe_and_build_indexes::<_, FROM_LEFT_SINGLE>(
                            hash_table,
                            key,
                            incomplete_ptr,
                            *idx,
                            probe_indexes,
                            build_indexes_ptr,
                            max_block_size,
                        )?;
                }
            }
        } else {
            for idx in 0..input.num_rows() {
                let key = unsafe { keys.key_unchecked(idx) };
                let ptr = unsafe { *pointers.get_unchecked(idx) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, mut incomplete_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }

                if FROM_LEFT_SINGLE && match_count > 0 {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = idx as u32 };
                    matched_idx += 1;
                }

                while matched_idx == max_block_size {
                    result_blocks.push(self.process_inner_join_block::<FROM_RIGHT_SINGLE>(
                        matched_idx,
                        input,
                        probe_indexes,
                        build_indexes,
                        &mut probe_state.generation_state,
                        &build_state.generation_state,
                        &mut right_single_scan_map,
                    )?);
                    (matched_idx, incomplete_ptr) = self
                        .fill_probe_and_build_indexes::<_, FROM_LEFT_SINGLE>(
                            hash_table,
                            key,
                            incomplete_ptr,
                            idx as u32,
                            probe_indexes,
                            build_indexes_ptr,
                            max_block_size,
                        )?;
                }
            }
        }

        if matched_idx > 0 {
            result_blocks.push(self.process_inner_join_block::<FROM_RIGHT_SINGLE>(
                matched_idx,
                input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                &mut right_single_scan_map,
            )?);
        }

        match &self.hash_join_state.hash_join_desc.other_predicate {
            None => Ok(result_blocks),
            Some(other_predicate) => {
                // Wrap `is_true` to `other_predicate`
                let other_predicate = cast_expr_to_non_null_boolean(other_predicate.clone())?;
                assert_eq!(other_predicate.data_type(), &DataType::Boolean);

                let mut filtered_blocks = Vec::with_capacity(result_blocks.len());
                for result_block in result_blocks {
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let evaluator =
                        Evaluator::new(&result_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    let predicate = evaluator
                        .run(&other_predicate)?
                        .try_downcast::<BooleanType>()
                        .unwrap();
                    let res = result_block.filter_boolean_value(&predicate)?;
                    if !res.is_empty() {
                        filtered_blocks.push(res);
                    }
                }

                Ok(filtered_blocks)
            }
        }
    }

    #[inline]
    fn process_inner_join_block<const FROM_RIGHT_SINGLE: bool>(
        &self,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        right_single_scan_map: &mut [*mut AtomicBool],
    ) -> Result<DataBlock> {
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

        let mut result_block = self.merge_eq_block(probe_block, build_block, matched_idx);

        if !self.hash_join_state.probe_to_build.is_empty() {
            for (index, (is_probe_nullable, is_build_nullable)) in
                self.hash_join_state.probe_to_build.iter()
            {
                let entry = match (is_probe_nullable, is_build_nullable) {
                    (true, true) | (false, false) => result_block.get_by_offset(*index).clone(),
                    (true, false) => result_block.get_by_offset(*index).clone().remove_nullable(),
                    (false, true) => wrap_true_validity(
                        result_block.get_by_offset(*index),
                        result_block.num_rows(),
                        &probe_state.true_validity,
                    ),
                };
                result_block.add_column(entry);
            }
        }

        if FROM_RIGHT_SINGLE {
            self.update_right_single_scan_map(
                &build_indexes[0..matched_idx],
                right_single_scan_map,
                None,
            )?;
        }

        Ok(result_block)
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn fill_probe_and_build_indexes<
        'a,
        H: HashJoinHashtableLike,
        const FROM_LEFT_SINGLE: bool,
    >(
        &self,
        hash_table: &H,
        key: &H::Key,
        incomplete_ptr: u64,
        idx: u32,
        probe_indexes: &mut [u32],
        build_indexes_ptr: *mut RowPtr,
        max_block_size: usize,
    ) -> Result<(usize, u64)>
    where
        H::Key: 'a,
    {
        let (match_count, ptr) =
            hash_table.next_probe(key, incomplete_ptr, build_indexes_ptr, 0, max_block_size);
        if match_count == 0 {
            return Ok((0, 0));
        }

        if FROM_LEFT_SINGLE {
            return Err(ErrorCode::Internal(
                "Scalar subquery can't return more than one row",
            ));
        }

        for i in 0..match_count {
            unsafe { *probe_indexes.get_unchecked_mut(i) = idx };
        }

        Ok((match_count, ptr))
    }
}
