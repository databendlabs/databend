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
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::KeyAccessor;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::sql::plans::JoinType;

impl HashJoinProbeState {
    pub(crate) fn probe_right_join<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        // Process States.
        let mut next_process_state = false;
        let process_state = probe_state.process_state.as_mut().unwrap();

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let probe_indexes = &mut mutable_indexes.probe_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let pointers = probe_state.hashes.as_mut_slice();

        // Build states.
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
        let outer_scan_map = &mut build_state.outer_scan_map;
        let mut right_single_scan_map =
            if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
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
            let selection = probe_state.selection.as_slice();
            for selection_idx in process_state.next_idx..probe_state.selection_count {
                let key_idx = unsafe { *selection.get_unchecked(selection_idx) };
                let key = unsafe { keys.key_unchecked(key_idx as usize) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx as usize) };

                // Probe hash table and fill `build_indexes`.
                let (match_count, next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }
                if ProbeState::check_used(
                    &mut probe_state.used_once,
                    max_block_size,
                    build_indexes_ptr,
                    matched_idx,
                ) {
                    continue;
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx };
                    matched_idx += 1;
                }

                if matched_idx == max_block_size {
                    next_process_state = self.next_process_state::<_, false>(
                        key,
                        hash_table,
                        probe_state.selection_count,
                        selection_idx,
                        key_idx as usize,
                        next_ptr,
                        pointers,
                        process_state,
                    )?;
                    break;
                }
            }
        } else {
            for key_idx in process_state.next_idx..process_state.input.num_rows() {
                let key = unsafe { keys.key_unchecked(key_idx) };
                let ptr = unsafe { *pointers.get_unchecked(key_idx) };

                // Probe hash table and fill build_indexes.
                let (match_count, next_ptr) =
                    hash_table.next_probe(key, ptr, build_indexes_ptr, matched_idx, max_block_size);
                if match_count == 0 {
                    continue;
                }
                if ProbeState::check_used(
                    &mut probe_state.used_once,
                    max_block_size,
                    build_indexes_ptr,
                    matched_idx,
                ) {
                    continue;
                }

                // Fill probe_indexes.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }

                if matched_idx == max_block_size {
                    next_process_state = self.next_process_state::<_, false>(
                        key,
                        hash_table,
                        process_state.input.num_rows(),
                        key_idx,
                        key_idx,
                        next_ptr,
                        pointers,
                        process_state,
                    )?;
                    break;
                }
            }
        }

        if matched_idx > 0 {
            self.process_right_join_block(
                &mut result_blocks,
                matched_idx,
                &process_state.input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                outer_scan_map,
                &mut right_single_scan_map,
                probe_state.filter_executor.as_mut(),
            )?;
        }

        if !next_process_state {
            probe_state.process_state = None;
        }

        Ok(result_blocks)
    }

    pub(crate) fn update_right_single_scan_map(
        &self,
        build_indexes: &[RowPtr],
        right_single_scan_map: &[*mut AtomicBool],
        selection: Option<&[u32]>,
    ) -> Result<()> {
        if let Some(selection) = selection {
            for idx in selection {
                let row_ptr = unsafe { build_indexes.get_unchecked(*idx as usize) };
                let old = unsafe {
                    (*right_single_scan_map[row_ptr.chunk_index as usize]
                        .add(row_ptr.row_index as usize))
                    .load(Ordering::SeqCst)
                };
                if old {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }
                let res = unsafe {
                    (*right_single_scan_map[row_ptr.chunk_index as usize]
                        .add(row_ptr.row_index as usize))
                    .compare_exchange_weak(old, true, Ordering::SeqCst, Ordering::SeqCst)
                };
                if res.is_err() {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }
            }
        } else {
            for row_ptr in build_indexes {
                let old = unsafe {
                    (*right_single_scan_map[row_ptr.chunk_index as usize]
                        .add(row_ptr.row_index as usize))
                    .load(Ordering::SeqCst)
                };
                if old {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }
                let res = unsafe {
                    (*right_single_scan_map[row_ptr.chunk_index as usize]
                        .add(row_ptr.row_index as usize))
                    .compare_exchange_weak(old, true, Ordering::SeqCst, Ordering::SeqCst)
                };
                if res.is_err() {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }
            }
        }

        Ok(())
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn process_right_join_block(
        &self,
        result_blocks: &mut Vec<DataBlock>,
        matched_idx: usize,
        input: &DataBlock,
        probe_indexes: &[u32],
        build_indexes: &[RowPtr],
        probe_state: &mut ProbeBlockGenerationState,
        build_state: &BuildBlockGenerationState,
        outer_scan_map: &mut [Vec<bool>],
        right_single_scan_map: &mut [*mut AtomicBool],
        filter_executor: Option<&mut FilterExecutor>,
    ) -> Result<()> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::aborting());
        }

        let probe_block = if probe_state.is_probe_projected {
            let probe_block = DataBlock::take(input, &probe_indexes[0..matched_idx])?;

            // The join type is right join, we need to wrap nullable for probe side.
            let nullable_columns = probe_block
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, matched_idx, &probe_state.true_validity))
                .collect::<Vec<_>>();
            Some(DataBlock::new(nullable_columns, matched_idx))
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

        let result_block = self.merge_eq_block(probe_block, build_block, matched_idx);

        if !result_block.is_empty() {
            if let Some(filter_executor) = filter_executor {
                let (result_block, selection, all_true, all_false) =
                    self.get_other_predicate_result_block(filter_executor, result_block)?;
                if all_true {
                    result_blocks.push(result_block);
                    if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                        self.update_right_single_scan_map(
                            &build_indexes[0..matched_idx],
                            right_single_scan_map,
                            None,
                        )?;
                    } else {
                        for row_ptr in &build_indexes[0..matched_idx] {
                            unsafe {
                                *outer_scan_map
                                    .get_unchecked_mut(row_ptr.chunk_index as usize)
                                    .get_unchecked_mut(row_ptr.row_index as usize) = true;
                            };
                        }
                    }
                } else if !all_false {
                    result_blocks.push(result_block);
                    // Safe to unwrap.
                    if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                        self.update_right_single_scan_map(
                            &build_indexes[0..matched_idx],
                            right_single_scan_map,
                            Some(selection),
                        )?;
                    } else {
                        for idx in selection {
                            unsafe {
                                let row_ptr = build_indexes.get_unchecked(*idx as usize);
                                *outer_scan_map
                                    .get_unchecked_mut(row_ptr.chunk_index as usize)
                                    .get_unchecked_mut(row_ptr.row_index as usize) = true;
                            }
                        }
                    }
                }
            } else {
                result_blocks.push(result_block);
                if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                    self.update_right_single_scan_map(
                        &build_indexes[0..matched_idx],
                        right_single_scan_map,
                        None,
                    )?;
                } else {
                    for row_ptr in &build_indexes[0..matched_idx] {
                        unsafe {
                            *outer_scan_map
                                .get_unchecked_mut(row_ptr.chunk_index as usize)
                                .get_unchecked_mut(row_ptr.row_index as usize) = true;
                        };
                    }
                }
            }
        }
        Ok(())
    }
}
