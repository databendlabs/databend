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
use databend_common_expression::KeyAccessor;

use crate::pipelines::processors::transforms::ProcessState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::probe_state::ProbeBlockGenerationState;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;

impl HashJoinProbeState {
    pub(crate) fn inner_join<
        'a,
        H: HashJoinHashtableLike,
        const FROM_LEFT_SINGLE: bool,
        const FROM_RIGHT_SINGLE: bool,
    >(
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

                if FROM_LEFT_SINGLE && match_count > 1 {
                    return Err(ErrorCode::Internal(format!(
                        "Scalar subquery can't return more than one row, but got {}",
                        match_count
                    )));
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx };
                    matched_idx += 1;
                }

                if matched_idx == max_block_size {
                    next_process_state = self.next_process_state::<_, FROM_LEFT_SINGLE>(
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

                if FROM_LEFT_SINGLE && match_count > 1 {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }

                // Fill `probe_indexes`.
                for _ in 0..match_count {
                    unsafe { *probe_indexes.get_unchecked_mut(matched_idx) = key_idx as u32 };
                    matched_idx += 1;
                }

                if matched_idx == max_block_size {
                    next_process_state = self.next_process_state::<_, FROM_LEFT_SINGLE>(
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
            result_blocks.push(self.process_inner_join_block::<FROM_RIGHT_SINGLE>(
                matched_idx,
                &process_state.input,
                probe_indexes,
                build_indexes,
                &mut probe_state.generation_state,
                &build_state.generation_state,
                &mut right_single_scan_map,
            )?);
        }

        if !next_process_state {
            probe_state.process_state = None;
        }

        match &mut probe_state.filter_executor {
            None => Ok(result_blocks),
            Some(filter_executor) => {
                let mut filtered_blocks = Vec::with_capacity(result_blocks.len());
                for result_block in result_blocks {
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::aborting());
                    }
                    let result_block = filter_executor.filter(result_block)?;
                    if !result_block.is_empty() {
                        filtered_blocks.push(result_block);
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
                result_block.add_entry(entry);
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
        next_ptr: u64,
        idx: u32,
        probe_indexes: &mut [u32],
        build_indexes_ptr: *mut RowPtr,
        max_block_size: usize,
    ) -> Result<(usize, u64)>
    where
        H::Key: 'a,
    {
        let (match_count, ptr) =
            hash_table.next_probe(key, next_ptr, build_indexes_ptr, 0, max_block_size);
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn next_process_state<H: HashJoinHashtableLike, const FROM_LEFT_SINGLE: bool>(
        &self,
        key: &H::Key,
        hash_table: &H,
        num_keys: usize,
        mut next_idx: usize,
        key_idx: usize,
        next_ptr: u64,
        pointers: &mut [u64],
        process_state: &mut ProcessState,
    ) -> Result<bool> {
        let next_matched_ptr = hash_table.next_matched_ptr(key, next_ptr);
        if next_matched_ptr == 0 {
            next_idx += 1;
        } else {
            if FROM_LEFT_SINGLE {
                return Err(ErrorCode::Internal(
                    "Scalar subquery can't return more than one row",
                ));
            }
            pointers[key_idx] = next_matched_ptr;
        }
        if next_idx >= num_keys {
            return Ok(false);
        }
        process_state.next_idx = next_idx;
        Ok(true)
    }
}
