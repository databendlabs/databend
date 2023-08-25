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

use std::iter::TrustedLen;
use std::sync::atomic::Ordering;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn probe_right_anti_join<'a, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let max_block_size = probe_state.max_block_size;
        let valids = &probe_state.valids;
        let mut matched_num = 0;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();

        let outer_scan_map = unsafe { &mut *self.hash_join_state.outer_scan_map.get() };

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = self.probe_key(
                hash_table,
                key,
                valids,
                i,
                local_build_indexes_ptr,
                matched_num,
                max_block_size,
            );
            if match_count == 0 {
                continue;
            }
            matched_num += match_count;
            if matched_num >= max_block_size {
                loop {
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    for row_ptr in local_build_indexes.iter() {
                        outer_scan_map[row_ptr.chunk_index as usize][row_ptr.row_index as usize] =
                            true;
                    }

                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        local_build_indexes_ptr,
                        matched_num,
                        max_block_size,
                    );
                    if match_count == 0 {
                        break;
                    }

                    matched_num += match_count;

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
        }

        for row_ptr in local_build_indexes.iter().take(matched_num) {
            outer_scan_map[row_ptr.chunk_index as usize][row_ptr.row_index as usize] = true;
        }

        Ok(vec![])
    }

    pub(crate) fn probe_right_anti_join_with_conjunct<'a, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
        is_probe_projected: bool,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let max_block_size = probe_state.max_block_size;
        let valids = &probe_state.valids;
        // The right join will return multiple data blocks of similar size.
        let mut matched_num = 0;
        let local_probe_indexes = &mut probe_state.probe_indexes;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();

        let data_blocks = unsafe { &*self.hash_join_state.chunks.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let outer_scan_map = unsafe { &mut *self.hash_join_state.outer_scan_map.get() };
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = self.probe_key(
                hash_table,
                key,
                valids,
                i,
                local_build_indexes_ptr,
                matched_num,
                max_block_size,
            );
            if match_count == 0 {
                continue;
            }

            for _ in 0..match_count {
                local_probe_indexes[matched_num] = i as u32;
                matched_num += 1;
            }
            if matched_num >= max_block_size {
                loop {
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let probe_block = if is_probe_projected {
                        Some(DataBlock::take(input, local_probe_indexes)?)
                    } else {
                        None
                    };
                    let build_block = if is_build_projected {
                        Some(self.hash_join_state.row_space.gather(
                            local_build_indexes,
                            data_blocks,
                            build_num_rows,
                        )?)
                    } else {
                        None
                    };
                    let result_block = self.merge_eq_block(probe_block, build_block, matched_num);

                    if !result_block.is_empty() {
                        let (bm, all_true, all_false) = self.get_other_filters(
                            &result_block,
                            self.hash_join_state
                                .hash_join_desc
                                .other_predicate
                                .as_ref()
                                .unwrap(),
                        )?;

                        if all_true {
                            for row_ptr in local_build_indexes.iter() {
                                outer_scan_map[row_ptr.chunk_index as usize]
                                    [row_ptr.row_index as usize] = true;
                            }
                        } else if !all_false {
                            // Safe to unwrap.
                            let validity = bm.unwrap();
                            let mut idx = 0;
                            while idx < matched_num {
                                let valid = unsafe { validity.get_bit_unchecked(idx) };
                                if valid {
                                    outer_scan_map[local_build_indexes[idx].chunk_index as usize]
                                        [local_build_indexes[idx].row_index as usize] = true;
                                }
                                idx += 1;
                            }
                        }
                    }
                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        local_build_indexes_ptr,
                        matched_num,
                        max_block_size,
                    );
                    if match_count == 0 {
                        break;
                    }

                    for _ in 0..match_count {
                        local_probe_indexes[matched_num] = i as u32;
                        matched_num += 1;
                    }

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
        }

        if matched_num == 0 {
            return Ok(vec![]);
        }

        let probe_block = if is_probe_projected {
            Some(DataBlock::take(
                input,
                &local_probe_indexes[0..matched_num],
            )?)
        } else {
            None
        };
        let build_block = if is_build_projected {
            Some(self.hash_join_state.row_space.gather(
                &local_build_indexes[0..matched_num],
                data_blocks,
                build_num_rows,
            )?)
        } else {
            None
        };
        let result_block = self.merge_eq_block(probe_block, build_block, matched_num);

        if !result_block.is_empty() {
            let (bm, all_true, all_false) = self.get_other_filters(
                &result_block,
                self.hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .as_ref()
                    .unwrap(),
            )?;

            if all_true {
                for row_ptr in local_build_indexes.iter().take(matched_num) {
                    outer_scan_map[row_ptr.chunk_index as usize][row_ptr.row_index as usize] = true;
                }
            } else if !all_false {
                // Safe to unwrap.
                let validity = bm.unwrap();
                let mut idx = 0;
                while idx < matched_num {
                    let valid = unsafe { validity.get_bit_unchecked(idx) };
                    if valid {
                        outer_scan_map[local_build_indexes[idx].chunk_index as usize]
                            [local_build_indexes[idx].row_index as usize] = true;
                    }
                    idx += 1;
                }
            }
        }

        Ok(vec![])
    }
}
