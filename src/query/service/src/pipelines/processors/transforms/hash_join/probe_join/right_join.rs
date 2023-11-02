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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;
use common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::sql::plans::JoinType;

impl HashJoinProbeState {
    pub(crate) fn probe_right_join<'a, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        pointers: &[u64],
        input: &DataBlock,
        is_probe_projected: bool,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let max_block_size = probe_state.max_block_size;
        let valids = probe_state.valids.as_ref();
        let true_validity = &probe_state.true_validity;
        let local_probe_indexes = &mut probe_state.probe_indexes;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();
        let string_items_buf = &mut probe_state.string_items_buf;

        let mut matched_num = 0;
        let mut result_blocks = vec![];

        let build_columns = unsafe { &*self.hash_join_state.build_columns.get() };
        let build_columns_data_type =
            unsafe { &*self.hash_join_state.build_columns_data_type.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let outer_scan_map = unsafe { &mut *self.hash_join_state.outer_scan_map.get() };
        let right_single_scan_map =
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
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        for (i, (key, ptr)) in keys_iter.zip(pointers).enumerate() {
            let (mut match_count, mut incomplete_ptr) = if valids.map_or(true, |v| v.get_bit(i)) {
                hash_table.next_probe(
                    key,
                    *ptr,
                    local_build_indexes_ptr,
                    matched_num,
                    max_block_size,
                )
            } else {
                continue;
            };

            if match_count == 0 {
                continue;
            }

            for _ in 0..match_count {
                local_probe_indexes[matched_num] = i as u32;
                matched_num += 1;
            }
            if matched_num >= max_block_size {
                loop {
                    // The matched_num must be equal to max_block_size.
                    debug_assert_eq!(matched_num, max_block_size);
                    if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let probe_block = if is_probe_projected {
                        let probe_block =
                            DataBlock::take(input, local_probe_indexes, string_items_buf)?;

                        // The join type is right join, we need to wrap nullable for probe side.
                        let nullable_columns = probe_block
                            .columns()
                            .iter()
                            .map(|c| wrap_true_validity(c, max_block_size, true_validity))
                            .collect::<Vec<_>>();
                        Some(DataBlock::new(nullable_columns, max_block_size))
                    } else {
                        None
                    };
                    let build_block = if is_build_projected {
                        Some(self.hash_join_state.row_space.gather(
                            local_build_indexes,
                            build_columns,
                            build_columns_data_type,
                            build_num_rows,
                            string_items_buf,
                        )?)
                    } else {
                        None
                    };
                    let result_block =
                        self.merge_eq_block(probe_block, build_block, max_block_size);

                    if !result_block.is_empty() {
                        if self
                            .hash_join_state
                            .hash_join_desc
                            .other_predicate
                            .is_none()
                        {
                            result_blocks.push(result_block);
                            if self.hash_join_state.hash_join_desc.join_type
                                == JoinType::RightSingle
                            {
                                self.update_right_single_scan_map(
                                    local_build_indexes,
                                    &right_single_scan_map,
                                    None,
                                )?;
                            } else {
                                for row_ptr in local_build_indexes.iter() {
                                    outer_scan_map[row_ptr.chunk_index as usize]
                                        [row_ptr.row_index as usize] = true;
                                }
                            }
                        } else {
                            let (bm, all_true, all_false) = self.get_other_filters(
                                &result_block,
                                self.hash_join_state
                                    .hash_join_desc
                                    .other_predicate
                                    .as_ref()
                                    .unwrap(),
                                &self.func_ctx,
                            )?;

                            if all_true {
                                result_blocks.push(result_block);
                                if self.hash_join_state.hash_join_desc.join_type
                                    == JoinType::RightSingle
                                {
                                    self.update_right_single_scan_map(
                                        local_build_indexes,
                                        &right_single_scan_map,
                                        None,
                                    )?;
                                } else {
                                    for row_ptr in local_build_indexes.iter() {
                                        outer_scan_map[row_ptr.chunk_index as usize]
                                            [row_ptr.row_index as usize] = true;
                                    }
                                }
                            } else if !all_false {
                                // Safe to unwrap.
                                let validity = bm.unwrap();
                                if self.hash_join_state.hash_join_desc.join_type
                                    == JoinType::RightSingle
                                {
                                    self.update_right_single_scan_map(
                                        local_build_indexes,
                                        &right_single_scan_map,
                                        Some(&validity),
                                    )?;
                                } else {
                                    let mut idx = 0;
                                    while idx < max_block_size {
                                        let valid = unsafe { validity.get_bit_unchecked(idx) };
                                        if valid {
                                            outer_scan_map
                                                [local_build_indexes[idx].chunk_index as usize]
                                                [local_build_indexes[idx].row_index as usize] =
                                                true;
                                        }
                                        idx += 1;
                                    }
                                }
                                let filtered_block =
                                    DataBlock::filter_with_bitmap(result_block, &validity)?;
                                result_blocks.push(filtered_block);
                            }
                        }
                    }
                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_probe(
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
            return Ok(result_blocks);
        }

        let probe_block = if is_probe_projected {
            let probe_block = DataBlock::take(
                input,
                &local_probe_indexes[0..matched_num],
                string_items_buf,
            )?;

            // The join type is right join, we need to wrap nullable for probe side.
            let nullable_columns = probe_block
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, matched_num, true_validity))
                .collect::<Vec<_>>();
            Some(DataBlock::new(nullable_columns, matched_num))
        } else {
            None
        };
        let build_block = if is_build_projected {
            Some(self.hash_join_state.row_space.gather(
                &local_build_indexes[0..matched_num],
                build_columns,
                build_columns_data_type,
                build_num_rows,
                string_items_buf,
            )?)
        } else {
            None
        };
        let result_block = self.merge_eq_block(probe_block, build_block, matched_num);

        if !result_block.is_empty() {
            if self
                .hash_join_state
                .hash_join_desc
                .other_predicate
                .is_none()
            {
                result_blocks.push(result_block);
                if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                    self.update_right_single_scan_map(
                        &local_build_indexes[0..matched_num],
                        &right_single_scan_map,
                        None,
                    )?;
                } else {
                    for row_ptr in local_build_indexes.iter().take(matched_num) {
                        outer_scan_map[row_ptr.chunk_index as usize][row_ptr.row_index as usize] =
                            true;
                    }
                }
            } else {
                let (bm, all_true, all_false) = self.get_other_filters(
                    &result_block,
                    self.hash_join_state
                        .hash_join_desc
                        .other_predicate
                        .as_ref()
                        .unwrap(),
                    &self.func_ctx,
                )?;

                if all_true {
                    result_blocks.push(result_block);
                    if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                        self.update_right_single_scan_map(
                            &local_build_indexes[0..matched_num],
                            &right_single_scan_map,
                            None,
                        )?;
                    } else {
                        for row_ptr in local_build_indexes.iter().take(matched_num) {
                            outer_scan_map[row_ptr.chunk_index as usize]
                                [row_ptr.row_index as usize] = true;
                        }
                    }
                } else if !all_false {
                    // Safe to unwrap.
                    let validity = bm.unwrap();
                    if self.hash_join_state.hash_join_desc.join_type == JoinType::RightSingle {
                        self.update_right_single_scan_map(
                            &local_build_indexes[0..matched_num],
                            &right_single_scan_map,
                            Some(&validity),
                        )?;
                    } else {
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
                    let filtered_block = DataBlock::filter_with_bitmap(result_block, &validity)?;
                    result_blocks.push(filtered_block);
                }
            }
        }
        Ok(result_blocks)
    }

    fn update_right_single_scan_map(
        &self,
        build_indexes: &[RowPtr],
        right_single_scan_map: &[*mut AtomicBool],
        bitmap: Option<&Bitmap>,
    ) -> Result<()> {
        let dummy_bitmap = Bitmap::new();
        let (has_bitmap, validity) = match bitmap {
            Some(validity) => (true, validity),
            None => (false, &dummy_bitmap),
        };
        for (idx, row_ptr) in build_indexes.iter().enumerate() {
            if has_bitmap && unsafe { !validity.get_bit_unchecked(idx) } {
                continue;
            }
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
        Ok(())
    }
}
