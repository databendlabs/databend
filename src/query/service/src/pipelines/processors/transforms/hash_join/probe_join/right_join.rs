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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_right_join<'a, H: HashJoinHashtableLike, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;
        let true_validity = &probe_state.true_validity;
        let local_probe_indexes = &mut probe_state.probe_indexes;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();

        let mut matched_num = 0;
        let mut probed_blocks = vec![];
        let mut probe_indexes_len = 0;

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let total_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());
        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };

        for (i, key) in keys_iter.enumerate() {
            let (mut match_count, mut incomplete_ptr) = self.probe_key(
                hash_table,
                key,
                valids,
                i,
                local_build_indexes_ptr,
                matched_num,
            );
            if match_count == 0 {
                continue;
            }
            matched_num += match_count;
            local_probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
            probe_indexes_len += 1;
            if matched_num >= JOIN_MAX_BLOCK_SIZE {
                loop {
                    // The matched_num must be equal to JOIN_MAX_BLOCK_SIZE.
                    debug_assert_eq!(matched_num, JOIN_MAX_BLOCK_SIZE);
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let build_block = self.row_space.gather(
                        local_build_indexes,
                        &data_blocks,
                        &total_num_rows,
                    )?;
                    let mut probe_block = DataBlock::take_compacted_indices(
                        input,
                        &local_probe_indexes[0..probe_indexes_len],
                        JOIN_MAX_BLOCK_SIZE,
                    )?;

                    // The join type is right join, we need to wrap nullable for probe side.
                    let nullable_columns = probe_block
                        .columns()
                        .iter()
                        .map(|c| Self::set_validity(c, JOIN_MAX_BLOCK_SIZE, true_validity))
                        .collect::<Vec<_>>();
                    probe_block = DataBlock::new(nullable_columns, JOIN_MAX_BLOCK_SIZE);

                    if !probe_block.is_empty() {
                        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;
                        if self.hash_join_desc.other_predicate.is_none() {
                            probed_blocks.push(merged_block);
                            for row_ptr in local_build_indexes.iter() {
                                outer_scan_bitmap[row_ptr.chunk_index].set(row_ptr.row_index, true);
                            }
                        } else {
                            let (bm, all_true, all_false) = self.get_other_filters(
                                &merged_block,
                                self.hash_join_desc.other_predicate.as_ref().unwrap(),
                            )?;

                            if all_true {
                                probed_blocks.push(merged_block);
                                for row_ptr in local_build_indexes.iter() {
                                    outer_scan_bitmap[row_ptr.chunk_index]
                                        .set(row_ptr.row_index, true);
                                }
                            } else if !all_false {
                                // Safe to unwrap.
                                let validity = bm.unwrap();
                                let mut idx = 0;
                                while idx < JOIN_MAX_BLOCK_SIZE {
                                    let valid = unsafe { validity.get_bit_unchecked(idx) };
                                    if valid {
                                        outer_scan_bitmap[local_build_indexes[idx].chunk_index]
                                            .set(local_build_indexes[idx].row_index, true);
                                    }
                                    idx += 1;
                                }
                                let filtered_block =
                                    DataBlock::filter_with_bitmap(merged_block, &validity)?;
                                if !filtered_block.is_empty() {
                                    probed_blocks.push(filtered_block);
                                }
                            }
                        }
                    }

                    probe_indexes_len = 0;
                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        local_build_indexes_ptr,
                        matched_num,
                        JOIN_MAX_BLOCK_SIZE,
                    );
                    if match_count == 0 {
                        break;
                    }

                    matched_num += match_count;
                    local_probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
                    probe_indexes_len += 1;

                    if matched_num < JOIN_MAX_BLOCK_SIZE {
                        break;
                    }
                }
            }
        }

        let build_block = self.row_space.gather(
            &local_build_indexes[0..matched_num],
            &data_blocks,
            &total_num_rows,
        )?;
        let mut probe_block = DataBlock::take_compacted_indices(
            input,
            &local_probe_indexes[0..probe_indexes_len],
            matched_num,
        )?;

        // The join type is right join, we need to wrap nullable for probe side.
        let mut validity = MutableBitmap::new();
        validity.extend_constant(matched_num, true);
        let validity: Bitmap = validity.into();
        let nullable_columns = probe_block
            .columns()
            .iter()
            .map(|c| Self::set_validity(c, probe_block.num_rows(), &validity))
            .collect::<Vec<_>>();
        probe_block = DataBlock::new(nullable_columns, validity.len());

        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        if !merged_block.is_empty() {
            if self.hash_join_desc.other_predicate.is_none() {
                probed_blocks.push(merged_block);
                for row_ptr in local_build_indexes.iter().take(matched_num) {
                    outer_scan_bitmap[row_ptr.chunk_index].set(row_ptr.row_index, true);
                }
            } else {
                let (bm, all_true, all_false) = self.get_other_filters(
                    &merged_block,
                    self.hash_join_desc.other_predicate.as_ref().unwrap(),
                )?;

                if all_true {
                    probed_blocks.push(merged_block);
                    for row_ptr in local_build_indexes.iter().take(matched_num) {
                        outer_scan_bitmap[row_ptr.chunk_index].set(row_ptr.row_index, true);
                    }
                } else if !all_false {
                    // Safe to unwrap.
                    let validity = bm.unwrap();
                    let mut idx = 0;
                    while idx < matched_num {
                        let valid = unsafe { validity.get_bit_unchecked(idx) };
                        if valid {
                            outer_scan_bitmap[local_build_indexes[idx].chunk_index]
                                .set(local_build_indexes[idx].row_index, true);
                        }
                        idx += 1;
                    }
                    let filtered_block = DataBlock::filter_with_bitmap(merged_block, &validity)?;
                    if !filtered_block.is_empty() {
                        probed_blocks.push(filtered_block);
                    }
                }
            }
        }

        Ok(probed_blocks)
    }
}
