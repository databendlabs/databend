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
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Value;
use common_hashtable::HashJoinHashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::plans::JoinType;

impl JoinHashTable {
    pub(crate) fn probe_left_join<
        'a,
        const WITH_OTHER_CONJUNCT: bool,
        H: HashJoinHashtableLike,
        IT,
    >(
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
        let probe_indexes = &mut probe_state.probe_indexes;
        let local_build_indexes = &mut probe_state.build_indexes;
        let local_build_indexes_ptr = local_build_indexes.as_mut_ptr();
        let input_num_rows = input.num_rows();
        if input_num_rows > JOIN_MAX_BLOCK_SIZE {
            probe_state.row_state = Some(vec![0; input_num_rows]);
        }
        // Safe to unwrap.
        // The row_state is used to record whether a row in probe input is matched.
        let row_state = probe_state.row_state.as_mut().unwrap();
        let mut dummy_row_state_indexes = vec![];
        // The row_state_indexes[idx] = i records the row_state[i] has been increased 1 by the idx,
        // if idx is filtered by other conditions, we will set row_state[idx] = row_state[idx] - 1.
        let row_state_indexes = if WITH_OTHER_CONJUNCT {
            // Safe to unwrap.
            probe_state.row_state_indexes.as_mut().unwrap()
        } else {
            &mut dummy_row_state_indexes
        };

        let mut matched_num = 0;
        let mut probe_indexes_occupied = 0;
        let mut result_blocks = vec![];

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let build_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());
        let outer_scan_bitmap = unsafe { &mut *self.outer_scan_bitmap.get() };

        // Start to probe hash table.
        for (i, key) in keys_iter.enumerate() {
            let (mut probe_matched, mut incomplete_ptr) =
                if self.hash_join_desc.from_correlated_subquery {
                    hash_table.probe_hash_table(
                        key,
                        local_build_indexes_ptr,
                        matched_num,
                        JOIN_MAX_BLOCK_SIZE,
                    )
                } else {
                    self.probe_key(
                        hash_table,
                        key,
                        valids,
                        i,
                        local_build_indexes_ptr,
                        matched_num,
                    )
                };
            let mut total_probe_matched = 0;
            if probe_matched > 0 {
                total_probe_matched += probe_matched;
                if self.hash_join_desc.join_type == JoinType::Single && total_probe_matched > 1 {
                    return Err(ErrorCode::Internal(
                        "Scalar subquery can't return more than one row",
                    ));
                }

                row_state[i] += probe_matched;
                if !WITH_OTHER_CONJUNCT {
                    matched_num += probe_matched;
                } else {
                    for _ in 0..probe_matched {
                        row_state_indexes[matched_num] = i;
                        matched_num += 1;
                    }
                }
                probe_indexes[probe_indexes_occupied] = (i as u32, probe_matched as u32);
                probe_indexes_occupied += 1;
            }
            if matched_num >= JOIN_MAX_BLOCK_SIZE || i == input_num_rows - 1 {
                loop {
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let build_block = self.row_space.gather(
                        &local_build_indexes[0..matched_num],
                        &data_blocks,
                        &build_num_rows,
                    )?;
                    let mut probe_block = DataBlock::take_compacted_indices(
                        input,
                        &probe_indexes[0..probe_indexes_occupied],
                        matched_num,
                    )?;

                    // For left join, wrap nullable for build block
                    let (nullable_columns, num_rows) = if self.row_space.datablocks().is_empty() {
                        (
                            build_block
                                .columns()
                                .iter()
                                .map(|c| BlockEntry {
                                    value: Value::Scalar(Scalar::Null),
                                    data_type: c.data_type.wrap_nullable(),
                                })
                                .collect::<Vec<_>>(),
                            matched_num,
                        )
                    } else if matched_num == JOIN_MAX_BLOCK_SIZE {
                        (
                            build_block
                                .columns()
                                .iter()
                                .map(|c| Self::set_validity(c, JOIN_MAX_BLOCK_SIZE, true_validity))
                                .collect::<Vec<_>>(),
                            JOIN_MAX_BLOCK_SIZE,
                        )
                    } else {
                        let mut validity = MutableBitmap::new();
                        validity.extend_constant(matched_num, true);
                        let validity: Bitmap = validity.into();
                        (
                            build_block
                                .columns()
                                .iter()
                                .map(|c| Self::set_validity(c, matched_num, &validity))
                                .collect::<Vec<_>>(),
                            matched_num,
                        )
                    };
                    let nullable_build_block = DataBlock::new(nullable_columns, num_rows);

                    // For full join, wrap nullable for probe block
                    if self.hash_join_desc.join_type == JoinType::Full {
                        let nullable_probe_columns = if matched_num == JOIN_MAX_BLOCK_SIZE {
                            probe_block
                                .columns()
                                .iter()
                                .map(|c| Self::set_validity(c, JOIN_MAX_BLOCK_SIZE, true_validity))
                                .collect::<Vec<_>>()
                        } else {
                            let mut validity = MutableBitmap::new();
                            validity.extend_constant(matched_num, true);
                            let validity: Bitmap = validity.into();
                            probe_block
                                .columns()
                                .iter()
                                .map(|c| Self::set_validity(c, matched_num, &validity))
                                .collect::<Vec<_>>()
                        };
                        probe_block = DataBlock::new(nullable_probe_columns, matched_num);
                    }

                    let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

                    if !merged_block.is_empty() {
                        if !WITH_OTHER_CONJUNCT {
                            result_blocks.push(merged_block);
                            if self.hash_join_desc.join_type == JoinType::Full {
                                for row_ptr in local_build_indexes.iter().take(matched_num) {
                                    outer_scan_bitmap[row_ptr.chunk_index]
                                        .set(row_ptr.row_index, true);
                                }
                            }
                        } else {
                            let (bm, all_true, all_false) = self.get_other_filters(
                                &merged_block,
                                self.hash_join_desc.other_predicate.as_ref().unwrap(),
                            )?;

                            if all_true {
                                result_blocks.push(merged_block);
                                if self.hash_join_desc.join_type == JoinType::Full {
                                    for row_ptr in local_build_indexes.iter().take(matched_num) {
                                        outer_scan_bitmap[row_ptr.chunk_index]
                                            .set(row_ptr.row_index, true);
                                    }
                                }
                            } else if all_false {
                                let mut idx = 0;
                                while idx < matched_num {
                                    row_state[row_state_indexes[idx]] -= 1;
                                    idx += 1;
                                }
                            } else {
                                // Safe to unwrap.
                                let validity = bm.unwrap();
                                if self.hash_join_desc.join_type == JoinType::Full {
                                    let mut idx = 0;
                                    while idx < matched_num {
                                        let valid = unsafe { validity.get_bit_unchecked(idx) };
                                        if valid {
                                            outer_scan_bitmap[local_build_indexes[idx].chunk_index]
                                                .set(local_build_indexes[idx].row_index, true);
                                        } else {
                                            row_state[row_state_indexes[idx]] -= 1;
                                        }
                                        idx += 1;
                                    }
                                } else {
                                    let mut idx = 0;
                                    while idx < matched_num {
                                        let valid = unsafe { validity.get_bit_unchecked(idx) };
                                        if !valid {
                                            row_state[row_state_indexes[idx]] -= 1;
                                        }
                                        idx += 1;
                                    }
                                }
                                let filtered_block =
                                    DataBlock::filter_with_bitmap(merged_block, &validity)?;
                                result_blocks.push(filtered_block);
                            }
                        }
                    }

                    matched_num = 0;
                    probe_indexes_occupied = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (probe_matched, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        local_build_indexes_ptr,
                        matched_num,
                        JOIN_MAX_BLOCK_SIZE,
                    );

                    total_probe_matched += probe_matched;
                    if self.hash_join_desc.join_type == JoinType::Single && total_probe_matched > 1
                    {
                        return Err(ErrorCode::Internal(
                            "Scalar subquery can't return more than one row",
                        ));
                    }

                    row_state[i] += probe_matched;
                    if !WITH_OTHER_CONJUNCT {
                        matched_num += probe_matched;
                    } else {
                        for _ in 0..probe_matched {
                            row_state_indexes[matched_num] = i;
                            matched_num += 1;
                        }
                    }
                    probe_indexes[probe_indexes_occupied] = (i as u32, probe_matched as u32);
                    probe_indexes_occupied += 1;

                    if matched_num < JOIN_MAX_BLOCK_SIZE && i != input_num_rows - 1 {
                        break;
                    }
                }
            }
        }

        probe_indexes_occupied = 0;
        let mut idx = 0;
        while idx < input_num_rows {
            if row_state[idx] == 0 {
                probe_indexes[probe_indexes_occupied] = (idx as u32, 1);
                probe_indexes_occupied += 1;
            }
            row_state[idx] = 0;
            idx += 1;
        }

        let null_build_block = DataBlock::new(
            self.row_space
                .data_schema
                .fields()
                .iter()
                .map(|df| BlockEntry {
                    data_type: df.data_type().clone(),
                    value: Value::Scalar(Scalar::Null),
                })
                .collect(),
            probe_indexes_occupied,
        );

        let mut probe_block = DataBlock::take_compacted_indices(
            input,
            &probe_indexes[0..probe_indexes_occupied],
            probe_indexes_occupied,
        )?;

        // For full join, wrap nullable for probe block
        if self.hash_join_desc.join_type == JoinType::Full {
            let num_rows = probe_block.num_rows();
            let nullable_probe_columns = probe_block
                .columns()
                .iter()
                .map(|c| {
                    let mut probe_validity = MutableBitmap::new();
                    probe_validity.extend_constant(num_rows, true);
                    let probe_validity: Bitmap = probe_validity.into();
                    Self::set_validity(c, num_rows, &probe_validity)
                })
                .collect::<Vec<_>>();
            probe_block = DataBlock::new(nullable_probe_columns, num_rows);
        }

        let merged_block = self.merge_eq_block(&null_build_block, &probe_block)?;

        if !merged_block.is_empty() {
            result_blocks.push(merged_block);
        }

        Ok(result_blocks)
    }
}
