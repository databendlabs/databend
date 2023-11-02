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
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::HashJoinHashtableLike;
use common_sql::executor::cast_expr_to_non_null_boolean;

use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;

impl HashJoinProbeState {
    pub(crate) fn probe_inner_join<'a, H: HashJoinHashtableLike, IT>(
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
        // The inner join will return multiple data blocks of similar size.
        let mut matched_num = 0;
        let mut result_blocks = vec![];
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();
        let string_items_buf = &mut probe_state.string_items_buf;

        let build_columns = unsafe { &*self.hash_join_state.build_columns.get() };
        let build_columns_data_type =
            unsafe { &*self.hash_join_state.build_columns_data_type.get() };
        let build_num_rows = unsafe { &*self.hash_join_state.build_num_rows.get() };
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);

        for (i, (key, ptr)) in keys_iter.zip(pointers.iter()).enumerate() {
            // If the join is derived from correlated subquery, then null equality is safe.
            let (mut match_count, mut incomplete_ptr) =
                if self.hash_join_state.hash_join_desc.from_correlated_subquery
                    || valids.map_or(true, |v| v.get_bit(i))
                {
                    hash_table.next_probe(key, *ptr, build_indexes_ptr, matched_num, max_block_size)
                } else {
                    continue;
                };

            if match_count == 0 {
                continue;
            }

            for _ in 0..match_count {
                probe_indexes[matched_num] = i as u32;
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
                        Some(DataBlock::take(input, probe_indexes, string_items_buf)?)
                    } else {
                        None
                    };
                    let build_block = if is_build_projected {
                        Some(self.hash_join_state.row_space.gather(
                            build_indexes,
                            build_columns,
                            build_columns_data_type,
                            build_num_rows,
                            string_items_buf,
                        )?)
                    } else {
                        None
                    };
                    let mut result_block =
                        self.merge_eq_block(probe_block, build_block, matched_num);
                    if !self.hash_join_state.probe_to_build.is_empty() {
                        for (index, (is_probe_nullable, is_build_nullable)) in
                            self.hash_join_state.probe_to_build.iter()
                        {
                            let entry = match (is_probe_nullable, is_build_nullable) {
                                (true, true) | (false, false) => {
                                    result_block.get_by_offset(*index).clone()
                                }
                                (true, false) => {
                                    result_block.get_by_offset(*index).clone().remove_nullable()
                                }
                                (false, true) => wrap_true_validity(
                                    result_block.get_by_offset(*index),
                                    result_block.num_rows(),
                                    &probe_state.true_validity,
                                ),
                            };
                            result_block.add_column(entry);
                        }
                    }

                    result_blocks.push(result_block);
                    matched_num = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_probe(
                        key,
                        incomplete_ptr,
                        build_indexes_ptr,
                        matched_num,
                        max_block_size,
                    );
                    if match_count == 0 {
                        break;
                    }

                    for _ in 0..match_count {
                        probe_indexes[matched_num] = i as u32;
                        matched_num += 1;
                    }

                    if matched_num < max_block_size {
                        break;
                    }
                }
            }
        }

        if matched_num > 0 {
            let probe_block = if is_probe_projected {
                Some(DataBlock::take(
                    input,
                    &probe_indexes[0..matched_num],
                    string_items_buf,
                )?)
            } else {
                None
            };
            let build_block = if is_build_projected {
                Some(self.hash_join_state.row_space.gather(
                    &build_indexes[0..matched_num],
                    build_columns,
                    build_columns_data_type,
                    build_num_rows,
                    string_items_buf,
                )?)
            } else {
                None
            };
            let mut result_block = self.merge_eq_block(probe_block, build_block, matched_num);
            if !self.hash_join_state.probe_to_build.is_empty() {
                for (index, (is_probe_nullable, is_build_nullable)) in
                    self.hash_join_state.probe_to_build.iter()
                {
                    let entry = match (is_probe_nullable, is_build_nullable) {
                        (true, true) | (false, false) => result_block.get_by_offset(*index).clone(),
                        (true, false) => {
                            result_block.get_by_offset(*index).clone().remove_nullable()
                        }
                        (false, true) => wrap_true_validity(
                            result_block.get_by_offset(*index),
                            result_block.num_rows(),
                            &probe_state.true_validity,
                        ),
                    };
                    result_block.add_column(entry);
                }
            }

            result_blocks.push(result_block);
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
}
