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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::HashJoinHashtableLike;
use common_sql::executor::cast_expr_to_non_null_boolean;

use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_inner_join<'a, H: HashJoinHashtableLike, IT>(
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
        let max_block_size = probe_state.max_block_size;
        let valids = &probe_state.valids;
        // The inner join will return multiple data blocks of similar size.
        let mut occupied = 0;
        let mut probed_blocks = vec![];
        let mut probe_indexes_len = 0;
        let probe_indexes = &mut probe_state.probe_indexes;
        let build_indexes = &mut probe_state.build_indexes;
        let build_indexes_ptr = build_indexes.as_mut_ptr();

        let data_blocks = self.row_space.chunks.read();
        let data_blocks = data_blocks
            .iter()
            .map(|c| &c.data_block)
            .collect::<Vec<_>>();
        let build_num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());
        let is_build_projected = self.is_build_projected.load(Ordering::Relaxed);

        for (i, key) in keys_iter.enumerate() {
            // If the join is derived from correlated subquery, then null equality is safe.
            let (mut match_count, mut incomplete_ptr) =
                if self.hash_join_desc.from_correlated_subquery {
                    hash_table.probe_hash_table(key, build_indexes_ptr, occupied, max_block_size)
                } else {
                    self.probe_key(
                        hash_table,
                        key,
                        valids,
                        i,
                        build_indexes_ptr,
                        occupied,
                        max_block_size,
                    )
                };
            if match_count == 0 {
                continue;
            }

            occupied += match_count;
            probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
            probe_indexes_len += 1;
            if occupied >= max_block_size {
                loop {
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let probe_block = if !input.is_empty() {
                        Some(DataBlock::take_compacted_indices(
                            input,
                            &probe_indexes[0..probe_indexes_len],
                            occupied,
                        )?)
                    } else {
                        None
                    };
                    let build_block = if is_build_projected {
                        Some(
                            self.row_space
                                .gather(build_indexes, &data_blocks, &build_num_rows)?,
                        )
                    } else {
                        None
                    };
                    let result_block = self.merge_eq_block(build_block, probe_block);

                    probed_blocks.push(result_block);

                    probe_indexes_len = 0;
                    occupied = 0;

                    if incomplete_ptr == 0 {
                        break;
                    }
                    (match_count, incomplete_ptr) = hash_table.next_incomplete_ptr(
                        key,
                        incomplete_ptr,
                        build_indexes_ptr,
                        occupied,
                        max_block_size,
                    );
                    if match_count == 0 {
                        break;
                    }

                    occupied += match_count;
                    probe_indexes[probe_indexes_len] = (i as u32, match_count as u32);
                    probe_indexes_len += 1;

                    if occupied < max_block_size {
                        break;
                    }
                }
            }
        }

        if occupied > 0 {
            let probe_block = if !input.is_empty() {
                Some(DataBlock::take_compacted_indices(
                    input,
                    &probe_indexes[0..probe_indexes_len],
                    occupied,
                )?)
            } else {
                None
            };
            let build_block = if is_build_projected {
                Some(self.row_space.gather(
                    &build_indexes[0..occupied],
                    &data_blocks,
                    &build_num_rows,
                )?)
            } else {
                None
            };
            let result_block = self.merge_eq_block(build_block, probe_block);

            probed_blocks.push(result_block);
        }

        match &self.hash_join_desc.other_predicate {
            None => Ok(probed_blocks),
            Some(other_predicate) => {
                // Wrap `is_true` to `other_predicate`
                let other_predicate = cast_expr_to_non_null_boolean(other_predicate.clone())?;
                assert_eq!(other_predicate.data_type(), &DataType::Boolean);

                let func_ctx = self.ctx.get_function_context()?;
                let mut filtered_blocks = Vec::with_capacity(probed_blocks.len());

                for probed_block in probed_blocks {
                    if self.interrupt.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let evaluator = Evaluator::new(&probed_block, &func_ctx, &BUILTIN_FUNCTIONS);
                    let predicate = evaluator
                        .run(&other_predicate)?
                        .try_downcast::<BooleanType>()
                        .unwrap();
                    let res = probed_block.filter_boolean_value(&predicate)?;
                    if !res.is_empty() {
                        filtered_blocks.push(res);
                    }
                }

                Ok(filtered_blocks)
            }
        }
    }
}
