// Copyright 2022 Datafuse Labs.
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
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_sql::executor::cast_expr_to_non_null_boolean;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    pub(crate) fn probe_inner_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
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
        // The inner join will return multiple data blocks of similar size.
        let mut probed_num = 0;
        let mut probed_blocks = vec![];
        let mut probe_indexes_len = 0;
        let probe_indexes = &mut probe_state.probe_indexes;
        let mut build_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);

        let data_blocks = self.row_space.datablocks();
        let num_rows = data_blocks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());

        for (i, key) in keys_iter.enumerate() {
            // If the join is derived from correlated subquery, then null equality is safe.
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.entry(key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };

            if let Some(v) = probe_result_ptr {
                let probed_rows = v.get();

                if probed_num + probed_rows.len() < JOIN_MAX_BLOCK_SIZE {
                    build_indexes.extend_from_slice(probed_rows);
                    probe_indexes[probe_indexes_len] = (i as u32, probed_rows.len() as u32);
                    probe_indexes_len += 1;
                    probed_num += probed_rows.len();
                } else {
                    let mut index = 0_usize;
                    let mut remain = probed_rows.len();

                    while index < probed_rows.len() {
                        if probed_num + remain < JOIN_MAX_BLOCK_SIZE {
                            build_indexes.extend_from_slice(&probed_rows[index..]);
                            probe_indexes[probe_indexes_len] = (i as u32, remain as u32);
                            probe_indexes_len += 1;
                            probed_num += remain;
                            index += remain;
                        } else {
                            if self.interrupt.load(Ordering::Relaxed) {
                                return Err(ErrorCode::AbortedQuery(
                                    "Aborted query, because the server is shutting down or the query was killed.",
                                ));
                            }

                            let addition = JOIN_MAX_BLOCK_SIZE - probed_num;
                            let new_index = index + addition;

                            build_indexes.extend_from_slice(&probed_rows[index..new_index]);
                            probe_indexes[probe_indexes_len] = (i as u32, addition as u32);
                            probe_indexes_len += 1;
                            probed_num += addition;

                            probed_blocks.push(
                                self.merge_eq_block(
                                    &self.row_space.gather(
                                        &build_indexes,
                                        &data_blocks,
                                        &num_rows,
                                    )?,
                                    &DataBlock::take_compacted_indices(
                                        input,
                                        &probe_indexes[0..probe_indexes_len],
                                        probed_num,
                                    )?,
                                )?,
                            );

                            index = new_index;
                            remain -= addition;

                            build_indexes.clear();
                            probe_indexes_len = 0;
                            probed_num = 0;
                        }
                    }
                }
            }
        }

        probed_blocks.push(
            self.merge_eq_block(
                &self
                    .row_space
                    .gather(&build_indexes, &data_blocks, &num_rows)?,
                &DataBlock::take_compacted_indices(
                    input,
                    &probe_indexes[0..probe_indexes_len],
                    probed_num,
                )?,
            )?,
        );

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
