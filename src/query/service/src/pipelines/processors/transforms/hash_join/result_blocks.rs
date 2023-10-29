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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Value;
use common_hashtable::HashJoinHashtableLike;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::sql::planner::plans::JoinType;

impl HashJoinProbeState {
    pub(crate) fn result_blocks<'a, H: HashJoinHashtableLike, IT>(
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
        match self.hash_join_state.hash_join_desc.join_type {
            JoinType::Inner => self.probe_inner_join(
                hash_table,
                probe_state,
                keys_iter,
                pointers,
                input,
                is_probe_projected,
            ),
            JoinType::LeftSemi => {
                if self
                    .hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .is_none()
                {
                    self.left_semi_anti_join::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                    )
                } else {
                    self.left_semi_anti_join_with_conjunct::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                }
            }
            JoinType::LeftAnti => {
                if self
                    .hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .is_none()
                {
                    self.left_semi_anti_join::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                    )
                } else {
                    self.left_semi_anti_join_with_conjunct::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                }
            }
            JoinType::RightSemi => {
                if self
                    .hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .is_none()
                {
                    self.probe_right_semi_join::<_, _>(hash_table, probe_state, keys_iter, pointers)
                } else {
                    self.probe_right_semi_join_with_conjunct::<_, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                }
            }
            JoinType::RightAnti => {
                if self
                    .hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .is_none()
                {
                    self.probe_right_anti_join::<_, _>(hash_table, probe_state, keys_iter, pointers)
                } else {
                    self.probe_right_anti_join_with_conjunct::<_, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                }
            }
            // Single join is similar to left join, but the result is a single row.
            JoinType::Left | JoinType::LeftSingle | JoinType::Full => {
                if self
                    .hash_join_state
                    .hash_join_desc
                    .other_predicate
                    .is_none()
                {
                    self.probe_left_join::<_, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                } else {
                    self.probe_left_join_with_conjunct::<_, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        pointers,
                        input,
                        is_probe_projected,
                    )
                }
            }
            JoinType::Right | JoinType::RightSingle => self.probe_right_join::<_, _>(
                hash_table,
                probe_state,
                keys_iter,
                pointers,
                input,
                is_probe_projected,
            ),
            // Three cases will produce Mark join:
            // 1. uncorrelated ANY subquery: only have one kind of join condition, equi-condition or non-equi-condition.
            // 2. correlated ANY subquery: must have two kinds of join condition, one is equi-condition and the other is non-equi-condition.
            //    equi-condition is subquery's outer columns with subquery's derived columns.
            //    non-equi-condition is subquery's child expr with subquery's output column.
            //    for example: select * from t1 where t1.a = ANY (select t2.a from t2 where t2.b = t1.b); [t1: a, b], [t2: a, b]
            //    subquery's outer columns: t1.b, and it'll derive a new column: subquery_5 when subquery cross join t1;
            //    so equi-condition is t1.b = subquery_5, and non-equi-condition is t1.a = t2.a.
            // 3. Correlated Exists subquery： only have one kind of join condition, equi-condition.
            //    equi-condition is subquery's outer columns with subquery's derived columns. (see the above example in correlated ANY subquery)
            JoinType::LeftMark => match self
                .hash_join_state
                .hash_join_desc
                .other_predicate
                .is_none()
            {
                true => {
                    self.probe_left_mark_join(hash_table, probe_state, keys_iter, pointers, input)
                }
                false => self.probe_left_mark_join_with_conjunct(
                    hash_table,
                    probe_state,
                    keys_iter,
                    pointers,
                    input,
                    is_probe_projected,
                ),
            },
            JoinType::RightMark => match self
                .hash_join_state
                .hash_join_desc
                .other_predicate
                .is_none()
            {
                true => self.probe_right_mark_join(
                    hash_table,
                    probe_state,
                    keys_iter,
                    pointers,
                    input,
                    is_probe_projected,
                ),
                false => self.probe_right_mark_join_with_conjunct(
                    hash_table,
                    probe_state,
                    keys_iter,
                    pointers,
                    input,
                    is_probe_projected,
                ),
            },
            _ => Err(ErrorCode::Unimplemented(format!(
                "{} is unimplemented",
                self.hash_join_state.hash_join_desc.join_type
            ))),
        }
    }

    pub(crate) fn left_fast_return(
        &self,
        input: DataBlock,
        is_probe_projected: bool,
        true_validity: &Bitmap,
    ) -> Result<Vec<DataBlock>> {
        if self.hash_join_state.hash_join_desc.join_type == JoinType::LeftAnti {
            return Ok(vec![input]);
        }
        let input_num_rows = input.num_rows();
        let is_build_projected = self
            .hash_join_state
            .is_build_projected
            .load(Ordering::Relaxed);
        let probe_block = if is_probe_projected {
            if matches!(
                self.hash_join_state.hash_join_desc.join_type,
                JoinType::Full
            ) {
                let nullable_columns = input
                    .columns()
                    .iter()
                    .map(|c| wrap_true_validity(c, input.num_rows(), true_validity))
                    .collect::<Vec<_>>();
                Some(DataBlock::new(nullable_columns, input.num_rows()))
            } else {
                Some(input)
            }
        } else {
            None
        };
        let build_block = if is_build_projected {
            let null_build_block = DataBlock::new(
                self.hash_join_state
                    .row_space
                    .build_schema
                    .fields()
                    .iter()
                    .map(|df| BlockEntry {
                        data_type: df.data_type().clone(),
                        value: Value::Scalar(Scalar::Null),
                    })
                    .collect(),
                input_num_rows,
            );
            Some(null_build_block)
        } else {
            None
        };
        let result_block = self.merge_eq_block(probe_block, build_block, input_num_rows);

        Ok(vec![result_block])
    }
}
