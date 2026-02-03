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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::KeyAccessor;
use databend_common_expression::Scalar;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashtableLike;
use crate::sql::planner::plans::JoinType;

impl HashJoinProbeState {
    /// The left/right single join is similar to left/right join, but the result is a single row.
    ///
    /// Three cases will produce Mark join:
    /// 1. uncorrelated ANY subquery: only have one kind of join condition, equi-condition or non-equi-condition.
    /// 2. correlated ANY subquery: must have two kinds of join condition, one is equi-condition and the other is non-equi-condition.
    ///    equi-condition is subquery's outer columns with subquery's derived columns.
    ///    non-equi-condition is subquery's child expr with subquery's output column.
    ///    for example: select * from t1 where t1.a = ANY (select t2.a from t2 where t2.b = t1.b); [t1: a, b], [t2: a, b]
    ///    subquery's outer columns: t1.b, and it'll derive a new column: subquery_5 when subquery cross join t1;
    ///    so equi-condition is t2.b = subquery_5, and non-equi-condition is t1.a = t2.a.
    /// 3. Correlated Exists subquery： only have one kind of join condition, equi-condition.
    ///    equi-condition is subquery's outer columns with subquery's derived columns. (see the above example in correlated ANY subquery)
    pub(crate) fn result_blocks<'a, H: HashJoinHashtableLike>(
        &self,
        probe_state: &mut ProbeState,
        keys: Box<dyn KeyAccessor<Key = H::Key>>,
        hash_table: &H,
    ) -> Result<Vec<DataBlock>>
    where
        H::Key: 'a,
    {
        if matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::InnerAny | JoinType::RightAny
        ) {
            let build_num_rows = unsafe {
                (*self.hash_join_state.build_state.get())
                    .generation_state
                    .build_num_rows
            };

            probe_state.used_once = Some(MutableBitmap::from_len_zeroed(build_num_rows))
        }
        let no_other_predicate = self
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .is_none();
        match self.hash_join_state.hash_join_desc.join_type {
            JoinType::Inner | JoinType::InnerAny => {
                match self.hash_join_state.hash_join_desc.single_to_inner {
                    Some(JoinType::LeftSingle) => {
                        self.inner_join::<_, true, false>(probe_state, keys, hash_table)
                    }
                    Some(JoinType::RightSingle) => {
                        self.inner_join::<_, false, true>(probe_state, keys, hash_table)
                    }
                    _ => self.inner_join::<_, false, false>(probe_state, keys, hash_table),
                }
            }
            JoinType::Left | JoinType::LeftAny | JoinType::Full => {
                if no_other_predicate {
                    self.left_join::<_, false>(probe_state, keys, hash_table)
                } else {
                    self.left_join_with_conjunct::<_, false>(probe_state, keys, hash_table)
                }
            }
            JoinType::LeftSingle => {
                if no_other_predicate {
                    self.left_join::<_, true>(probe_state, keys, hash_table)
                } else {
                    self.left_join_with_conjunct::<_, true>(probe_state, keys, hash_table)
                }
            }
            JoinType::LeftSemi => {
                if no_other_predicate {
                    self.left_semi_join(probe_state, keys, hash_table)
                } else {
                    self.left_semi_join_with_conjunct(probe_state, keys, hash_table)
                }
            }
            JoinType::LeftAnti => {
                if no_other_predicate {
                    self.left_anti_join(probe_state, keys, hash_table)
                } else {
                    self.left_anti_join_with_conjunct(probe_state, keys, hash_table)
                }
            }
            JoinType::LeftMark => {
                if no_other_predicate {
                    self.left_mark_join(probe_state, keys, hash_table)
                } else {
                    self.left_mark_join_with_conjunct(probe_state, keys, hash_table)
                }
            }
            JoinType::Right | JoinType::RightAny | JoinType::RightSingle => {
                self.probe_right_join(probe_state, keys, hash_table)
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                if no_other_predicate {
                    self.right_semi_anti_join(probe_state, keys, hash_table)
                } else {
                    self.right_semi_anti_join_with_conjunct(probe_state, keys, hash_table)
                }
            }
            JoinType::RightMark => {
                if no_other_predicate {
                    self.right_mark_join(probe_state, keys, hash_table)
                } else {
                    self.right_mark_join_with_conjunct(probe_state, keys, hash_table)
                }
            }
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
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
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
        let build_block = if build_state.generation_state.is_build_projected {
            let entries = self.hash_join_state.build_schema.fields().iter().map(|df| {
                BlockEntry::new_const_column(df.data_type().clone(), Scalar::Null, input_num_rows)
            });
            Some(DataBlock::from_iter(entries, input_num_rows))
        } else {
            None
        };
        let result_block = self.merge_eq_block(probe_block, build_block, input_num_rows);

        Ok(vec![result_block])
    }
}
