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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashJoinHashtableLike;

use super::JoinHashTable;
use super::ProbeState;
use crate::sql::planner::plans::JoinType;

impl JoinHashTable {
    pub(crate) fn result_blocks<'a, H: HashJoinHashtableLike, IT>(
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
        match self.hash_join_desc.join_type {
            JoinType::Inner => self.probe_inner_join(hash_table, probe_state, keys_iter, input),
            JoinType::LeftSemi => {
                self.probe_left_semi_join(hash_table, probe_state, keys_iter, input)
            }
            JoinType::LeftAnti => {
                self.probe_left_anti_semi_join(hash_table, probe_state, keys_iter, input)
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                self.probe_right_join::<_, _>(hash_table, probe_state, keys_iter, input)
            }
            // Single join is similar to left join, but the result is a single row.
            JoinType::Left | JoinType::Single | JoinType::Full => {
                if self.hash_join_desc.other_predicate.is_none() {
                    self.probe_left_join::<false, _, _>(hash_table, probe_state, keys_iter, input)
                } else {
                    self.probe_left_join::<true, _, _>(hash_table, probe_state, keys_iter, input)
                }
            }
            JoinType::Right => {
                self.probe_right_join::<_, _>(hash_table, probe_state, keys_iter, input)
            }
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
            JoinType::LeftMark => match self.hash_join_desc.other_predicate.is_none() {
                true => self.probe_left_mark_join(hash_table, probe_state, keys_iter, input),
                false => self.probe_left_mark_join_with_conjunct(
                    hash_table,
                    probe_state,
                    keys_iter,
                    input,
                ),
            },
            JoinType::RightMark => match self.hash_join_desc.other_predicate.is_none() {
                true => self.probe_right_mark_join(hash_table, probe_state, keys_iter, input),
                false => self.probe_right_mark_join_with_conjunct(
                    hash_table,
                    probe_state,
                    keys_iter,
                    input,
                ),
            },
            _ => Err(ErrorCode::Unimplemented(format!(
                "{} is unimplemented",
                self.hash_join_desc.join_type
            ))),
        }
    }
}
