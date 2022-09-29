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

use common_exception::Result;
use common_planner::ColumnSet;
use common_planner::IndexType;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Filter;
use crate::sql::plans::JoinType;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

pub struct RuleEliminateOuterJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RuleEliminateOuterJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateOuterJoin,
            // Filter
            // \
            // Inner Join
            //  /  \
            // *    *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalInnerJoin,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn find_nullable_columns(
        &self,
        predicate: &Scalar,
        left_output_columns: &ColumnSet,
        right_output_columns: &ColumnSet,
        nullable_columns: &mut Vec<IndexType>,
    ) -> Result<()> {
        match predicate {
            Scalar::BoundColumnRef(column_binding) => {
                nullable_columns.push(column_binding.column.index);
            }
            Scalar::OrExpr(expr) => {
                let mut left_cols = vec![];
                let mut right_cols = vec![];
                self.find_nullable_columns(
                    &expr.left,
                    left_output_columns,
                    right_output_columns,
                    &mut left_cols,
                )?;
                self.find_nullable_columns(
                    &expr.right,
                    left_output_columns,
                    right_output_columns,
                    &mut right_cols,
                )?;
                if !left_cols.is_empty() && !right_cols.is_empty() {
                    for left_col in left_cols.iter() {
                        for right_col in right_cols.iter() {
                            if (left_output_columns.contains(left_col)
                                && left_output_columns.contains(right_col))
                                || (right_output_columns.contains(left_col)
                                    && right_output_columns.contains(right_col))
                            {
                                nullable_columns.push(*left_col);
                                break;
                            }
                        }
                    }
                }
            }
            Scalar::ComparisonExpr(expr) => {
                // For any comparison expr, if input is null, the compare result is false
                self.find_nullable_columns(
                    &expr.left,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
                self.find_nullable_columns(
                    &expr.right,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
            }
            Scalar::CastExpr(expr) => {
                self.find_nullable_columns(
                    &expr.argument,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
            }
            // `predicate` can't be `Scalar::AndExpr`
            // because `Scalar::AndExpr` had been split in binder
            _ => {}
        }
        Ok(())
    }
}

impl Rule for RuleEliminateOuterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut join: LogicalInnerJoin = s_expr.child(0)?.plan().clone().try_into()?;
        let origin_join_type = join.join_type.clone();
        let s_join_expr = s_expr.child(0)?;
        let join_expr = RelExpr::with_s_expr(s_join_expr);
        let left_child_output_column = join_expr.derive_relational_prop_child(0)?.output_columns;
        let right_child_output_column = join_expr.derive_relational_prop_child(1)?.output_columns;
        let predicates = &filter.predicates;
        let mut nullable_columns: Vec<IndexType> = vec![];
        for predicate in predicates {
            self.find_nullable_columns(
                predicate,
                &left_child_output_column,
                &right_child_output_column,
                &mut nullable_columns,
            )?;
        }

        if join.join_type == JoinType::Left
            || join.join_type == JoinType::Right
            || join.join_type == JoinType::Full
        {
            let mut left_join = false;
            let mut right_join = false;
            for col in nullable_columns.iter() {
                if left_child_output_column.contains(col) {
                    right_join = true;
                }
                if right_child_output_column.contains(col) {
                    left_join = true;
                }
            }

            match join.join_type {
                JoinType::Left => {
                    if left_join {
                        join.join_type = JoinType::Inner
                    }
                }
                JoinType::Right => {
                    if right_join {
                        join.join_type = JoinType::Inner
                    }
                }
                JoinType::Full => {
                    if left_join && right_join {
                        join.join_type = JoinType::Inner
                    } else if left_join {
                        join.join_type = JoinType::Right
                    } else if right_join {
                        join.join_type = JoinType::Left
                    }
                }
                _ => unreachable!(),
            }
        }

        let changed_join_type = join.join_type.clone();
        let mut result = SExpr::create_binary(
            join.into(),
            s_join_expr.child(0)?.clone(),
            s_join_expr.child(1)?.clone(),
        );
        // wrap filter s_expr
        result = SExpr::create_unary(filter.into(), result);

        if changed_join_type != origin_join_type {
            state.add_result(result);
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
