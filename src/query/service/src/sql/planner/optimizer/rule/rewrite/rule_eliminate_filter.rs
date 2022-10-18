// Copyright 2021 Datafuse Labs.
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

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::TransformResult;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::Filter;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

pub struct RuleEliminateFilter {
    id: RuleID,
    pattern: SExpr,
}

impl RuleEliminateFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateFilter,
            // Filter
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleEliminateFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut predicates = Vec::with_capacity(filter.predicates.len());
        // Delete the predicate if the predicate is `a = a` syntax.
        for predicate in filter.predicates.iter() {
            if let Scalar::ComparisonExpr(ComparisonExpr {
                left, right, op, ..
            }) = predicate
            {
                if op == &ComparisonOp::Equal {
                    if let Scalar::BoundColumnRef(BoundColumnRef {
                        column: left_column,
                    }) = &**left
                    {
                        if let Scalar::BoundColumnRef(BoundColumnRef {
                            column: right_column,
                        }) = &**right
                        {
                            if left_column.index == right_column.index {
                                continue;
                            }
                        }
                    }
                }
            }
            predicates.push(predicate.clone());
        }

        if predicates.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
