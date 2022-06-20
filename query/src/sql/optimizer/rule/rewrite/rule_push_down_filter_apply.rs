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

use crate::sql::binder::satisfied_by;
use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Filter;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;

pub struct RulePushDownFilterCrossApply {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterCrossApply {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterCrossApply,
            // Filter
            //  \
            //   CrossApply
            //   | \
            //   |  *
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::CrossApply,
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
}

impl Rule for RulePushDownFilterCrossApply {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let apply_expr = s_expr.child(0)?;

        let rel_expr = RelExpr::with_s_expr(apply_expr);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let mut push_down = vec![];
        let mut original_predicates = vec![];

        for predicate in filter.predicates.into_iter() {
            if satisfied_by(&predicate, &input_prop) {
                push_down.push(predicate);
            } else {
                original_predicates.push(predicate);
            }
        }

        if push_down.is_empty() {
            return Ok(());
        }

        let mut result = SExpr::create_binary(
            apply_expr.plan().clone().into(),
            SExpr::create_unary(
                Filter {
                    predicates: push_down,
                    is_having: false,
                }
                .into(),
                apply_expr.child(0)?.clone(),
            ),
            apply_expr.child(1)?.clone(),
        );

        if !original_predicates.is_empty() {
            result = SExpr::create_unary(
                Filter {
                    predicates: original_predicates,
                    is_having: false,
                }
                .into(),
                result,
            );
        }

        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
