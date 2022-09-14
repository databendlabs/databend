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

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Filter;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::ScalarExpr;

pub struct RulePushDownFilterEvalScalar {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterEvalScalar,
            // Filter
            //  \
            //   EvalScalar
            //    \
            //     *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
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

impl Rule for RulePushDownFilterEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let input = s_expr.child(0)?;
        let eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;

        let rel_expr = RelExpr::with_s_expr(input);
        let eval_scalar_child_prop = rel_expr.derive_relational_prop_child(0)?;

        let mut used_columns = ColumnSet::new();
        for pred in filter.predicates.iter() {
            used_columns = used_columns.union(&pred.used_columns()).cloned().collect();
        }

        // Check if `Filter` can be satisfied by children of `EvalScalar`
        if used_columns.is_subset(&eval_scalar_child_prop.output_columns) {
            // TODO(leiysky): partial push down conjunctions
            // For example, `select a from (select a, a+1 as b from t) where a = 1 and b = 2`
            // can be optimized as `select a from (select a, a+1 as b from t where a = 1) where b = 2`
            let new_expr = SExpr::create_unary(
                eval_scalar.into(),
                SExpr::create_unary(filter.into(), input.child(0)?.clone()),
            );
            state.add_result(new_expr);
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
