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
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::ScalarExpr;

// Merge two adjacent `EvalScalar`s into one
pub struct RuleMergeEvalScalar {
    id: RuleID,
    pattern: SExpr,
}

impl RuleMergeEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeEvalScalar,
            // EvalScalar
            // \
            //  EvalScalar
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::EvalScalar,
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

impl Rule for RuleMergeEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let up_eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        let down_eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;

        let mut used_columns = ColumnSet::new();
        for item in up_eval_scalar.items.iter() {
            used_columns = used_columns
                .union(&item.scalar.used_columns())
                .cloned()
                .collect();
        }

        let rel_expr = RelExpr::with_s_expr(s_expr.child(0)?);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Check if the up EvalScalar denpends on the down EvalScalar
        if used_columns.is_subset(&input_prop.output_columns) {
            // TODO(leiysky): eliminate duplicated scalars
            let items = up_eval_scalar
                .items
                .into_iter()
                .chain(down_eval_scalar.items.into_iter())
                .collect();
            let merged = EvalScalar { items };

            let new_expr = SExpr::create_unary(merged.into(), s_expr.child(0)?.child(0)?.clone());
            state.add_result(new_expr);
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
