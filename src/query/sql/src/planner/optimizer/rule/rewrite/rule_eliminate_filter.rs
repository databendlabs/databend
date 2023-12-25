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

use std::sync::Arc;

use databend_common_exception::Result;
use itertools::Itertools;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

pub struct RuleEliminateFilter {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleEliminateFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateFilter,
            // Filter
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Filter,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )],
        }
    }
}

impl Rule for RuleEliminateFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: Filter = s_expr.plan().clone().try_into()?;
        // First, de-duplication predicates.
        let origin_predicates = eval_scalar.predicates.clone();
        let predicates = eval_scalar
            .predicates
            .into_iter()
            .unique()
            .collect::<Vec<ScalarExpr>>();
        // Delete identically equal predicate
        // After constant fold is ready, we can delete the following code
        let predicates = predicates
            .into_iter()
            .filter(|predicate| match predicate {
                ScalarExpr::FunctionCall(func) if func.func_name == "eq" => {
                    if let (
                        ScalarExpr::BoundColumnRef(left_col),
                        ScalarExpr::BoundColumnRef(right_col),
                    ) = (&func.arguments[0], &func.arguments[1])
                    {
                        left_col.column.index != right_col.column.index
                            || left_col.column.data_type.is_nullable()
                    } else {
                        true
                    }
                }
                _ => true,
            })
            .collect::<Vec<ScalarExpr>>();

        if predicates.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
        } else if origin_predicates.len() != predicates.len() {
            let filter = Filter { predicates };
            state.add_result(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(s_expr.child(0)?.clone()),
            ));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
