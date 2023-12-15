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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;

// Merge two adjacent `Filter`s into one
pub struct RuleMergeFilter {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleMergeFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeFilter,
            // Filter
            // \
            //  Filter
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Filter,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
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
                )),
            )],
        }
    }
}

impl Rule for RuleMergeFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let up_filter: Filter = s_expr.plan().clone().try_into()?;
        let down_filter: Filter = s_expr.child(0)?.plan().clone().try_into()?;

        let predicates = up_filter
            .predicates
            .into_iter()
            .chain(down_filter.predicates)
            .collect();
        let merged = Filter { predicates };

        let new_expr = SExpr::create_unary(
            Arc::new(merged.into()),
            Arc::new(s_expr.child(0)?.child(0)?.clone()),
        );
        state.add_result(new_expr);
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
