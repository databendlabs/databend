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
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;
use crate::plans::Sort;

/// Input:  Filter
///           \
///          Sort
///             \
///              *
///
/// Output: Sort
///           \
///          Filter
///             \
///              *
pub struct RulePushDownFilterSort {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownFilterSort {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterSort,
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
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan { plan_type: Pattern }.into(),
                    ))),
                )),
            )],
        }
    }
}

impl Rule for RulePushDownFilterSort {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let sort: Sort = s_expr.child(0)?.plan().clone().try_into()?;
        let sort_expr = s_expr.child(0)?;

        let mut result = SExpr::create_unary(
            Arc::new(sort.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(sort_expr.child(0)?.clone()),
            )),
        );
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
