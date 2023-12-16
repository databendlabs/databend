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
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::ProjectSet;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;

/// Input:   Filter
///           \
///            ProjectSet
///             \
///              *
///
/// Output:
/// (1)      ProjectSet
///           \
///            Filter
///             \
///              *
///
/// (2)
///          Filter(remaining)
///           \
///            ProjectSet
///             \
///              Filter(pushed down)
///               \
///                *
pub struct RulePushDownFilterProjectSet {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownFilterProjectSet {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterProjectSet,
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
                            plan_type: RelOp::ProjectSet,
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

impl Rule for RulePushDownFilterProjectSet {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let project_set_expr = s_expr.child(0)?;
        let project_set: ProjectSet = project_set_expr.plan().clone().try_into()?;
        let project_set_child_prop =
            RelExpr::with_s_expr(project_set_expr).derive_relational_prop_child(0)?;
        let mut pushed_down_predicates = vec![];
        let mut remaining_predicates = vec![];
        for pred in filter.predicates.into_iter() {
            let pred_used_columns = pred.used_columns();
            if pred_used_columns.is_subset(&project_set_child_prop.output_columns) {
                pushed_down_predicates.push(pred);
            } else {
                remaining_predicates.push(pred)
            }
        }
        if !pushed_down_predicates.is_empty() {
            let pushed_down_filter = Filter {
                predicates: pushed_down_predicates,
            };
            let mut result = if remaining_predicates.is_empty() {
                SExpr::create_unary(
                    Arc::new(project_set.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(pushed_down_filter.into()),
                        Arc::new(project_set_expr.child(0)?.clone()),
                    )),
                )
            } else {
                let remaining_filter = Filter {
                    predicates: remaining_predicates,
                };
                SExpr::create_unary(
                    Arc::new(remaining_filter.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(project_set.into()),
                        Arc::new(SExpr::create_unary(
                            Arc::new(pushed_down_filter.into()),
                            Arc::new(project_set_expr.child(0)?.clone()),
                        )),
                    )),
                )
            };
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
