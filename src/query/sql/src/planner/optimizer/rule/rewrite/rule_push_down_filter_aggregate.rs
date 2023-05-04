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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;
use crate::plans::RelOperator;

/// Heuristic optimizer runs in a bottom-up recursion fashion. If we match a plan like
/// Filter-Aggregate-* and push down filter to Filter(Optional)-Aggregate-Filter-*, this will not
/// work. RuleSplitAggregate will be applied first, since it's bottom up, then this rule, which
/// cause the plan be like Filter(Optional)-Aggregate-Filter-Aggregate-*, which makes no sense.
/// Hence we match 2 bundled Aggregate Ops:
///
/// Input:  Filter
///           \
///          Aggregate(Final)
///            \
///          Aggregate(Partial)
///             \
///              *
///
/// Output: Filter(Optional)
///           \
///          Aggregate(Final)
///             \
///            Aggregate(Partial)
///               \
///              Filter
///                \
///                 *
pub struct RulePushDownFilterAggregate {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownFilterAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterAggregate,
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                        SExpr::create_leaf(PatternPlan { plan_type: Pattern }.into()),
                    ),
                ),
            )],
        }
    }
}

impl Rule for RulePushDownFilterAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> common_exception::Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        if filter.is_having {
            let agg_parent = s_expr.child(0)?;
            let agg_parent_plan: Aggregate = agg_parent.plan().clone().try_into()?;
            let agg_child = agg_parent.child(0)?;
            let agg_child_plan: Aggregate = agg_child.plan().clone().try_into()?;
            if agg_parent_plan.mode == AggregateMode::Final
                && agg_child_plan.mode == AggregateMode::Partial
            {
                let mut push_predicates = vec![];
                let mut remaining_predicates = vec![];
                for predicate in filter.predicates {
                    let used_columns = predicate.used_columns();
                    let mut pushable = true;
                    for col in used_columns {
                        if !agg_parent_plan.group_columns()?.contains(&col) {
                            pushable = false;
                            break;
                        }
                    }
                    if pushable {
                        push_predicates.push(predicate);
                    } else {
                        remaining_predicates.push(predicate);
                    }
                }
                let mut result: SExpr;
                // No change since nothing can be pushed down.
                if push_predicates.is_empty() {
                    result = s_expr.clone();
                } else {
                    let filter_push_down_expr = SExpr::create_unary(
                        RelOperator::Filter(Filter {
                            predicates: push_predicates,
                            is_having: false,
                        }),
                        agg_child.child(0)?.clone(),
                    );
                    let agg_with_filter_push_down_expr = SExpr::create_unary(
                        RelOperator::Aggregate(agg_parent_plan),
                        SExpr::create_unary(
                            RelOperator::Aggregate(agg_child_plan),
                            filter_push_down_expr,
                        ),
                    );
                    // All filters are pushed down.
                    if remaining_predicates.is_empty() {
                        result = agg_with_filter_push_down_expr;
                    } else {
                        // Partial filter can be pushed down.
                        result = SExpr::create_unary(
                            RelOperator::Filter(Filter {
                                predicates: remaining_predicates,
                                is_having: true,
                            }),
                            agg_with_filter_push_down_expr,
                        );
                    }
                }
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
