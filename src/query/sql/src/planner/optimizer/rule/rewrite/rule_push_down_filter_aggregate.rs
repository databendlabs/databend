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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::Filter;
use crate::plans::RelOp;

/// Input:   Filter
///           \
///            Aggregate(Final or Partial)
///             \
///              *
///
/// Output:
/// (1)      Aggregate(Final or Partial)
///           \
///            Filter
///             \
///              *
///
/// (2)
///          Filter(remaining)
///           \
///            Aggregate(Final or Partial)
///             \
///              Filter(pushed down)
///               \
///                *
pub struct RulePushDownFilterAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterAggregate,

            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownFilterAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let aggregate_expr = s_expr.child(0)?;
        let aggregate: Aggregate = aggregate_expr.plan().clone().try_into()?;
        let aggregate_child_prop =
            RelExpr::with_s_expr(aggregate_expr).derive_relational_prop_child(0)?;
        let aggregate_group_columns = aggregate.group_columns()?;
        let mut pushed_down_predicates = vec![];
        let mut remaining_predicates = vec![];
        for predicate in filter.predicates.into_iter() {
            let predicate_used_columns = predicate.used_columns();
            if predicate_used_columns.is_subset(&aggregate_child_prop.output_columns)
                && predicate_used_columns.is_subset(&aggregate_group_columns)
            {
                pushed_down_predicates.push(predicate);
            } else {
                remaining_predicates.push(predicate)
            }
        }
        if !pushed_down_predicates.is_empty() {
            let pushed_down_filter = Filter {
                predicates: pushed_down_predicates,
            };
            let mut result = if remaining_predicates.is_empty() {
                SExpr::create_unary(
                    Arc::new(aggregate.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(pushed_down_filter.into()),
                        Arc::new(aggregate_expr.child(0)?.clone()),
                    )),
                )
            } else {
                let remaining_filter = Filter {
                    predicates: remaining_predicates,
                };
                SExpr::create_unary(
                    Arc::new(remaining_filter.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(aggregate.into()),
                        Arc::new(SExpr::create_unary(
                            Arc::new(pushed_down_filter.into()),
                            Arc::new(aggregate_expr.child(0)?.clone()),
                        )),
                    )),
                )
            };
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
