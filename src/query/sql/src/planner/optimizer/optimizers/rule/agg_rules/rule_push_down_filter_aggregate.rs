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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
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
        let filter = s_expr.plan().as_any().downcast_ref::<Filter>().unwrap();
        let aggregate_expr = s_expr.child(0)?;
        let aggregate = aggregate_expr
            .plan()
            .as_any()
            .downcast_ref::<Aggregate>()
            .unwrap();
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
                pushed_down_predicates.push(predicate.clone());
                // If all used columns in predicate are in group columns
                // we can push down the predicate, otherwise we need to keep the predicate
                // eg: explain select * from (select number % 3 a, number % 4 b from
                // range(1, 1000)t(number) group by cube(a,b))  where a is null and b is null;

                // we can't push down the predicate cause filter will remove all rows
                // but we can't remove the predicate cause the null will be generated
                // by group by cube
                if let Some(grouping_sets) = &aggregate.grouping_sets {
                    if grouping_sets
                        .sets
                        .iter()
                        .all(|s| predicate_used_columns.iter().all(|c| s.contains(c)))
                    {
                        remaining_predicates.push(predicate);
                    } else {
                        return Ok(());
                    }
                }
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

impl Default for RulePushDownFilterAggregate {
    fn default() -> Self {
        Self::new()
    }
}
