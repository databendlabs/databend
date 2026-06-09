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

use crate::ColumnSet;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Window;
use crate::plans::WindowGroup;

/// Input:   Filter
///           \
///            Window
///             \
///              *
///
/// Output:
/// (1)      Window
///           \
///            Filter
///             \
///              *
///
/// (2)
///          Filter(remaining)
///           \
///            Window
///             \
///              Filter(pushed down)
///               \
///                *
///
/// note that only push down filter used in `Window.partition_by` columns
pub struct RulePushDownFilterWindow {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterWindow {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterWindow,
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Window,
                        children: vec![Matcher::Leaf],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::WindowGroup,
                        children: vec![Matcher::Leaf],
                    }],
                },
            ],
        }
    }
}

impl Rule for RulePushDownFilterWindow {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let Filter { predicates } = s_expr.plan().clone().try_into()?;
        let window_expr = s_expr.child(0)?;
        let (window_plan, allowed, rejected) =
            if matches!(window_expr.plan(), RelOperator::WindowGroup(_)) {
                let window_group: WindowGroup = window_expr.plan().clone().try_into()?;
                let allowed = window_group_partition_by_columns(&window_group)?;
                let rejected = window_group_rejected_columns(&window_group)?;
                (RelOperator::WindowGroup(window_group), allowed, rejected)
            } else {
                let window: Window = window_expr.plan().clone().try_into()?;
                let allowed = window.partition_by_columns()?;
                let rejected = ColumnSet::from_iter(
                    window
                        .order_by_columns()?
                        .into_iter()
                        .chain(window.function.used_columns()),
                );
                (RelOperator::Window(window), allowed, rejected)
            };

        let (pushed_down, remaining): (Vec<_>, Vec<_>) =
            predicates.into_iter().partition(|predicate| {
                let used = predicate.used_columns();
                used.is_subset(&allowed) && used.is_disjoint(&rejected)
            });
        if pushed_down.is_empty() {
            return Ok(());
        }

        let pushed_down_filter = Filter {
            predicates: pushed_down,
        };
        let result = if remaining.is_empty() {
            SExpr::create_unary(
                Arc::new(window_plan),
                Arc::new(SExpr::create_unary(
                    Arc::new(pushed_down_filter.into()),
                    Arc::new(window_expr.child(0)?.clone()),
                )),
            )
        } else {
            let remaining_filter = Filter {
                predicates: remaining,
            };
            let mut s_expr = SExpr::create_unary(
                Arc::new(remaining_filter.into()),
                Arc::new(SExpr::create_unary(
                    Arc::new(window_plan),
                    Arc::new(SExpr::create_unary(
                        Arc::new(pushed_down_filter.into()),
                        Arc::new(window_expr.child(0)?.clone()),
                    )),
                )),
            );
            s_expr.set_applied_rule(&self.id);
            s_expr
        };
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn window_group_partition_by_columns(
    window_group: &WindowGroup,
) -> databend_common_exception::Result<ColumnSet> {
    let Some(first) = window_group.windows.first() else {
        return Ok(ColumnSet::new());
    };

    let mut allowed = first.partition_by_columns()?;
    for window in window_group.windows.iter().skip(1) {
        allowed = allowed
            .intersection(&window.partition_by_columns()?)
            .copied()
            .collect();
    }
    Ok(allowed)
}

fn window_group_rejected_columns(
    window_group: &WindowGroup,
) -> databend_common_exception::Result<ColumnSet> {
    let mut rejected = ColumnSet::new();
    for window in &window_group.windows {
        rejected.extend(window.order_by_columns()?);
        rejected.extend(window.function.used_columns());
    }
    Ok(rejected)
}

impl Default for RulePushDownFilterWindow {
    fn default() -> Self {
        Self::new()
    }
}
