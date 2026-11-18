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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::AsyncFunction;
use crate::plans::Filter;
use crate::plans::RelOp;

/// Push predicates that do not use the computed items below `AsyncFunction`.
///
/// The rule does not apply if an item calls `nextval`. Each call advances the
/// sequence, so removing input rows changes the values returned for the rows
/// that remain. `Udf` has no such rule: a UDF may keep state or have side
/// effects, and the planner cannot check that.
///
/// Input:   Filter
///           \
///            AsyncFunction
///             \
///              *
///
/// Output:  Filter(remaining)
///           \
///            AsyncFunction
///             \
///              Filter(pushed down)
///               \
///                *
pub struct RulePushDownFilterAsyncFunction {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterAsyncFunction {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterAsyncFunction,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::AsyncFunction,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownFilterAsyncFunction {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let async_func_expr = s_expr.child(0)?;
        let async_func: AsyncFunction = async_func_expr.plan().clone().try_into()?;
        if async_func
            .items
            .iter()
            .any(|item| item.scalar.contains_nextval())
        {
            return Ok(());
        }
        let child_prop = RelExpr::with_s_expr(async_func_expr).derive_relational_prop_child(0)?;

        let (pushed_down_predicates, remaining_predicates): (Vec<_>, Vec<_>) = filter
            .predicates
            .into_iter()
            .partition(|pred| pred.used_columns().is_subset(&child_prop.output_columns));
        if pushed_down_predicates.is_empty() {
            return Ok(());
        }

        let pushed_down = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: pushed_down_predicates,
                }
                .into(),
            ),
            Arc::new(async_func_expr.child(0)?.clone()),
        );
        let mut result = async_func_expr.replace_children([Arc::new(pushed_down)]);
        if !remaining_predicates.is_empty() {
            result = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: remaining_predicates,
                    }
                    .into(),
                ),
                Arc::new(result),
            );
        }
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownFilterAsyncFunction {
    fn default() -> Self {
        Self::new()
    }
}
