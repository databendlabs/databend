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
use crate::plans::Filter;
use crate::plans::RelOp;

/// Push predicates that do not use the computed items below `Udf` or `AsyncFunction`.
///
/// Input:   Filter
///           \
///            Udf / AsyncFunction
///             \
///              *
///
/// Output:  Filter(remaining)
///           \
///            Udf / AsyncFunction
///             \
///              Filter(pushed down)
///               \
///                *
pub struct RulePushDownFilterUdf {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterUdf {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterUdf,
            matchers: [RelOp::Udf, RelOp::AsyncFunction]
                .into_iter()
                .map(|op_type| Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type,
                        children: vec![Matcher::Leaf],
                    }],
                })
                .collect(),
        }
    }
}

impl Rule for RulePushDownFilterUdf {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let udf_expr = s_expr.child(0)?;
        let child_prop = RelExpr::with_s_expr(udf_expr).derive_relational_prop_child(0)?;

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
            Arc::new(udf_expr.child(0)?.clone()),
        );
        let mut result = udf_expr.replace_children([Arc::new(pushed_down)]);
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

impl Default for RulePushDownFilterUdf {
    fn default() -> Self {
        Self::new()
    }
}
