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

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::EvalScalar;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Sort;

/// Input:  Order by
///           \
///          Expression
///             \
///              *
///
/// Output: Expression
///           \
///          Order By
///             \
///               *
pub struct RulePushDownSortEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownSortEvalScalar {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownSortEvalScalar,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Sort,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::Leaf],
                }],
            }],
            metadata,
        }
    }
}

impl Rule for RulePushDownSortEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        // If lazy materialization isn't enabled, we don't need push down the sort expression.
        if self.metadata.read().lazy_columns().is_empty() {
            return Ok(());
        }
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let eval_plan = s_expr.child(0)?;
        let eval_child_output_cols = &RelExpr::with_s_expr(eval_plan.child(0)?)
            .derive_relational_prop()?
            .output_columns;
        // Check if the sort expression is a subset of the output columns of the eval child plan.
        if !sort.used_columns().is_subset(eval_child_output_cols) {
            return Ok(());
        }
        let eval_scalar: EvalScalar = eval_plan.plan().clone().try_into()?;

        let sort_expr = SExpr::create_unary(
            Arc::new(RelOperator::Sort(sort)),
            Arc::new(eval_plan.child(0)?.clone()),
        );
        let mut result = SExpr::create_unary(
            Arc::new(RelOperator::EvalScalar(eval_scalar)),
            Arc::new(sort_expr),
        );

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
