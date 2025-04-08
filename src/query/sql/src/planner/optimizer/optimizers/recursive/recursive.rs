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

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::RuleFactory;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;

/// A recursive optimizer that will apply the given rules recursively.
/// It will keep applying the rules on the substituted expression
/// until no more rules can be applied.
pub struct RecursiveOptimizer {
    ctx: Arc<OptimizerContext>,
    rules: &'static [RuleID],
}

impl RecursiveOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>, rules: &'static [RuleID]) -> Self {
        Self { ctx, rules }
    }

    /// Run the optimizer on the given expression.
    #[recursive::recursive]
    pub fn optimize_sync(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr)
    }

    #[recursive::recursive]
    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        let mut children_changed = false;
        for expr in s_expr.children() {
            let optimized_child = self.optimize_sync(expr)?;
            if !optimized_child.eq(expr) {
                children_changed = true;
            }
            optimized_children.push(Arc::new(optimized_child));
        }
        let mut optimized_expr = s_expr.clone();
        if children_changed {
            optimized_expr = s_expr.replace_children(optimized_children);
        }

        let result = self.apply_transform_rules(&optimized_expr, self.rules)?;

        Ok(result)
    }

    fn apply_transform_rules(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        for rule_id in rules {
            let rule = RuleFactory::create_rule(*rule_id, self.ctx.clone())?;
            let mut state = TransformResult::new();
            if rule
                .matchers()
                .iter()
                .any(|matcher| matcher.matches(&s_expr))
                && !s_expr.applied_rule(&rule.id())
            {
                s_expr.set_applied_rule(&rule.id());
                rule.apply(&s_expr, &mut state)?;
                if !state.results().is_empty() {
                    // Recursive optimize the result
                    let result = &state.results()[0];
                    let optimized_result = self.optimize_expression(result)?;
                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}

#[async_trait::async_trait]
impl Optimizer for RecursiveOptimizer {
    fn name(&self) -> &'static str {
        "RecursiveOptimizer"
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
