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
use std::time::Instant;

use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::RuleFactory;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::pipeline::OptimizerTraceCollector;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;

/// Optimizer that recursively applies a set of transformation rules
#[derive(Clone)]
pub struct RecursiveRuleOptimizer {
    ctx: Arc<OptimizerContext>,
    rules: &'static [RuleID],
    trace_collector: Option<Arc<OptimizerTraceCollector>>,
}

impl RecursiveRuleOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>, rules: &'static [RuleID]) -> Self {
        Self {
            ctx,
            rules,
            trace_collector: None,
        }
    }

    /// Set the trace collector for this optimizer
    pub fn set_trace_collector(&mut self, collector: Arc<OptimizerTraceCollector>) {
        self.trace_collector = Some(collector);
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

            // Check if this rule should be skipped based on optimizer_skip_list
            let rule_name = rule.name();
            if self.ctx.is_optimizer_disabled(&rule_name) {
                continue;
            }

            let mut state = TransformResult::new();
            if rule
                .matchers()
                .iter()
                .any(|matcher| matcher.matches(&s_expr))
                && !s_expr.applied_rule(&rule.id())
            {
                s_expr.set_applied_rule(&rule.id());

                // Record the expression before rule application and start time
                let before_expr = s_expr.clone();
                let start_time = Instant::now();

                // Apply the rule
                rule.apply(&s_expr, &mut state)?;

                // Calculate execution time
                let duration = start_time.elapsed();

                if !state.results().is_empty() {
                    let result = &state.results()[0];

                    // If tracing is enabled, record the rule application
                    if let Some(collector) = &self.trace_collector {
                        let metadata_ref = self.ctx.get_metadata();
                        let metadata = &metadata_ref.read();

                        // Record detailed information about the rule application
                        collector.trace_rule(
                            rule_name,
                            self.name(),
                            duration,
                            &before_expr,
                            result,
                            metadata,
                        )?;
                    }

                    // Recursively optimize the result
                    let optimized_result = self.optimize_expression(result)?;
                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}

#[async_trait::async_trait]
impl Optimizer for RecursiveRuleOptimizer {
    fn name(&self) -> String {
        let total = self.rules.len();
        let preview = if total <= 3 {
            self.rules
                .iter()
                .map(|rule_id| format!("{:?}", rule_id))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            format!("{:?},{:?},...({})", self.rules[0], self.rules[1], total - 2)
        };
        format!("RecursiveRuleOptimizer[{}]", preview)
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
