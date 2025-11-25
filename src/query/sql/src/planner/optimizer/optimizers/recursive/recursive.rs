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
use std::time::Duration;
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

    /// Run the optimizer on the given expression.
    #[recursive::recursive]
    pub fn optimize_sync(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr)
    }

    #[recursive::recursive]
    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut current = s_expr.clone();

        loop {
            let mut optimized_children = Vec::with_capacity(current.arity());
            let mut children_changed = false;
            for expr in current.children() {
                let optimized_child = self.optimize_sync(expr)?;
                if !optimized_child.eq(expr) {
                    children_changed = true;
                }
                optimized_children.push(Arc::new(optimized_child));
            }

            if children_changed {
                current = current.replace_children(optimized_children);
            }

            match self.apply_transform_rules(&current, self.rules)? {
                Some(new_expr) => {
                    current = new_expr;
                }
                None => return Ok(current),
            }
        }
    }

    /// Trace rule execution, regardless of whether the rule had an effect
    fn trace_rule_execution(
        &self,
        rule_name: String,
        duration: Duration,
        before_expr: &SExpr,
        state: &TransformResult,
    ) -> Result<()> {
        if self.ctx.get_enable_trace() && self.trace_collector.is_some() {
            let collector = self.trace_collector.as_ref().unwrap();
            let metadata_ref = self.ctx.get_metadata();
            let metadata = &metadata_ref.read();

            // Determine result expression and check for actual differences
            let result_expr = if !state.results().is_empty() {
                &state.results()[0]
            } else {
                before_expr
            };

            // Record the rule execution
            collector.trace_rule(
                rule_name,
                self.name(),
                duration,
                before_expr,
                result_expr,
                metadata,
            )?;
        }

        Ok(())
    }

    fn apply_transform_rules(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<Option<SExpr>> {
        let mut s_expr = s_expr.clone();
        for rule_id in rules {
            let rule = RuleFactory::create_rule(*rule_id, self.ctx.clone())?;

            // Skip disabled rules
            if self.ctx.is_optimizer_disabled(&rule.name()) {
                continue;
            }

            // For tracing only
            let trace_enabled = self.ctx.get_enable_trace() && self.trace_collector.is_some();
            let start_time = Instant::now();
            let before_expr = s_expr.clone();

            // Core optimization logic - exactly as original
            let mut state = TransformResult::new();

            for (idx, matcher) in rule.matchers().iter().enumerate() {
                if matcher.matches(&s_expr) && !s_expr.applied_rule(&rule.id()) {
                    s_expr.set_applied_rule(&rule.id());
                    rule.apply_matcher(idx, &s_expr, &mut state)?;
                    if let Some(result) = state.results().first() {
                        let result = result.clone();

                        // For tracing only
                        if trace_enabled {
                            let duration = start_time.elapsed();
                            self.trace_rule_execution(rule.name(), duration, &before_expr, &state)?;
                        }

                        return Ok(Some(result));
                    }

                    break;
                }
            }

            // For tracing only
            if trace_enabled {
                let duration = start_time.elapsed();
                self.trace_rule_execution(rule.name(), duration, &before_expr, &state)?;
            }
        }

        Ok(None)
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

    /// Set the trace collector for this optimizer
    fn set_trace_collector(&mut self, collector: Arc<OptimizerTraceCollector>) {
        self.trace_collector = Some(collector);
    }
}
