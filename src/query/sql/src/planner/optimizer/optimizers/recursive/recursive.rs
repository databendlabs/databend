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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::Result;

use crate::Symbol;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::RuleFactory;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::pipeline::OptimizerTraceCollector;
use crate::plans::RelOperator;

/// Optimizer that recursively applies a set of transformation rules
#[derive(Clone)]
pub struct RecursiveRuleOptimizer {
    ctx: Arc<OptimizerContext>,
    rules: &'static [RuleID],
    trace_collector: Option<Arc<OptimizerTraceCollector>>,
    materialized_view_output_columns: Option<HashSet<Symbol>>,
}

impl RecursiveRuleOptimizer {
    fn materialized_view_child_output_columns(
        s_expr: &SExpr,
        output_columns: Option<&HashSet<Symbol>>,
    ) -> Result<Option<HashSet<Symbol>>> {
        let Some(output_columns) = output_columns else {
            return Ok(None);
        };
        let mut child_output_columns = output_columns.clone();
        match s_expr.plan() {
            RelOperator::Limit(_) => {}
            RelOperator::Sort(sort) => child_output_columns.extend(sort.used_columns()),
            RelOperator::TopN(top_n) => child_output_columns.extend(top_n.used_columns()),
            _ => return Ok(None),
        }
        child_output_columns.extend(
            s_expr
                .child(0)?
                .derive_relational_prop()?
                .output_columns
                .iter()
                .copied(),
        );
        Ok(Some(child_output_columns))
    }

    pub fn new(ctx: Arc<OptimizerContext>, rules: &'static [RuleID]) -> Self {
        Self {
            ctx,
            rules,
            trace_collector: None,
            materialized_view_output_columns: None,
        }
    }

    pub fn new_with_materialized_view_output_columns(
        ctx: Arc<OptimizerContext>,
        rules: &'static [RuleID],
        output_columns: Option<HashSet<Symbol>>,
    ) -> Self {
        Self {
            ctx,
            rules,
            trace_collector: None,
            materialized_view_output_columns: output_columns,
        }
    }

    /// Run the optimizer on the given expression.
    #[recursive::recursive]
    pub fn optimize_sync(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr, self.materialized_view_output_columns.as_ref())
    }

    #[recursive::recursive]
    fn optimize_expression(
        &self,
        s_expr: &SExpr,
        materialized_view_output_columns: Option<&HashSet<Symbol>>,
    ) -> Result<SExpr> {
        let mut current = s_expr.clone();

        loop {
            // Materialized-view substitution must inspect the largest query subtree before its
            // children. A bottom-up attempt at Filter's child treats predicate-only columns as
            // required outputs and cannot consume a predicate guaranteed by the MV definition.
            if self.rules.contains(&RuleID::TryApplyMaterializedView)
                && materialized_view_output_columns.is_some()
                && let Some(new_expr) = self.apply_transform_rules(
                    &current,
                    &[RuleID::TryApplyMaterializedView],
                    materialized_view_output_columns,
                )?
            {
                current = new_expr;
                continue;
            }

            let child_materialized_view_output_columns =
                Self::materialized_view_child_output_columns(
                    &current,
                    materialized_view_output_columns,
                )?;
            let mut optimized_children = Vec::with_capacity(current.arity());
            let mut children_changed = false;
            for expr in current.children() {
                let optimized_child = self
                    .optimize_expression(expr, child_materialized_view_output_columns.as_ref())?;
                if !optimized_child.eq(expr) {
                    children_changed = true;
                }
                optimized_children.push(Arc::new(optimized_child));
            }

            if children_changed {
                current = current.replace_children(optimized_children);
            }

            match self.apply_transform_rules(
                &current,
                self.rules,
                materialized_view_output_columns,
            )? {
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

    fn apply_transform_rules(
        &self,
        s_expr: &SExpr,
        rules: &[RuleID],
        materialized_view_output_columns: Option<&HashSet<Symbol>>,
    ) -> Result<Option<SExpr>> {
        let mut s_expr = s_expr.clone();
        for rule_id in rules {
            if *rule_id == RuleID::TryApplyMaterializedView
                && materialized_view_output_columns.is_none()
            {
                continue;
            }
            let rule = if *rule_id == RuleID::TryApplyMaterializedView {
                Box::new(
                    crate::optimizer::optimizers::rule::RuleTryApplyMaterializedView::new_with_output_columns(
                        self.ctx.clone(),
                        materialized_view_output_columns.cloned(),
                    ),
                ) as crate::optimizer::optimizers::rule::RulePtr
            } else {
                RuleFactory::create_rule(*rule_id, self.ctx.clone())?
            };

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
