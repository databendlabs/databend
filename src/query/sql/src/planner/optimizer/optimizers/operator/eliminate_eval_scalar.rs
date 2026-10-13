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

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::PExpr;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::pipeline::OptimizerTraceCollector;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOp;

/// Post-search cleanup for EvalScalar nodes. Logical recursive rewrites and their
/// materialized-view handling are deliberately not part of this physical pass.
pub struct EliminateEvalScalarOptimizer {
    ctx: Arc<OptimizerContext>,
    trace_collector: Option<Arc<OptimizerTraceCollector>>,
}

impl EliminateEvalScalarOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        Self {
            ctx,
            trace_collector: None,
        }
    }

    /// Preserve children-first traversal and repeat after a successful elimination.
    #[recursive::recursive]
    pub fn optimize_sync(&self, mut current: PExpr) -> Result<PExpr> {
        loop {
            let mut children = Vec::with_capacity(current.children.len());
            for child in std::mem::take(&mut current.children) {
                children.push(Arc::new(self.optimize_sync(Arc::unwrap_or_clone(child))?));
            }
            current = current.replace_children(children);
            match self.eliminate(&current)? {
                Some(expr) => current = expr,
                None => return Ok(current),
            }
        }
    }

    fn eliminate(&self, expr: &PExpr) -> Result<Option<PExpr>> {
        let id = RuleID::EliminateEvalScalar;
        if self.ctx.is_optimizer_disabled(&id.to_string()) {
            return Ok(None);
        }
        let start = Instant::now();
        let before = expr;
        let mut expr = expr.clone();
        let matcher = Matcher::MatchOp {
            op_type: RelOp::EvalScalar,
            children: vec![Matcher::Leaf],
        };
        let result = if matcher.matches(&expr) && !expr.applied_rule(&id) {
            expr.set_applied_rule(&id);
            Self::eliminate_eval_scalar(&expr)?
        } else {
            None
        };
        if self.ctx.get_enable_trace()
            && let Some(collector) = &self.trace_collector
        {
            collector.trace_rule(
                id.to_string(),
                self.name(),
                start.elapsed(),
                before,
                result.as_ref().unwrap_or(&expr),
                &self.ctx.get_metadata().read(),
                self.ctx.get_stat_context(),
            )?;
        }
        Ok(result)
    }

    fn eliminate_eval_scalar(expr: &PExpr) -> Result<Option<PExpr>> {
        // Eliminate empty EvalScalar
        let eval_scalar: EvalScalar = expr.plan().clone().try_into()?;
        if eval_scalar.items.is_empty() {
            return Ok(Some(expr.child(0)?.clone()));
        }

        let child = expr.child(0)?;
        let child_output_cols = child
            .plan()
            .derive_relational_prop(&RelExpr::with_p_expr(child))?
            .output_columns
            .clone();
        let eval_scalar_output_cols: ColumnSet =
            eval_scalar.items.iter().map(|x| x.index).collect();

        if eval_scalar_output_cols.is_subset(&child_output_cols) {
            // check if there's f(#x) as #x, if so we can't eliminate the eval scalar
            for item in eval_scalar.items {
                match item.scalar {
                    ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => {
                        // A constant with an existing output index shadows the child column.
                        // It cannot be eliminated as an identity projection.
                        return Ok(None);
                    }
                    ScalarExpr::FunctionCall(func) => {
                        if func.arguments.len() == 1 {
                            if let ScalarExpr::BoundColumnRef(bound_column_ref) = &func.arguments[0]
                                && bound_column_ref.column.index == item.index
                            {
                                return Ok(None);
                            }
                        }
                    }
                    ScalarExpr::CastExpr(cast) => {
                        if let ScalarExpr::BoundColumnRef(bound_column_ref) = cast.argument.as_ref()
                            && bound_column_ref.column.index == item.index
                        {
                            return Ok(None);
                        }
                    }
                    _ => {}
                }
            }
            return Ok(Some(expr.child(0)?.clone()));
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Optimizer<PExpr> for EliminateEvalScalarOptimizer {
    fn name(&self) -> String {
        // Preserve the existing setting/trace identifier despite separating the pass.
        "RecursiveRuleOptimizer[EliminateEvalScalar]".to_string()
    }

    async fn optimize(&mut self, expr: PExpr) -> Result<PExpr> {
        self.optimize_sync(expr)
    }

    fn set_trace_collector(&mut self, collector: Arc<OptimizerTraceCollector>) {
        self.trace_collector = Some(collector);
    }
}
