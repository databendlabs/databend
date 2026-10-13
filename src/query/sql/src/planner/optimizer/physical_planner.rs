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

use super::ir::PExpr;
use super::ir::PlannedQuery;
use super::optimizers::operator::EliminateEvalScalarOptimizer;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::CascadesOptimizer;
use crate::optimizer::optimizers::operator::CleanupUnusedCTEOptimizer;
use crate::optimizer::optimizers::operator::FinalizeSpatialJoinOptimizer;
use crate::optimizer::pipeline::OptimizerTraceCollector;

/// Selects a physical expression from a logical input, then runs physical cleanup
/// and finalization separately from the logical optimizer pipeline.
pub struct PhysicalPlanner {
    opt_ctx: Arc<OptimizerContext>,
    trace: Arc<OptimizerTraceCollector>,
    memo: Option<Memo>,
    trace_offset: usize,
}

impl PhysicalPlanner {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            opt_ctx,
            trace: Arc::new(OptimizerTraceCollector::new()),
            memo: None,
            trace_offset: 0,
        }
    }

    pub fn with_trace_collector(
        mut self,
        trace: Arc<OptimizerTraceCollector>,
        offset: usize,
    ) -> Self {
        self.trace = trace;
        self.trace_offset = offset;
        self
    }

    pub fn memo(&self) -> Memo {
        self.memo
            .clone()
            .unwrap_or_else(|| Memo::new(self.opt_ctx.get_stat_context().clone()))
    }

    pub async fn plan(&mut self, input: SExpr) -> Result<PlannedQuery> {
        self.plan_inner(input, false, true).await
    }

    /// Force local planning after the caller configures input distribution, without
    /// rerunning logical preparation.
    pub(crate) async fn plan_local(&mut self, input: SExpr) -> Result<PlannedQuery> {
        self.plan_inner(input, true, true).await
    }

    pub(crate) async fn search_memo(&mut self, input: SExpr) -> Result<Memo> {
        self.plan_inner(input, false, false).await?;
        Ok(self.memo())
    }

    async fn plan_inner(
        &mut self,
        input: SExpr,
        local: bool,
        finalize: bool,
    ) -> Result<PlannedQuery> {
        self.memo = None;
        if local {
            self.opt_ctx.set_enable_distributed_optimization(false);
        }
        let mut expr = self.run_search(input, if finalize { 4 } else { 1 })?;
        if finalize {
            expr = self
                .run_pass(
                    EliminateEvalScalarOptimizer::new(self.opt_ctx.clone()),
                    expr,
                    1,
                    4,
                )
                .await?;
            expr = self.run_pass(CleanupUnusedCTEOptimizer, expr, 2, 4).await?;
            expr = self
                .run_pass(
                    FinalizeSpatialJoinOptimizer::new(self.opt_ctx.clone()),
                    expr,
                    3,
                    4,
                )
                .await?;
        }
        if self.opt_ctx.get_enable_trace() {
            self.trace.log_report();
            log::info!(
                "Final planned query:\n{}",
                expr.pretty_format(
                    &self.opt_ctx.get_metadata().read(),
                    self.opt_ctx.get_stat_context()
                )?
            );
        }
        Ok(PlannedQuery::new(expr))
    }

    /// Search is a logical-to-physical transition, not a same-type rewrite pass.
    fn run_search(&mut self, input: SExpr, total: usize) -> Result<PExpr> {
        #[cfg(debug_assertions)]
        {
            input.validate_types(&self.opt_ctx.get_metadata())?;
            input.validate_column_scope(&self.opt_ctx.get_metadata())?;
        }
        let mut search = CascadesOptimizer::new(self.opt_ctx.clone())?;
        if self.opt_ctx.is_optimizer_disabled(CascadesOptimizer::NAME) {
            return Ok(input.into());
        }
        // Only tracing needs a physical copy of the input for the existing diff API.
        // Search itself consumes the original logical expression directly.
        let before = self
            .opt_ctx
            .get_enable_trace()
            .then(|| PExpr::from(input.clone()));
        let start = Instant::now();
        let output = search.optimize_sync(input)?;
        self.validate(&output).map_err(|e| {
            e.add_message_back(" (after physical planning pass `CascadesOptimizer`)")
        })?;
        self.memo = Some(search.memo().clone());
        if let Some(before) = before {
            self.trace.trace_optimizer(
                CascadesOptimizer::NAME.to_string(),
                self.trace_offset,
                total + self.trace_offset,
                start.elapsed(),
                &before,
                &output,
                &self.opt_ctx.get_metadata().read(),
                self.opt_ctx.get_stat_context(),
            )?;
        }
        Ok(output)
    }

    /// Run a physical rewrite pass with skip-list handling, validation and tracing.
    async fn run_pass<T: Optimizer<PExpr>>(
        &mut self,
        mut pass: T,
        input: PExpr,
        index: usize,
        total: usize,
    ) -> Result<PExpr> {
        let name = pass.name();
        if self.opt_ctx.is_optimizer_disabled(&name) {
            return Ok(input);
        }
        let tracing = self.opt_ctx.get_enable_trace();
        if tracing {
            pass.set_trace_collector(self.trace.clone());
        }
        let before = tracing.then(|| input.clone());
        let start = Instant::now();
        let output = pass.optimize(input).await?;
        self.validate(&output)
            .map_err(|e| e.add_message_back(format!(" (after physical planning pass `{name}`)")))?;
        if let Some(before) = before {
            self.trace.trace_optimizer(
                name,
                index + self.trace_offset,
                total + self.trace_offset,
                start.elapsed(),
                &before,
                &output,
                &self.opt_ctx.get_metadata().read(),
                self.opt_ctx.get_stat_context(),
            )?;
        }
        Ok(output)
    }

    fn validate(&self, _expr: &PExpr) -> Result<()> {
        #[cfg(debug_assertions)]
        {
            _expr.validate_types(&self.opt_ctx.get_metadata())?;
            _expr.validate_column_scope(&self.opt_ctx.get_metadata())?;
        }
        Ok(())
    }
}
