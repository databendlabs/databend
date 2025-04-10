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
use log::info;

use super::common::contains_local_table_scan;
use super::common::contains_warehouse_table_scan;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::pipeline::trace::OptimizerExecution;
use crate::optimizer::pipeline::trace::OptimizerTraceCollector;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;

/// A pipeline of optimizers that are executed in sequence.
pub struct OptimizerPipeline {
    /// The optimizer context
    opt_ctx: Arc<OptimizerContext>,
    /// The sequence of optimizers to be applied
    optimizers: Vec<Box<dyn Optimizer>>,
    /// The memo captured during optimization (if any)
    memo: Option<Memo>,

    /// The trace collector for generating reports
    trace_collector: OptimizerTraceCollector,

    s_expr: SExpr,
}

impl OptimizerPipeline {
    /// Create a new optimizer pipeline
    pub async fn new(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<Self> {
        let pipeline = Self {
            opt_ctx,
            optimizers: Vec::new(),
            memo: None,
            trace_collector: OptimizerTraceCollector::new(),
            s_expr,
        };

        pipeline
            .configure_distributed_optimization(&pipeline.s_expr)
            .await?;
        Ok(pipeline)
    }

    /// Add an optimizer to the pipeline
    #[allow(clippy::should_implement_trait)]
    pub fn add<T: Optimizer + 'static>(mut self, optimizer: T) -> Self {
        self.optimizers.push(Box::new(optimizer));
        self
    }

    /// Add an optimizer to the pipeline conditionally
    pub fn add_if<T: Optimizer + 'static>(self, condition: bool, optimizer: T) -> Self {
        if condition {
            self.add(optimizer)
        } else {
            self
        }
    }

    /// Configure distributed optimization based on table types
    async fn configure_distributed_optimization(&self, s_expr: &SExpr) -> Result<()> {
        let metadata = self.opt_ctx.get_metadata();

        if contains_local_table_scan(s_expr, &metadata) {
            self.opt_ctx.set_enable_distributed_optimization(false);
            info!("Disable distributed optimization due to local table scan.");
        } else if contains_warehouse_table_scan(s_expr, &metadata) {
            let warehouse = self.opt_ctx.get_table_ctx().get_warehouse_cluster().await?;

            if !warehouse.is_empty() {
                self.opt_ctx.set_enable_distributed_optimization(true);
                info!("Enable distributed optimization due to warehouse table scan.");
            }
        }

        Ok(())
    }

    /// Execute the pipeline on the given expression
    pub async fn execute(&mut self) -> Result<SExpr> {
        // Then apply all optimizers in sequence
        let mut current_expr = self.s_expr.clone();
        let total_optimizers = self.optimizers.len();

        // Update trace status from context
        self.trace_collector
            .set_enable_trace(self.opt_ctx.get_enable_trace());
        for (idx, optimizer) in self.optimizers.iter_mut().enumerate() {
            let before_expr = current_expr.clone();

            // Measure optimizer execution time
            let start_time = Instant::now();
            current_expr = optimizer.optimize(&current_expr).await?;
            let duration = start_time.elapsed();

            if let Some(memo) = optimizer.memo() {
                self.memo = Some(memo.clone());
            }

            // Create execution info and trace optimizer
            {
                let metadata_ref = self.opt_ctx.get_metadata();
                let execution = OptimizerExecution {
                    name: optimizer.name(),
                    index: idx,
                    total: total_optimizers,
                    time: duration,
                    before: &before_expr,
                    after: &current_expr,
                };

                self.trace_collector
                    .trace_optimizer(execution, &metadata_ref.read())?;
            }
        }

        // Generate and log the report
        self.trace_collector.log_report();

        Ok(current_expr)
    }

    /// Get the memo captured during optimization
    ///
    /// If no memo was captured during optimization, an empty memo is created and returned.
    /// This ensures the method always returns a valid Memo.
    pub fn memo(&self) -> Memo {
        match &self.memo {
            Some(memo) => memo.clone(),
            None => {
                // Create and return an empty memo
                Memo::create()
            }
        }
    }
}
