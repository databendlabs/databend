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
use log::debug;
use log::info;

use super::common::contains_local_table_scan;
use super::common::contains_warehouse_table_scan;
use crate::optimizer::ir::SExpr;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;

/// A pipeline of optimizers that are executed in sequence.
pub struct OptimizerPipeline {
    /// The optimizer context
    opt_ctx: Arc<OptimizerContext>,
    /// The sequence of optimizers to be applied
    optimizers: Vec<Box<dyn Optimizer>>,
}

impl OptimizerPipeline {
    /// Create a new optimizer pipeline
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            opt_ctx,
            optimizers: Vec::new(),
        }
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
    pub async fn execute(&mut self, expr: SExpr) -> Result<SExpr> {
        // Configure distributed optimization first
        self.configure_distributed_optimization(&expr).await?;

        // Then apply all optimizers in sequence
        let mut current_expr = expr;
        for optimizer in &mut self.optimizers {
            let optimizer_name = optimizer.name();
            debug!("Applying optimizer: {}", optimizer_name);

            current_expr = optimizer.optimize(&current_expr).await?;

            debug!("Optimizer {} completed", optimizer_name);
        }

        Ok(current_expr)
    }
}
