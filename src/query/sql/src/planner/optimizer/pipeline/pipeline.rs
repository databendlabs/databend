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
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
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

    s_expr: SExpr,
}

impl OptimizerPipeline {
    /// Create a new optimizer pipeline
    pub async fn new(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<Self> {
        let pipeline = Self {
            opt_ctx,
            optimizers: Vec::new(),
            memo: None,
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
        for optimizer in &mut self.optimizers {
            let optimizer_name = optimizer.name();
            debug!("Applying optimizer: {}", optimizer_name);

            current_expr = optimizer.optimize(&current_expr).await?;
            if let Some(memo) = optimizer.memo() {
                self.memo = Some(memo.clone());
            }

            debug!("Optimizer {} completed", optimizer_name);
        }

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
