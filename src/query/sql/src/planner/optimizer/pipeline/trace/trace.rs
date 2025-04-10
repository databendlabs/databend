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

use std::time::Duration;

use databend_common_exception::Result;
use log::info;

use crate::optimizer::ir::SExpr;
use crate::Metadata;

/// Represents a trace entry for an optimizer execution
pub struct OptimizerTrace {
    /// The name of the optimizer
    pub optimizer_name: String,
    /// The index of the optimizer in the pipeline
    pub optimizer_index: usize,
    /// The total number of optimizers in the pipeline
    pub total_optimizers: usize,
    /// The execution time of the optimizer
    pub execution_time: Duration,
}

impl OptimizerTrace {
    /// Create a new optimizer trace
    pub fn new(
        optimizer_name: String,
        optimizer_index: usize,
        total_optimizers: usize,
        execution_time: Duration,
    ) -> Self {
        Self {
            optimizer_name,
            optimizer_index,
            total_optimizers,
            execution_time,
        }
    }

    /// Log the trace entry with expression diff
    pub fn log(&self, before_expr: &SExpr, after_expr: &SExpr, metadata: &Metadata) -> Result<()> {
        let diff = before_expr.diff(after_expr, metadata)?;
        let mut output = String::new();

        output.push_str(&format!(
            "================ Applied Optimizer [{}/{}]: {} (execution time: {:.2?}) ================\n",
            self.optimizer_index + 1,
            self.total_optimizers,
            self.optimizer_name,
            self.execution_time
        ));

        output.push_str(&format!("{}\n", diff));
        info!("{}", output);

        Ok(())
    }
}
