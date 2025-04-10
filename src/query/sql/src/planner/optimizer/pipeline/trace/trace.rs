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
#[derive(Clone)]
pub struct OptimizerTrace {
    /// The name of the optimizer
    pub optimizer_name: String,
    /// The index of the optimizer in the pipeline
    pub optimizer_index: usize,
    /// The total number of optimizers in the pipeline
    pub total_optimizers: usize,
    /// The execution time of the optimizer
    pub execution_time: Duration,
    /// Whether the optimizer had an effect (changed the expression)
    pub had_effect: bool,
}

impl OptimizerTrace {
    /// Create a new optimizer trace
    pub fn new(
        optimizer_name: String,
        optimizer_index: usize,
        total_optimizers: usize,
        execution_time: Duration,
        had_effect: bool,
    ) -> Self {
        Self {
            optimizer_name,
            optimizer_index,
            total_optimizers,
            execution_time,
            had_effect,
        }
    }

    /// Log the trace entry with expression diff
    pub fn log(&self, before_expr: &SExpr, after_expr: &SExpr, metadata: &Metadata) -> Result<()> {
        let mut output = String::new();

        // Add header with effect information
        let effect_status = if self.had_effect {
            "Applied"
        } else {
            "No Effect"
        };

        output.push_str(&format!(
            "================ {} Optimizer [{}/{}]: {} (execution time: {:.2?}) ================\n",
            effect_status,
            self.optimizer_index,
            self.total_optimizers,
            self.optimizer_name,
            self.execution_time
        ));

        // Only generate and show diff if there was an effect
        if self.had_effect {
            let diff = before_expr.diff(after_expr, metadata)?;
            output.push_str(&format!("{}\n", diff));
        }

        info!("{}", output);

        Ok(())
    }
}

/// Contains information about an optimizer execution
pub struct OptimizerExecution<'a> {
    /// Name of the optimizer
    pub name: String,
    /// Index of the optimizer in the pipeline
    pub index: usize,
    /// Total number of optimizers in the pipeline
    pub total: usize,
    /// Execution time
    pub time: Duration,
    /// Expression before optimization
    pub before: &'a SExpr,
    /// Expression after optimization
    pub after: &'a SExpr,
}

/// Collects and manages optimizer traces for generating reports
#[derive(Default)]
pub struct OptimizerTraceCollector {
    /// Collected traces from optimizer executions
    traces: Vec<OptimizerTrace>,
    /// Whether tracing is enabled
    enable_trace: bool,
}

impl OptimizerTraceCollector {
    /// Create a new optimizer trace collector
    pub fn new() -> Self {
        Self {
            traces: Vec::new(),
            enable_trace: false,
        }
    }

    /// Set whether tracing is enabled
    pub fn set_enable_trace(&mut self, enable: bool) {
        self.enable_trace = enable;
    }

    /// Create a trace, add it to the collection, and log it
    pub fn trace_optimizer(
        &mut self,
        execution: OptimizerExecution,
        metadata: &Metadata,
    ) -> Result<()> {
        if !self.enable_trace {
            return Ok(());
        }

        // More robust check for expression changes
        let diff = execution.before.diff(execution.after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        let trace = OptimizerTrace::new(
            execution.name,
            execution.index,
            execution.total,
            execution.time,
            had_effect,
        );

        // Add to collection
        self.traces.push(trace.clone());

        // Log the trace
        trace.log(execution.before, execution.after, metadata)?;

        Ok(())
    }

    /// Generate and log the report
    pub fn log_report(&self) {
        if self.enable_trace && !self.traces.is_empty() {
            info!("{}", self.generate_report());
        }
    }

    /// Generate a summary report of applied optimizers
    pub fn generate_report(&self) -> String {
        if self.traces.is_empty() {
            return "No optimizers were applied.".to_string();
        }

        let mut report = String::new();
        report.push_str("================ Optimizer Execution Summary ================\n\n");

        // Report optimizers that had an effect
        report.push_str("Applied Optimizers:\n");
        let mut has_applied = false;

        for trace in &self.traces {
            if trace.had_effect {
                has_applied = true;
                report.push_str(&format!(
                    "  - [{}] {} (execution time: {:.2?})\n",
                    trace.optimizer_index, trace.optimizer_name, trace.execution_time
                ));
            }
        }

        if !has_applied {
            report.push_str("  None\n");
        }

        // Report optimizers that had no effect
        report.push_str("\nNon-Applied Optimizers:\n");
        let mut has_non_applied = false;

        for trace in &self.traces {
            if !trace.had_effect {
                has_non_applied = true;
                report.push_str(&format!(
                    "  - [{}] {} (execution time: {:.2?})\n",
                    trace.optimizer_index, trace.optimizer_name, trace.execution_time
                ));
            }
        }

        if !has_non_applied {
            report.push_str("  None\n");
        }

        // Total statistics
        let total_optimizers = self.traces.len();
        let applied_optimizers = self.traces.iter().filter(|t| t.had_effect).count();
        let total_time: Duration = self.traces.iter().map(|t| t.execution_time).sum();

        report.push_str(&format!(
            "\nSummary: {}/{} optimizers had effect (total execution time: {:.2?})\n",
            applied_optimizers, total_optimizers, total_time
        ));

        report
    }
}
