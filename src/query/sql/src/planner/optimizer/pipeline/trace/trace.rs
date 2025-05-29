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

use std::collections::HashMap;
use std::time::Duration;

use databend_common_exception::Result;
use log::info;
use parking_lot::Mutex;

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

/// Represents the execution of a rule
#[derive(Clone)]
pub struct RuleExecution {
    /// The name of the rule
    pub name: String,
    /// The execution time of the rule
    pub time: Duration,
    /// The optimizer this rule belongs to
    pub optimizer_name: String,
    /// Whether the rule had an effect (changed the expression)
    pub had_effect: bool,
}

impl RuleExecution {
    /// Create a new rule execution record
    pub fn new(name: String, optimizer_name: String, time: Duration, had_effect: bool) -> Self {
        Self {
            name,
            optimizer_name,
            time,
            had_effect,
        }
    }

    /// Log the rule execution with expression diff
    pub fn log(&self, before_expr: &SExpr, after_expr: &SExpr, metadata: &Metadata) -> Result<()> {
        let mut output = String::new();

        // Add header with effect information
        let effect_status = if self.had_effect {
            "Applied"
        } else {
            "No Effect"
        };

        output.push_str(&format!(
            "================ {} Rule: {} (in optimizer: {}) (execution time: {:.2?}) ================\n",
            effect_status,
            self.name,
            self.optimizer_name,
            self.time
        ));

        // Only generate and show diff if there was an effect
        if self.had_effect {
            let diff = before_expr.diff(after_expr, metadata)?;
            output.push_str(&format!("{}", diff));
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

/// Represents a trace collector for optimizer and rule executions
pub struct OptimizerTraceCollector {
    /// The collection of optimizer traces
    traces: Mutex<Vec<OptimizerTrace>>,
    /// The collection of rule executions, organized by optimizer
    rule_executions: Mutex<HashMap<String, Vec<RuleExecution>>>,
}

impl OptimizerTraceCollector {
    /// Create a new trace collector
    pub fn new() -> Self {
        Self {
            traces: Mutex::new(Vec::new()),
            rule_executions: Mutex::new(HashMap::new()),
        }
    }

    /// Set whether tracing is enabled (this is now a no-op as tracing is controlled externally)
    pub fn set_enable_trace(&self, _enable: bool) {
        // No-op - tracing is now controlled externally
    }

    /// Create a trace, add it to the collection, and log it
    pub fn trace_optimizer(
        &self,
        execution: OptimizerExecution,
        metadata: &Metadata,
    ) -> Result<()> {
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
        self.traces.lock().push(trace.clone());

        // Log the trace
        trace.log(execution.before, execution.after, metadata)?;

        Ok(())
    }

    /// Create a trace for a rule, add it to the collection, and log it
    pub fn trace_rule(
        &self,
        rule_name: String,
        optimizer_name: String,
        time: Duration,
        before: &SExpr,
        after: &SExpr,
        metadata: &Metadata,
    ) -> Result<()> {
        // Check if the rule had an effect on the expression
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // Create a rule execution record
        let execution = RuleExecution::new(rule_name, optimizer_name.clone(), time, had_effect);

        // Log the trace
        execution.log(before, after, metadata)?;

        // Add the rule execution to the corresponding optimizer's collection
        self.rule_executions
            .lock()
            .entry(optimizer_name)
            .or_default()
            .push(execution);

        Ok(())
    }

    /// Generate and log the report
    pub fn log_report(&self) {
        let mut report = String::new();
        report.push_str("================ Optimizer Trace Report ================\n");

        // Log optimizer traces
        let traces = self.traces.lock();
        if !traces.is_empty() {
            report.push_str("--- Optimizers ---\n");
            for trace in &*traces {
                let effect_status = if trace.had_effect {
                    "Applied"
                } else {
                    "No Effect"
                };
                report.push_str(&format!(
                    "{} Optimizer [{}/{}]: {} (execution time: {:.2?})\n",
                    effect_status,
                    trace.optimizer_index,
                    trace.total_optimizers,
                    trace.optimizer_name,
                    trace.execution_time
                ));
            }
        }

        // Log rule traces
        let rule_executions = self.rule_executions.lock();
        if !rule_executions.is_empty() {
            report.push_str("\n--- Rules by Optimizer ---\n");

            // Output each optimizer and its rules
            for (optimizer_name, rules) in &*rule_executions {
                report.push_str(&format!("Optimizer: {}\n", optimizer_name));

                // Count the number of effective rules for this optimizer
                let applied_rules = rules.iter().filter(|r| r.had_effect).count();

                // Calculate the total execution time of all rules for this optimizer
                let total_time: Duration = rules.iter().map(|r| r.time).sum();

                // Output each rule's execution status
                for rule in rules {
                    let effect_status = if rule.had_effect {
                        "Applied"
                    } else {
                        "No Effect"
                    };
                    report.push_str(&format!(
                        "  {} Rule: {} (execution time: {:.2?})\n",
                        effect_status, rule.name, rule.time
                    ));
                }

                // Output a summary of rule executions for this optimizer
                report.push_str(&format!(
                    "  Summary: {}/{} rules had effect (total execution time: {:.2?})\n\n",
                    applied_rules,
                    rules.len(),
                    total_time
                ));
            }
        }

        // Output overall summary
        let traces = self.traces.lock();
        let rule_executions = self.rule_executions.lock();

        let total_optimizers = traces.len();
        let applied_optimizers = traces.iter().filter(|t| t.had_effect).count();
        let total_optimizer_time: Duration = traces.iter().map(|t| t.execution_time).sum();

        let total_rules: usize = rule_executions.values().map(|rules| rules.len()).sum();
        let applied_rules: usize = rule_executions
            .values()
            .flat_map(|rules| rules.iter())
            .filter(|r| r.had_effect)
            .count();
        let total_rule_time: Duration = rule_executions
            .values()
            .flat_map(|rules| rules.iter())
            .map(|r| r.time)
            .sum();

        report.push_str(&format!(
                "\nSummary:\n  Optimizers: {}/{} had effect (total time: {:.2?})\n  Rules: {}/{} had effect (total time: {:.2?})\n",
                applied_optimizers, total_optimizers, total_optimizer_time,
                applied_rules, total_rules, total_rule_time
            ));

        info!("{}", report);
    }

    /// Generate a summary report of applied optimizers and rules
    pub fn generate_report(&self) -> String {
        if self.traces.lock().is_empty() && self.rule_executions.lock().is_empty() {
            return "No optimizers or rules were applied.\nEnable tracing with 'SET enable_optimizer_trace = 1;'".to_string();
        }

        let mut report = String::new();
        report.push_str("================ Optimizer Execution Summary ================\n\n");

        // Report optimizers that had an effect
        report.push_str("Applied Optimizers:\n");
        let mut has_applied = false;
        let traces = self.traces.lock();

        for trace in &*traces {
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
        let traces = self.traces.lock();

        for trace in &*traces {
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

        // Report rules that had an effect
        report.push_str("\nApplied Rules by Optimizer:\n");
        let mut has_applied_rules = false;
        let rule_executions = self.rule_executions.lock();

        for (optimizer_name, rules) in &*rule_executions {
            let applied_rules: Vec<_> = rules.iter().filter(|r| r.had_effect).collect();
            if !applied_rules.is_empty() {
                has_applied_rules = true;
                report.push_str(&format!("  Optimizer: {}\n", optimizer_name));
                for rule in applied_rules {
                    report.push_str(&format!(
                        "    - {} (execution time: {:.2?})\n",
                        rule.name, rule.time
                    ));
                }
            }
        }

        if !has_applied_rules {
            report.push_str("  None\n");
        }

        // Total statistics
        let traces = self.traces.lock();
        let rule_executions = self.rule_executions.lock();

        let total_optimizers = traces.len();
        let applied_optimizers = traces.iter().filter(|t| t.had_effect).count();
        let total_optimizer_time: Duration = traces.iter().map(|t| t.execution_time).sum();

        let total_rules: usize = rule_executions.values().map(|rules| rules.len()).sum();
        let applied_rules: usize = rule_executions
            .values()
            .flat_map(|rules| rules.iter())
            .filter(|r| r.had_effect)
            .count();
        let total_rule_time: Duration = rule_executions
            .values()
            .flat_map(|rules| rules.iter())
            .map(|r| r.time)
            .sum();

        report.push_str(&format!(
            "\nSummary:\n  Optimizers: {}/{} had effect (total time: {:.2?})\n  Rules: {}/{} had effect (total time: {:.2?})\n",
            applied_optimizers, total_optimizers, total_optimizer_time,
            applied_rules, total_rules, total_rule_time
        ));

        report
    }
}

impl Default for OptimizerTraceCollector {
    fn default() -> Self {
        Self::new()
    }
}
