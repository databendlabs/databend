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

use std::collections::BTreeMap;
use std::time::Duration;

use databend_common_exception::Result;
use log::info;
use parking_lot::Mutex;

use crate::optimizer::ir::SExpr;
use crate::Metadata;

/// Represents a trace entry for a rule execution
#[derive(Clone)]
pub struct RuleTrace {
    /// The name of the rule
    pub name: String,
    /// The execution time of the rule
    pub time: Duration,
    /// Whether the rule had an effect (changed the expression)
    pub had_effect: bool,
    /// The diff between before and after expressions
    pub diff: String,
}

impl RuleTrace {
    /// Create a new rule trace
    pub fn new(name: String, time: Duration, had_effect: bool, diff: String) -> Self {
        Self {
            name,
            time,
            had_effect,
            diff,
        }
    }
}

/// Represents a trace entry for an optimizer execution
#[derive(Clone)]
pub struct OptimizerTrace {
    /// The name of the optimizer
    pub name: String,
    /// The index of the optimizer in the pipeline
    pub index: usize,
    /// The total number of optimizers in the pipeline
    pub total: usize,
    /// The execution time of the optimizer
    pub time: Duration,
    /// Whether the optimizer had an effect (changed the expression)
    pub had_effect: bool,
    /// The diff between before and after expressions
    pub diff: String,
}

impl OptimizerTrace {
    /// Create a new optimizer trace
    pub fn new(
        name: String,
        index: usize,
        total: usize,
        time: Duration,
        had_effect: bool,
        diff: String,
    ) -> Self {
        Self {
            name,
            index,
            total,
            time,
            had_effect,
            diff,
        }
    }
}

/// Represents a trace collector for optimizer and rule executions
pub struct OptimizerTraceCollector {
    /// The collection of optimizer traces, ordered by index
    optimizers: Mutex<BTreeMap<usize, OptimizerTrace>>,
    /// The collection of rule traces, organized by optimizer name
    rules: Mutex<BTreeMap<String, Vec<RuleTrace>>>,
}

impl OptimizerTraceCollector {
    /// Create a new trace collector
    pub fn new() -> Self {
        Self {
            optimizers: Mutex::new(BTreeMap::new()),
            rules: Mutex::new(BTreeMap::new()),
        }
    }

    /// Create a trace, add it to the collection
    pub fn trace_optimizer(
        &self,
        name: String,
        index: usize,
        total: usize,
        time: Duration,
        before: &SExpr,
        after: &SExpr,
        metadata: &Metadata,
    ) -> Result<()> {
        // Check for expression changes
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // Create optimizer trace
        let trace = OptimizerTrace::new(name.clone(), index, total, time, had_effect, diff);

        // Store optimizer trace
        let mut optimizers = self.optimizers.lock();
        optimizers.insert(index, trace);

        Ok(())
    }

    /// Create a trace for a rule, add it to the collection
    pub fn trace_rule(
        &self,
        rule_name: String,
        optimizer_name: String,
        time: Duration,
        before: &SExpr,
        after: &SExpr,
        metadata: &Metadata,
    ) -> Result<()> {
        // Check if the rule had an effect
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // Create the rule trace
        let rule_trace = RuleTrace::new(rule_name, time, had_effect, diff);

        // IMPORTANT: Always acquire locks in the same order as log_report to prevent deadlocks
        // First acquire optimizers lock
        if had_effect {
            let mut optimizers = self.optimizers.lock();
            for optimizer in optimizers.values_mut() {
                if optimizer.name == optimizer_name {
                    optimizer.had_effect = true;
                    break;
                }
            }
        }

        // Then acquire rules lock
        let mut rules = self.rules.lock();
        rules.entry(optimizer_name).or_default().push(rule_trace);

        Ok(())
    }

    /// Generate and log the optimizer trace report
    pub fn log_report(&self) {
        // Get copies of the data
        let optimizers = self.optimizers.lock().clone();
        let rules = self.rules.lock().clone();

        // Check if there's any data to report
        if optimizers.is_empty() && rules.is_empty() {
            info!("No optimizers or rules were applied. Enable tracing with 'SET enable_optimizer_trace = 1;'");
            return;
        }

        // First, generate and log the summary report
        self.log_summary_report(&optimizers, &rules);

        // Then, generate and log detailed reports for each optimizer
        for optimizer in optimizers.values() {
            self.log_optimizer_detail_report(optimizer, rules.get(&optimizer.name));
        }
    }

    /// Generate and log the summary report
    fn log_summary_report(
        &self,
        optimizers: &BTreeMap<usize, OptimizerTrace>,
        rules: &BTreeMap<String, Vec<RuleTrace>>,
    ) {
        let mut report = String::new();
        report.push_str("================ Optimizer Execution Summary ================\n\n");

        // Calculate statistics
        let total_optimizers = optimizers.len();
        let applied_optimizers = optimizers.values().filter(|t| t.had_effect).count();
        let total_optimizer_time: Duration = optimizers.values().map(|t| t.time).sum();

        let total_rules: usize = rules.values().map(|rules| rules.len()).sum();
        let applied_rules: usize = rules
            .values()
            .flat_map(|rules| rules.iter())
            .filter(|r| r.had_effect)
            .count();
        let total_rule_time: Duration = rules
            .values()
            .flat_map(|rules| rules.iter())
            .map(|r| r.time)
            .sum();

        // Report applied optimizers
        report.push_str("Applied Optimizers:\n");
        let mut has_applied = false;

        for optimizer in optimizers.values() {
            if optimizer.had_effect {
                has_applied = true;

                // Output optimizer information
                report.push_str(&format!(
                    "  - [{}] {} (execution time: {:.2?})\n",
                    optimizer.index, optimizer.name, optimizer.time
                ));

                // Find rules for this optimizer
                if let Some(optimizer_rules) = rules.get(&optimizer.name) {
                    // Process rules
                    let applied_rules: Vec<_> =
                        optimizer_rules.iter().filter(|r| r.had_effect).collect();
                    let non_applied_rules: Vec<_> =
                        optimizer_rules.iter().filter(|r| !r.had_effect).collect();

                    if !applied_rules.is_empty() {
                        report.push_str("    ├── Applied Rules:\n");
                        for (i, rule) in applied_rules.iter().enumerate() {
                            let is_last =
                                i == applied_rules.len() - 1 && non_applied_rules.is_empty();
                            let prefix = if is_last {
                                "    │   └── "
                            } else {
                                "    │   ├── "
                            };
                            report.push_str(&format!(
                                "{prefix}[{}.{}] {} (execution time: {:.2?})\n",
                                optimizer.index,
                                i + 1,
                                rule.name,
                                rule.time
                            ));
                        }
                    }

                    if !non_applied_rules.is_empty() {
                        report.push_str("    └── Non-Applied Rules:\n");
                        for (i, rule) in non_applied_rules.iter().enumerate() {
                            let is_last = i == non_applied_rules.len() - 1;
                            let prefix = if is_last {
                                "        └── "
                            } else {
                                "        ├── "
                            };
                            report.push_str(&format!(
                                "{prefix}[{}.{}] {} (execution time: {:.2?})\n",
                                optimizer.index,
                                applied_rules.len() + i + 1,
                                rule.name,
                                rule.time
                            ));
                        }
                    }
                }
            }
        }

        // If no optimizers had an effect
        if !has_applied {
            report.push_str("  None\n");
        }

        // Report non-applied optimizers
        report.push_str("\nNon-Applied Optimizers:\n");
        let mut has_non_applied = false;

        for optimizer in optimizers.values() {
            if !optimizer.had_effect {
                has_non_applied = true;

                // Output optimizer information
                report.push_str(&format!(
                    "  - [{}] {} (execution time: {:.2?})\n",
                    optimizer.index, optimizer.name, optimizer.time
                ));

                // Find rules for this optimizer
                if let Some(optimizer_rules) = rules.get(&optimizer.name) {
                    // Process non-applied rules
                    let non_applied_rules: Vec<_> =
                        optimizer_rules.iter().filter(|r| !r.had_effect).collect();

                    if !non_applied_rules.is_empty() {
                        report.push_str("    └── Non-Applied Rules:\n");
                        for (i, rule) in non_applied_rules.iter().enumerate() {
                            let is_last = i == non_applied_rules.len() - 1;
                            let prefix = if is_last {
                                "        └── "
                            } else {
                                "        ├── "
                            };
                            report.push_str(&format!(
                                "{prefix}[{}.{}] {} (execution time: {:.2?})\n",
                                optimizer.index,
                                i + 1,
                                rule.name,
                                rule.time
                            ));
                        }
                    }
                }
            }
        }

        // If all optimizers had an effect
        if !has_non_applied {
            report.push_str("  None\n");
        }

        // Output summary
        report.push_str(&format!(
            "\nSummary: {}/{} optimizers had effect (total execution time: {:.2?})\n",
            applied_optimizers, total_optimizers, total_optimizer_time
        ));

        if total_rules > 0 {
            report.push_str(&format!(
                "Rules: {}/{} rules had effect (total execution time: {:.2?})\n",
                applied_rules, total_rules, total_rule_time
            ));
        }

        info!("{}", report);
    }

    /// Generate and log detailed report for a specific optimizer
    fn log_optimizer_detail_report(
        &self,
        optimizer: &OptimizerTrace,
        optimizer_rules: Option<&Vec<RuleTrace>>,
    ) {
        let effect_status = if optimizer.had_effect {
            "Applied"
        } else {
            "No Effect"
        };

        // Build the complete report in a single string
        let mut report = format!(
            "================ {} Optimizer [{}/{}]: {} (execution time: {:.2?}) ================\n",
            effect_status, optimizer.index, optimizer.total, optimizer.name, optimizer.time
        );

        // Add optimizer diff if it had an effect
        if optimizer.had_effect {
            report.push_str(&optimizer.diff);
            report.push('\n');
        }

        // Add rule details if available
        if let Some(rules) = optimizer_rules {
            for (i, rule) in rules.iter().enumerate() {
                if rule.had_effect {
                    report.push_str(&format!(
                        "---- Rule [{}.{}]: {} (execution time: {:.2?}) ----\n",
                        optimizer.index,
                        i + 1,
                        rule.name,
                        rule.time
                    ));

                    // Add rule diff
                    report.push_str(&rule.diff);
                    report.push('\n');
                }
            }
        }

        // Log the entire report in a single call
        info!("{}", report);
    }
}

impl Default for OptimizerTraceCollector {
    fn default() -> Self {
        Self::new()
    }
}
