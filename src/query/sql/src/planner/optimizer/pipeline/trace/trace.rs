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

use crate::Metadata;
use crate::optimizer::ir::SExpr;

/// Represents a trace entry for a rule execution
#[derive(Clone)]
pub struct RuleTrace {
    /// Name of the rule
    pub name: String,
    /// Time spent executing the rule
    pub time: Duration,
    /// Whether the rule had an effect
    pub had_effect: bool,
    /// Diff of the expression before and after the rule
    pub diff: String,
    /// Sequence number of the rule in the optimizer
    pub sequence: usize,
}

impl RuleTrace {
    /// Create a new rule trace
    pub fn new(
        name: String,
        time: Duration,
        had_effect: bool,
        diff: String,
        sequence: usize,
    ) -> Self {
        Self {
            name,
            time,
            had_effect,
            diff,
            sequence,
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

/// Collects and organizes traces from optimizer and rule executions.
///
/// This collector maintains two primary data structures:
/// - A map of optimizer traces indexed by their position in the pipeline
/// - A nested map of rule traces organized by optimizer name and rule name
pub struct OptimizerTraceCollector {
    /// Optimizer traces ordered by their index in the pipeline and optimizer name
    optimizers: Mutex<BTreeMap<usize, BTreeMap<String, OptimizerTrace>>>,

    /// Rule traces organized by optimizer name and rule name
    /// Uses BTreeMap for O(log n) lookup performance
    rules: Mutex<BTreeMap<String, BTreeMap<String, RuleTrace>>>,
}

impl OptimizerTraceCollector {
    /// Creates a new empty trace collector.
    pub fn new() -> Self {
        Self {
            optimizers: Mutex::new(BTreeMap::new()),
            rules: Mutex::new(BTreeMap::new()),
        }
    }

    /// Records a trace for an optimizer execution.
    ///
    /// # Arguments
    /// * `name` - Name of the optimizer
    /// * `index` - Position of the optimizer in the pipeline
    /// * `total` - Total number of optimizers in the pipeline
    /// * `time` - Duration of the optimizer execution
    /// * `before` - Expression state before optimization
    /// * `after` - Expression state after optimization
    /// * `metadata` - Metadata for expression comparison
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
        // Calculate diff and determine if optimizer had an effect
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // Create and store the optimizer trace
        let trace = OptimizerTrace::new(name.clone(), index, total, time, had_effect, diff);
        let mut optimizers = self.optimizers.lock();

        // Get or create the BTreeMap for this index
        let index_map = optimizers.entry(index).or_default();
        index_map.insert(name, trace);

        Ok(())
    }

    /// Records a trace for a rule execution within an optimizer.
    ///
    /// # Arguments
    /// * `rule_name` - Name of the rule
    /// * `optimizer_name` - Name of the parent optimizer
    /// * `time` - Duration of the rule execution
    /// * `before` - Expression state before rule application
    /// * `after` - Expression state after rule application
    /// * `metadata` - Metadata for expression comparison
    pub fn trace_rule(
        &self,
        rule_name: String,
        optimizer_name: String,
        time: Duration,
        before: &SExpr,
        after: &SExpr,
        metadata: &Metadata,
    ) -> Result<()> {
        // Calculate diff and determine if rule had an effect
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // If rule had an effect, update the parent optimizer's status
        if had_effect {
            self.mark_optimizer_as_effective(&optimizer_name);
        }

        // Add or update the rule trace
        self.record_rule_trace(rule_name, optimizer_name, time, had_effect, diff);

        Ok(())
    }

    /// Explicitly marks an optimizer as having an effect.
    pub fn update_optimizer_effect(&self, optimizer_name: String) -> Result<()> {
        self.mark_optimizer_as_effective(&optimizer_name);
        Ok(())
    }

    /// Helper method to mark an optimizer as having an effect.
    fn mark_optimizer_as_effective(&self, optimizer_name: &str) {
        let mut optimizers = self.optimizers.lock();

        // Iterate through all indices and find the optimizer by name
        for index_map in optimizers.values_mut() {
            if let Some(optimizer) = index_map.get_mut(optimizer_name) {
                optimizer.had_effect = true;
                break;
            }
        }
    }

    /// Helper method to record or update a rule trace.
    fn record_rule_trace(
        &self,
        rule_name: String,
        optimizer_name: String,
        time: Duration,
        had_effect: bool,
        diff: String,
    ) {
        // Acquire lock on rules collection
        let mut rules = self.rules.lock();

        // Get or create the rules map for this optimizer
        let optimizer_rules = rules.entry(optimizer_name).or_default();

        // Determine sequence number for new rules
        let sequence = optimizer_rules.len();

        // Add or update the rule trace
        match optimizer_rules.entry(rule_name.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                // Update existing rule
                let existing = entry.get_mut();
                existing.time += time;
                existing.had_effect |= had_effect;
                if had_effect {
                    existing.diff = diff;
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                // Insert new rule with sequence number
                entry.insert(RuleTrace::new(rule_name, time, had_effect, diff, sequence));
            }
        }
    }

    /// Generates and logs a comprehensive report of optimizer and rule executions.
    ///
    /// This method produces two separate log entries:
    /// 1. A summary of all optimizers and their effects
    /// 2. Detailed execution information for each optimizer
    pub fn log_report(&self) {
        let optimizers = self.optimizers.lock();
        let rules = self.rules.lock();

        if optimizers.is_empty() {
            return;
        }

        // Generate detailed reports and output each one
        let details_reports = self.generate_optimizers_details_reports(&optimizers, &rules);
        for detail_report in details_reports {
            info!("{}", detail_report);
        }

        // Generate summary report and output
        let summary_report = self.generate_optimizers_summary_report(&optimizers, &rules);
        info!("{}", summary_report);
    }

    fn generate_optimizers_summary_report(
        &self,
        optimizers: &BTreeMap<usize, BTreeMap<String, OptimizerTrace>>,
        rules: &BTreeMap<String, BTreeMap<String, RuleTrace>>,
    ) -> String {
        let mut summary = String::new();
        summary.push_str("========== OPTIMIZERS SUMMARY TRACE ==========\n\n");

        for optimizer_map in optimizers.values() {
            for optimizer in optimizer_map.values() {
                let status_symbol = if optimizer.had_effect { "✓" } else { "✗" };

                // Calculate rule statistics
                let (applied_rules, total_rules) =
                    if let Some(optimizer_rules) = rules.get(&optimizer.name) {
                        let applied = optimizer_rules.values().filter(|r| r.had_effect).count();
                        let total = optimizer_rules.len();
                        (applied, total)
                    } else {
                        (0, 0)
                    };

                summary.push_str(&format!(
                    "[{}] {}: {} ({:.2?})\n",
                    status_symbol, optimizer.index, optimizer.name, optimizer.time
                ));

                if total_rules > 0 {
                    let applied_percentage = if total_rules > 0 {
                        (applied_rules * 100) / total_rules
                    } else {
                        0
                    };

                    summary.push_str(&format!(
                        "  └── Rules: {}/{} Applied ({}%)\n",
                        applied_rules, total_rules, applied_percentage
                    ));

                    // Add detailed rules summary if available
                    if let Some(optimizer_rules) = rules.get(&optimizer.name) {
                        if !optimizer_rules.is_empty() {
                            let rules_summary =
                                self.generate_rules_summary_report(optimizer, optimizer_rules);

                            // Add indentation to rules summary
                            for line in rules_summary.lines() {
                                if !line.trim().is_empty() {
                                    summary.push_str(&format!("  {}\n", line));
                                }
                            }
                        }
                    }
                    summary.push('\n');
                } else {
                    summary.push('\n');
                }
            }
        }

        summary
    }

    fn generate_optimizers_details_reports(
        &self,
        optimizers: &BTreeMap<usize, BTreeMap<String, OptimizerTrace>>,
        rules: &BTreeMap<String, BTreeMap<String, RuleTrace>>,
    ) -> Vec<String> {
        let mut reports = Vec::new();

        for optimizer_map in optimizers.values() {
            for optimizer in optimizer_map.values() {
                let status_symbol = if optimizer.had_effect { "✓" } else { "✗" };
                let mut detail = String::new();

                // Add basic optimizer information
                detail.push_str(&format!(
                    "[{}] {}: {} ({:.2?})\n\n",
                    status_symbol, optimizer.index, optimizer.name, optimizer.time
                ));

                // Add expression changes if any
                if optimizer.had_effect && !optimizer.diff.is_empty() {
                    detail.push_str("  Changes:\n");
                    for line in optimizer.diff.lines() {
                        detail.push_str(&format!("    {}\n", line));
                    }
                    detail.push('\n');
                }

                // Add rule information if available
                if let Some(optimizer_rules) = rules.get(&optimizer.name) {
                    if !optimizer_rules.is_empty() {
                        let rules_summary =
                            self.generate_rules_summary_report(optimizer, optimizer_rules);
                        detail.push_str(&rules_summary);

                        let applied_rules_details =
                            self.generate_applied_rules_details_report(optimizer, optimizer_rules);
                        detail.push_str(&applied_rules_details);
                    }
                }

                reports.push(detail);
            }
        }

        reports
    }

    fn generate_rules_summary_report(
        &self,
        optimizer: &OptimizerTrace,
        optimizer_rules: &BTreeMap<String, RuleTrace>,
    ) -> String {
        let mut report = String::new();

        // Sort rules by sequence number
        let mut all_rules: Vec<_> = optimizer_rules.values().collect();
        all_rules.sort_by_key(|r| r.sequence);

        if all_rules.is_empty() {
            return report;
        }

        report.push_str(&format!("[{}]  Rules Summary:\n", optimizer.name));

        // List all rules with their status
        for rule in &all_rules {
            let status_symbol = if rule.had_effect { "✓" } else { "✗" };
            report.push_str(&format!(
                "    [{}] {}.{}: {} ({:.2?})\n",
                status_symbol, optimizer.index, rule.sequence, rule.name, rule.time
            ));
        }
        report.push('\n');

        // Calculate statistics
        let total_rules = optimizer_rules.len();
        let applied_rules = optimizer_rules.values().filter(|r| r.had_effect).count();

        let applied_percentage = if total_rules > 0 {
            (applied_rules * 100) / total_rules
        } else {
            0
        };

        let non_applied_percentage = if total_rules > 0 {
            100 - applied_percentage
        } else {
            0
        };

        // Add statistics to report
        report.push_str(&format!(
            "  Total Applied Rules: {}/{} ({}%)\n",
            applied_rules, total_rules, applied_percentage
        ));

        report.push_str(&format!(
            "  Total Non-Applied Rules: {}/{} ({}%)\n\n",
            total_rules - applied_rules,
            total_rules,
            non_applied_percentage
        ));

        report
    }

    fn generate_applied_rules_details_report(
        &self,
        optimizer: &OptimizerTrace,
        optimizer_rules: &BTreeMap<String, RuleTrace>,
    ) -> String {
        let mut report = String::new();

        // Get rules and filter for those that had an effect
        let mut all_rules: Vec<_> = optimizer_rules.values().collect();
        all_rules.sort_by_key(|r| r.sequence);
        let applied_rules: Vec<_> = all_rules.iter().filter(|r| r.had_effect).collect();

        if applied_rules.is_empty() {
            return report;
        }

        report.push_str("  Applied Rules Details:\n");

        for rule in applied_rules {
            // Always use checkmark since we only show rules that had an effect
            let status_symbol = "✓";
            report.push_str(&format!(
                "    [{}] {}.{}: {}.{} ({:.2?})\n",
                status_symbol, optimizer.index, rule.sequence, optimizer.name, rule.name, rule.time
            ));

            // Add expression changes if available
            if !rule.diff.is_empty() {
                report.push_str("      Changes:\n");
                for line in rule.diff.lines() {
                    report.push_str(&format!("        {}\n", line));
                }
            }

            report.push('\n');
        }

        report
    }
}

impl Default for OptimizerTraceCollector {
    fn default() -> Self {
        Self::new()
    }
}
