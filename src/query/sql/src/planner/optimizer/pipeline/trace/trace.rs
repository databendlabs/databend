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

/// Represents a trace collector for optimizer and rule executions
pub struct OptimizerTraceCollector {
    /// The collection of optimizer traces, ordered by index
    optimizers: Mutex<BTreeMap<usize, OptimizerTrace>>,
    /// The collection of rule traces, organized by optimizer name
    /// Using BTreeMap<rule_name, RuleTrace> for faster lookup instead of Vec<RuleTrace>
    rules: Mutex<BTreeMap<String, BTreeMap<String, RuleTrace>>>,
}

impl OptimizerTraceCollector {
    /// Create a new trace collector
    pub fn new() -> Self {
        Self {
            optimizers: Mutex::new(BTreeMap::new()),
            rules: Mutex::new(BTreeMap::new()),
        }
    }

    /// Create a trace for an optimizer, add it to the collection
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
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        let trace = OptimizerTrace::new(name, index, total, time, had_effect, diff);
        self.optimizers.lock().insert(index, trace);

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
        let diff = before.diff(after, metadata)?;
        let had_effect = !diff.is_empty() && diff != "No differences found.";

        // Update optimizer effect status if rule had effect
        if had_effect {
            if let Some(optimizer) = self
                .optimizers
                .lock()
                .values_mut()
                .find(|o| o.name == optimizer_name)
            {
                optimizer.had_effect = true;
            }
        }

        // Add or update rule trace using BTreeMap for O(log n) lookup
        let mut rules = self.rules.lock();
        let optimizer_rules = rules.entry(optimizer_name.clone()).or_default();

        // Get the next sequence number for this rule if it's new
        // Sequence starts from 0 for each optimizer
        let sequence = optimizer_rules.len();

        // Using BTreeMap's entry API for cleaner and more efficient update
        match optimizer_rules.entry(rule_name.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                // Update existing rule
                let existing = entry.get_mut();
                existing.time += time;
                existing.had_effect |= had_effect;
                if had_effect {
                    existing.diff = diff;
                }
                // Sequence number remains unchanged
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                // Insert new rule with sequence number
                entry.insert(RuleTrace::new(rule_name, time, had_effect, diff, sequence));
            }
        }

        Ok(())
    }

    /// Update the had_effect flag for an optimizer
    pub fn update_optimizer_effect(&self, optimizer_name: String) -> Result<()> {
        if let Some(optimizer) = self
            .optimizers
            .lock()
            .values_mut()
            .find(|o| o.name == optimizer_name)
        {
            optimizer.had_effect = true;
        }
        Ok(())
    }

    /// Generate and log the optimizer trace report
    pub fn log_report(&self) {
        let optimizers = self.optimizers.lock();
        let rules = self.rules.lock();

        if optimizers.is_empty() {
            return;
        }

        // Log optimizer summary as a single log entry
        self.log_optimizers_summary(&optimizers, &rules);

        self.log_optimizers_details(&optimizers, &rules);
    }

    /// Log optimizers summary
    fn log_optimizers_summary(
        &self,
        optimizers: &BTreeMap<usize, OptimizerTrace>,
        rules: &BTreeMap<String, BTreeMap<String, RuleTrace>>,
    ) {
        // Create a single summary log for all optimizers
        let mut summary = String::new();
        summary.push_str("========== OPTIMIZERS SUMMARY TRACE ==========\n\n");

        for optimizer in optimizers.values() {
            let status_symbol = if optimizer.had_effect { "✓" } else { "✗" };

            // Count applied rules for this optimizer
            let optimizer_rules = rules.get(&optimizer.name);
            let (applied_rules, total_rules) = if let Some(optimizer_rules) = optimizer_rules {
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

            // Only show rules information if the optimizer has rules
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

                // Show rules summary for this optimizer
                if let Some(optimizer_rules) = optimizer_rules {
                    if !optimizer_rules.is_empty() {
                        let mut rules_summary = String::new();
                        self.build_rules_summary(&mut rules_summary, optimizer, optimizer_rules);

                        // Add extra indentation to the rules summary
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

        // Log the entire summary as one log entry
        info!("{}", summary);
    }

    /// Log detailed optimizer executions
    fn log_optimizers_details(
        &self,
        optimizers: &BTreeMap<usize, OptimizerTrace>,
        rules: &BTreeMap<String, BTreeMap<String, RuleTrace>>,
    ) {
        info!("========== OPTIMIZERS EXECUTIONS TRACE==========\n");
        for optimizer in optimizers.values() {
            let status_symbol = if optimizer.had_effect { "✓" } else { "✗" };

            // Build the optimizer detail report
            let mut detail = String::new();
            detail.push_str(&format!(
                "[{}] {}: {} ({:.2?})\n\n",
                status_symbol, optimizer.index, optimizer.name, optimizer.time
            ));

            // Add changes if any
            if optimizer.had_effect && !optimizer.diff.is_empty() {
                detail.push_str("  Changes:\n");
                for line in optimizer.diff.lines() {
                    detail.push_str(&format!("    {}\n", line));
                }
                detail.push('\n');
            }

            // Add rules for this optimizer
            let optimizer_rules = rules.get(&optimizer.name);
            if let Some(optimizer_rules) = optimizer_rules {
                if !optimizer_rules.is_empty() {
                    // Add rules summary
                    let mut rules_summary = String::new();
                    self.build_rules_summary(&mut rules_summary, optimizer, optimizer_rules);
                    detail.push_str(&rules_summary);

                    // Add applied rules details
                    let mut applied_rules_details = String::new();
                    self.build_applied_rules_details(
                        &mut applied_rules_details,
                        optimizer,
                        optimizer_rules,
                    );
                    detail.push_str(&applied_rules_details);
                }
            }

            // Log the entire optimizer detail as one log entry
            info!("{}", detail);
        }
    }

    /// Build rules summary for an optimizer
    fn build_rules_summary(
        &self,
        report: &mut String,
        optimizer: &OptimizerTrace,
        optimizer_rules: &BTreeMap<String, RuleTrace>,
    ) {
        let mut all_rules: Vec<_> = optimizer_rules.values().collect();
        all_rules.sort_by_key(|r| r.sequence);

        if all_rules.is_empty() {
            return;
        }

        report.push_str(&format!("[{}]  Rules Summary:\n", optimizer.name));

        // Add each rule
        for rule in &all_rules {
            let status_symbol = if rule.had_effect { "✓" } else { "✗" };
            report.push_str(&format!(
                "    [{}] {}.{}: {} ({:.2?})\n",
                status_symbol, optimizer.index, rule.sequence, rule.name, rule.time
            ));
        }

        report.push('\n');

        // Add rules statistics
        let applied_rules = all_rules.iter().filter(|r| r.had_effect).count();
        let total_rules = optimizer_rules.len();
        let applied_percentage = if total_rules > 0 {
            (applied_rules * 100) / total_rules
        } else {
            0
        };

        report.push_str(&format!(
            "  Total Applied Rules: {}/{} ({}%)\n",
            applied_rules, total_rules, applied_percentage
        ));

        report.push_str(&format!(
            "  Total Non-Applied Rules: {}/{} ({}%)\n\n",
            total_rules - applied_rules,
            total_rules,
            if total_rules > 0 {
                100 - applied_percentage
            } else {
                0
            }
        ));
    }

    /// Build applied rules details for an optimizer
    fn build_applied_rules_details(
        &self,
        report: &mut String,
        optimizer: &OptimizerTrace,
        optimizer_rules: &BTreeMap<String, RuleTrace>,
    ) {
        let mut all_rules: Vec<_> = optimizer_rules.values().collect();
        all_rules.sort_by_key(|r| r.sequence);

        let applied_rules: Vec<_> = all_rules.iter().filter(|r| r.had_effect).collect();

        if applied_rules.is_empty() {
            return;
        }

        report.push_str("  Applied Rules Details:\n");

        for rule in applied_rules {
            let status_symbol = "✓";
            report.push_str(&format!(
                "    [{}] {}.{}: {} ({:.2?})\n",
                status_symbol, optimizer.index, rule.sequence, rule.name, rule.time
            ));

            if !rule.diff.is_empty() {
                report.push_str("      Changes:\n");
                for line in rule.diff.lines() {
                    report.push_str(&format!("        {}\n", line));
                }
            }

            report.push('\n');
        }
    }
}

impl Default for OptimizerTraceCollector {
    fn default() -> Self {
        Self::new()
    }
}
