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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainKind {
    Ast(String),
    Syntax(String),
    // The display string will be filled by optimizer, as we
    // don't want to expose `Memo` to other crates.
    Memo(String),
    Graph,
    Pipeline,
    Fragments,

    /// `EXPLAIN RAW` will be deprecated in the future, use EXPLAIN(LOGICAL) instead
    Raw,
    /// `EXPLAIN DECORRELATED` will show the plan after subquery decorrelation
    /// `EXPLAIN DECORRELATED` will be deprecated in the future, use `EXPLAIN(LOGICAL, DECORRELATED)` instead
    Decorrelated,
    /// `EXPLAIN OPTIMIZED` will be deprecated in the future, use `EXPLAIN(LOGICAL, OPTIMIZED)` instead
    Optimized,

    Plan,

    Join,

    // Explain analyze plan
    AnalyzePlan,

    Graphical,

    Perf,

    Trace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainOption {
    Verbose,
    Logical,
    Optimized,
    Decorrelated,
}

/// Trace level for EXPLAIN TRACE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Drive, DriveMut)]
pub enum TraceLevel {
    /// Only high-level spans (excludes processor-level spans like *::process)
    High,
    /// All spans (default)
    #[default]
    All,
}

impl Display for TraceLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceLevel::High => write!(f, "high"),
            TraceLevel::All => write!(f, "all"),
        }
    }
}

/// Comparison operator for trace filter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum TraceFilterOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
}

impl Display for TraceFilterOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceFilterOp::Gt => write!(f, ">"),
            TraceFilterOp::Gte => write!(f, ">="),
            TraceFilterOp::Lt => write!(f, "<"),
            TraceFilterOp::Lte => write!(f, "<="),
            TraceFilterOp::Eq => write!(f, "="),
        }
    }
}

/// Filter condition for EXPLAIN TRACE
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum TraceFilter {
    /// Filter by duration (stored in microseconds for precision)
    Duration {
        op: TraceFilterOp,
        threshold_us: u64,
    },
    /// Filter by span name pattern (supports LIKE syntax with % wildcard)
    Name { pattern: String, negated: bool },
    /// Logical AND of two filters
    And(Box<TraceFilter>, Box<TraceFilter>),
    /// Logical OR of two filters
    Or(Box<TraceFilter>, Box<TraceFilter>),
}

impl Display for TraceFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceFilter::Duration { op, threshold_us } => {
                // Display in the most readable unit
                if *threshold_us >= 1_000_000 && threshold_us % 1_000_000 == 0 {
                    write!(f, "duration {} {}s", op, threshold_us / 1_000_000)
                } else if *threshold_us >= 1_000 && threshold_us % 1_000 == 0 {
                    write!(f, "duration {} {}ms", op, threshold_us / 1_000)
                } else {
                    write!(f, "duration {} {}us", op, threshold_us)
                }
            }
            TraceFilter::Name { pattern, negated } => {
                if *negated {
                    write!(f, "name NOT LIKE '{}'", pattern)
                } else {
                    write!(f, "name LIKE '{}'", pattern)
                }
            }
            TraceFilter::And(a, b) => write!(f, "({} AND {})", a, b),
            TraceFilter::Or(a, b) => write!(f, "({} OR {})", a, b),
        }
    }
}

/// Options for EXPLAIN TRACE
#[derive(Debug, Clone, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct ExplainTraceOptions {
    pub level: TraceLevel,
    pub filter: Option<TraceFilter>,
}

impl Display for ExplainTraceOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.level != TraceLevel::All {
            parts.push(format!("LEVEL = '{}'", self.level));
        }
        if let Some(filter) = &self.filter {
            parts.push(format!("FILTER {}", filter));
        }
        if !parts.is_empty() {
            write!(f, "({})", parts.join(", "))?;
        }
        Ok(())
    }
}
