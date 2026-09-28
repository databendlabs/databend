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

    /// `EXPLAIN PERF [CPU | MEMORY] [(format = '...', limit = <n>)] <statement>`
    Perf {
        mode: ExplainPerfMode,
        format: ExplainPerfFormat,
        /// The number of rows per group in the `table` and `folded` formats, `None` for the default.
        limit: Option<u64>,
    },
}

/// How `EXPLAIN PERF` presents the samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainPerfFormat {
    /// An HTML report with flamegraphs, for people.
    #[default]
    Html,
    /// A bounded result set of the hottest plan nodes and functions, for agents and scripts.
    Table,
    /// Folded call stacks, one per row, for flamegraph tools and further analysis.
    Folded,
}

impl ExplainPerfFormat {
    pub fn from_name(name: &str) -> Option<ExplainPerfFormat> {
        match name.to_lowercase().as_str() {
            "html" => Some(ExplainPerfFormat::Html),
            "table" => Some(ExplainPerfFormat::Table),
            "folded" => Some(ExplainPerfFormat::Folded),
            _ => None,
        }
    }
}

impl std::fmt::Display for ExplainPerfFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExplainPerfFormat::Html => write!(f, "html"),
            ExplainPerfFormat::Table => write!(f, "table"),
            ExplainPerfFormat::Folded => write!(f, "folded"),
        }
    }
}

/// What `EXPLAIN PERF` samples while running the statement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainPerfMode {
    /// Samples call stacks on CPU time.
    #[default]
    Cpu,
    /// Samples call stacks on allocated bytes, grouped by plan node.
    Memory,
}

impl std::fmt::Display for ExplainPerfMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExplainPerfMode::Cpu => write!(f, "CPU"),
            ExplainPerfMode::Memory => write!(f, "MEMORY"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainOption {
    Verbose,
    Logical,
    Optimized,
    Decorrelated,
}
