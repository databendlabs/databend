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
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct QueryProfile {
    /// Query ID of the query profile
    pub query_id: String,

    /// Flattened plan node profiles
    pub operator_profiles: Vec<OperatorProfile>,
}

impl QueryProfile {
    pub fn new(query_id: String, operator_profiles: Vec<OperatorProfile>) -> Self {
        QueryProfile {
            query_id,
            operator_profiles,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorProfile {
    /// ID of the plan node
    pub id: u32,

    /// Type of the plan operator, e.g. `HashJoin`
    pub operator_type: OperatorType,

    /// IDs of the children plan nodes
    pub children: Vec<u32>,

    /// The time spent to process data
    pub cpu_time: Duration,

    /// Attribute of the plan operator
    pub attribute: OperatorAttribute,
}

#[derive(Debug, Clone)]
pub enum OperatorType {
    Join,
    Aggregate,
    AggregateExpand,
    Filter,
    ProjectSet,
    EvalScalar,
    Limit,
    TableScan,
    Sort,
    UnionAll,
    Project,
    Window,
    RowFetch,
    Exchange,
    RuntimeFilter,
    Insert,
}

impl Display for OperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorType::Join => write!(f, "Join"),
            OperatorType::Aggregate => write!(f, "Aggregate"),
            OperatorType::AggregateExpand => write!(f, "AggregateExpand"),
            OperatorType::Filter => write!(f, "Filter"),
            OperatorType::ProjectSet => write!(f, "ProjectSet"),
            OperatorType::EvalScalar => write!(f, "EvalScalar"),
            OperatorType::Limit => write!(f, "Limit"),
            OperatorType::TableScan => write!(f, "TableScan"),
            OperatorType::Sort => write!(f, "Sort"),
            OperatorType::UnionAll => write!(f, "UnionAll"),
            OperatorType::Project => write!(f, "Project"),
            OperatorType::Window => write!(f, "Window"),
            OperatorType::RowFetch => write!(f, "RowFetch"),
            OperatorType::Exchange => write!(f, "Exchange"),
            OperatorType::RuntimeFilter => write!(f, "RuntimeFilter"),
            OperatorType::Insert => write!(f, "Insert"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OperatorAttribute {
    Join(JoinAttribute),
    Aggregate(AggregateAttribute),
    AggregateExpand(AggregateExpandAttribute),
    Filter(FilterAttribute),
    EvalScalar(EvalScalarAttribute),
    ProjectSet(ProjectSetAttribute),
    Limit(LimitAttribute),
    TableScan(TableScanAttribute),
    Sort(SortAttribute),
    Window(WindowAttribute),
    Exchange(ExchangeAttribute),
    Empty,
}

#[derive(Debug, Clone)]
pub struct JoinAttribute {
    pub join_type: String,
    pub equi_conditions: String,
    pub non_equi_conditions: String,
}

#[derive(Debug, Clone)]
pub struct AggregateAttribute {
    pub group_keys: String,
    pub functions: String,
}

#[derive(Debug, Clone)]
pub struct AggregateExpandAttribute {
    pub group_keys: String,
    pub aggr_exprs: String,
}

#[derive(Debug, Clone)]
pub struct EvalScalarAttribute {
    pub scalars: String,
}

#[derive(Debug, Clone)]
pub struct ProjectSetAttribute {
    pub functions: String,
}

#[derive(Debug, Clone)]
pub struct FilterAttribute {
    pub predicate: String,
}

#[derive(Debug, Clone)]
pub struct LimitAttribute {
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub struct SortAttribute {
    pub sort_keys: String,
}

#[derive(Debug, Clone)]
pub struct TableScanAttribute {
    pub qualified_name: String,
}

#[derive(Debug, Clone)]
pub struct WindowAttribute {
    pub functions: String,
}

#[derive(Debug, Clone)]
pub struct ExchangeAttribute {
    pub exchange_mode: String,
}
