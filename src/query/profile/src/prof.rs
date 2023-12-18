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

use crate::ProcessorProfile;

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

    /// The execution information of the plan operator
    pub execution_info: OperatorExecutionInfo,

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
    CteScan,
    Sort,
    UnionAll,
    Project,
    Window,
    RowFetch,
    Exchange,
    Insert,
    ConstantTableScan,
    Udf,
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
            OperatorType::Insert => write!(f, "Insert"),
            OperatorType::CteScan => write!(f, "CteScan"),
            OperatorType::ConstantTableScan => write!(f, "ConstantTableScan"),
            OperatorType::Udf => write!(f, "Udf"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OperatorExecutionInfo {
    pub process_time: Duration,
    pub input_rows: usize,
    pub input_bytes: usize,
    pub output_rows: usize,
    pub output_bytes: usize,
}

impl From<ProcessorProfile> for OperatorExecutionInfo {
    fn from(value: ProcessorProfile) -> Self {
        (&value).into()
    }
}

impl From<&ProcessorProfile> for OperatorExecutionInfo {
    fn from(value: &ProcessorProfile) -> Self {
        OperatorExecutionInfo {
            process_time: value.cpu_time,
            input_rows: value.input_rows,
            input_bytes: value.input_bytes,
            output_rows: value.output_rows,
            output_bytes: value.output_bytes,
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
    CteScan(CteScanAttribute),
    Udf(UdfAttribute),
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
pub struct CteScanAttribute {
    pub cte_idx: usize,
}

#[derive(Debug, Clone)]
pub struct WindowAttribute {
    pub functions: String,
}

#[derive(Debug, Clone)]
pub struct ExchangeAttribute {
    pub exchange_mode: String,
}

#[derive(Debug, Clone)]
pub struct UdfAttribute {
    pub scalars: String,
}
