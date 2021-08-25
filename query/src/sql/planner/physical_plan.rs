// Copyright 2020 Datafuse Labs.
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

use crate::sql::planner::logical_plan::LogicalFilter;
use crate::sql::planner::logical_plan::LogicalGet;
use crate::sql::planner::logical_plan::LogicalProjection;
use crate::sql::planner::logical_plan::LogicalUnion;
use common_datavalues::{DataSchema, DataSchemaRef};

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    // Set operation
    Union(PhysicalUnion),

    // Join
    HashJoin(HashJoin),

    // Aggregation
    HashAggregation(HashAggregation),

    Projection(PhysicalProjection),
    Filter(PhysicalFilter),

    TableScan(TableScan),
}

// impl PhysicalPlan {
//     pub fn get_schema(&self) -> DataSchemaRef {
//         match self {
//             Self::TableScan(table_scan) => {
//                 table_scan.get_plan.
//             }
//         }
//     }
// }

#[derive(Debug, Clone)]
pub struct TableScan {
    pub get_plan: LogicalGet,
}

#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    pub filter_plan: LogicalFilter,
    pub child: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct PhysicalProjection {
    pub projection_plan: LogicalProjection,
    pub child: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct PhysicalUnion {
    pub union_plan: LogicalUnion,
}

#[derive(Debug, Clone)]
pub struct HashAggregation {}

#[derive(Debug, Clone)]
pub struct HashJoin {}
