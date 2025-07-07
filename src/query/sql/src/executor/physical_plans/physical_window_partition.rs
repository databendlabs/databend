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

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::physical_sort::SortStep;
use crate::executor::physical_plans::SortDesc;
use crate::executor::PhysicalPlan;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowPartition {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub partition_by: Vec<IndexType>,
    pub order_by: Vec<SortDesc>,
    pub sort_step: SortStep,
    pub top_n: Option<WindowPartitionTopN>,

    pub stat_info: Option<PlanStatsInfo>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowPartitionTopN {
    pub func: WindowPartitionTopNFunc,
    pub top: usize,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowPartitionTopNFunc {
    RowNumber,
    Rank,
    DenseRank,
}

impl WindowPartition {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}
