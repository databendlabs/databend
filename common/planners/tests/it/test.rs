// Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::PartInfo;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::Statistics;

pub struct Test {}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct PlannerTestPartInfo {}

#[typetag::serde(name = "planner_test")]
impl PartInfo for PlannerTestPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<PlannerTestPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl PlannerTestPartInfo {
    pub fn create() -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(PlannerTestPartInfo {}))
    }
}

impl Test {
    pub fn create() -> Self {
        Self {}
    }

    pub fn generate_source_plan_for_test(&self, total: usize) -> Result<PlanNode> {
        let schema = DataSchemaRefExt::create(vec![DataField::new("number", u64::to_data_type())]);

        let statistics = Statistics {
            read_rows: total,
            read_bytes: total * 8,
            partitions_scanned: 8,
            partitions_total: 8,
            is_exact: true,
        };

        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            source_info: SourceInfo::TableSource(TableInfo::simple("system", "numbers_mt", schema)),
            scan_fields: None,
            parts: Self::generate_partitions(8, total as u64),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.numbers_mt table, Exactly Read Rows:{}, Read Bytes:{})",
                statistics.read_rows, statistics.read_bytes
            ),
            tbl_args: None,
            push_downs: None,
        }))
    }

    pub fn generate_partitions(workers: u64, total: u64) -> Partitions {
        let part_size = total / workers;
        // let part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers as usize);
        if part_size == 0 {
            partitions.push(PlannerTestPartInfo::create())
        } else {
            for _part in 0..workers {
                // let part_begin = part * part_size;
                // let mut part_end = (part + 1) * part_size;
                // if part == (workers - 1) && part_remain > 0 {
                //     part_end += part_remain;
                // }
                partitions.push(PlannerTestPartInfo::create())
            }
        }
        partitions
    }
}
