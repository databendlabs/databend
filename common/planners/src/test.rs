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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::TableInfo;

use crate::Part;
use crate::Partitions;
use crate::PlanNode;
use crate::ReadDataSourcePlan;
use crate::Statistics;

pub struct Test {}

impl Test {
    pub fn create() -> Self {
        Self {}
    }

    pub fn generate_source_plan_for_test(&self, total: usize) -> Result<PlanNode> {
        let schema =
            DataSchemaRefExt::create(vec![DataField::new("number", DataType::UInt64, false)]);

        let statistics = Statistics {
            read_rows: total,
            read_bytes: total * 8,
            is_exact: true,
        };

        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            table_info: TableInfo::simple("system", "numbers_mt", schema),
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
        let part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers as usize);
        if part_size == 0 {
            partitions.push(Part {
                name: format!("{}-{}-{}", total, 0, total,),
                version: 0,
            })
        } else {
            for part in 0..workers {
                let part_begin = part * part_size;
                let mut part_end = (part + 1) * part_size;
                if part == (workers - 1) && part_remain > 0 {
                    part_end += part_remain;
                }
                partitions.push(Part {
                    name: format!("{}-{}-{}", total, part_begin, part_end,),
                    version: 0,
                })
            }
        }
        partitions
    }
}
