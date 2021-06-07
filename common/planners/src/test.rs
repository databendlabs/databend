// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

use crate::plan_partition::Partition;
use crate::Partitions;
use crate::PlanNode;
use crate::ReadDataSourcePlan;
use crate::ScanPlan;
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
        };

        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            db: "system".to_string(),
            table: "numbers_mt".to_string(),
            schema,
            partitions: Self::generate_partitions(8, total as u64),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.numbers_mt table, Read Rows:{}, Read Bytes:{})",
                statistics.read_rows, statistics.read_bytes
            ),
            scan_plan: Arc::new(ScanPlan::empty()),
            remote: false,
        }))
    }

    pub fn generate_partitions(workers: u64, total: u64) -> Partitions {
        let part_size = total / workers;
        let part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers as usize);
        if part_size == 0 {
            partitions.push(Partition {
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
                partitions.push(Partition {
                    name: format!("{}-{}-{}", total, part_begin, part_end,),
                    version: 0,
                })
            }
        }
        partitions
    }
}
