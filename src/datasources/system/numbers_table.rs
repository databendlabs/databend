// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datasources::{system::NumbersStream, ITable, Partition, Partitions};
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};

pub struct NumbersTable {
    schema: DataSchemaRef,
}

impl NumbersTable {
    pub fn create() -> Self {
        NumbersTable {
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )])),
        }
    }

    pub fn generate_parts(&self, total: u64) -> Partitions {
        let workers = 8u64;
        let chunk_size = total / workers;
        let mut partitions = Vec::with_capacity(workers as usize);

        if chunk_size == 0 {
            partitions.push(Partition {
                name: format!("{}-{}-{}", total, 0, total),
                version: 0,
            })
        } else {
            let parts = workers;
            let remain = total % workers;
            for part in 0..parts {
                let start = part * chunk_size;
                let mut end = (part + 1) * chunk_size - 1;
                if part == (parts - 1) && remain > 0 {
                    end += remain;
                }
                partitions.push(Partition {
                    name: format!("{}-{}-{}", total, start, end,),
                    version: 0,
                })
            }
        }
        partitions
    }
}

#[async_trait]
impl ITable for NumbersTable {
    fn name(&self) -> &str {
        "numbers_mt"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, push_down_plan: PlanNode) -> FuseQueryResult<ReadDataSourcePlan> {
        let mut total = 10000_u64;

        if let PlanNode::Scan(plan) = push_down_plan {
            let ScanPlan { table_args, .. } = plan;
            if let Some(args) = table_args {
                if let ExpressionPlan::Constant(DataValue::UInt64(Some(v))) = args {
                    total = v;
                }

                if let ExpressionPlan::Constant(DataValue::Int64(Some(v))) = args {
                    total = v as u64;
                }
            }
        }

        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            table_type: "System",
            schema: self.schema.clone(),
            partitions: self.generate_parts(total),
            description: "(Read from system.numbers_mt table)".to_string(),
        })
    }

    async fn read(&self, parts: Vec<Partition>) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(NumbersStream::create(self.schema.clone(), parts)))
    }
}
