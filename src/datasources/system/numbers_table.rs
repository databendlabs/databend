// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::mem::size_of;
use std::sync::Arc;

use async_trait::async_trait;

use crate::contexts::FuseQueryContextRef;
use crate::datasources::{system::NumbersStream, ITable, Partition, Partitions, Statistics};
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};

pub struct NumbersTable {
    table: &'static str,
    schema: DataSchemaRef,
}

impl NumbersTable {
    pub fn create(table: &'static str) -> Self {
        NumbersTable {
            table,
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )])),
        }
    }

    pub fn generate_parts(&self, workers: u64, block_size: u64, total: u64) -> Partitions {
        let block_nums = total / block_size;
        let block_remain = total % block_size;

        let mut partitions = Vec::with_capacity(workers as usize);

        if block_nums == 0 {
            partitions.push(Partition {
                name: format!("{}-{}-{}", total, 0, total - 1,),
                version: 0,
            })
        } else {
            for part in 0..block_nums {
                let start = part * block_size;
                let mut end = (part + 1) * block_size - 1;
                if part == (block_nums - 1) && block_remain > 0 {
                    end += block_remain;
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
        self.table
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        let mut total = ctx.get_max_block_size()? as u64;

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

        let statistics = Statistics {
            read_rows: total as usize,
            read_bytes: (total) * size_of::<u64>() as u64,
        };
        ctx.set_statistics(&statistics)?;

        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: self.generate_parts(
                ctx.get_max_threads()?,
                ctx.get_max_block_size()?,
                total,
            ),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
                self.table, statistics.read_rows, statistics.read_bytes
            ),
        })
    }

    async fn read(
        &self,
        _ctx: FuseQueryContextRef,
        parts: Vec<Partition>,
    ) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(NumbersStream::create(self.schema.clone(), parts)))
    }
}
