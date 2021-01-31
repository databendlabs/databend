// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, UInt8Array};
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub struct OneTable {
    schema: DataSchemaRef,
}

impl OneTable {
    pub fn create() -> Self {
        OneTable {
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "dummy",
                DataType::UInt8,
                false,
            )])),
        }
    }
}

#[async_trait]
impl ITable for OneTable {
    fn name(&self) -> &str {
        "one"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        _push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.one table)".to_string(),
        })
    }

    async fn read(&self, _: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let block = DataBlock::create(
            self.schema.clone(),
            vec![Arc::new(UInt8Array::from(vec![1u8]))],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
