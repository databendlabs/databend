// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use fuse_query_datavalues::{
    DataField, DataSchema, DataSchemaRef, DataType, StringArray, UInt32Array,
};

use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};
use crate::sessions::FuseQueryContextRef;

pub struct ClustersTable {
    schema: DataSchemaRef,
}

impl ClustersTable {
    pub fn create() -> Self {
        ClustersTable {
            schema: Arc::new(DataSchema::new(vec![
                DataField::new("name", DataType::Utf8, false),
                DataField::new("address", DataType::Utf8, false),
                DataField::new("cpus", DataType::Int32, false),
            ])),
        }
    }
}

#[async_trait]
impl ITable for ClustersTable {
    fn name(&self) -> &str {
        "clusters"
    }

    fn engine(&self) -> &str {
        "SystemClusters"
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
            description: "(Read from system.clusters table)".to_string(),
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let nodes = ctx.try_get_cluster()?.get_nodes()?;
        let names: Vec<&str> = nodes.iter().map(|x| x.name.as_str()).collect();
        let addresses: Vec<&str> = nodes.iter().map(|x| x.address.as_str()).collect();
        let cpus: Vec<u32> = nodes.iter().map(|x| x.cpus as u32).collect();
        let block = DataBlock::create(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(addresses)),
                Arc::new(UInt32Array::from(cpus)),
            ],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
