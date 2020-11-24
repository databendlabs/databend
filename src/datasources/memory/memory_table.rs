// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition};
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};

use super::MemoryStream;

pub struct MemoryTable {
    name: String,
    schema: DataSchemaRef,
    partitions: HashMap<String, DataBlock>,
}

impl MemoryTable {
    pub fn create(name: &str, schema: DataSchemaRef) -> Self {
        MemoryTable {
            name: name.to_string(),
            schema,
            partitions: Default::default(),
        }
    }

    pub fn add_partition(&mut self, name: &str, partition: DataBlock) -> FuseQueryResult<()> {
        self.partitions.insert(name.to_string(), partition);
        Ok(())
    }
}

#[async_trait]
impl ITable for MemoryTable {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, _plans: Vec<PlanNode>) -> FuseQueryResult<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            description: "(Read from InMemory table)".to_string(),
            table_type: "InMemory",
            partitions: self
                .partitions
                .keys()
                .clone()
                .map(|x| Partition {
                    name: x.to_string(),
                    version: 0,
                })
                .collect::<Vec<Partition>>(),
        })
    }

    async fn read(&self, parts: Vec<Partition>) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(MemoryStream::create(
            parts,
            self.partitions.clone(),
        )))
    }
}
