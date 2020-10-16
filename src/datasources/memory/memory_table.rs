// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::datatypes::DataSchemaRef;
use crate::error::Result;
use crate::planners::{PlanNode, ReadDataSourcePlan};

use crate::datasources::ITable;

pub struct MemoryTable {
    schema: DataSchemaRef,
    partitions: Vec<Vec<DataBlock>>,
}

impl MemoryTable {
    pub fn new(schema: DataSchemaRef) -> Self {
        MemoryTable {
            schema,
            partitions: vec![],
        }
    }

    pub fn add_partition(&mut self, partition: Vec<DataBlock>) -> Result<()> {
        self.partitions.push(partition);
        Ok(())
    }
}

impl ITable for MemoryTable {
    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, _plans: Vec<PlanNode>) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            description: "(Read from InMemory table)".to_string(),
            table_type: "InMemory",
            read_parts: self.partitions.len(),
        })
    }
}
