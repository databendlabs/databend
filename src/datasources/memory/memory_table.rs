// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datatypes::DataSchemaRef;
use crate::error::Result;
use crate::planners::{IPlanNode, ReadDataSourcePlan};

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

    fn read_plan(&self, _plans: Vec<Arc<dyn IPlanNode>>) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            table_type: "InMemory",
            read_parts: self.partitions.len(),
        })
    }
}
