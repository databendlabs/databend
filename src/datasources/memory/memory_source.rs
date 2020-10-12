// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use super::*;

pub struct MemorySource {
    schema: DataSchemaRef,
    partitions: Vec<Vec<DataBlock>>,
}

impl MemorySource {
    pub fn new(schema: DataSchemaRef) -> Self {
        MemorySource {
            schema,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: Vec<DataBlock>) -> Result<()> {
        self.partitions.push(partition);
        Ok(())
    }
}

impl IDataSourceProvider for MemorySource {
    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, _plans: Vec<PlanNode>) -> Result<PlanNode> {
        unimplemented!()
    }
}
