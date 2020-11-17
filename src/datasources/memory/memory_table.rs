// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;

use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition, TableType};
use crate::datavalues::DataSchemaRef;
use crate::error::{Error, Result};
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub struct MemoryTable {
    name: String,
    schema: DataSchemaRef,
    partitions: HashMap<String, DataBlock>,
}

impl MemoryTable {
    pub fn new(name: &str, schema: DataSchemaRef) -> Self {
        MemoryTable {
            name: name.to_string(),
            schema,
            partitions: Default::default(),
        }
    }

    pub fn add_partition(&mut self, name: &str, partition: DataBlock) -> Result<()> {
        self.partitions.insert(name.to_string(), partition);
        Ok(())
    }
}

impl ITable for MemoryTable {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn table_type(&self) -> TableType {
        TableType::Memory
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, _plans: Vec<PlanNode>) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            description: "(Read from InMemory table)".to_string(),
            table_type: "InMemory",
            partitions: vec![],
        })
    }

    fn read_partition(&self, part: &Partition) -> Result<DataBlock> {
        Ok(self
            .partitions
            .get(part.name.as_str())
            .ok_or_else(|| Error::Internal(format!("Can not find the partition: {}", part.name)))?
            .clone())
    }
}
