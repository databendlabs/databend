// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;

use crate::datasources::csv::CsvStream;
use crate::datasources::{ITable, Partition};
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::Result;
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub struct CsvTable {
    name: String,
    batch_size: usize,
    schema: DataSchemaRef,
    partitions: Vec<Partition>,
}

impl CsvTable {
    pub fn create(
        name: &str,
        batch_size: usize,
        schema: DataSchemaRef,
        partitions: Vec<Partition>,
    ) -> Self {
        CsvTable {
            name: name.to_string(),
            batch_size,
            schema,
            partitions,
        }
    }
}

#[async_trait]
impl ITable for CsvTable {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(&self, _plans: Vec<PlanNode>) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            description: "(Read from CSV table)".to_string(),
            table_type: "CsvTable",
            partitions: self.partitions.clone(),
        })
    }

    async fn read(&self, parts: Vec<Partition>) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(CsvStream::try_create(
            parts,
            self.batch_size,
            self.schema.clone(),
        )?))
    }
}
