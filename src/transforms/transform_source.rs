// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::contexts::Context;
use crate::datasources::{MemoryTableStream, Partition, TableType};
use crate::datastreams::DataBlockStream;
use crate::error::Result;
use crate::processors::IProcessor;

pub struct SourceTransform {
    ctx: Context,
    db: String,
    table: String,
    partitions: Vec<Partition>,
}

impl SourceTransform {
    pub fn create(ctx: Context, db: &str, table: &str, partitions: Vec<Partition>) -> Self {
        SourceTransform {
            ctx,
            db: db.to_string(),
            table: table.to_string(),
            partitions,
        }
    }
}

#[async_trait]
impl IProcessor for SourceTransform {
    fn name(&self) -> &'static str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) {
        unimplemented!()
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let table = self.ctx.table(self.db.as_str(), self.table.as_str())?;
        Ok(Box::pin(match table.table_type() {
            TableType::Memory => MemoryTableStream::new(self.partitions.clone(), table),
        }))
    }
}
