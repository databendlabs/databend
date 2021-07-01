// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct NullTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
}

impl NullTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        _options: TableOptions,
    ) -> Result<Box<dyn Table>> {
        let table = Self { db, name, schema };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl Table for NullTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "Null"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::new_exact(0, 0),
            description: format!("(Read from Null Engine table  {}.{})", self.db, self.name),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        _ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.schema.clone());

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
