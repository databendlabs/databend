// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct DatabasesTable {
    schema: DataSchemaRef,
}

impl DatabasesTable {
    pub fn create() -> Self {
        DatabasesTable {
            schema: DataSchemaRefExt::create(vec![DataField::new("name", DataType::Utf8, false)]),
        }
    }
}

#[async_trait::async_trait]
impl Table for DatabasesTable {
    fn name(&self) -> &str {
        "databases"
    }

    fn engine(&self) -> &str {
        "SystemDatabases"
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
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.databases table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        ctx.get_datasource()
            .get_databases()
            .map(|databases_name| -> SendableDataBlockStream {
                let databases_name_str: Vec<&str> = databases_name
                    .iter()
                    .map(|database_name| database_name.as_str())
                    .collect();

                let block = DataBlock::create_by_array(self.schema.clone(), vec![Series::new(
                    databases_name_str,
                )]);

                Box::pin(DataBlockStream::create(self.schema.clone(), None, vec![
                    block,
                ]))
            })
    }
}
