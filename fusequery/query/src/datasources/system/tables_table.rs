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

pub struct TablesTable {
    schema: DataSchemaRef,
}

impl TablesTable {
    pub fn create() -> Self {
        TablesTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("database", DataType::Utf8, false),
                DataField::new("name", DataType::Utf8, false),
                DataField::new("engine", DataType::Utf8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for TablesTable {
    fn name(&self) -> &str {
        "tables"
    }

    fn engine(&self) -> &str {
        "SystemTables"
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
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.functions table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let database_tables = ctx.get_datasource().get_all_tables()?;

        let databases: Vec<&str> = database_tables.iter().map(|(d, _)| d.as_str()).collect();
        let names: Vec<&str> = database_tables
            .iter()
            .map(|(_, v)| v.datasource().name())
            .collect();
        let engines: Vec<&str> = database_tables
            .iter()
            .map(|(_, v)| v.datasource().engine())
            .collect();

        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(databases),
            Series::new(names),
            Series::new(engines),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
