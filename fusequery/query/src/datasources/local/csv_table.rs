// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::datasources::local::ParquetTable;
use crate::datasources::table_factory::TableCreatorFactory;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{CsvStream, SendableDataBlockStream};
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{PlanNode, ReadDataSourcePlan, TableOptions};
use crate::sessions::FuseQueryContextRef;

pub struct CsvTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    file: String,
}

impl CsvTable {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        db: String,
        name: String,
        schema: SchemaRef,
        options: TableOptions,
    ) -> FuseQueryResult<Box<dyn ITable>> {
        let file = options.get("location");
        return match file {
            Some(file) => {
                let table = CsvTable {
                    db,
                    name,
                    schema,
                    file: file.trim_matches(|s| s == '\'' || s == '"').to_string(),
                };
                Ok(Box::new(table))
            }
            _ => Err(FuseQueryError::build_internal_error(
                "CSV Engine must contains file location options".to_string(),
            )),
        };
    }

    pub fn register(map: TableCreatorFactory) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("CSV", CsvTable::try_create);
        map.insert("Parquet", ParquetTable::try_create);
        Ok(())
    }
}

#[async_trait]
impl ITable for CsvTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "CSV"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        _push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", self.db, self.name),
        })
    }

    async fn read(&self, _ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let reader = File::open(self.file.clone()).unwrap();
        Ok(Box::pin(CsvStream::try_create(
            self.schema.clone(),
            reader,
        )?))
    }
}
