// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::{DataField, DataSchema, DataSchemaRef, DataType, StringArray};
use common_planners::{Partition, PlanNode, ReadDataSourcePlan, Statistics};
use common_streams::{DataBlockStream, SendableDataBlockStream};

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct TablesTable {
    schema: DataSchemaRef,
}

impl TablesTable {
    pub fn create() -> Self {
        TablesTable {
            schema: Arc::new(DataSchema::new(vec![
                DataField::new("database", DataType::Utf8, false),
                DataField::new("name", DataType::Utf8, false),
                DataField::new("engine", DataType::Utf8, false),
            ])),
        }
    }
}

#[async_trait]
impl ITable for TablesTable {
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

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        _push_down_plan: PlanNode,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.functions table)".to_string(),
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        let database_tables = ctx.get_datasource().read().list_database_tables()?;

        let databases: Vec<&str> = database_tables.iter().map(|(d, _)| d.as_str()).collect();
        let names: Vec<&str> = database_tables.iter().map(|(_, v)| v.name()).collect();
        let engines: Vec<&str> = database_tables.iter().map(|(_, v)| v.engine()).collect();

        let block = DataBlock::create(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(databases)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(engines)),
            ],
        );

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
