// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datasources::table_factory::TableCreatorFactory;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, StringArray};
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan, TableOptions};
use crate::sessions::FuseQueryContextRef;
use arrow::datatypes::SchemaRef;

pub struct NullTable {
    ctx: FuseQueryContextRef,
    db: String,
    name: String,
    schema: DataSchemaRef,
    options: TableOptions,
}

impl NullTable {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        db: String,
        name: String,
        schema: SchemaRef,
        options: TableOptions,
    ) -> FuseQueryResult<Box<dyn ITable>> {
        let table = Self {
            ctx,
            db,
            name,
            schema,
            options,
        };
        Ok(Box::new(table))
    }

    pub fn register(map: TableCreatorFactory) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("Null", NullTable::try_create);
        Ok(())
    }
}

#[async_trait]
impl ITable for NullTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "Null"
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
            description: format!("(Read from Null Engine table  {}.{})", self.db, self.name),
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let database_tables = ctx.get_datasource().lock()?.list_database_tables();

        let databases: Vec<&str> = database_tables.iter().map(|(d, _)| d.as_str()).collect();
        let names: Vec<&str> = database_tables.iter().map(|(_, v)| v.name()).collect();
        let engines: Vec<&str> = database_tables.iter().map(|(_, v)| v.engine()).collect();

        let block = DataBlock::empty();

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
