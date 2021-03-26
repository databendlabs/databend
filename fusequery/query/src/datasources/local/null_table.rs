// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::DataSchemaRef;
use crate::datasources::table_factory::TableCreatorFactory;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan, TableOptions};
use crate::sessions::FuseQueryContextRef;

pub struct NullTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
}

impl NullTable {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        db: String,
        name: String,
        schema: SchemaRef,
        _options: TableOptions,
    ) -> FuseQueryResult<Box<dyn ITable>> {
        let table = Self { db, name, schema };
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

    async fn read(&self, _ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.schema.clone());

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
