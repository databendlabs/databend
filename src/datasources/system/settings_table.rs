// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, StringArray};
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub struct SettingsTable {
    schema: DataSchemaRef,
}

impl SettingsTable {
    pub fn create() -> Self {
        SettingsTable {
            schema: Arc::new(DataSchema::new(vec![
                DataField::new("name", DataType::Utf8, false),
                DataField::new("value", DataType::Utf8, false),
            ])),
        }
    }
}

#[async_trait]
impl ITable for SettingsTable {
    fn name(&self) -> &str {
        "settings"
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
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            description: "(Read from system.settings table)".to_string(),
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _parts: Vec<Partition>,
    ) -> FuseQueryResult<SendableDataBlockStream> {
        let (names, values) = ctx.get_settings()?;
        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let values: Vec<&str> = values.iter().map(|x| x.as_str()).collect();
        let block = DataBlock::create(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(values)),
            ],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
