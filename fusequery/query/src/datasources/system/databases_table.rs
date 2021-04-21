// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::StringArray;
use common_planners::Partition;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct DatabasesTable {
    schema: DataSchemaRef,
}

impl DatabasesTable {
    pub fn create() -> Self {
        DatabasesTable {
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "name",
                DataType::Utf8,
                false,
            )])),
        }
    }
}

#[async_trait::async_trait]
impl ITable for DatabasesTable {
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
        ctx.get_datasource()
            .get_databases()
            .map(|databases_name| -> SendableDataBlockStream {
                let databases_name_str: Vec<&str> = databases_name
                    .iter()
                    .map(|database_name| database_name.as_str())
                    .collect();

                let block = DataBlock::create(
                    self.schema.clone(),
                    vec![Arc::new(StringArray::from(databases_name_str))],
                );

                Box::pin(DataBlockStream::create(
                    self.schema.clone(),
                    None,
                    vec![block],
                ))
            })
    }
}
