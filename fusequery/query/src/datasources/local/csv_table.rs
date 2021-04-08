// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fs::File;

use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataSchemaRef;
use common_planners::Partition;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::CsvStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct CsvTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    file: String,
}

impl CsvTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        options: TableOptions,
    ) -> Result<Box<dyn ITable>> {
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
            _ => bail!("CSV Engine must contains file location options"),
        };
    }
}

#[async_trait::async_trait]
impl ITable for CsvTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "CSV"
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

    async fn read(&self, _ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        let reader = File::open(self.file.clone())?;
        Ok(Box::pin(CsvStream::try_create(
            self.schema.clone(),
            reader,
        )?))
    }
}
