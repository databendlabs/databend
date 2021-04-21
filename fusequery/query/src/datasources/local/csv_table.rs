// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::process::Command;

use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataSchemaRef;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::CsvStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::generate_parts;
use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct CsvTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    file: String,
    has_header: bool,
}

impl CsvTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        options: TableOptions,
    ) -> Result<Box<dyn ITable>> {
        let has_header = options.get("has_header").is_some();
        let file = match options.get("location") {
            None => {
                bail!("CSV Engine must contains file location options")
            }
            Some(v) => v.trim_matches(|s| s == '\'' || s == '"').to_string(),
        };

        Ok(Box::new(Self {
            db,
            name,
            schema,
            file,
            has_header,
        }))
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

    fn read_plan(&self, ctx: FuseQueryContextRef, _scan: &ScanPlan) -> Result<ReadDataSourcePlan> {
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!("cat {:?}|wc -l", self.file))
            .output()
            .expect("Failed to execute process")
            .stdout;
        let lines: u64 = output
            .as_slice()
            .iter()
            .map(|v| *v as char)
            .collect::<String>()
            .trim()
            .parse()
            .expect("Can not parse line number to int");

        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: generate_parts(ctx.get_max_threads()?, lines),
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", self.db, self.name),
        })
    }

    async fn read(&self, _ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(CsvStream::try_create(
            self.schema.clone(),
            self.file.clone(),
            self.has_header,
        )?))
    }
}
