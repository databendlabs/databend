// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fs::File;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use common_datavalues::DataSchemaRef;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;

use crate::datasources::local::CsvTableStream;
use crate::datasources::util::count_lines;
use crate::datasources::util::generate_parts;
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
        let file = &self.file;
        let lines_count = count_lines(
            File::open(file.clone()).with_context(|| format!("Cannot find file:{}", file))?,
        )?;
        let start_line = if self.has_header { 1 } else { 0 };

        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: generate_parts(start_line, ctx.get_max_block_size()?, lines_count as u64),
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", self.db, self.name),
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(CsvTableStream::try_create(
            ctx,
            self.schema.clone(),
            self.file.clone(),
        )?))
    }
}
