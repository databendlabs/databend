// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use anyhow::Context;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;

use crate::datasources::local::CsvTableStream;
use crate::datasources::Common;
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
                return Result::Err(ErrorCodes::BadOption(
                    "CSV Engine must contains file location options",
                ));
            }
            Some(v) => v.clone(),
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

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        let start_line: usize = if self.has_header { 1 } else { 0 };
        let file = &self.file;
        let lines_count = Common::count_lines(
            File::open(file.clone())
                .with_context(|| format!("Cannot find file:{}", file))
                .map_err(ErrorCodes::from)?,
        )
        .map_err(|e| ErrorCodes::CannotReadFile(e.to_string()))?;

        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: Common::generate_parts(
                start_line as u64,
                ctx.get_max_threads()?,
                lines_count as u64,
            ),
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", self.db, self.name),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
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
