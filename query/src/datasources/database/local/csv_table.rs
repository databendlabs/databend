// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::datasources::common::count_lines;
use crate::datasources::common::generate_parts;
use crate::datasources::database::local::CsvTableStream;
use crate::sessions::DatafuseQueryContextRef;

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
    ) -> Result<Box<dyn Table>> {
        let has_header = options.get("has_header").is_some();
        let file = match options.get("location") {
            None => {
                return Result::Err(ErrorCode::BadOption(
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
impl Table for CsvTable {
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
        ctx: DatafuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        let start_line: usize = if self.has_header { 1 } else { 0 };
        let file = &self.file;
        let lines_count = count_lines(File::open(file.clone())?)?;

        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema.clone(),
            parts: generate_parts(
                start_line as u64,
                ctx.get_settings().get_max_threads()?,
                lines_count as u64,
            ),
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", self.db, self.name),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(CsvTableStream::try_create(
            ctx,
            self.schema.clone(),
            self.file.clone(),
        )?))
    }
}
