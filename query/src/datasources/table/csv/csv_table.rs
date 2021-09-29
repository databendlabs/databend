//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::catalogs::TableInfo;
use crate::datasources::common::count_lines;
use crate::datasources::common::generate_parts;
use crate::datasources::table::csv::csv_table_stream::CsvTableStream;
use crate::sessions::DatabendQueryContextRef;

pub struct CsvTable {
    tbl_info: TableInfo,
    file: String,
    has_header: bool,
}

impl CsvTable {
    pub fn try_create(tbl_info: TableInfo) -> Result<Box<dyn Table>> {
        let options = &tbl_info.table_option;
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
            tbl_info,
            file,
            has_header,
        }))
    }
}

#[async_trait::async_trait]
impl Table for CsvTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn database(&self) -> &str {
        &self.tbl_info.db
    }

    fn engine(&self) -> &str {
        &self.tbl_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.tbl_info.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        ctx: DatabendQueryContextRef,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        let start_line: usize = if self.has_header { 1 } else { 0 };
        let file = &self.file;
        let lines_count = count_lines(File::open(file.clone())?)?;

        let db = &self.tbl_info.db;
        let name = &self.tbl_info.name;
        Ok(ReadDataSourcePlan {
            db: db.to_owned(),
            table: name.to_owned(),
            table_id: self.tbl_info.table_id,
            table_version: None,
            schema: self.tbl_info.schema.clone(),
            parts: generate_parts(
                start_line as u64,
                ctx.get_settings().get_max_threads()?,
                lines_count as u64,
            ),
            statistics: Statistics::default(),
            description: format!("(Read from CSV Engine table  {}.{})", db, name),
            scan_plan: Arc::new(ScanPlan::empty()),
            remote: false,
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(CsvTableStream::try_create(
            ctx,
            self.tbl_info.schema.clone(),
            self.file.clone(),
        )?))
    }
}
