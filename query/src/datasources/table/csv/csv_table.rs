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

use common_context::DataContext;
use common_context::IOContext;
use common_context::TableIOContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::datasources::common::count_lines;
use crate::datasources::common::generate_parts;
use crate::datasources::table::csv::csv_table_stream::CsvTableStream;
use crate::sessions::DatabendQueryContext;

pub struct CsvTable {
    table_info: TableInfo,
    file: String,
    has_header: bool,
}

impl CsvTable {
    pub fn try_create(
        table_info: TableInfo,
        _data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        let options = table_info.options();
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
            table_info,
            file,
            has_header,
        }))
    }
}

#[async_trait::async_trait]
impl Table for CsvTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn read_partitions(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<(Statistics, Partitions)> {
        let start_line: usize = if self.has_header { 1 } else { 0 };
        let file = &self.file;
        let lines_count = count_lines(File::open(file.clone())?)?;
        let bytes = File::open(file.clone())?.metadata()?.len() as usize;

        let parts = generate_parts(
            start_line as u64,
            io_ctx.get_max_threads() as u64,
            lines_count as u64,
        );
        Ok((Statistics::new_estimated(lines_count, bytes), parts))
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        Ok(Box::pin(CsvTableStream::try_create(
            ctx,
            self.table_info.schema(),
            self.file.clone(),
        )?))
    }
}
