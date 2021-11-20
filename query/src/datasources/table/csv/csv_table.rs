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

use async_stream::stream;
use common_base::tokio;
use common_dal::Local;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::CsvSource;
use common_streams::SendableDataBlockStream;
use common_streams::Source;

use crate::catalogs::Table;
use crate::datasources::common::count_lines;
use crate::datasources::context::TableContext;
use crate::sessions::DatabendQueryContextRef;

pub struct CsvTable {
    table_info: TableInfo,
    // TODO: support s3 protocol && support gob matcher files
    file: String,
    has_header: bool,
}

impl CsvTable {
    pub fn try_create(table_info: TableInfo, _table_ctx: TableContext) -> Result<Box<dyn Table>> {
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
        _ctx: DatabendQueryContextRef,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let file = &self.file;
        let lines_count = count_lines(File::open(file.clone())?)?;
        let bytes = File::open(file.clone())?.metadata()?.len() as usize;

        let parts = vec![Part {
            name: file.clone(),
            version: 0,
        }];
        Ok((Statistics::new_estimated(lines_count, bytes), parts))
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let conf = ctx.get_config().storage.disk;
        let local = Local::new(conf.temp_data_path.as_str());

        let ctx_clone = ctx.clone();
        let schema = plan.schema();
        let block_size = ctx.get_settings().get_max_block_size()? as usize;
        let has_header = self.has_header;

        let conf = ctx.get_config().storage;
        let dal = Arc::new(Local::new(conf.disk.temp_data_path.as_str()));

        let s = stream! {
            loop {
                let partitions = ctx_clone.try_get_partitions(1);
                match partitions {
                    Ok(partitions) => {
                        if partitions.is_empty() {
                            break;
                        }

                        let part = partitions.get(0).unwrap();
                        let mut source = CsvSource::try_create(dal.clone(), part.name.clone(), schema.clone(), has_header, block_size)?;

                        loop {
                            let block = source.read().await;
                            match block {
                                Ok(None) => break,
                                Ok(Some(b)) =>  yield(Ok(b)),
                                Err(e) => yield(Err(e)),
                            }
                        }
                    }
                    Err(e) =>  yield(Err(e))
                }
            }
        };
        Ok(Box::pin(s))
    }
}
