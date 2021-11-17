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
use std::sync::Arc;

use async_stream::stream;
use common_context::DataContext;
use common_context::IOContext;
use common_context::TableIOContext;
use common_dal::Local;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::ParquetSource;
use common_streams::SendableDataBlockStream;
use common_streams::Source;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct ParquetTable {
    table_info: TableInfo,
    file: String,
}

impl ParquetTable {
    pub fn try_create(
        table_info: TableInfo,
        _data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        let options = table_info.options();
        let file = options.get("location").cloned();
        return match file {
            Some(file) => {
                let table = ParquetTable {
                    table_info,
                    file: file.trim_matches(|s| s == '\'' || s == '"').to_string(),
                };
                Ok(Box::new(table))
            }
            _ => Result::Err(ErrorCode::BadOption(
                "Parquet Engine must contains file location options".to_string(),
            )),
        };
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn read_partitions(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let parts = vec![Part {
            name: self.file.clone(),
            version: 0,
        }];
        Ok((Statistics::default(), parts))
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");
        let ctx_clone = ctx.clone();
        let table_schema = self.get_table_info().schema();
        let projection = plan.projections();
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

                        let mut source = ParquetSource::new(dal.clone(), part.name.clone(), table_schema.clone(), projection.clone());

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
