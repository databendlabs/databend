// Copyright 2023 Datafuse Labs.
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

use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::ResultScanTableInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_users::UserApiProvider;

use crate::ResultCacheReader;

const RESULT_SCAN: &str = "result_scan";

pub struct ResultScan {
    table_info: TableInfo,
    query_id: String,
    blocks: Option<Vec<DataBlock>>,
}

impl ResultScan {
    pub fn try_create(
        table_schema: TableSchema,
        query_id: String,
        blocks: Option<Vec<DataBlock>>,
    ) -> Result<Arc<dyn Table>> {
        let table_info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("''.'{RESULT_SCAN}'"),
            name: String::from(RESULT_SCAN),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                engine: String::from(RESULT_SCAN),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(ResultScan {
            table_info,
            query_id,
            blocks,
        }))
    }

    pub fn from_info(info: &ResultScanTableInfo) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(ResultScan {
            table_info: info.table_info.clone(),
            query_id: info.query_id.clone(),
            blocks: None,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ResultScan {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::ResultScanSource(ResultScanTableInfo {
            table_info: self.table_info.clone(),
            query_id: self.query_id.clone(),
        })
    }

    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![Scalar::String(self.query_id.as_bytes().to_vec())];

        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                ResultScanSource::create(
                    ctx.clone(),
                    output,
                    self.query_id.clone(),
                    self.blocks.clone(),
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct ResultScanSource {
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_query_id: String,
    blocks: Option<Vec<DataBlock>>,
}

impl ResultScanSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_query_id: String,
        blocks: Option<Vec<DataBlock>>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ResultScanSource {
            ctx,
            finish: false,
            arg_query_id,
            blocks,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ResultScanSource {
    const NAME: &'static str = "result_scan";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        if self.ctx.get_settings().get_enable_query_result_cache()? {
            if let Some(ref blocks) = self.blocks {
                if self.ctx.get_settings().get_enable_query_result_cache()? {
                    return Ok(Some(DataBlock::concat(blocks)?));
                }
            }

            let meta_key = self.ctx.get_result_cache_key(&self.arg_query_id);
            if let Some(meta_key) = meta_key {
                let kv_store = UserApiProvider::instance().get_meta_store_client();
                let cache_reader = ResultCacheReader::create_with_meta_key(meta_key, kv_store);

                let blocks = cache_reader.try_read_cached_result().await?;
                return match blocks {
                    Some(blocks) => Ok(Some(DataBlock::concat(&blocks)?)),
                    None => Ok(None),
                };
            }
        }
        Ok(None)
    }
}
