// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storages_fuse::table_functions::string_literal;
use databend_common_storages_fuse::table_functions::string_value;

use crate::stream_table::StreamStatus;
use crate::stream_table::StreamTable;

const STREAM_STATUS: &str = "stream_status";

pub struct StreamStatusTable {
    table_info: TableInfo,
    stream_name: String,
}

impl StreamStatusTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(STREAM_STATUS, Some(1))?;
        let stream_name = string_value(&args[0])?;

        let engine = STREAM_STATUS.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: schema(),
                engine,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(StreamStatusTable {
            table_info,
            stream_name,
        }))
    }
}

#[async_trait::async_trait]
impl Table for StreamStatusTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![string_literal(
            self.stream_name.as_str(),
        )]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                StreamStatusDataSource::create(ctx.clone(), output, self.stream_name.to_owned())
            },
            1,
        )?;

        Ok(())
    }
}

impl TableFunction for StreamStatusTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct StreamStatusDataSource {
    ctx: Arc<dyn TableContext>,
    finish: bool,
    cat_name: String,
    db_name: String,
    stream_name: String,
}

impl StreamStatusDataSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        stream_name: String,
    ) -> Result<ProcessorPtr> {
        let (cat_name, db_name, stream_name) =
            Self::extract_fully_qualified_stream_name(ctx.as_ref(), stream_name.as_str())?;
        AsyncSourcer::create(ctx.clone(), output, StreamStatusDataSource {
            ctx,
            finish: false,
            cat_name,
            db_name,
            stream_name,
        })
    }

    fn extract_fully_qualified_stream_name(
        ctx: &dyn TableContext,
        target: &str,
    ) -> Result<(String, String, String)> {
        let current_cat_name;
        let current_db_name;
        let stream_name_vec: Vec<&str> = target.split('.').collect();
        let (cat, db, stream) = {
            match stream_name_vec.len() {
                1 => {
                    current_cat_name = ctx.get_current_catalog();
                    current_db_name = ctx.get_current_database();
                    (
                        current_cat_name,
                        current_db_name,
                        stream_name_vec[0].to_owned(),
                    )
                }
                2 => {
                    current_cat_name = ctx.get_current_catalog();
                    (
                        current_cat_name,
                        stream_name_vec[0].to_owned(),
                        stream_name_vec[1].to_owned(),
                    )
                }
                3 => (
                    stream_name_vec[0].to_owned(),
                    stream_name_vec[1].to_owned(),
                    stream_name_vec[2].to_owned(),
                ),
                _ => {
                    return Err(ErrorCode::BadArguments(
                        "Invalid stream name. Use the format '[catalog.][database.]stream'",
                    ));
                }
            }
        };
        Ok((cat, db, stream))
    }
}

#[async_trait::async_trait]
impl AsyncSource for StreamStatusDataSource {
    const NAME: &'static str = "stream_status";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let tenant_id = self.ctx.get_tenant();
        let tbl = self
            .ctx
            .get_catalog(&self.cat_name)
            .await?
            .get_table(&tenant_id, &self.db_name, &self.stream_name)
            .await?;

        let tbl = StreamTable::try_from_table(tbl.as_ref())?;

        let has_data = matches!(
            tbl.check_stream_status(self.ctx.clone()).await?,
            StreamStatus::MayHaveData
        );

        Ok(Some(DataBlock::new_from_columns(vec![
            BooleanType::from_data(vec![has_data]),
        ])))
    }
}

fn schema() -> Arc<TableSchema> {
    TableSchemaRefExt::create(vec![TableField::new("has_data", TableDataType::Boolean)])
}
