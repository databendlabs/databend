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

use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::StreamStatus;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_catalog::table_function::TableFunction;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_storages_fuse::table_functions::parse_db_tb_args;
use common_storages_fuse::table_functions::string_literal;

use crate::stream_table::StreamTable;

const STREAM_STATUS: &str = "stream_status";

pub struct StreamStatusTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl StreamStatusTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        // TODO use single string arg
        let (arg_database_name, arg_table_name) = parse_db_tb_args(&table_args, STREAM_STATUS)?;

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
            arg_database_name,
            arg_table_name,
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
        Some(TableArgs::new_positioned(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ]))
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
                StreamStatusDataSource::create(
                    ctx.clone(),
                    output,
                    self.arg_database_name.to_owned(),
                    self.arg_table_name.to_owned(),
                )
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
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_database_name: String,
    arg_table_name: String,
}

impl StreamStatusDataSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_database_name: String,
        arg_table_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, StreamStatusDataSource {
            ctx,
            finish: false,
            arg_table_name,
            arg_database_name,
        })
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
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                tenant_id.as_str(),
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        let tbl = StreamTable::try_from_table(tbl.as_ref())?;

        let has_data = matches!(
            tbl.new_check_stream_status(self.ctx.clone()).await?,
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
