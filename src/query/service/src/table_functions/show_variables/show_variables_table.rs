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
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::validate_function_arg;

const SHOW_VARIABLES: &str = "show_variables";

pub struct ShowVariables {
    table_info: TableInfo,
}

// show variables
impl ShowVariables {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.positioned;
        // Check args len.
        validate_function_arg(table_func_name, args.len(), None, 0)?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: SHOW_VARIABLES.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self { table_info }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("value", TableDataType::String),
            TableField::new("type", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for ShowVariables {
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
        Some(TableArgs::new_positioned(vec![]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(|output| ShowVariablesSource::create(ctx.clone(), output), 1)?;

        Ok(())
    }
}

struct ShowVariablesSource {
    ctx: Arc<dyn TableContext>,
    finished: bool,
}

impl ShowVariablesSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, ShowVariablesSource {
            ctx,
            finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ShowVariablesSource {
    const NAME: &'static str = "show_variables";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let res = show_variables(self.ctx.clone()).await?;

        // Mark done.
        self.finished = true;
        Ok(res)
    }
}

async fn show_variables(ctx: Arc<dyn TableContext>) -> Result<Option<DataBlock>> {
    let var = ctx.get_all_variables();

    let mut names = vec![];
    let mut vars = vec![];
    let mut types = vec![];
    for (name, var) in var {
        names.push(name.to_string());
        vars.push(var.to_string());
        types.push(var.as_ref().infer_data_type().to_string());
    }

    Ok(Some(DataBlock::new_from_columns(vec![
        StringType::from_data(names),
        StringType::from_data(vars),
        StringType::from_data(types),
    ])))
}

impl TableFunction for ShowVariables {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
