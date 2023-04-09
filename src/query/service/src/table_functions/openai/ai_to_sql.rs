//  Copyright 2023 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_openai::OpenAI;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_storages_factory::Table;
use common_storages_fuse::table_functions::string_literal;
use common_storages_fuse::TableContext;
use common_storages_view::view_table::VIEW_ENGINE;
use tracing::info;

pub struct GPT2SQLTable {
    prompt: String,
    table_info: TableInfo,
}

impl GPT2SQLTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        // Check args.
        let args = table_args.expect_all_positioned(table_func_name, Some(1))?;
        let prompt = String::from_utf8(
            args[0]
                .clone()
                .into_string()
                .map_err(|_| ErrorCode::BadArguments("Expected string argument."))?,
        )?;

        let schema = TableSchema::new(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("generated_sql", TableDataType::String),
        ]);
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from(table_func_name),
            meta: TableMeta {
                schema: Arc::new(schema),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                updated_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(GPT2SQLTable { prompt, table_info }))
    }
}

impl TableFunction for GPT2SQLTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for GPT2SQLTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((PartStatistics::default_exact(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![string_literal(
            self.prompt.as_str(),
        )]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| GPT2SQLSource::create(ctx.clone(), output, self.prompt.clone()),
            1,
        )?;
        Ok(())
    }
}

struct GPT2SQLSource {
    ctx: Arc<dyn TableContext>,
    prompt: String,
    finished: bool,
}

impl GPT2SQLSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        prompt: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, GPT2SQLSource {
            prompt,
            ctx,
            finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for GPT2SQLSource {
    const NAME: &'static str = "gpt_to_sql";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        // ### Postgres SQL tables, with their properties:
        // #
        // # Employee(id, name, department_id)
        // # Department(id, name, address)
        // # Salary_Payments(id, employee_id, amount, date)
        // #
        // ### A query to list the names of the departments which employed more than 10 employees in the last 3 months
        // SELECT
        let database = self.ctx.get_current_database();
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(CATALOG_DEFAULT)?;

        let mut template = vec![];
        template.push("### Postgres SQL tables, with their properties:".to_string());
        template.push("#".to_string());

        for table in catalog.list_tables(tenant.as_str(), &database).await? {
            let fields = if table.engine() == VIEW_ENGINE {
                continue;
            } else {
                table.schema().fields().clone()
            };

            let columns_name = fields
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();
            template.push(format!("{}({})", table.name(), columns_name.join(",")));
        }
        template.push("#".to_string());
        template.push(format!("### {}", self.prompt.clone()));
        template.push("#".to_string());
        template.push("SELECT".to_string());

        let prompt = template.join("");
        info!("openai request prompt: {}", prompt);

        // Response.
        let api_base = GlobalConfig::instance().query.openai_api_base_url.clone();
        let api_key = GlobalConfig::instance().query.openai_api_key.clone();
        let api_embedding_model = GlobalConfig::instance()
            .query
            .openai_api_embedding_model
            .clone();
        let api_completion_model = GlobalConfig::instance()
            .query
            .openai_api_completion_model
            .clone();
        let openai = OpenAI::create(api_base, api_key, api_embedding_model, api_completion_model);
        let (sql, _) = openai.completion_sql_request(prompt)?;

        let sql = format!("SELECT{}", sql);
        info!("openai response sql: {}", sql);
        let database = self.ctx.get_current_database();
        let database: Vec<Vec<u8>> = vec![database.into_bytes()];
        let sql: Vec<Vec<u8>> = vec![sql.into_bytes()];

        // Mark done.
        self.finished = true;

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(database),
            StringType::from_data(sql),
        ])))
    }
}
