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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::validate_function_arg;
use itertools::Itertools;

const SHOW_ROLES: &str = "show_roles";

pub struct ShowRoles {
    table_info: TableInfo,
}

impl ShowRoles {
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
                engine: SHOW_ROLES.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self { table_info }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new(
                "inherited_roles",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("inherited_roles_name", TableDataType::String),
            TableField::new("is_current", TableDataType::Boolean),
            TableField::new("is_default", TableDataType::Boolean),
            TableField::new("comment", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for ShowRoles {
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
        pipeline.add_source(|output| ShowRolesSource::create(ctx.clone(), output), 1)?;

        Ok(())
    }
}

struct ShowRolesSource {
    ctx: Arc<dyn TableContext>,
    finished: bool,
}

impl ShowRolesSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, ShowRolesSource {
            ctx,
            finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ShowRolesSource {
    const NAME: &'static str = "show_roles";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let res = show_roles(self.ctx.clone()).await?;
        // Mark done.
        self.finished = true;
        Ok(res)
    }
}

async fn show_roles(ctx: Arc<dyn TableContext>) -> Result<Option<DataBlock>> {
    let mut roles = ctx.get_all_available_roles().await?;
    roles.sort_by(|a, b| a.name.cmp(&b.name));

    let current_role_name = ctx.get_current_role().map(|r| r.name).unwrap_or_default();
    let default_role_name = ctx
        .get_current_user()?
        .option
        .default_role()
        .cloned()
        .unwrap_or_default();

    let names = roles.iter().map(|x| x.name.clone()).collect::<Vec<_>>();
    let comments = roles
        .iter()
        .map(|x| x.clone().comment.unwrap_or("".to_string()))
        .collect::<Vec<_>>();
    let inherited_roles: Vec<u64> = roles
        .iter()
        .map(|x| x.grants.roles().len() as u64)
        .collect();
    let inherited_roles_names: Vec<String> = roles
        .iter()
        .map(|x| x.grants.roles().iter().sorted().join(", ").to_string())
        .collect();
    let is_currents: Vec<bool> = roles.iter().map(|r| r.name == current_role_name).collect();
    let is_defaults: Vec<bool> = roles.iter().map(|r| r.name == default_role_name).collect();

    Ok(Some(DataBlock::new_from_columns(vec![
        StringType::from_data(names),
        UInt64Type::from_data(inherited_roles),
        StringType::from_data(inherited_roles_names),
        BooleanType::from_data(is_currents),
        BooleanType::from_data(is_defaults),
        StringType::from_data(comments),
    ])))
}

impl TableFunction for ShowRoles {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
