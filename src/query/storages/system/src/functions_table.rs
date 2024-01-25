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

use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::TypeChecker;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct FunctionsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for FunctionsTable {
    const NAME: &'static str = "system.functions";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let mut scalar_func_names: Vec<String> = BUILTIN_FUNCTIONS.registered_names();
        scalar_func_names.extend(
            TypeChecker::all_sugar_functions()
                .iter()
                .map(|name| name.to_string()),
        );
        scalar_func_names.extend(
            databend_common_ast::ast::Expr::all_function_like_syntaxes()
                .iter()
                .map(|name| name.to_lowercase()),
        );
        scalar_func_names.sort();
        let aggregate_function_factory = AggregateFunctionFactory::instance();
        let aggr_func_names = aggregate_function_factory.registered_names();
        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        let udfs = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker().await?;
            let udfs = FunctionsTable::get_udfs(ctx).await?;
            udfs.into_iter()
                .filter(|udf| visibility_checker.check_udf_visibility(&udf.name))
                .collect::<Vec<_>>()
        } else {
            FunctionsTable::get_udfs(ctx).await?
        };

        let names: Vec<&str> = scalar_func_names
            .iter()
            .chain(&aggr_func_names)
            .chain(udfs.iter().map(|udf| &udf.name))
            .map(|x| x.as_str())
            .collect();

        let builtin_func_len = scalar_func_names.len() + aggr_func_names.len();

        let is_builtin = (0..names.len())
            .map(|i| i < builtin_func_len)
            .collect::<Vec<bool>>();

        let is_aggregate = (0..names.len())
            .map(|i| i >= scalar_func_names.len() && i < builtin_func_len)
            .collect::<Vec<bool>>();

        let definitions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    "".to_string()
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("".to_string(), |udf| udf.definition.to_string())
                }
            })
            .collect::<Vec<String>>();

        let categories = (0..names.len())
            .map(|i| if i < builtin_func_len { "" } else { "UDF" })
            .collect::<Vec<&str>>();

        let descriptions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    ""
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.description.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let syntaxes = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    "".to_string()
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("".to_string(), |udf| udf.definition.to_string())
                }
            })
            .collect::<Vec<String>>();

        let examples = (0..names.len()).map(|_| "").collect::<Vec<&str>>();
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            BooleanType::from_data(is_builtin),
            BooleanType::from_data(is_aggregate),
            StringType::from_data(
                definitions
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<&str>>(),
            ),
            StringType::from_data(categories),
            StringType::from_data(descriptions),
            StringType::from_data(syntaxes.iter().map(String::as_str).collect::<Vec<&str>>()),
            StringType::from_data(examples),
        ]))
    }
}

impl FunctionsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("is_builtin", TableDataType::Boolean),
            TableField::new("is_aggregate", TableDataType::Boolean),
            TableField::new("definition", TableDataType::String),
            TableField::new("category", TableDataType::String),
            TableField::new("description", TableDataType::String),
            TableField::new("syntax", TableDataType::String),
            TableField::new("example", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'functions'".to_string(),
            name: "functions".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemFunctions".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(FunctionsTable { table_info })
    }

    #[async_backtrace::framed]
    async fn get_udfs(ctx: Arc<dyn TableContext>) -> Result<Vec<UserDefinedFunction>> {
        let tenant = ctx.get_tenant();
        UserApiProvider::instance().get_udfs(&tenant).await
    }
}
