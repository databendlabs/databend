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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct UserFunctionsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for UserFunctionsTable {
    const NAME: &'static str = "system.user_functions";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        let user_functions = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker().await?;
            let udfs = UserFunctionsTable::get_udfs(&ctx.get_tenant()).await?;
            udfs.into_iter()
                .filter(|udf| visibility_checker.check_udf_visibility(&udf.name))
                .collect::<Vec<_>>()
        } else {
            UserFunctionsTable::get_udfs(&ctx.get_tenant()).await?
        };

        let mut names = Vec::with_capacity(user_functions.len());
        let mut is_aggregate = Vec::with_capacity(user_functions.len());
        let mut languages = Vec::with_capacity(user_functions.len());
        let mut descriptions = Vec::with_capacity(user_functions.len());
        let mut arguments = Vec::with_capacity(user_functions.len());
        let mut definitions = Vec::with_capacity(user_functions.len());
        let mut created_on = Vec::with_capacity(user_functions.len());

        for user_function in &user_functions {
            names.push(user_function.name.as_str());
            is_aggregate.push(None);
            languages.push(user_function.language.as_str());
            descriptions.push(user_function.description.as_str());
            arguments.push(serde_json::to_vec(&user_function.arguments)?);
            definitions.push(user_function.definition.as_str());
            created_on.push(user_function.created_on.timestamp_micros());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            BooleanType::from_opt_data(is_aggregate),
            StringType::from_data(descriptions),
            VariantType::from_data(arguments),
            StringType::from_data(languages),
            StringType::from_data(definitions),
            TimestampType::from_data(created_on),
        ]))
    }
}

#[derive(serde::Serialize)]
pub struct UserFunctionArguments {
    #[serde(skip_serializing_if = "std::option::Option::is_none")]
    return_type: Option<String>,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    arg_types: Vec<String>,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    parameters: Vec<String>,
}

#[derive(serde::Serialize)]
pub struct UserFunction {
    name: String,
    is_aggregate: bool,
    description: String,
    language: String,
    definition: String,
    created_on: DateTime<Utc>,
    arguments: UserFunctionArguments,
}

impl UserFunctionsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new(
                "is_aggregate",
                TableDataType::Nullable(Box::new(TableDataType::Boolean)),
            ),
            TableField::new("description", TableDataType::String),
            TableField::new("arguments", TableDataType::Variant),
            TableField::new("language", TableDataType::String),
            TableField::new("definition", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'user_functions'".to_string(),
            name: "user_functions".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemUserFunctions".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(UserFunctionsTable { table_info })
    }

    #[async_backtrace::framed]
    pub async fn get_udfs(tenant: &Tenant) -> Result<Vec<UserFunction>> {
        let user_functions_defined = UserApiProvider::instance().list_udf(&tenant).await?;

        Ok(user_functions_defined
            .into_iter()
            .map(|user_function| UserFunction {
                name: user_function.name,
                is_aggregate: false,
                description: user_function.description,
                language: match &user_function.definition {
                    UDFDefinition::LambdaUDF(_) => String::from("SQL"),
                    UDFDefinition::UDFServer(x) => x.language.clone(),
                    UDFDefinition::UDFScript(x) => x.language.clone(),
                },
                definition: user_function.definition.to_string(),
                created_on: user_function.created_on.clone(),
                arguments: match &user_function.definition {
                    UDFDefinition::LambdaUDF(x) => UserFunctionArguments {
                        return_type: None,
                        arg_types: vec![],
                        parameters: x.parameters.clone(),
                    },
                    UDFDefinition::UDFServer(x) => UserFunctionArguments {
                        parameters: vec![],
                        return_type: Some(x.return_type.to_string()),
                        arg_types: x.arg_types.iter().map(ToString::to_string).collect(),
                    },
                    UDFDefinition::UDFScript(x) => UserFunctionArguments {
                        parameters: vec![],
                        return_type: Some(x.return_type.to_string()),
                        arg_types: x.arg_types.iter().map(ToString::to_string).collect(),
                    },
                },
            })
            .collect())
    }
}
