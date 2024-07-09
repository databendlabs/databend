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
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;
use serde_json::json;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

// Encode arguments into jsonb::Value.
fn encode_arguments(udf_definition: &UDFDefinition) -> jsonb::Value {
    match udf_definition {
        UDFDefinition::LambdaUDF(x) => (&json!({
            "parameters": &x.parameters,
        }))
            .into(),
        UDFDefinition::UDFServer(x) => (&json!({
            "arg_types": &x.arg_types.clone().into_iter().map(|dt| dt.to_string()).collect::<Vec<String>>(),
            "return_type": &x.return_type.to_string(),
        }))
            .into(),
         UDFDefinition::UDFScript(x) => (&json!({
            "arg_types": &x.arg_types.clone().into_iter().map(|dt| dt.to_string()).collect::<Vec<String>>(),
            "return_type": &x.return_type.to_string(),
        }))
            .into(),
    }
}

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
        let udfs = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker().await?;
            let udfs = UserFunctionsTable::get_udfs(ctx).await?;
            udfs.into_iter()
                .filter(|udf| visibility_checker.check_udf_visibility(&udf.name))
                .collect::<Vec<_>>()
        } else {
            UserFunctionsTable::get_udfs(ctx).await?
        };

        let names: Vec<&str> = udfs
            .iter()
            .map(|udf| &udf.name)
            .map(|x| x.as_str())
            .collect();

        let is_aggregate: Vec<Option<bool>> = (0..names.len()).map(|_| None).collect();

        let languages: Vec<&str> = (0..names.len())
            .map(|i| {
                udfs.get(i).map_or("", |udf| match &udf.definition {
                    UDFDefinition::LambdaUDF(_) => "SQL",
                    UDFDefinition::UDFServer(x) => &x.language,
                    UDFDefinition::UDFScript(x) => &x.language,
                })
            })
            .collect();

        let descriptions = (0..names.len())
            .map(|i| udfs.get(i).map_or("", |udf| udf.description.as_str()))
            .collect::<Vec<&str>>();

        let arguments: Vec<Vec<u8>> = (0..names.len())
            .map(|i| {
                udfs.get(i)
                    .map_or(vec![], |udf| encode_arguments(&udf.definition).to_vec())
            })
            .collect();

        let definitions = (0..names.len())
            .map(|i| {
                udfs.get(i)
                    .map_or("".to_string(), |udf| udf.definition.to_string())
            })
            .collect();

        let created_on = (0..names.len())
            .map(|i| {
                udfs.get(i)
                    .map_or(DateTime::<Utc>::default().timestamp_micros(), |udf| {
                        udf.created_on.timestamp_micros()
                    })
            })
            .collect();

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
    async fn get_udfs(ctx: Arc<dyn TableContext>) -> Result<Vec<UserDefinedFunction>> {
        let tenant = ctx.get_tenant();

        UserApiProvider::instance().list_udf(&tenant).await
    }
}
