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

use std::str::FromStr;
use std::sync::Arc;
use std::vec;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use jsonb::OwnedJsonb;
use serde_json;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct StagesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for StagesTable {
    const NAME: &'static str = "system.stages";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        let stages = UserApiProvider::instance().get_stages(&tenant).await?;
        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        let stages = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker(false, Object::Stage).await?;
            stages
                .into_iter()
                .filter(|stage| {
                    !stage.is_temporary
                        && visibility_checker.check_stage_visibility(&stage.stage_name)
                })
                .collect::<Vec<_>>()
        } else {
            stages
        };

        let user_api = UserApiProvider::instance();
        let mut owners: Vec<Option<String>> = vec![];
        let mut name: Vec<String> = Vec::with_capacity(stages.len());
        let mut stage_type: Vec<String> = Vec::with_capacity(stages.len());
        let mut storage_type: Vec<String> = Vec::with_capacity(stages.len());
        let mut urls: Vec<String> = Vec::with_capacity(stages.len());
        let mut endpoints: Vec<Option<String>> = Vec::with_capacity(stages.len());
        let mut has_credentials: Vec<bool> = Vec::with_capacity(stages.len());
        let mut has_encryption_key: Vec<bool> = Vec::with_capacity(stages.len());
        let mut storage_params = Vec::with_capacity(stages.len());
        let mut file_format_options = Vec::with_capacity(stages.len());
        let mut creator: Vec<Option<String>> = Vec::with_capacity(stages.len());
        let mut created_on = Vec::with_capacity(stages.len());
        let mut comment: Vec<String> = Vec::with_capacity(stages.len());

        for stage in stages.into_iter() {
            let stage_name = stage.stage_name;
            name.push(stage_name.clone());
            owners.push(
                user_api
                    .get_ownership(&tenant, &OwnershipObject::Stage { name: stage_name })
                    .await
                    .ok()
                    .and_then(|ownership| ownership.map(|o| o.role.clone())),
            );
            stage_type.push(stage.stage_type.to_string());

            let storage = stage.stage_params.storage.clone();
            storage_type.push(storage.storage_type());
            urls.push(storage.url().unwrap_or_default());
            endpoints.push(storage.endpoint());
            has_credentials.push(storage.has_credentials());
            has_encryption_key.push(storage.has_encryption_key());

            let storage_json = serde_json::to_string(&storage)?;
            let storage_jsonb = OwnedJsonb::from_str(&storage_json).map_err(|err| {
                ErrorCode::Internal(format!("failed to encode storage params as JSONB: {err}"))
            })?;
            storage_params.push(storage_jsonb.to_vec());

            let format_json = serde_json::to_string(&stage.file_format_params)?;
            let format_jsonb = OwnedJsonb::from_str(&format_json).map_err(|err| {
                ErrorCode::Internal(format!(
                    "failed to encode file format params as JSONB: {err}"
                ))
            })?;
            file_format_options.push(format_jsonb.to_vec());

            creator.push(stage.creator.map(|c| c.display().to_string()));
            created_on.push(stage.created_on.timestamp_micros());
            comment.push(stage.comment.clone());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(name),
            StringType::from_data(stage_type),
            StringType::from_data(storage_type),
            StringType::from_data(urls),
            StringType::from_opt_data(endpoints),
            BooleanType::from_data(has_credentials),
            BooleanType::from_data(has_encryption_key),
            VariantType::from_data(storage_params),
            VariantType::from_data(file_format_options),
            StringType::from_opt_data(creator),
            TimestampType::from_data(created_on),
            StringType::from_data(comment),
            StringType::from_opt_data(owners),
        ]))
    }
}

impl StagesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("stage_type", TableDataType::String),
            TableField::new("storage_type", TableDataType::String),
            TableField::new("url", TableDataType::String),
            TableField::new(
                "endpoint",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("has_credentials", TableDataType::Boolean),
            TableField::new("has_encryption_key", TableDataType::Boolean),
            TableField::new("storage_params", TableDataType::Variant),
            TableField::new("file_format_options", TableDataType::Variant),
            TableField::new(
                "creator",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("comment", TableDataType::String),
            TableField::new(
                "owner",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'stages'".to_string(),
            name: "stages".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemStages".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(StagesTable { table_info })
    }
}
