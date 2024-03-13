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
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct PasswordPoliciesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for PasswordPoliciesTable {
    const NAME: &'static str = "system.password_policies";

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
        let password_policies = UserApiProvider::instance()
            .get_password_policies(&tenant)
            .await?;

        let mut names = Vec::with_capacity(password_policies.len());
        let mut comments = Vec::with_capacity(password_policies.len());
        let mut options = Vec::with_capacity(password_policies.len());
        let mut created_on_columns = Vec::with_capacity(password_policies.len());
        let mut updated_on_columns = Vec::with_capacity(password_policies.len());
        for password_policy in password_policies {
            names.push(password_policy.name.clone());
            comments.push(password_policy.comment.clone());

            let values = vec![
                format!("MIN_LENGTH={}", password_policy.min_length),
                format!("MAX_LENGTH={}", password_policy.max_length),
                format!(
                    "MIN_UPPER_CASE_CHARS={}",
                    password_policy.min_upper_case_chars
                ),
                format!(
                    "MIN_LOWER_CASE_CHARS={}",
                    password_policy.min_lower_case_chars
                ),
                format!("MIN_NUMERIC_CHARS={}", password_policy.min_numeric_chars),
                format!("MIN_SPECIAL_CHARS={}", password_policy.min_special_chars),
                format!("MIN_AGE_DAYS={}", password_policy.min_age_days),
                format!("MAX_AGE_DAYS={}", password_policy.max_age_days),
                format!("MAX_RETRIES={}", password_policy.max_retries),
                format!("LOCKOUT_TIME_MINS={}", password_policy.lockout_time_mins),
                format!("HISTORY={}", password_policy.history),
            ];
            let option = values.join(", ");
            options.push(option);

            created_on_columns.push(password_policy.create_on.timestamp_micros());
            updated_on_columns.push(password_policy.update_on.map(|u| u.timestamp_micros()));
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(comments),
            StringType::from_data(options),
            TimestampType::from_data(created_on_columns),
            TimestampType::from_opt_data(updated_on_columns),
        ]))
    }
}

impl PasswordPoliciesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("comment", TableDataType::String),
            TableField::new("options", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "updated_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'password_policies'".to_string(),
            name: "password_policies".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemPasswordPolicies".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(PasswordPoliciesTable { table_info })
    }
}
