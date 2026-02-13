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

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct UsersTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for UsersTable {
    const NAME: &'static str = "system.users";

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
        let users = UserApiProvider::instance().get_users(&tenant).await?;

        let workload_mgr = GlobalInstance::get::<Arc<WorkloadMgr>>();

        let mut names: Vec<String> = users.iter().map(|x| x.name.clone()).collect();
        let mut hostnames: Vec<String> = users.iter().map(|x| x.hostname.clone()).collect();
        let mut auth_types: Vec<String> = users
            .iter()
            .map(|x| x.auth_info.get_type().to_str().to_string())
            .collect();
        let mut default_roles: Vec<String> = users
            .iter()
            .map(|x| x.option.default_role().cloned().unwrap_or_default())
            .collect();
        let mut default_warehouses: Vec<String> = users
            .iter()
            .map(|x| x.option.default_warehouse().cloned().unwrap_or_default())
            .collect();
        let mut is_configureds: Vec<String> = vec!["NO".to_string(); users.len()];
        let mut disableds: Vec<bool> = users
            .iter()
            .map(|x| x.option.disabled().cloned().unwrap_or_default())
            .collect();
        let mut roles: Vec<String> = users
            .iter()
            .map(|user| user.grants.roles().iter().sorted().join(", ").to_string())
            .collect();
        let mut network_policies: Vec<Option<String>> = users
            .iter()
            .map(|x| x.option.network_policy().cloned())
            .collect();
        let mut password_policies: Vec<Option<String>> = users
            .iter()
            .map(|x| x.option.password_policy().cloned())
            .collect();
        let mut must_change_passwords: Vec<Option<bool>> = users
            .iter()
            .map(|x| x.option.must_change_password().cloned())
            .collect();
        let mut created_on: Vec<Option<i64>> = users
            .iter()
            .map(|user| Some(user.created_on.timestamp_micros()))
            .collect();
        let mut update_on: Vec<Option<i64>> = users
            .iter()
            .map(|user| Some(user.update_on.timestamp_micros()))
            .collect();

        let mut workload_groups = vec![];
        for user in users {
            match user.option.workload_group() {
                Some(w) => {
                    let wg = workload_mgr
                        .get_by_id(w)
                        .await
                        .map_or(None, |w| Some(w.name));
                    workload_groups.push(wg)
                }
                None => workload_groups.push(None),
            }
        }
        let configured_users = UserApiProvider::instance().get_configured_users();
        for (name, auth_info) in configured_users {
            names.push(name.clone());
            hostnames.push("%".to_string());
            auth_types.push(auth_info.get_type().to_str().to_string());
            default_roles.push(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
            default_warehouses.push(String::new());
            is_configureds.push("YES".to_string());
            disableds.push(false);
            roles.push(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
            network_policies.push(None);
            password_policies.push(None);
            must_change_passwords.push(None);
            created_on.push(None);
            update_on.push(None);
            workload_groups.push(None);
        }

        // please note that do NOT display the auth_string field in the result, because there're risks of
        // password leak. even though it's been hashed, it's still not a good thing.
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(hostnames),
            StringType::from_data(auth_types),
            StringType::from_data(default_roles),
            StringType::from_data(default_warehouses),
            StringType::from_data(is_configureds),
            BooleanType::from_data(disableds),
            StringType::from_data(roles),
            StringType::from_opt_data(network_policies),
            StringType::from_opt_data(password_policies),
            BooleanType::from_opt_data(must_change_passwords),
            TimestampType::from_opt_data(created_on),
            TimestampType::from_opt_data(update_on),
            StringType::from_opt_data(workload_groups),
        ]))
    }
}

impl UsersTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        // QUERY show user is rewrite to `SELECT name, hostname, auth_type, is_configured FROM system.users ORDER BY name`
        // If users table column has been modified, need to check the show user query.
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("hostname", TableDataType::String),
            TableField::new("auth_type", TableDataType::String),
            TableField::new("default_role", TableDataType::String),
            TableField::new("default_warehouse", TableDataType::String),
            TableField::new("is_configured", TableDataType::String),
            TableField::new("disabled", TableDataType::Boolean),
            TableField::new("roles", TableDataType::String),
            TableField::new(
                "network_policy",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "password_policy",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "must_change_password",
                TableDataType::Nullable(Box::new(TableDataType::Boolean)),
            ),
            TableField::new(
                "created_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "update_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "workload_groups",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'users'".to_string(),
            name: "users".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemUsers".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(UsersTable { table_info })
    }
}
