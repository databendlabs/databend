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
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;

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

        let mut names: Vec<Vec<u8>> = users.iter().map(|x| x.name.as_bytes().to_vec()).collect();
        let mut hostnames: Vec<Vec<u8>> = users
            .iter()
            .map(|x| x.hostname.as_bytes().to_vec())
            .collect();
        let mut auth_types: Vec<Vec<u8>> = users
            .iter()
            .map(|x| x.auth_info.get_type().to_str().as_bytes().to_vec())
            .collect();
        let mut default_roles: Vec<Vec<u8>> = users
            .iter()
            .map(|x| {
                x.option
                    .default_role()
                    .cloned()
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec()
            })
            .collect();
        let mut is_configureds: Vec<Vec<u8>> = vec!["NO".as_bytes().to_vec(); users.len()];

        let configured_users = UserApiProvider::instance().get_configured_users();
        for (name, auth_info) in configured_users {
            names.push(name.as_bytes().to_vec());
            hostnames.push("%".as_bytes().to_vec());
            auth_types.push(auth_info.get_type().to_str().as_bytes().to_vec());
            default_roles.push(BUILTIN_ROLE_ACCOUNT_ADMIN.as_bytes().to_vec());
            is_configureds.push("YES".as_bytes().to_vec());
        }

        // please note that do NOT display the auth_string field in the result, because there're risks of
        // password leak. even though it's been hashed, it's still not a good thing.
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(hostnames),
            StringType::from_data(auth_types),
            StringType::from_data(default_roles),
            StringType::from_data(is_configureds),
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
            TableField::new("is_configured", TableDataType::String),
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
