// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::sessions::QueryContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct UsersTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for UsersTable {
    const NAME: &'static str = "system.users";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let users = ctx.get_user_manager().get_users(&tenant).await?;

        let names: Vec<&str> = users.iter().map(|x| x.name.as_str()).collect();
        let hostnames: Vec<&str> = users.iter().map(|x| x.hostname.as_str()).collect();
        let auth_types: Vec<String> = users
            .iter()
            .map(|x| x.auth_info.get_type().to_str().to_owned())
            .collect();
        let auth_strings: Vec<String> = users
            .iter()
            .map(|x| x.auth_info.get_auth_string())
            .collect();

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(hostnames),
            Series::from_data(auth_types),
            Series::from_data(auth_strings),
        ]))
    }
}

impl UsersTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("hostname", Vu8::to_data_type()),
            DataField::new("auth_type", Vu8::to_data_type()),
            DataField::new_nullable("auth_string", Vu8::to_data_type()),
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
        };

        AsyncOneBlockSystemTable::create(UsersTable { table_info })
    }
}
