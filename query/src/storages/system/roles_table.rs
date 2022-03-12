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
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::Vu8;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use super::table::AsyncOneBlockSystemTable;
use super::table::AsyncSystemTable;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct RolesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for RolesTable {
    const NAME: &'static str = "system.roles";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let roles = ctx.get_user_manager().get_roles(&tenant).await?;

        let names: Vec<&str> = roles.iter().map(|x| x.name.as_str()).collect();
        let inherited_roles: Vec<u64> = roles
            .iter()
            .map(|x| x.grants.roles().len() as u64)
            .collect();
        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(inherited_roles),
        ]))
    }
}

impl RolesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("inherited_roles", u64::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'roles'".to_string(),
            name: "roles".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemRoles".to_string(),
                ..Default::default()
            },
        };
        AsyncOneBlockSystemTable::create(RolesTable { table_info })
    }
}
