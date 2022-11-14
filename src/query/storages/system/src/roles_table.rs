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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::SchemaDataType;
use common_expression::Value;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct RolesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for RolesTable {
    const NAME: &'static str = "system.roles";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<Chunk> {
        let tenant = ctx.get_tenant();
        let roles = UserApiProvider::instance().get_roles(&tenant).await?;

        let names: Vec<&str> = roles.iter().map(|x| x.name.as_str()).collect();
        let inherited_roles: Vec<u64> = roles
            .iter()
            .map(|x| x.grants.roles().len() as u64)
            .collect();

        let rows_len = names.len();
        Ok(Chunk::new(
            vec![
                (Value::Column(Column::from_data(names)), DataType::String),
                (
                    Value::Column(Column::from_data(inherited_roles)),
                    DataType::String,
                ),
            ],
            rows_len,
        ))
    }
}

impl RolesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", SchemaDataType::String),
            DataField::new(
                "inherited_roles",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
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
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(RolesTable { table_info })
    }
}
