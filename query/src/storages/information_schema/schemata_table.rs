// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::storages::view::view_table::QUERY;
use crate::storages::view::ViewTable;
use crate::storages::Table;

pub struct SchemataTable {}

impl SchemataTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
            name AS catalog_name,
            name AS schema_name,
            'default' AS schema_owner,
            NULL AS default_character_set_catalog,
            NULL AS default_character_set_schema,
            NULL AS default_character_set_name,
            NULL AS sql_path,
            name AS CATALOG_NAME,
            name AS SCHEMA_NAME,
            'default' AS SCHEMA_OWNER,
            NULL AS DEFAULT_CHARACTER_SET_CATALOG,
            NULL AS DEFAULT_CHARACTER_SET_SCHEMA,
            NULL AS DEFAULT_CHARACTER_SET_NAME,
            NULL AS SQL_PATH
        FROM system.databases;";

        let mut options = HashMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'information_schema'.'SCHEMATA'".to_string(),
            name: "SCHEMATA".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
        };

        ViewTable::create(table_info)
    }
}
