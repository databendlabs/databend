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

pub struct ViewsTable<const UPPER: bool> {}

impl<const UPPER: bool> ViewsTable<UPPER> {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
            database AS table_catalog,
            database AS table_schema,
            name AS table_name,
            NULL AS view_definition,
            'NONE' AS check_option,
            0 AS is_updatable,
            engine = 'MaterializedView' AS is_insertable_into,
            0 AS is_trigger_updatable,
            0 AS is_trigger_deletable,
            0 AS is_trigger_insertable_into,
            database AS TABLE_CATALOG,
            database AS TABLE_SCHEMA,
            name AS TABLE_NAME,
            NULL AS VIEW_DEFINITION,
            'NONE' AS CHECK_OPTION,
            0 AS IS_UPDATABLE,
            engine = 'MaterializedView' AS IS_INSERTABLE_INTO,
            0 AS IS_TRIGGER_UPDATABLE,
            0 AS IS_TRIGGER_DELETABLE,
            0 AS IS_TRIGGER_INSERTABLE_INTO
        FROM system.tables
        WHERE engine LIKE '%View';";

        let name = if UPPER { "VIEWS" } else { "views" };
        let mut options = HashMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: format!("'information_schema'.'{}'", name),
            name: name.to_string(),
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
