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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::storages::view::view_table::QUERY;
use crate::storages::view::ViewTable;
use crate::storages::Table;

pub struct TablesTable {}

impl TablesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
            database AS table_catalog,
            database AS table_schema,
            name AS table_name,
            'BASE TABLE' AS table_type,
            engine AS engine,
            created_on AS create_time,
            dropped_on AS drop_time,
            0 AS data_length,
            0 AS index_length,
            '' AS table_comment,
            num_rows,
            data_size,
            data_compressed_size,
            index_size,
            database AS TABLE_CATALOG,
            database AS TABLE_SCHEMA,
            name AS TABLE_NAME,
            'BASE TABLE' AS TABLE_TYPE,
            engine AS ENGINE,
            created_on AS CREATE_TIME,
            0 AS DATA_LENGTH,
            0 AS INDEX_LENGTH,
            '' AS TABLE_COMMENT
        FROM system.tables;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'INFORMATION_SCHEMA'.'TABLES'".to_string(),
            name: "TABLES".to_string(),
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
