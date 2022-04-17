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

use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::storages::view::view_table::QUERY;
use crate::storages::view::ViewTable;
use crate::storages::Table;

pub struct ColumnsTable {}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
            database AS table_catalog,
            database AS table_schema,
            table AS table_name,
            name AS column_name,
            1 AS ordinal_position,
            NULL AS column_default,
            is_nullable AS is_nullable,
            data_type AS data_type,
            NULL AS character_maximum_length,
            NULL as character_octet_length,
            NULL as numeric_precision,
            NULL as numeric_precision_radix,
            NULL as numeric_scale,
            NULL as datetime_precision,
            NULL AS character_set_catalog,
            NULL AS character_set_schema,
            NULL AS character_set_name,
            NULL AS collation_catalog,
            NULL AS collation_schema,
            NULL AS collation_name,
            NULL AS domain_catalog,
            NULL AS domain_schema,
            NULL AS domain_name,
            database AS TABLE_CATALOG,
            database AS TABLE_SCHEMA,
            table AS TABLE_NAME,
            name AS COLUMN_NAME,
            1 AS ORDINAL_POSITION,
            NULL AS COLUMN_DEFAULT,
            is_nullable AS IS_NULLABLE,
            data_type AS DATA_TYPE,
            NULL AS CHARACTER_MAXIMUM_LENGTH,
            NULL as CHARACTER_OCTET_LENGTH,
            NULL as NUMERIC_PRECISION,
            NULL as NUMERIC_PRECISION_RADIX,
            NULL as NUMERIC_SCALE,
            NULL as DATETIME_PRECISION,
            NULL AS CHARACTER_SET_CATALOG,
            NULL AS CHARACTER_SET_SCHEMA,
            NULL AS CHARACTER_SET_NAME,
            NULL AS COLLATION_CATALOG,
            NULL AS COLLATION_SCHEMA,
            NULL AS COLLATION_NAME,
            NULL AS DOMAIN_CATALOG,
            NULL AS DOMAIN_SCHEMA,
            NULL AS DOMAIN_NAME
        FROM system.columns;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'INFORMATION_SCHEMA'.'COLUMNS'".to_string(),
            name: "COLUMNS".to_string(),
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
