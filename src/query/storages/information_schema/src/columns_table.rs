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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_view::view_table::ViewTable;
use databend_common_storages_view::view_table::QUERY;

pub struct ColumnsTable {}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
            database AS table_catalog,
            database AS table_schema,
            \"table\" AS table_name,
            name AS column_name,
            1 AS ordinal_position,
            NULL AS column_default,
            comment AS column_comment,
            NULL AS column_key,
            case when is_nullable='NO' then 0
            when is_nullable='YES' then 1
            end as nullable,
            is_nullable AS is_nullable,
            type AS data_type,
            data_type AS column_type,
            NULL AS character_maximum_length,
            NULL AS character_octet_length,
            NULL AS numeric_precision,
            NULL AS numeric_precision_radix,
            NULL AS numeric_scale,
            NULL AS datetime_precision,
            NULL AS character_set_catalog,
            NULL AS character_set_schema,
            NULL AS character_set_name,
            NULL AS collation_catalog,
            NULL AS collation_schema,
            NULL AS collation_name,
            NULL AS domain_catalog,
            NULL AS domain_schema,
            NULL AS domain_name,
            NULL AS privileges,
            default_expression as default,
            NULL AS extra
        FROM system.columns;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'information_schema'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
