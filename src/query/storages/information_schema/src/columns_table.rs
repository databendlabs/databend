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
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_system::generate_catalog_meta;

pub struct ColumnsTable {}

impl ColumnsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let query = format!(
            "SELECT
            database AS table_catalog,
            database AS table_schema,
            table AS table_name,
            name AS column_name,
            1 AS ordinal_position,
            NULL AS column_default,
            comment AS column_comment,
            NULL AS column_key,
            case when is_nullable='NO' then 0
            when is_nullable='YES' then 1
            end as nullable,
            is_nullable AS is_nullable,
            LOWER(data_type) AS data_type,
            CASE
                WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN CONCAT('varchar(', '16382', ')')
                ELSE LOWER(data_type)
            END AS column_type,
            CASE
                WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN 16382
                ELSE NULL
            END AS character_maximum_length,
            CASE
                WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN 16382 * 4
                ELSE NULL
            END AS character_octet_length,
            NULL AS numeric_precision,
            NULL AS numeric_precision_radix,
            NULL AS numeric_scale,
            NULL AS datetime_precision,
            NULL AS character_set_catalog,
            NULL AS character_set_schema,
            CASE
                WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN 'utf8mb4'
                ELSE NULL
            END AS character_set_name,
            NULL AS collation_catalog,
            NULL AS collation_schema,
            CASE
                WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN 'utf8mb4_general_ci'
                ELSE NULL
            END AS collation_name,
            NULL AS domain_catalog,
            NULL AS domain_schema,
            NULL AS domain_name,
            NULL AS privileges,
            default_expression as default,
            NULL AS extra
        FROM {}.system.columns;",
            ctl_name
        );

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
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
