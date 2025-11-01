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
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_system::generate_catalog_meta;

/// A minimal INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS table.
/// It exposes the correct column layout but contains no rows.
pub struct ReferentialConstraintsTable;

impl ReferentialConstraintsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let query = "SELECT \
                CAST(NULL AS String) AS constraint_catalog, \
                CAST(NULL AS String) AS constraint_schema, \
                CAST(NULL AS String) AS constraint_name, \
                CAST(NULL AS String) AS unique_constraint_catalog, \
                CAST(NULL AS String) AS unique_constraint_schema, \
                CAST(NULL AS String) AS unique_constraint_name, \
                CAST(NULL AS String) AS match_option, \
                CAST(NULL AS String) AS update_rule, \
                CAST(NULL AS String) AS delete_rule, \
                CAST(NULL AS String) AS table_name, \
                CAST(NULL AS String) AS referenced_table_name \
            WHERE 1 = 0"
            .to_string();

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query);
        let table_info = TableInfo {
            desc: "'information_schema'.'referential_constraints'".to_string(),
            name: "referential_constraints".to_string(),
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
