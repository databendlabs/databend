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

use databend_common_ast::parser::token::all_reserved_keywords;
use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_system::generate_default_catalog_meta;

pub struct KeywordsTable {}

impl KeywordsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let all_keywords_vec = all_reserved_keywords();
        let all_keywords = all_keywords_vec.join(", ");
        let query = "SELECT '".to_owned() + &all_keywords + "' AS KEYWORDS, 1 AS RESERVED";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query);
        let table_info = TableInfo {
            desc: "'information_schema'.'keywords'".to_string(),
            name: "keywords".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_default_catalog_meta(),
                ..Default::default()
            }),
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
