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

pub struct StatisticsTable {}

impl StatisticsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT \
        NULL as table_catalog, \
        NULL as table_schema, \
        NULL as table_name, \
        NULL as non_unique, \
        NULL as index_schema, \
        NULL as index_name, \
        NULL as seq_in_index, \
        NULL as column_name, \
        NULL as collation, \
        NULL as cardinality, \
        NULL as sub_part, \
        NULL as packed, \
        NULL as nullable, \
        NULL as index_type, \
        NULL as comment, \
        NULL as index_comment"
            .to_string();

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query);
        let table_info = TableInfo {
            desc: "'information_schema'.'statistics'".to_string(),
            name: "statistics".to_string(),
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
