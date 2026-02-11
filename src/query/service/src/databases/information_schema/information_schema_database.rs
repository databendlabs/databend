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

use std::sync::Arc;

use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_information_schema::CharacterSetsTable;
use databend_common_storages_information_schema::ColumnsTable;
use databend_common_storages_information_schema::KeyColumnUsageTable;
use databend_common_storages_information_schema::KeywordsTable;
use databend_common_storages_information_schema::ReferentialConstraintsTable;
use databend_common_storages_information_schema::SchemataTable;
use databend_common_storages_information_schema::StatisticsTable;
use databend_common_storages_information_schema::TablesTable;
use databend_common_storages_information_schema::ViewsTable;
use databend_meta_types::SeqV;

use crate::catalogs::InMemoryMetas;
use crate::databases::Database;
use crate::storages::Table;

#[derive(Clone)]
pub struct InformationSchemaDatabase {
    db_info: DatabaseInfo,
}

impl InformationSchemaDatabase {
    pub fn create(sys_db_meta: &mut InMemoryMetas, ctl_name: &str) -> Self {
        let table_list: Vec<Arc<dyn Table>> = vec![
            ColumnsTable::create(sys_db_meta.next_table_id(), ctl_name),
            CharacterSetsTable::create(sys_db_meta.next_table_id(), ctl_name),
            TablesTable::create(sys_db_meta.next_table_id(), ctl_name),
            KeywordsTable::create(sys_db_meta.next_table_id(), ctl_name),
            ViewsTable::create(sys_db_meta.next_table_id(), ctl_name),
            SchemataTable::create(sys_db_meta.next_table_id(), ctl_name),
            StatisticsTable::create(sys_db_meta.next_table_id(), ctl_name),
            KeyColumnUsageTable::create(sys_db_meta.next_table_id(), ctl_name),
            ReferentialConstraintsTable::create(sys_db_meta.next_table_id(), ctl_name),
        ];

        let db = "information_schema";

        for tbl in table_list.into_iter() {
            sys_db_meta.insert(db, tbl);
        }

        let db_info = DatabaseInfo {
            database_id: DatabaseId::new(sys_db_meta.next_db_id()),
            name_ident: DatabaseNameIdent::new(Tenant::new_literal("dummy"), db),
            meta: SeqV::new(0, DatabaseMeta {
                engine: "SYSTEM".to_string(),
                ..Default::default()
            }),
        };

        Self { db_info }
    }
}

#[async_trait::async_trait]
impl Database for InformationSchemaDatabase {
    fn name(&self) -> &str {
        "information_schema"
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }
}
