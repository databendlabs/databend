//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;

use crate::catalogs::InMemoryMetas;
use crate::databases::Database;
use crate::storages::system;
use crate::storages::Table;

#[derive(Clone)]
pub struct SystemDatabase {
    db_info: DatabaseInfo,
}

impl SystemDatabase {
    pub fn create(sys_db_meta: &mut InMemoryMetas) -> Self {
        let table_list: Vec<Arc<dyn Table>> = vec![
            system::OneTable::create(sys_db_meta.next_id()),
            system::FunctionsTable::create(sys_db_meta.next_id()),
            system::ContributorsTable::create(sys_db_meta.next_id()),
            system::CreditsTable::create(sys_db_meta.next_id()),
            system::SettingsTable::create(sys_db_meta.next_id()),
            system::TablesTable::create(sys_db_meta.next_id()),
            system::ClustersTable::create(sys_db_meta.next_id()),
            system::DatabasesTable::create(sys_db_meta.next_id()),
            Arc::new(system::TracingTable::create(sys_db_meta.next_id())),
            system::ProcessesTable::create(sys_db_meta.next_id()),
            system::ConfigsTable::create(sys_db_meta.next_id()),
            system::MetricsTable::create(sys_db_meta.next_id()),
            system::ColumnsTable::create(sys_db_meta.next_id()),
            system::UsersTable::create(sys_db_meta.next_id()),
            system::WarehousesTable::create(sys_db_meta.next_id()),
            Arc::new(system::QueryLogTable::create(sys_db_meta.next_id())),
            system::EnginesTable::create(sys_db_meta.next_id()),
            system::RolesTable::create(sys_db_meta.next_id()),
        ];

        for tbl in table_list.into_iter() {
            sys_db_meta.insert(tbl);
        }

        let db_info = DatabaseInfo {
            database_id: 0,
            db: "system".to_string(),
            meta: DatabaseMeta {
                engine: "SYSTEM".to_string(),
                ..Default::default()
            },
        };

        Self { db_info }
    }
}

#[async_trait::async_trait]
impl Database for SystemDatabase {
    fn name(&self) -> &str {
        "system"
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }
}
