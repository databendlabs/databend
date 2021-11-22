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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;

use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::Table;
use crate::datasources::database::system;

#[derive(Clone)]
pub struct SystemDatabase {
    sys_db_meta: Arc<InMemoryMetas>,
}

impl SystemDatabase {
    pub fn create(sys_db_meta: Arc<InMemoryMetas>) -> Self {
        let table_list: Vec<Arc<dyn Table>> = vec![
            Arc::new(system::OneTable::create(sys_db_meta.next_id())),
            Arc::new(system::FunctionsTable::create(sys_db_meta.next_id())),
            Arc::new(system::ContributorsTable::create(sys_db_meta.next_id())),
            Arc::new(system::CreditsTable::create(sys_db_meta.next_id())),
            Arc::new(system::SettingsTable::create(sys_db_meta.next_id())),
            Arc::new(system::TablesTable::create(sys_db_meta.next_id())),
            Arc::new(system::ClustersTable::create(sys_db_meta.next_id())),
            Arc::new(system::DatabasesTable::create(sys_db_meta.next_id())),
            Arc::new(system::TracingTable::create(sys_db_meta.next_id())),
            Arc::new(system::ProcessesTable::create(sys_db_meta.next_id())),
            Arc::new(system::ConfigsTable::create(sys_db_meta.next_id())),
            Arc::new(system::MetricsTable::create(sys_db_meta.next_id())),
            Arc::new(system::ColumnsTable::create(sys_db_meta.next_id())),
            Arc::new(system::UsersTable::create(sys_db_meta.next_id())),
        ];

        for tbl in table_list.into_iter() {
            sys_db_meta.insert(tbl);
        }

        Self { sys_db_meta }
    }
}

#[async_trait::async_trait]
impl Database for SystemDatabase {
    fn name(&self) -> &str {
        "system"
    }

    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        // Check the database.
        if db_name != self.name() {
            return Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            )));
        }

        let table = self
            .sys_db_meta
            .get_by_name(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;

        Ok(table.clone())
    }

    async fn list_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        // ensure db exists
        if db_name != self.name() {
            return Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            )));
        }
        self.sys_db_meta.get_all_tables()
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot create system database table",
        ))
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply> {
        Err(ErrorCode::UnImplement("Cannot drop system database table"))
    }
}
