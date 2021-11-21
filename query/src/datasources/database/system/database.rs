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
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::catalogs::Database1;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::Table;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::catalogs::SYS_TBL_ID_END;
use crate::configs::Config;
use crate::datasources::database::system;

#[derive(Clone)]
pub struct SystemDatabase1 {
    sys_db_meta: Arc<InMemoryMetas>,
}

impl SystemDatabase1 {
    pub fn create(_config: &Config) -> Self {
        let mut id = SYS_TBL_ID_BEGIN;
        let mut next_id = || -> u64 {
            // 10000 table ids reserved for system tables
            if id >= SYS_TBL_ID_END {
                // Fatal error, gives up
                panic!("system table id used up")
            } else {
                let r = id;
                id += 1;
                r
            }
        };

        let table_list: Vec<Arc<dyn Table>> = vec![
            Arc::new(system::OneTable::create(next_id())),
            Arc::new(system::FunctionsTable::create(next_id())),
            Arc::new(system::ContributorsTable::create(next_id())),
            Arc::new(system::CreditsTable::create(next_id())),
            Arc::new(system::SettingsTable::create(next_id())),
            Arc::new(system::TablesTable::create(next_id())),
            Arc::new(system::ClustersTable::create(next_id())),
            Arc::new(system::DatabasesTable::create(next_id())),
            Arc::new(system::TracingTable::create(next_id())),
            Arc::new(system::ProcessesTable::create(next_id())),
            Arc::new(system::ConfigsTable::create(next_id())),
            Arc::new(system::MetricsTable::create(next_id())),
            Arc::new(system::ColumnsTable::create(next_id())),
            Arc::new(system::UsersTable::create(next_id())),
        ];

        let mut tables = InMemoryMetas::create();
        for tbl in table_list.into_iter() {
            tables.insert(tbl);
        }

        let tables = Arc::new(tables);
        Self {
            sys_db_meta: tables,
        }
    }
}

#[async_trait::async_trait]
impl Database1 for SystemDatabase1 {
    fn name(&self) -> &str {
        "system"
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        // Check the database.
        if db_name != self.name() {
            return Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            )));
        }

        let table = self
            .sys_db_meta
            .name_to_table
            .get(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;

        Ok(table.clone())
    }

    async fn get_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        // Check the database.
        if db_name != self.name() {
            return Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            )));
        }
        Ok(self.sys_db_meta.name_to_table.values().cloned().collect())
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let table =
            self.sys_db_meta.id_to_table.get(&table_id).ok_or_else(|| {
                ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
            })?;
        let ti = table.get_table_info();
        Ok((ti.ident.clone(), Arc::new(ti.meta.clone())))
    }

    async fn create_table(&self, _req: CreateTableReq) -> common_exception::Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot create system database table",
        ))
    }

    async fn drop_table(&self, _req: DropTableReq) -> common_exception::Result<DropTableReply> {
        Err(ErrorCode::UnImplement("Cannot drop system database table"))
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let table_id = table_info.ident.table_id;

        let table =
            self.sys_db_meta.id_to_table.get(&table_id).ok_or_else(|| {
                ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
            })?;

        Ok(table.clone())
    }
}
