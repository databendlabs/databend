// Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::catalog::Catalog;
use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::Table;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::catalogs::SYS_TBL_ID_END;
use crate::configs::Config;
use crate::datasources::database::system;
use crate::datasources::database::system::SystemDatabase;

/// System Catalog contains ... all the system databases (no surprise :)
/// Currently, this is only one database here, the "system" db.
/// "information_schema" db is supposed to held here
pub struct SystemCatalog {
    sys_db: Arc<SystemDatabase>,
    sys_db_meta: Arc<InMemoryMetas>,
}

impl SystemCatalog {
    pub fn try_create_with_config(_conf: &Config) -> Result<Self> {
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
        ];

        let mut tables = InMemoryMetas::create();
        for tbl in table_list.into_iter() {
            tables.insert(tbl);
        }

        let tables = Arc::new(tables);
        let sys_db = Arc::new(SystemDatabase::create("system"));
        Ok(Self {
            sys_db,
            sys_db_meta: tables,
        })
    }
}

impl Catalog for SystemCatalog {
    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![self.sys_db.clone()])
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        if db_name == "system" {
            return Ok(self.sys_db.clone());
        }
        Err(ErrorCode::UnknownDatabase(format!(
            "unknown database {}",
            db_name
        )))
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        // ensure db exists
        let _db = self.get_database(db_name)?;

        let table = self
            .sys_db_meta
            .name_to_table
            .get(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;

        Ok(table.clone())
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        // ensure db exists
        let _db = self.get_database(db_name)?;

        Ok(self.sys_db_meta.name_to_table.values().cloned().collect())
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<dyn Table>> {
        let table =
            self.sys_db_meta.id_to_table.get(&table_id).ok_or_else(|| {
                ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
            })?;
        Ok(table.clone())
    }

    fn upsert_table_option(
        &self,
        table_id: MetaId,
        _table_version: MetaVersion,
        _key: String,
        _value: String,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::UnImplement(format!(
            "commit table not allowed for system catalog {}",
            table_id
        )))
    }

    fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        unimplemented!("programming error: SystemCatalog does not support create table")
    }

    fn drop_table(&self, _plan: DropTablePlan) -> Result<()> {
        unimplemented!("programming error: SystemCatalog does not support drop table")
    }

    fn create_database(&self, _plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::UnImplement("Cannot create system database"))
    }

    fn drop_database(&self, _plan: DropDatabasePlan) -> Result<()> {
        Err(ErrorCode::UnImplement("Cannot drop system database"))
    }
}
