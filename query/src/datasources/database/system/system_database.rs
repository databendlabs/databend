// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::impls::util::in_memory_metas::InMemoryMetas;
use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::catalogs::TableMeta;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::catalogs::SYS_TBL_ID_END;
use crate::datasources::database::system;

pub struct SystemDatabase {
    name: String,
    tables: InMemoryMetas,
}

impl SystemDatabase {
    pub fn create(name: impl Into<String>) -> Self {
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

        let name = name.into();

        // Table list.
        let table_list: Vec<Arc<dyn Table>> = vec![
            Arc::new(system::OneTable::create(next_id(), &name)),
            Arc::new(system::FunctionsTable::create(next_id(), &name)),
            Arc::new(system::ContributorsTable::create(next_id(), &name)),
            Arc::new(system::CreditsTable::create(next_id(), &name)),
            Arc::new(system::EnginesTable::create(next_id(), &name)),
            Arc::new(system::SettingsTable::create(next_id(), &name)),
            Arc::new(system::TablesTable::create(next_id(), &name)),
            Arc::new(system::ClustersTable::create(next_id(), &name)),
            Arc::new(system::DatabasesTable::create(next_id(), &name)),
            Arc::new(system::TracingTable::create(next_id(), &name)),
            Arc::new(system::ProcessesTable::create(next_id(), &name)),
            Arc::new(system::ConfigsTable::create(next_id(), &name)),
        ];

        let tbl_meta_list = table_list.into_iter().map(|t| {
            let id = t.get_id();
            TableMeta::create(t, id)
        });
        let mut tables = InMemoryMetas::create();
        for tbl in tbl_meta_list.into_iter() {
            tables.insert(tbl);
        }

        SystemDatabase { name, tables }
    }
}

impl Database for SystemDatabase {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn is_local(&self) -> bool {
        true
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        let table =
            self.tables.name2meta.get(table_name).ok_or_else(|| {
                ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name))
            })?;
        Ok(table.clone())
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let table =
            self.tables.id2meta.get(&table_id).ok_or_else(|| {
                ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
            })?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        Ok(self.tables.name2meta.values().cloned().collect())
    }

    fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot create table for system database",
        ))
    }

    fn drop_table(&self, _plan: DropTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot drop table for system database",
        ))
    }
}
