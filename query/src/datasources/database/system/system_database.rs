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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::impls::SYS_TBL_ID_BEGIN;
use crate::catalogs::impls::SYS_TBL_ID_END;
use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;
use crate::catalogs::TableFunctionMeta;
use crate::catalogs::TableMeta;
use crate::datasources::database::system;

pub struct SystemDatabase {
    tables: InMemoryMetas,
    table_functions: HashMap<String, Arc<TableFunctionMeta>>,
}

impl SystemDatabase {
    pub fn create() -> Self {
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
        // Table list.
        let table_list: Vec<Arc<dyn Table>> = vec![
            Arc::new(system::OneTable::create()),
            Arc::new(system::FunctionsTable::create()),
            Arc::new(system::ContributorsTable::create()),
            Arc::new(system::CreditsTable::create()),
            Arc::new(system::EnginesTable::create()),
            Arc::new(system::SettingsTable::create()),
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::NumbersTable::create("numbers_local")),
            Arc::new(system::TablesTable::create()),
            Arc::new(system::ClustersTable::create()),
            Arc::new(system::DatabasesTable::create()),
            Arc::new(system::TracingTable::create()),
            Arc::new(system::ProcessesTable::create()),
            Arc::new(system::ConfigsTable::create()),
        ];
        let tbl_meta_list = table_list
            .iter()
            .map(|t| TableMeta::create(t.clone(), next_id()));
        let mut tables = InMemoryMetas::create();
        for tbl in tbl_meta_list.into_iter() {
            tables.insert(tbl);
        }

        // Table function list.
        let table_function_list: Vec<Arc<dyn TableFunction>> = vec![
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::NumbersTable::create("numbers_local")),
        ];
        let mut table_functions = HashMap::default();
        for tbl_func in table_function_list.iter() {
            let name = tbl_func.name();
            table_functions.insert(
                name.to_string(),
                Arc::new(TableFunctionMeta::create(
                    tbl_func.clone(),
                    tables
                        .name2meta
                        .get(name)
                        .expect("prelude function miss-assemblied")
                        .meta_id(),
                )),
            );
        }

        SystemDatabase {
            tables,
            table_functions,
        }
    }
}

impl Database for SystemDatabase {
    fn name(&self) -> &str {
        "system"
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

    fn exists_table(&self, table_name: &str) -> Result<bool> {
        Ok(self.tables.name2meta.get(table_name).is_some())
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

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(self.table_functions.values().cloned().collect())
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
