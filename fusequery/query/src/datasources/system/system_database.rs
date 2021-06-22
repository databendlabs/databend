// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::datasources::system;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

pub struct SystemDatabase {
    tables: HashMap<String, Arc<dyn Table>>,
    table_functions: HashMap<String, Arc<dyn TableFunction>>,
}

impl SystemDatabase {
    pub fn create() -> Self {
        // Table list.
        let table_list: Vec<Arc<dyn Table>> = vec![
            Arc::new(system::OneTable::create()),
            Arc::new(system::FunctionsTable::create()),
            Arc::new(system::ContributorsTable::create()),
            Arc::new(system::SettingsTable::create()),
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::NumbersTable::create("numbers_local")),
            Arc::new(system::TablesTable::create()),
            Arc::new(system::ClustersTable::create()),
            Arc::new(system::DatabasesTable::create()),
        ];
        let mut tables: HashMap<String, Arc<dyn Table>> = HashMap::default();
        for tbl in table_list.iter() {
            tables.insert(tbl.name().to_string(), tbl.clone());
        }

        // Table function list.
        let table_function_list: Vec<Arc<dyn TableFunction>> = vec![
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::NumbersTable::create("numbers_local")),
        ];
        let mut table_functions: HashMap<String, Arc<dyn TableFunction>> = HashMap::default();
        for tbl_func in table_function_list.iter() {
            table_functions.insert(tbl_func.name().to_string(), tbl_func.clone());
        }

        SystemDatabase {
            tables,
            table_functions,
        }
    }
}

#[async_trait::async_trait]
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

    fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        Ok(self.tables.values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<dyn TableFunction>>> {
        Ok(self.table_functions.values().cloned().collect())
    }

    async fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot create table for system database",
        ))
    }

    async fn drop_table(&self, _plan: DropTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot drop table for system database",
        ))
    }
}
