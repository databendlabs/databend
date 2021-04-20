// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use common_planners::CreateTablePlan;

use crate::datasources::system;
use crate::datasources::IDatabase;
use crate::datasources::ITable;
use crate::datasources::ITableFunction;

pub struct SystemDatabase {
    tables: HashMap<String, Arc<dyn ITable>>,
    table_functions: HashMap<String, Arc<dyn ITableFunction>>,
}

impl SystemDatabase {
    pub fn create() -> Self {
        // Table list.
        let table_list: Vec<Arc<dyn ITable>> = vec![
            Arc::new(system::OneTable::create()),
            Arc::new(system::FunctionsTable::create()),
            Arc::new(system::SettingsTable::create()),
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::TablesTable::create()),
            Arc::new(system::ClustersTable::create()),
            Arc::new(system::DatabasesTable::create()),
        ];
        let mut tables: HashMap<String, Arc<dyn ITable>> = HashMap::default();
        for tbl in table_list.iter() {
            tables.insert(tbl.name().to_string(), tbl.clone());
        }

        // Table function list.
        let table_function_list: Vec<Arc<dyn ITableFunction>> = vec![
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
        ];
        let mut table_functions: HashMap<String, Arc<dyn ITableFunction>> = HashMap::default();
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
impl IDatabase for SystemDatabase {
    fn name(&self) -> &str {
        "system"
    }

    fn engine(&self) -> &str {
        "system"
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow!("DataSource Error: Unknown table: '{}'", table_name))?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>> {
        Ok(self.tables.values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>> {
        Ok(self.table_functions.values().cloned().collect())
    }

    async fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        bail!("DataSource Error: cannot create table for system database")
    }
}
