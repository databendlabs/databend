// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::{ITable, ITableFunction};

pub trait IDataSource: Sync + Send {
    fn add_database(&mut self, db_name: &str) -> Result<()>;
    fn check_database(&mut self, db_name: &str) -> Result<()>;
    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> Result<()>;
    fn add_table_function(&mut self, table: Arc<dyn ITableFunction>) -> Result<()>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>>;
    fn get_table_function(&self, function_name: &str) -> Result<Arc<dyn ITableFunction>>;
    fn list_database_tables(&self) -> Vec<(String, Arc<dyn ITable>)>;
}

pub type DatabaseHashMap = HashMap<&'static str, Vec<Arc<dyn ITable>>>;

pub struct DataSource {
    databases: HashMap<String, HashMap<String, Arc<dyn ITable>>>,
    table_functions: HashMap<String, Arc<dyn ITableFunction>>,
}

impl DataSource {
    pub fn try_create() -> Result<Self> {
        let mut datasource = DataSource {
            databases: Default::default(),
            table_functions: Default::default(),
        };
        datasource.add_database("default")?;

        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    fn register_system_database(&mut self) -> Result<()> {
        let factory = SystemFactory::create();
        let map = factory.get_tables()?;
        for (database, tables) in map {
            self.add_database(database)?;
            for tbl in tables {
                self.add_table(database, tbl)?;
            }
        }

        let table_functions = factory.get_table_functions()?;
        for tbl in table_functions {
            self.add_table_function(tbl)?;
        }
        Ok(())
    }

    fn register_local_database(&mut self) -> Result<()> {
        let map = LocalFactory::create().get_tables()?;
        for (database, tables) in map {
            self.add_database(database)?;
            for tbl in tables {
                self.add_table(database, tbl)?;
            }
        }
        Ok(())
    }

    fn register_remote_database(&mut self) -> Result<()> {
        let map = RemoteFactory::create().get_tables()?;
        for (database, tables) in map {
            self.add_database(database)?;
            for tbl in tables {
                self.add_table(database, tbl)?;
            }
        }
        Ok(())
    }
}

impl IDataSource for DataSource {
    fn add_database(&mut self, db_name: &str) -> Result<()> {
        self.databases
            .insert(db_name.to_string(), Default::default());
        Ok(())
    }

    fn check_database(&mut self, db_name: &str) -> Result<()> {
        self.databases.get(db_name).ok_or_else(|| {
            anyhow::Error::msg(format!("DataSource Error: Unknown database: '{}'", db_name))
        })?;
        Ok(())
    }

    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> Result<()> {
        self.databases
            .get_mut(db_name)
            .ok_or_else(|| {
                anyhow::Error::msg(format!("DataSource Error: Unknown database: '{}'", db_name))
            })?
            .insert(table.name().to_string(), table.clone());
        Ok(())
    }

    fn add_table_function(&mut self, table: Arc<dyn ITableFunction>) -> Result<()> {
        self.table_functions
            .insert(table.function_name().to_string(), table.clone());
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>> {
        let database = self.databases.get(db_name).ok_or_else(|| {
            anyhow::Error::msg(format!("DataSource Error: Unknown database: '{}'", db_name))
        })?;
        let table = database.get(table_name).ok_or_else(|| {
            anyhow::Error::msg(format!(
                "DataSource Error: Unknown table: '{}.{}'",
                db_name, table_name
            ))
        })?;
        Ok(table.clone())
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn ITableFunction>> {
        let table = self.table_functions.get(name).ok_or_else(|| {
            anyhow::Error::msg(format!(
                "DataSource Error: Unknown table function: '{}'",
                name
            ))
        })?;

        Ok(table.clone())
    }

    fn list_database_tables(&self) -> Vec<(String, Arc<dyn ITable>)> {
        let mut results = vec![];
        for (k, v) in self.databases.iter() {
            for (_, table) in v.iter() {
                results.push((k.clone(), table.clone()));
            }
        }
        results
    }
}
