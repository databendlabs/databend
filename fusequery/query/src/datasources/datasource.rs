// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use common_planners::{CreateDatabasePlan, DatabaseEngineType};

use crate::configs::Config;
use crate::datasources::local::{LocalDatabase, LocalFactory};
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::{IDatabase, ITable, ITableFunction};

pub trait IDataSource: Sync + Send {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn IDatabase>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn ITable>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn ITableFunction>>;
    fn create_database(&mut self, plan: CreateDatabasePlan) -> Result<()>;
}

// Maintain all the databases of user.
pub struct DataSource {
    conf: Config,
    databases: HashMap<String, Arc<dyn IDatabase>>,
    table_functions: HashMap<String, Arc<dyn ITableFunction>>,
}

impl DataSource {
    pub fn try_create() -> Result<Self> {
        let mut datasource = DataSource {
            conf: Config::default(),
            databases: Default::default(),
            table_functions: Default::default(),
        };

        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_default_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    pub fn try_create_with_config(conf: Config) -> Result<Self> {
        let mut ds = Self::try_create()?;
        ds.conf = conf;
        Ok(ds)
    }

    fn insert_databases(&mut self, databases: Vec<Arc<dyn IDatabase>>) -> Result<()> {
        for database in databases {
            self.databases
                .insert(database.name().to_lowercase(), database.clone());
            for tbl_func in database.get_table_functions()? {
                self.table_functions
                    .insert(tbl_func.name().to_string(), tbl_func.clone());
            }
        }
        Ok(())
    }

    // Register local database with System engine.
    fn register_system_database(&mut self) -> Result<()> {
        let factory = SystemFactory::create();
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register local database with Local engine.
    fn register_local_database(&mut self) -> Result<()> {
        let factory = LocalFactory::create();
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register remote database with Remote engine.
    fn register_remote_database(&mut self) -> Result<()> {
        let factory = RemoteFactory::create(self.conf.clone());
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register default database with Local engine.
    fn register_default_database(&mut self) -> Result<()> {
        let default_db = LocalDatabase::create();
        self.databases
            .insert("default".to_string(), Arc::new(default_db));
        Ok(())
    }
}

impl IDataSource for DataSource {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn IDatabase>> {
        let database = self
            .databases
            .get(db_name)
            .ok_or_else(|| anyhow!("DataSource Error: Unknown database: '{}'", db_name))?;
        Ok(database.clone())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>> {
        let database = self
            .databases
            .get(db_name)
            .ok_or_else(|| anyhow!("DataSource Error: Unknown database: '{}'", db_name))?;
        let table = database.get_table(table_name)?;
        Ok(table.clone())
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn ITable>)>> {
        let mut results = vec![];
        for (k, v) in self.databases.iter() {
            let tables = v.get_tables()?;
            for table in tables {
                results.push((k.clone(), table.clone()));
            }
        }
        Ok(results)
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn ITableFunction>> {
        let table = self
            .table_functions
            .get(name)
            .ok_or_else(|| anyhow!("DataSource Error: Unknown table function: '{}'", name))?;

        Ok(table.clone())
    }

    fn create_database(&mut self, plan: CreateDatabasePlan) -> Result<()> {
        match plan.engine {
            DatabaseEngineType::Local => {
                let database = LocalDatabase::create();
                self.databases.insert(plan.db, Arc::new(database));
            }
            DatabaseEngineType::Remote => {}
        }
        Ok(())
    }
}
