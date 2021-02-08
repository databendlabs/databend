// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::ITable;
use crate::error::{FuseQueryError, FuseQueryResult};

pub trait IDataSource: Sync + Send {
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()>;
    fn check_database(&mut self, db_name: &str) -> FuseQueryResult<()>;
    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()>;
    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>>;
}

pub type DatabaseHashMap = HashMap<&'static str, Vec<Arc<dyn ITable>>>;

pub struct DataSource {
    databases: HashMap<String, HashMap<String, Arc<dyn ITable>>>,
}

impl DataSource {
    pub fn try_create() -> FuseQueryResult<Self> {
        let mut datasource = DataSource {
            databases: Default::default(),
        };
        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    fn register_system_database(&mut self) -> FuseQueryResult<()> {
        let map = SystemFactory::create().get_tables()?;
        for (database, tables) in map {
            self.add_database(database)?;
            for tbl in tables {
                self.add_table(database, tbl)?;
            }
        }
        Ok(())
    }

    fn register_local_database(&mut self) -> FuseQueryResult<()> {
        let map = LocalFactory::create().get_tables()?;
        for (database, tables) in map {
            self.add_database(database)?;
            for tbl in tables {
                self.add_table(database, tbl)?;
            }
        }
        Ok(())
    }

    fn register_remote_database(&mut self) -> FuseQueryResult<()> {
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
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()> {
        self.databases
            .insert(db_name.to_string(), Default::default());
        Ok(())
    }

    fn check_database(&mut self, db_name: &str) -> FuseQueryResult<()> {
        self.databases
            .get(db_name)
            .ok_or_else(|| FuseQueryError::Internal(format!("Unknown database: '{}'", db_name)))?;
        Ok(())
    }

    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()> {
        self.databases
            .get_mut(db_name)
            .ok_or_else(|| FuseQueryError::Internal(format!("Unknown database: '{}'", db_name)))?
            .insert(table.name().to_string(), table);
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        let database = self
            .databases
            .get(db_name)
            .ok_or_else(|| FuseQueryError::Internal(format!("Unknown database: '{}'", db_name)))?;
        let table = database.get(table_name).ok_or_else(|| {
            FuseQueryError::Internal(format!("Unknown table: '{}.{}'", db_name, table_name))
        })?;
        Ok(table.clone())
    }
}
