// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datasources::{system, ITable};
use crate::error::{FuseQueryError, FuseQueryResult};

pub trait IDataSource: Sync + Send {
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()>;
    fn check_database(&mut self, db_name: &str) -> FuseQueryResult<()>;
    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()>;
    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>>;
}

pub struct DataSource {
    databases: HashMap<String, HashMap<String, Arc<dyn ITable>>>,
}

impl DataSource {
    pub fn try_create() -> FuseQueryResult<Self> {
        let mut datasource = DataSource {
            databases: Default::default(),
        };
        datasource.register_system_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    fn register_system_database(&mut self) -> FuseQueryResult<()> {
        self.add_database("system")?;
        self.add_table("system", Arc::new(system::NumbersTable::create("numbers")))?;
        self.add_table(
            "system",
            Arc::new(system::NumbersTable::create("numbers_mt")),
        )?;
        self.add_table("system", Arc::new(system::FunctionsTable::create()))?;
        self.add_table("system", Arc::new(system::SettingsTable::create()))?;
        self.add_table("system", Arc::new(system::OneTable::create()))?;
        Ok(())
    }

    fn register_remote_database(&mut self) -> FuseQueryResult<()> {
        // TODO
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
