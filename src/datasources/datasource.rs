// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::datasources::{CsvDataSource, ITable, MemoryDataSource};
use crate::error::{FuseQueryError, FuseQueryResult};

pub trait IDataSource: Sync + Send {
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()>;
    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()>;
    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>>;
}

pub fn get_datasource(dsn_str: &str) -> FuseQueryResult<Arc<Mutex<dyn IDataSource>>> {
    let dsn = dsn::parse(dsn_str)?;
    match dsn.driver.to_lowercase().as_str() {
        "csv" => CsvDataSource::try_create(dsn.address),
        _ => MemoryDataSource::try_create(),
    }
}

pub struct DataSource {
    databases: HashMap<String, HashMap<String, Arc<dyn ITable>>>,
}

impl DataSource {
    pub fn create() -> Self {
        DataSource {
            databases: Default::default(),
        }
    }
}

impl IDataSource for DataSource {
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()> {
        self.databases
            .insert(db_name.to_string(), Default::default());
        Ok(())
    }

    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()> {
        self.databases
            .get_mut(db_name)
            .ok_or_else(|| {
                FuseQueryError::Internal(format!("Can not find the database: {}", db_name))
            })?
            .insert(table.name().to_string(), table);
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        let database = self.databases.get(db_name).ok_or_else(|| {
            FuseQueryError::Internal(format!("Can not find the database: {}", db_name))
        })?;
        let table = database.get(table_name).ok_or_else(|| {
            FuseQueryError::Internal(format!("Can not find the table: {}", table_name))
        })?;
        Ok(table.clone())
    }
}
