// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::datasources::{DataSource, IDataSource, ITable};
use crate::error::FuseQueryResult;

pub struct MemoryDataSource {
    datasource: DataSource,
}

impl MemoryDataSource {
    pub fn try_create() -> FuseQueryResult<Arc<Mutex<dyn IDataSource>>> {
        Ok(Arc::new(Mutex::new(MemoryDataSource {
            datasource: DataSource::create(),
        })))
    }
}

impl IDataSource for MemoryDataSource {
    fn add_database(&mut self, db_name: &str) -> FuseQueryResult<()> {
        self.datasource.add_database(db_name)
    }

    fn add_table(&mut self, db_name: &str, table: Arc<dyn ITable>) -> FuseQueryResult<()> {
        self.datasource.add_table(db_name, table)
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.datasource.get_table(db_name, table_name)
    }
}
