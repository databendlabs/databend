// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::info;
use std::sync::{Arc, Mutex};

use crate::datasources::{DataSource, IDataSource, ITable};
use crate::error::FuseQueryResult;

pub struct CsvDataSource {
    path: String,
    datasource: DataSource,
}

impl CsvDataSource {
    pub fn try_create(path: String) -> FuseQueryResult<Arc<Mutex<dyn IDataSource>>> {
        let source = CsvDataSource {
            path,
            datasource: DataSource::create(),
        };
        source.load_databases()?;

        Ok(Arc::new(Mutex::new(source)))
    }

    fn load_databases(&self) -> FuseQueryResult<()> {
        info!("Load CSV databases from path: {}", self.path);
        Ok(())
    }
}

impl IDataSource for CsvDataSource {
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
