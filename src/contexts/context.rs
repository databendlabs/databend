// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::datasources::{IDataSource, ITable};
use crate::error::FuseQueryResult;

pub struct FuseQueryContext {
    pub worker_threads: usize,
    default_db: Mutex<String>,
    datasource: Arc<Mutex<dyn IDataSource>>,
}

impl FuseQueryContext {
    pub fn create_ctx(worker_threads: usize, datasource: Arc<Mutex<dyn IDataSource>>) -> Self {
        FuseQueryContext {
            worker_threads,
            default_db: Mutex::new("default".to_string()),
            datasource,
        }
    }

    pub fn get_current_database(&self) -> FuseQueryResult<String> {
        Ok(self.default_db.lock()?.clone())
    }

    pub fn set_current_database(&self, db: &str) -> FuseQueryResult<()> {
        *self.default_db.lock()? = db.to_string();
        Ok(())
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.datasource.lock()?.get_table(db_name, table_name)
    }
}
