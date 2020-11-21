// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datasources::{IDatabase, ITable};
use crate::error::{FuseQueryError, FuseQueryResult};

#[derive(Clone)]
pub struct DataSource {
    databases: HashMap<String, Arc<dyn IDatabase>>,
}

impl DataSource {
    pub fn create() -> DataSource {
        DataSource {
            databases: Default::default(),
        }
    }

    pub fn add_database(&mut self, db: Arc<dyn IDatabase>) -> FuseQueryResult<()> {
        self.databases.insert(db.name().to_string(), db);
        Ok(())
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.databases
            .get(db_name)
            .ok_or_else(|| {
                FuseQueryError::Internal(format!("Can not find the database: {}", db_name))
            })?
            .get_table(table_name)
    }
}
