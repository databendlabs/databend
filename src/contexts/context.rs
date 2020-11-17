// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datasources::{DataSource, ITable};
use crate::error::Result;

#[derive(Clone)]
pub struct Context {
    pub default_db: String,
    datasource: Arc<DataSource>,
}

impl Context {
    pub fn create_ctx(datasource: Arc<DataSource>) -> Self {
        Context {
            default_db: "default".to_string(),
            datasource,
        }
    }

    pub fn table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>> {
        self.datasource.get_table(db_name, table_name)
    }
}
