// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datasources::{IDataSourceProvider, ITable};
use crate::error::Result;

#[derive(Clone)]
pub struct Context {
    provider: Arc<dyn IDataSourceProvider>,
}

impl Context {
    pub fn create_ctx(provider: Arc<dyn IDataSourceProvider>) -> Self {
        Context { provider }
    }

    pub fn table(&self, db: &str, table: &str) -> Result<Arc<dyn ITable>> {
        self.provider.get_table(db.to_string(), table.to_string())
    }
}
