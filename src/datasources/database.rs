// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datasources::ITable;
use crate::error::Result;

pub trait IDatabase: Sync + Send {
    fn name(&self) -> &str;
    fn add_table(&mut self, table: Arc<dyn ITable>) -> Result<()>;
    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>>;
}
