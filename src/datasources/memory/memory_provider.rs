// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};

use crate::datasources::{IDataSourceProvider, ITable};

pub struct MemoryProvider {
    tables: HashMap<String, Arc<dyn ITable>>,
}

impl MemoryProvider {
    pub fn create() -> Self {
        MemoryProvider {
            tables: Default::default(),
        }
    }

    pub fn add_table(&mut self, _db: &str, table: &str, source: Arc<dyn ITable>) -> Result<()> {
        self.tables.insert(table.to_string(), source);
        Ok(())
    }
}

impl IDataSourceProvider for MemoryProvider {
    fn get_table(&self, _db: String, table: String) -> Result<Arc<dyn ITable>> {
        let res = self.tables.get(&table);
        match res {
            Some(v) => Ok(v.clone()),
            None => Err(Error::Unsupported(format!(
                "Can not find the table: {} from the MemoryProvider",
                table
            ))),
        }
    }
}
