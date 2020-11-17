// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datasources::{IDatabase, ITable};
use crate::error::{Error, Result};

pub struct MemoryDatabase {
    name: String,
    tables: HashMap<String, Arc<dyn ITable>>,
}

impl MemoryDatabase {
    pub fn create(name: &str) -> Self {
        MemoryDatabase {
            name: name.to_string(),
            tables: Default::default(),
        }
    }
}

impl IDatabase for MemoryDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn add_table(&mut self, table: Arc<dyn ITable>) -> Result<()> {
        self.tables.insert(table.name().to_string(), table);
        Ok(())
    }

    fn get_table(&self, table: &str) -> Result<Arc<dyn ITable>> {
        Ok(self
            .tables
            .get(table)
            .ok_or_else(|| Error::Internal(format!("Can not find the table: {}", table)))?
            .clone())
    }
}
