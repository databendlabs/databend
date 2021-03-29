// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use anyhow::Result;

use crate::datasources::datasource::DatabaseHashMap;
use crate::datasources::local::NullTable;
use crate::datasources::local::{CsvTable, ParquetTable};
use crate::datasources::table_factory::TableCreatorFactory;

pub struct LocalFactory;

impl LocalFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn get_tables(&self) -> Result<DatabaseHashMap> {
        let hashmap: DatabaseHashMap = HashMap::default();
        Ok(hashmap)
    }

    pub fn register(map: TableCreatorFactory) -> Result<()> {
        NullTable::register(map.clone())?;
        CsvTable::register(map.clone())?;
        ParquetTable::register(map.clone())?;
        Ok(())
    }
}
