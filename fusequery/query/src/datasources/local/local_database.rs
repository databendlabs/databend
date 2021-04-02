// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_infallible::RwLock;
use common_planners::{CreatePlan, EngineType};

use crate::datasources::local::{CsvTable, NullTable, ParquetTable};
use crate::datasources::{IDatabase, ITable, ITableFunction};

pub struct LocalDatabase {
    tables: RwLock<HashMap<String, Arc<dyn ITable>>>,
}

impl LocalDatabase {
    pub fn create() -> Self {
        LocalDatabase {
            tables: RwLock::new(HashMap::default()),
        }
    }
}

impl IDatabase for LocalDatabase {
    fn name(&self) -> &str {
        "local"
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>> {
        let table_lock = self.tables.read();
        let table = table_lock.get(table_name).ok_or_else(|| {
            anyhow::Error::msg(format!("DataSource Error: Unknown table: '{}'", table_name))
        })?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>> {
        let mut result = vec![];
        for table in self.tables.read().values() {
            result.push(table.clone());
        }
        Ok(result)
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>> {
        Ok(vec![])
    }

    fn create_table(&self, plan: CreatePlan) -> Result<()> {
        let table_name = plan.table.clone();

        let table = match &plan.engine {
            EngineType::Parquet => {
                ParquetTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            EngineType::Csv => {
                CsvTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            EngineType::Null => {
                NullTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            _ => bail!("Unsupported engine: {:?}", plan.engine),
        };

        self.tables.write().insert(table_name, Arc::from(table));
        Ok(())
    }
}
