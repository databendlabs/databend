// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::{Result, ErrorCodes};
use common_infallible::RwLock;
use common_planners::CreateTablePlan;
use common_planners::TableEngineType;

use crate::datasources::local::CsvTable;
use crate::datasources::local::NullTable;
use crate::datasources::local::ParquetTable;
use crate::datasources::IDatabase;
use crate::datasources::ITable;
use crate::datasources::ITableFunction;

pub struct LocalDatabase {
    tables: RwLock<HashMap<String, Arc<dyn ITable>>>
}

impl LocalDatabase {
    pub fn create() -> Self {
        LocalDatabase {
            tables: RwLock::new(HashMap::default())
        }
    }
}

#[async_trait::async_trait]
impl IDatabase for LocalDatabase {
    fn name(&self) -> &str {
        "local"
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>> {
        let table_lock = self.tables.read();
        let table = table_lock
            .get(table_name)
            .ok_or_else(|| {
                ErrorCodes::UnknownTable(format!("DataSource Error: Unknown table: '{}'", table_name))
            })?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>> {
        Ok(self.tables.read().values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>> {
        Ok(vec![])
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let table_name = plan.table.clone();

        let table = match &plan.engine {
            TableEngineType::Parquet => {
                ParquetTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            TableEngineType::Csv => {
                CsvTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            TableEngineType::Null => {
                NullTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            _ => {
                return Result::Err(ErrorCodes::UnImplement(
                    format!("Local database does not support {:?} table engine", plan.engine)
                ));
            }
        };

        self.tables.write().insert(table_name, Arc::from(table));
        Ok(())
    }
}
