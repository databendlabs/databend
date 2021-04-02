// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_infallible::RwLock;
use common_planners::CreateTablePlan;

use crate::configs::Config;
use crate::datasources::{IDatabase, ITable, ITableFunction};

pub struct RemoteDatabase {
    name: String,
    tables: RwLock<HashMap<String, Arc<dyn ITable>>>,
}

impl RemoteDatabase {
    pub fn create(_conf: Config, name: &str) -> Self {
        RemoteDatabase {
            name: name.to_string(),
            tables: RwLock::new(HashMap::default()),
        }
    }
}

impl IDatabase for RemoteDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn get_table(&self, _table_name: &str) -> Result<Arc<dyn ITable>> {
        bail!("RemoteDatabase get_table not yet implemented")
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>> {
        Ok(self.tables.read().values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>> {
        Ok(vec![])
    }

    fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        bail!("RemoteDatabase create_table not yet implemented")
    }
}
