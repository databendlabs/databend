// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_infallible::RwLock;
use common_planners::CreateTablePlan;

use crate::configs::Config;
use crate::datasources::remote::remote_table::RemoteTable;
use crate::datasources::{IDatabase, ITable, ITableFunction};
use crate::rpcs::store::StoreClient;

pub struct RemoteDatabase {
    name: String,
    conf: Config,
    tables: RwLock<HashMap<String, Arc<dyn ITable>>>,
}

impl RemoteDatabase {
    pub fn create(conf: Config, name: String) -> Self {
        RemoteDatabase {
            name,
            conf,
            tables: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait::async_trait]
impl IDatabase for RemoteDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn engine(&self) -> &str {
        "remote"
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

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        // Call remote create.
        let mut client = StoreClient::try_create(self.conf.store_api_address.clone()).await?;
        client.create_table(plan.clone()).await?;

        // Update cache.
        let table = RemoteTable::try_create(plan.db, plan.table, plan.schema, plan.options)?;
        let mut tables = self.tables.write();
        tables.insert(table.name().to_string(), Arc::from(table));
        Ok(())
    }
}
