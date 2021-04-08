// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_flights::StoreClient;
use common_infallible::RwLock;
use common_planners::CreateTablePlan;

use crate::configs::Config;
use crate::datasources::remote::remote_table::RemoteTable;
use crate::datasources::{IDatabase, ITable, ITableFunction};

pub struct RemoteDatabase {
    name: String,
    conf: Config,
    tables: RwLock<HashMap<String, Arc<dyn ITable>>>,
    store_client: RwLock<Option<StoreClient>>,
}

impl RemoteDatabase {
    pub fn create(conf: Config, name: String) -> Self {
        RemoteDatabase {
            name,
            conf,
            tables: RwLock::new(HashMap::default()),
            store_client: RwLock::new(None),
        }
    }

    async fn try_get_client(&self) -> Result<StoreClient> {
        if self.store_client.read().is_none() {
            let store_addr = self.conf.store_api_address.clone();
            let username = self.conf.store_api_username.clone();
            let password = self.conf.store_api_password.clone();
            let client = StoreClient::try_create(store_addr, username, password).await?;
            *self.store_client.write() = Some(client);
        }
        Ok(self.store_client.read().as_ref().unwrap().clone())
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
        let mut client = self.try_get_client().await?;
        client.create_table(plan.clone()).await?;

        // Update cache.
        let table = RemoteTable::try_create(plan.db, plan.table, plan.schema, plan.options)?;
        let mut tables = self.tables.write();
        tables.insert(table.name().to_string(), Arc::from(table));
        Ok(())
    }
}
