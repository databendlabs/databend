// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;
use common_store_api::MetaApi;

use crate::catalog::utils::InMemoryMetas;
use crate::catalog::utils::TableFunctionMeta;
use crate::catalog::utils::TableMeta;
use crate::datasources::remote::remote_table::RemoteTable;
use crate::datasources::remote::store_client_provider::StoreClientProvider;
use crate::datasources::Database;

pub struct RemoteDatabase {
    name: String,
    store_client_provider: StoreClientProvider,
    tables: RwLock<InMemoryMetas>,
}

impl RemoteDatabase {
    pub fn create(store_client_provider: StoreClientProvider, name: String) -> Self {
        RemoteDatabase {
            name,
            store_client_provider,
            tables: RwLock::new(InMemoryMetas::new()),
        }
    }
}

#[async_trait::async_trait]
impl Database for RemoteDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table(&self, _table_name: &str) -> Result<Arc<TableMeta>> {
        match self.tables.read().name2meta.get(_table_name) {
            Some(tbl) => Ok(tbl.clone()),
            None => Err(ErrorCode::UnknownTable(format!(
                "Unknown table: '{}'",
                _table_name
            ))),
        }
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        match self.tables.read().id2meta.get(&table_id) {
            Some(tbl) => Ok(tbl.clone()),
            None => Err(ErrorCode::UnknownTable(format!(
                "unknown table id {}",
                table_id
            ))),
        }
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        Ok(self.tables.read().name2meta.values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let table_name = plan.table.as_str();
        if self.tables.read().name2meta.get(table_name).is_some() {
            return if plan.if_not_exists {
                Ok(())
            } else {
                return Err(ErrorCode::UnImplement(format!(
                    "Table: '{}.{}' already exists.",
                    db_name, table_name
                )));
            };
        }

        // Call remote create.
        let clone = plan.clone();
        let provider = self.store_client_provider.clone();
        let table = RemoteTable::try_create(
            plan.db,
            plan.table,
            plan.schema,
            provider.clone(),
            plan.options,
        )?;
        let mut client = provider.try_get_client().await?;
        client.create_table(clone).await.map(|t| {
            let mut tables = self.tables.write();
            tables.insert(TableMeta::new(table.into(), t.table_id));
        })?;
        Ok(())
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let table_name = plan.table.as_str();
        if self.tables.read().name2meta.get(table_name).is_none() {
            return if plan.if_exists {
                Ok(())
            } else {
                Err(ErrorCode::UnknownTable(format!(
                    "Unknown table: '{}.{}'",
                    plan.db, plan.table
                )))
            };
        }

        // Call remote create.
        let mut client = self.store_client_provider.try_get_client().await?;
        client.drop_table(plan.clone()).await.map(|_| {
            let mut tables = self.tables.write();
            tables.remove(table_name);
        })?;
        Ok(())
    }
}
