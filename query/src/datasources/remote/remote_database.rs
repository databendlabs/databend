// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::meta_store_client::DBMetaStoreClient;
use crate::catalogs::utils::TableFunctionMeta;
use crate::catalogs::utils::TableMeta;
use crate::datasources::Database;

pub struct RemoteDatabase {
    _id: MetaId,
    name: String,
    meta_client: Arc<dyn DBMetaStoreClient>,
}

impl RemoteDatabase {
    pub fn create_new(
        id: MetaId,
        name: impl Into<String>,
        cli: Arc<dyn DBMetaStoreClient>,
    ) -> Self {
        Self {
            _id: id,
            name: name.into(),
            meta_client: cli,
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

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        self.meta_client.get_table(&self.name, table_name)
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        self.meta_client
            .get_table_by_id(&self.name, table_id, table_version)
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        self.meta_client.get_db_tables(&self.name)
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        self.meta_client.create_table(plan).await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.meta_client.drop_table(plan).await
    }
}
