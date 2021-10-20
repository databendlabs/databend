// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::LruCache;
use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableInfo;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

type TableInfoCache = LruCache<(MetaId, MetaVersion), Arc<TableInfo>>;

/// A `MetaApi` impl with table cached in memory, backed with another `MetaApi`.
#[derive(Clone)]
pub struct MetaCached {
    table_meta_cache: Arc<RwLock<TableInfoCache>>,
    pub inner: Arc<dyn MetaApi>,
}

impl MetaCached {
    pub fn create(inner: Arc<dyn MetaApi>) -> MetaCached {
        MetaCached {
            table_meta_cache: Arc::new(RwLock::new(LruCache::new(100))),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl MetaApi for MetaCached {
    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        self.inner.create_database(plan).await
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.inner.drop_database(plan).await
    }

    async fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        self.inner.get_database(db_name).await
    }

    async fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        self.inner.get_databases().await
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        // TODO validate plan by table engine first
        self.inner.create_table(plan).await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.inner.drop_table(plan).await
    }

    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        self.inner.get_table(db_name, table_name).await
    }

    async fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        self.inner.get_tables(db_name).await
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
        version: Option<MetaVersion>,
    ) -> Result<Arc<TableInfo>> {
        if let Some(ver) = version {
            let mut cached = self.table_meta_cache.write().await;
            if let Some(meta) = cached.get(&(table_id, ver)) {
                return Ok(meta.clone());
            }
        }

        let reply = self.inner.get_table_by_id(table_id, version).await?;

        let mut cache = self.table_meta_cache.write().await;
        // TODO version
        cache.put((reply.table_id, 0), reply.clone());
        Ok(reply)
    }

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        option_key: String,
        option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        self.inner
            .upsert_table_option(table_id, table_version, option_key, option_value)
            .await
    }

    fn name(&self) -> String {
        format!("meta-cached({})", self.inner.name())
    }
}
