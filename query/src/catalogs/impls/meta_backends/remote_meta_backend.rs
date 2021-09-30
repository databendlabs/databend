// Copyright 2020 Datafuse Labs.
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
//

use std::sync::Arc;
use std::time::Duration;

use common_base::Runtime;
use common_cache::Cache;
use common_cache::LruCache;
use common_exception::Result;
use common_infallible::Mutex;
use common_meta_api_vo::DatabaseInfo;
use common_meta_api_vo::TableInfo;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::meta_backend::MetaBackend;
use crate::common::StoreApiProvider;

type TableMetaCache = LruCache<(MetaId, MetaVersion), Arc<TableInfo>>;

#[derive(Clone)]
pub struct RemoteMeteStoreClient {
    rt: Arc<Runtime>,
    rpc_time_out: Option<Duration>,
    table_meta_cache: Arc<Mutex<TableMetaCache>>,
    store_api_provider: Arc<StoreApiProvider>,
}

impl RemoteMeteStoreClient {
    pub fn create(apis_provider: Arc<StoreApiProvider>) -> RemoteMeteStoreClient {
        Self::with_timeout_setting(apis_provider, Some(Duration::from_secs(5)))
    }

    pub fn with_timeout_setting(
        apis_provider: Arc<StoreApiProvider>,
        timeout: Option<Duration>,
    ) -> RemoteMeteStoreClient {
        let rt = Runtime::with_worker_threads(1).expect("remote catalogs initialization failure");
        RemoteMeteStoreClient {
            rt: Arc::new(rt),
            // TODO configuration
            rpc_time_out: timeout,
            table_meta_cache: Arc::new(Mutex::new(LruCache::new(100))),
            store_api_provider: apis_provider,
        }
    }
}

impl MetaBackend for RemoteMeteStoreClient {
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let cli_provider = self.store_api_provider.clone();
        let reply = {
            let tbl_name = table_name.to_string();
            let db_name = db_name.to_string();
            self.rt.block_on(
                async move {
                    let client = cli_provider.try_get_meta_client().await?;
                    client.get_table(db_name, tbl_name).await
                },
                self.rpc_time_out,
            )??
        };

        let table_info = TableInfo {
            db: reply.db,
            table_id: reply.table_id,
            name: reply.name.clone(),
            schema: reply.schema,
            engine: reply.engine,
            options: reply.options,
        };
        Ok(Arc::new(table_info))
    }

    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableInfo>> {
        if let Some(ver) = table_version {
            let mut cached = self.table_meta_cache.lock();
            if let Some(meta) = cached.get(&(table_id, ver)) {
                return Ok(meta.clone());
            }
        }

        let cli = self.store_api_provider.clone();
        let reply = self.rt.block_on(
            async move {
                let client = cli.try_get_meta_client().await?;
                client.get_table_by_id(table_id, table_version).await
            },
            self.rpc_time_out,
        )??;

        let res = TableInfo {
            db: db_name.to_owned(),
            table_id: reply.table_id,
            name: reply.name.clone(),
            schema: reply.schema.clone(),
            engine: reply.engine.clone(),
            options: reply.options.clone(),
        };

        let mut cache = self.table_meta_cache.lock();
        let res = Arc::new(res);
        // TODO version
        cache.put((reply.table_id, 0), res.clone());
        Ok(res)
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        let cli_provider = self.store_api_provider.clone();
        let db = {
            let db_name = db_name.to_owned();
            self.rt.block_on(
                async move {
                    let client = cli_provider.try_get_meta_client().await?;
                    client.get_database(&db_name).await
                },
                self.rpc_time_out,
            )??
        };

        let database_info = DatabaseInfo {
            database_id: db.database_id,
            db: db_name.to_owned(),
            engine: db.engine,
        };

        Ok(Arc::new(database_info))
    }

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        let cli_provider = self.store_api_provider.clone();
        let dbs = self.rt.block_on(
            async move {
                let client = cli_provider.try_get_meta_client().await?;
                client.get_databases().await
            },
            self.rpc_time_out,
        )??;
        Ok(dbs.into_iter().map(Arc::new).collect())
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let cli = self.store_api_provider.clone();
        let db_name = db_name.to_owned();
        let tbls = self.rt.block_on(
            async move {
                let client = cli.try_get_meta_client().await?;
                client.get_tables(db_name).await
            },
            self.rpc_time_out,
        )??;
        Ok(tbls.into_iter().map(Arc::new).collect())
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        // TODO validate plan by table engine first
        let cli = self.store_api_provider.clone();
        let _r = self.rt.block_on(
            async move {
                let client = cli.try_get_meta_client().await?;
                client.create_table(plan).await
            },
            self.rpc_time_out,
        )??;
        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let cli = self.store_api_provider.clone();
        let _r = self.rt.block_on(
            async move {
                let client = cli.try_get_meta_client().await?;
                client.drop_table(plan.clone()).await
            },
            self.rpc_time_out,
        )??;
        Ok(())
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let cli_provider = self.store_api_provider.clone();
        let _r = self.rt.block_on(
            async move {
                let cli = cli_provider.try_get_meta_client().await?;
                cli.create_database(plan).await
            },
            self.rpc_time_out,
        )??;
        Ok(())
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let cli_provider = self.store_api_provider.clone();
        let _r = self.rt.block_on(
            async move {
                let cli = cli_provider.try_get_meta_client().await?;
                cli.drop_database(plan).await
            },
            self.rpc_time_out,
        )??;
        Ok(())
    }

    fn name(&self) -> String {
        "remote metastore backend".to_owned()
    }
}
