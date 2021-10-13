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

use common_base::BlockingWait;
use common_base::Runtime;
use common_cache::Cache;
use common_cache::LruCache;
use common_exception::Result;
use common_infallible::Mutex;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableInfo;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::backends::CatalogBackend;
use crate::common::MetaClientProvider;

type TableInfoCache = LruCache<(MetaId, MetaVersion), Arc<TableInfo>>;

#[derive(Clone)]
pub struct RemoteCatalogBackend {
    rt: Arc<Runtime>,
    rpc_time_out: Option<Duration>,
    table_meta_cache: Arc<Mutex<TableInfoCache>>,
    store_api_provider: Arc<MetaClientProvider>,
}

impl RemoteCatalogBackend {
    pub fn create(apis_provider: Arc<MetaClientProvider>) -> RemoteCatalogBackend {
        Self::with_timeout_setting(apis_provider, Some(Duration::from_secs(5)))
    }

    pub fn with_timeout_setting(
        apis_provider: Arc<MetaClientProvider>,
        timeout: Option<Duration>,
    ) -> RemoteCatalogBackend {
        let rt = Runtime::with_worker_threads(1).expect("remote catalogs initialization failure");
        RemoteCatalogBackend {
            rt: Arc::new(rt),
            // TODO configuration
            rpc_time_out: timeout,
            table_meta_cache: Arc::new(Mutex::new(LruCache::new(100))),
            store_api_provider: apis_provider,
        }
    }
}

impl CatalogBackend for RemoteCatalogBackend {
    fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        let cli_provider = self.store_api_provider.clone();
        let create_database = async move {
            let cli = cli_provider.try_get_meta_client().await?;
            cli.create_database(plan).await
        };
        let res = create_database.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(res)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let cli_provider = self.store_api_provider.clone();
        let drop_database = async move {
            let cli = cli_provider.try_get_meta_client().await?;
            cli.drop_database(plan).await
        };
        let _res = drop_database.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(())
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        let cli_provider = self.store_api_provider.clone();
        let db = {
            let db_name = db_name.to_owned();
            let get_database = async move {
                let client = cli_provider.try_get_meta_client().await?;
                client.get_database(&db_name).await
            };
            get_database.wait_in(&self.rt, self.rpc_time_out)??
        };

        let database_info = DatabaseInfo {
            database_id: db.database_id,
            db: db_name.to_owned(),
            engine: db.engine.clone(),
        };

        Ok(Arc::new(database_info))
    }

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        let cli_provider = self.store_api_provider.clone();
        let get_databases = async move {
            let client = cli_provider.try_get_meta_client().await?;
            client.get_databases().await
        };
        let res = get_databases.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(res)
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        // TODO validate plan by table engine first
        let cli = self.store_api_provider.clone();
        let create_table = async move {
            let client = cli.try_get_meta_client().await?;
            client.create_table(plan).await
        };
        let res = create_table.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(res)
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let cli = self.store_api_provider.clone();
        let drop_table = async move {
            let client = cli.try_get_meta_client().await?;
            client.drop_table(plan.clone()).await
        };
        let _res = drop_table.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let cli_provider = self.store_api_provider.clone();
        let reply = {
            let tbl_name = table_name.to_string();
            let db_name = db_name.to_string();
            let get_table = async move {
                let client = cli_provider.try_get_meta_client().await?;
                client.get_table(&db_name, &tbl_name).await
            };
            get_table.wait_in(&self.rt, self.rpc_time_out)??
        };

        let table_info = TableInfo {
            database_id: 0,
            db: reply.db.clone(),
            table_id: reply.table_id,
            version: 0,
            name: reply.name.clone(),
            schema: reply.schema.clone(),
            engine: reply.engine.clone(),
            options: reply.options.clone(),
        };
        Ok(Arc::new(table_info))
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let cli = self.store_api_provider.clone();
        let db_name = db_name.to_owned();
        let get_tables = async move {
            let client = cli.try_get_meta_client().await?;
            client.get_tables(&db_name).await
        };
        let res = get_tables.wait_in(&self.rt, self.rpc_time_out)??;
        Ok(res)
    }

    fn get_table_by_id(
        &self,
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
        let get_table_by_id = async move {
            let client = cli.try_get_meta_client().await?;
            client.get_table_by_id(table_id, table_version).await
        };
        let reply = get_table_by_id.wait_in(&self.rt, self.rpc_time_out)??;
        let res = TableInfo {
            db: reply.db.clone(),
            database_id: 0,
            table_id: reply.table_id,
            version: 0,
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

    fn name(&self) -> String {
        "remote metastore backend".to_owned()
    }
}
