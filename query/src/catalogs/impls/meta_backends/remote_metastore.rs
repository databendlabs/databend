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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow_flight::FlightData;
use common_cache::Cache;
use common_cache::LruCache;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::StoreClient;
use common_infallible::Mutex;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_runtime::Runtime;
use common_store_api::MetaApi;

use crate::catalogs::meta_backend::DatabaseInfo;
use crate::catalogs::meta_backend::MetaBackend;
use crate::catalogs::meta_backend::TableInfo;
use crate::datasources::store_client::StoreApis;
use crate::datasources::store_client::StoreApisProvider;

type CatalogTable = common_metatypes::Table;
type TableMetaCache = LruCache<(MetaId, MetaVersion), Arc<TableInfo>>;

#[derive(Clone)]
pub struct RemoteMeteStoreClient<T = StoreClient>
where T: 'static + StoreApis + Clone
{
    rt: Arc<Runtime>,
    rpc_time_out: Option<Duration>,
    table_meta_cache: Arc<Mutex<TableMetaCache>>,
    store_api_provider: StoreApisProvider<T>,
}

impl<T> RemoteMeteStoreClient<T>
where T: 'static + StoreApis + Clone
{
    pub fn create(apis_provider: StoreApisProvider<T>) -> RemoteMeteStoreClient<T> {
        Self::with_timeout_setting(apis_provider, Some(Duration::from_secs(5)))
    }

    pub fn with_timeout_setting(
        apis_provider: StoreApisProvider<T>,
        timeout: Option<Duration>,
    ) -> RemoteMeteStoreClient<T> {
        let rt = Runtime::with_worker_threads(1).expect("remote catalogs initialization failure");
        RemoteMeteStoreClient {
            rt: Arc::new(rt),
            // TODO configuration
            rpc_time_out: timeout,
            table_meta_cache: Arc::new(Mutex::new(LruCache::new(100))),
            store_api_provider: apis_provider,
        }
    }

    // Poor man's runtime::block_on
    fn do_block<F>(&self, f: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = channel();
        let _jh = self.rt.spawn(async move {
            let r = f.await;
            let _ = tx.send(r);
        });
        let reply = match self.rpc_time_out {
            Some(to) => rx
                .recv_timeout(to)
                .map_err(|timeout_err| ErrorCode::Timeout(timeout_err.to_string()))?,
            None => rx.recv().map_err(ErrorCode::from_std_error)?,
        };
        Ok(reply)
    }

    fn to_table_info(&self, db_name: &str, t_name: &str, tbl: &CatalogTable) -> Result<TableInfo> {
        let schema_bin = &tbl.schema;
        let t_id = tbl.table_id;
        let arrow_schema = ArrowSchema::try_from(&FlightData {
            data_header: schema_bin.clone(),
            ..Default::default()
        })?;
        let schema = DataSchema::from(arrow_schema);

        let info = TableInfo {
            db: db_name.to_owned(),
            table_id: t_id,
            name: t_name.to_owned(),
            schema: Arc::new(schema),
            table_option: tbl.table_options.clone(),
            engine: tbl.table_engine.clone(),
        };
        Ok(info)
    }
}

impl MetaBackend for RemoteMeteStoreClient {
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let cli_provider = self.store_api_provider.clone();
        let reply = {
            let tbl_name = table_name.to_string();
            let db_name = db_name.to_string();
            self.do_block(async move {
                let mut client = cli_provider.try_get_store_apis().await?;
                client.get_table(db_name, tbl_name).await
            })??
        };

        let table_info = TableInfo {
            db: reply.db,
            table_id: reply.table_id,
            name: reply.name.clone(),
            schema: reply.schema,
            engine: reply.engine,
            table_option: reply.options,
        };
        Ok(Arc::new(table_info))
    }

    fn exist_table(&self, db_name: &str, _table_name: &str) -> Result<bool> {
        let databases = self.get_databases()?;
        for database in databases {
            if database.name == db_name {
                return Ok(true);
            }
        }
        Ok(false)
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
        let reply = self.do_block(async move {
            let mut client = cli.try_get_store_apis().await?;
            client.get_table_ext(table_id, table_version).await
        })??;

        let res = TableInfo {
            db: db_name.to_owned(),
            table_id: reply.table_id,
            name: reply.name.clone(),
            schema: reply.schema.clone(),
            engine: reply.engine.clone(),
            table_option: reply.options.clone(),
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
            self.do_block(async move {
                let mut client = cli_provider.try_get_store_apis().await?;
                client.get_database(&db_name).await
            })??
        };

        let database_info = DatabaseInfo {
            name: db_name.to_owned(),
            engine: db.engine,
        };

        Ok(Arc::new(database_info))
    }

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        let cli_provider = self.store_api_provider.clone();
        let db = self.do_block(async move {
            let mut client = cli_provider.try_get_store_apis().await?;
            client.get_database_meta(None).await
        })??;

        match db {
            None => Ok(vec![]),
            Some(snapshot) => {
                let mut res = vec![];
                let db_metas = snapshot.db_metas;
                for (name, _database) in db_metas {
                    res.push(Arc::new(DatabaseInfo {
                        name,
                        // TODO keep engine info in meta store
                        engine: "default".to_string(),
                    }));
                }
                Ok(res)
            }
        }
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        let databases = self.get_databases()?;
        for database in databases {
            if database.name == db_name {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let cli = self.store_api_provider.clone();
        let reply = self.do_block(async move {
            let mut client = cli.try_get_store_apis().await?;
            // always take the latest snapshot
            client.get_database_meta(None).await
        })??;

        match reply {
            None => Ok(vec![]),
            Some(snapshot) => {
                let id_tbls = snapshot.tbl_metas.into_iter().collect::<HashMap<_, _>>();
                let dbs = snapshot.db_metas;
                let mut res = vec![];
                for (db, database) in dbs {
                    if db == db_name {
                        for (t_name, t_id) in database.tables {
                            let tbl = id_tbls.get(&t_id).ok_or_else(|| {
                                ErrorCode::IllegalMetaState(format!(
                                    "db meta inconsistent with table meta, table of id {}, not found",
                                    t_id
                                ))
                            })?;

                            let tbl_info = self.to_table_info(&db, &t_name, tbl)?;
                            res.push(Arc::new(tbl_info));
                        }
                    }
                }
                Ok(res)
            }
        }
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        // TODO validate plan by table engine first
        let cli = self.store_api_provider.clone();
        let _r = self.do_block(async move {
            let mut client = cli.try_get_store_apis().await?;
            client.create_table(plan).await
        })??;
        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let cli = self.store_api_provider.clone();
        let _r = self.do_block(async move {
            let mut client = cli.try_get_store_apis().await?;
            client.drop_table(plan.clone()).await
        })??;
        Ok(())
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let cli_provider = self.store_api_provider.clone();
        let _r = self.do_block(async move {
            let mut cli = cli_provider.try_get_store_apis().await?;
            cli.create_database(plan).await
        })?;
        Ok(())
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let cli_provider = self.store_api_provider.clone();
        let _r = self.do_block(async move {
            let mut cli = cli_provider.try_get_store_apis().await?;
            cli.drop_database(plan).await
        })?;
        Ok(())
    }

    fn name(&self) -> String {
        "remote metastore backend".to_owned()
    }
}
