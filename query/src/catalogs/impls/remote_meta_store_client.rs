// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::sync::mpsc::channel;
use std::time::Duration;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow_flight::FlightData;
use common_datavalues::prelude::Arc;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::StoreClient;
use common_infallible::Mutex;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;
use common_planners::TableOptions;
use common_runtime::Runtime;
use lru::LruCache;

use crate::catalogs::meta_store_client::DBMetaStoreClient;
use crate::catalogs::utils::TableMeta;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::remote::RemoteTable;
use crate::datasources::remote::StoreApis;
use crate::datasources::remote::StoreApisProvider;
use crate::datasources::Database;

type CatalogTable = common_metatypes::Table;
type TableMetaCache = LruCache<(MetaId, MetaVersion), Arc<TableMeta>>;
#[derive(Clone)]
pub struct RemoteMetaStoreClient<T = StoreClient>
where T: 'static + StoreApis + Clone
{
    rt: Arc<Runtime>,
    rpc_time_out: Duration,
    table_meta_cache: Arc<Mutex<TableMetaCache>>,
    store_api_provider: StoreApisProvider<T>,
}

impl<T> RemoteMetaStoreClient<T>
where T: 'static + StoreApis + Clone
{
    pub fn create(apis_provider: StoreApisProvider<T>) -> RemoteMetaStoreClient<T> {
        let rt = Runtime::with_worker_threads(1).expect("remote catalogs initialization failure");
        RemoteMetaStoreClient {
            rt: Arc::new(rt),
            // TODO configuration
            rpc_time_out: Duration::from_secs(5),
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
        let time_out = self.rpc_time_out;
        let (tx, rx) = channel();
        let _jh = self.rt.spawn(async move {
            let r = f.await;
            let _ = tx.send(r);
        });
        let reply = rx
            .recv_timeout(time_out)
            .map_err(|chan_err| ErrorCode::Timeout(chan_err.to_string()))?;
        Ok(reply)
    }

    fn to_table_meta(&self, db_name: &str, t_name: &str, tbl: &CatalogTable) -> Result<TableMeta> {
        let schema_bin = &tbl.schema;
        let t_id = tbl.table_id;
        let arrow_schema = ArrowSchema::try_from(&FlightData {
            data_header: schema_bin.clone(),
            ..Default::default()
        })?;
        let schema = DataSchema::from(arrow_schema);
        let remote_table = RemoteTable::create(
            db_name,
            t_name,
            Arc::new(schema),
            //self.store_client_provider.clone(),
            self.store_api_provider.clone(),
            TableOptions::new(),
        );
        let tbl_meta = TableMeta::new(remote_table.into(), t_id);
        Ok(tbl_meta)
    }
}

#[async_trait::async_trait]
impl<T> DBMetaStoreClient for RemoteMetaStoreClient<T>
where T: 'static + StoreApis + Clone
{
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let cli_provider = self.store_api_provider.clone();
        let db = {
            let db_name = db_name.to_owned();
            self.do_block(async move {
                let mut client = cli_provider.try_get_store_apis().await?;
                client.get_database(&db_name).await
            })??
        };

        Ok(Arc::new(RemoteDatabase::create_new(
            db.database_id,
            db_name,
            Arc::new(self.clone()),
        )))
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        let cli_provider = self.store_api_provider.clone();
        let db = self.do_block(async move {
            let mut client = cli_provider.try_get_store_apis().await?;
            client.get_database_meta(None).await
        })??;
        db.map_or_else(
            || Ok(vec![]),
            |snapshot| {
                let res = snapshot
                    .db_metas
                    .iter()
                    .map(|(n, _)| n.to_string())
                    .collect::<Vec<String>>();
                Ok(res)
            },
        )
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let cli_provider = self.store_api_provider.clone();
        let reply = {
            let tbl_name = table_name.to_string();
            let db_name = db_name.to_string();
            self.do_block(async move {
                let mut client = cli_provider.try_get_store_apis().await?;
                client.get_table(db_name, tbl_name).await
            })??
        };
        let tbl = RemoteTable::create(
            db_name,
            table_name,
            reply.schema,
            //self.store_client_provider.clone(),
            self.store_api_provider.clone(),
            TableOptions::new(),
        );
        let tbl_meta = TableMeta::new(tbl.into(), reply.table_id);
        Ok(Arc::new(tbl_meta))
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        let cli = self.store_api_provider.clone();
        let db = self.do_block(async move {
            let mut client = cli.try_get_store_apis().await?;
            // always take the latest snapshot
            client.get_database_meta(None).await
        })??;

        match db {
            None => Ok(vec![]),
            Some(snapshot) => {
                let id_tbls = snapshot.tbl_metas.into_iter().collect::<HashMap<_, _>>();
                let dbs = snapshot.db_metas;
                let mut res: Vec<(String, Arc<TableMeta>)> = vec![];
                for (db_name, db) in dbs {
                    for (t_name, t_id) in db.tables {
                        let tbl = id_tbls.get(&t_id).ok_or_else(|| {
                            ErrorCode::IllegalMetaState(format!(
                                "db meta inconsistent with table meta, table of id {}, not found",
                                t_id
                            ))
                        })?;
                        let tbl_meta = self.to_table_meta(&db_name, &t_name, tbl)?;
                        let r = (db_name.clone(), Arc::new(tbl_meta));
                        res.push(r);
                    }
                }
                Ok(res)
            }
        }
    }

    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
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
        let tbl = RemoteTable::create(
            db_name,
            reply.name,
            reply.schema,
            //self.store_client_provider.clone(),
            self.store_api_provider.clone(),
            TableOptions::new(),
        );
        let tbl_meta = TableMeta::new(tbl.into(), reply.table_id);
        let meta_id = tbl_meta.meta_id();
        let mut cache = self.table_meta_cache.lock();
        let res = Arc::new(tbl_meta);
        // TODO version
        cache.put((meta_id, 0), res.clone());
        Ok(res)
    }

    fn get_db_tables(&self, db_name: &str) -> Result<Vec<Arc<TableMeta>>> {
        let all_tables = self.get_all_tables()?;
        Ok(all_tables
            .into_iter()
            .filter(|item| item.0 == db_name)
            .map(|item| item.1)
            .collect::<Vec<_>>())
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let mut client = self.store_api_provider.try_get_store_apis().await?;
        let _reply = client.create_table(plan).await?;
        Ok(())
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let mut cli = self.store_api_provider.try_get_store_apis().await?;
        cli.drop_table(plan.clone()).await?;
        Ok(())
    }
}
