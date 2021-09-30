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
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow_flight::FlightData;
use common_base::Runtime;
use common_cache::Cache;
use common_cache::LruCache;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
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

type CatalogTable = common_metatypes::Table;
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
            options: tbl.table_options.clone(),
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
        let db = self.rt.block_on(
            async move {
                let client = cli_provider.try_get_meta_client().await?;
                client.get_database_meta(None).await
            },
            self.rpc_time_out,
        )??;

        match db {
            None => Ok(vec![]),
            Some(snapshot) => {
                let mut res = vec![];
                let db_metas = snapshot.db_metas;
                for (name, database) in db_metas {
                    res.push(Arc::new(DatabaseInfo {
                        database_id: database.database_id,
                        db: name,
                        engine: database.database_engine,
                    }));
                }
                Ok(res)
            }
        }
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let cli = self.store_api_provider.clone();
        let reply = self.rt.block_on(
            async move {
                let client = cli.try_get_meta_client().await?;
                // always take the latest snapshot
                client.get_database_meta(None).await
            },
            self.rpc_time_out,
        )??;

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
