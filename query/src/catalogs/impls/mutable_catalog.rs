//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::catalogs::backends::MetaBackend;
use crate::catalogs::catalog::Catalog;
use crate::catalogs::CatalogContext;
use crate::common::MetaClientProvider;
use crate::configs::Config;
use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::databases::DatabaseFactory;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::StorageFactory;
use crate::storages::Table;

/// Catalog based on MetaStore
/// - System Database NOT included
/// - Meta data of databases are saved in meta store
/// - Instances of `Database` are created by using database factories according to the engine
/// - Database engines are free to save table meta in metastore or not
#[derive(Clone)]
pub struct MutableCatalog {
    ctx: CatalogContext,
}

impl MutableCatalog {
    /// The component hierarchy is layered as:
    /// ```text
    /// Remote:
    ///
    ///                                        RPC
    /// MetaRemote -------> Meta server      Meta server      Meta Server
    ///                      raft <---------->   raft   <----------> raft
    ///                     MetaEmbedded     MetaEmbedded     MetaEmbedded
    ///
    /// Embedded:
    ///
    /// MetaEmbedded
    /// ```
    pub async fn try_create_with_config(conf: Config) -> Result<Self> {
        let local_mode = conf.meta.meta_address.is_empty();

        let meta: Arc<dyn MetaApi> = if local_mode {
            tracing::info!("use embedded meta");
            // TODO(xp): This can only be used for test: data will be removed when program quit.

            let meta_embedded = MetaEmbedded::get_meta().await?;
            meta_embedded
        } else {
            tracing::info!("use remote meta");

            let meta_client_provider =
                Arc::new(MetaClientProvider::new(conf.meta.to_grpc_client_config()));
            let meta_backend = MetaBackend::create(meta_client_provider);
            Arc::new(meta_backend)
        };

        let tenant = conf.query.tenant_id.clone();

        // Create default database.
        let req = CreateDatabaseReq {
            if_not_exists: true,
            tenant,
            db: "default".to_string(),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..Default::default()
            },
        };
        meta.create_database(req).await?;

        // Storage factory.
        let storage_factory = StorageFactory::create(conf.clone());

        // Database factory.
        let database_factory = DatabaseFactory::create(conf.clone());

        let ctx = CatalogContext {
            meta,
            storage_factory: Arc::new(storage_factory),
            database_factory: Arc::new(database_factory),
            in_memory_data: Arc::new(Default::default()),
        };
        Ok(MutableCatalog { ctx })
    }

    fn build_db_instance(&self, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let ctx = DatabaseContext {
            meta: self.ctx.meta.clone(),
            in_memory_data: self.ctx.in_memory_data.clone(),
        };
        self.ctx.database_factory.get_database(ctx, db_info)
    }
}

#[async_trait::async_trait]
impl Catalog for MutableCatalog {
    async fn get_database(&self, tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        let db_info = self
            .ctx
            .meta
            .get_database(GetDatabaseReq::new(tenant, db_name))
            .await?;
        self.build_db_instance(&db_info)
    }

    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        let dbs = self
            .ctx
            .meta
            .list_databases(ListDatabaseReq {
                tenant: tenant.to_string(),
            })
            .await?;

        dbs.iter().try_fold(vec![], |mut acc, item| {
            let db = self.build_db_instance(item)?;
            acc.push(db);
            Ok(acc)
        })
    }

    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        // Create database.
        let res = self.ctx.meta.create_database(req.clone()).await?;
        tracing::error!("db name: {}, engine: {}", &req.db, &req.meta.engine);

        // Initial the database after creating.
        let db_info = Arc::new(DatabaseInfo {
            database_id: res.database_id,
            db: req.db.clone(),
            meta: req.meta.clone(),
        });
        let database = self.build_db_instance(&db_info)?;
        database.init_database(&req.tenant).await?;
        Ok(CreateDatabaseReply {
            database_id: res.database_id,
        })
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<()> {
        self.ctx.meta.drop_database(req).await?;
        Ok(())
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = self.ctx.storage_factory.clone();
        let ctx = StorageContext {
            meta: self.ctx.meta.clone(),
            in_memory_data: self.ctx.in_memory_data.clone(),
        };
        storage.get_table(ctx, table_info)
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let res = self.ctx.meta.get_table_by_id(table_id).await?;
        Ok(res)
    }

    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let table_info = self
            .ctx
            .meta
            .get_table(GetTableReq::new(tenant, db_name, table_name))
            .await?;
        self.get_table_by_info(table_info.as_ref())
    }

    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(tenant, db_name))
            .await?;

        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.get_table_by_info(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<()> {
        self.ctx.meta.create_table(req).await?;
        Ok(())
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply> {
        let res = self.ctx.meta.drop_table(req).await?;
        Ok(res)
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let res = self.ctx.meta.upsert_table_option(req).await?;
        Ok(res)
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        self.ctx.storage_factory.get_storage_descriptors()
    }
}
