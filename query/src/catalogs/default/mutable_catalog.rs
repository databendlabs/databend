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
use common_meta_api::SchemaApi;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::MetaId;
use common_tracing::tracing;

use super::catalog_context::CatalogContext;
use crate::catalogs::catalog::Catalog;
use crate::common::MetaStoreProvider;
use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::databases::DatabaseFactory;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::StorageFactory;
use crate::storages::Table;
use crate::Config;

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
        let meta = {
            let provider = Arc::new(MetaStoreProvider::new(conf.meta.to_meta_grpc_client_conf()));
            let meta_backend = provider.try_get_meta_store().await?;
            meta_backend
        };

        let tenant = conf.query.tenant_id.clone();

        // Create default database.
        let req = CreateDatabaseReq {
            if_not_exists: true,
            name_ident: DatabaseNameIdent {
                tenant,
                db_name: "default".to_string(),
            },
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
            meta: self.ctx.meta.clone().arc(),
            in_memory_data: self.ctx.in_memory_data.clone(),
        };
        self.ctx.database_factory.get_database(ctx, db_info)
    }

    fn load_tables(&self, table_infos: Vec<Arc<TableInfo>>) -> Result<Vec<Arc<dyn Table>>> {
        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.get_table_by_info(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
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
        tracing::error!(
            "db name: {}, engine: {}",
            &req.name_ident.db_name,
            &req.meta.engine
        );

        // Initial the database after creating.
        let db_info = Arc::new(DatabaseInfo {
            ident: DatabaseIdent {
                db_id: res.db_id,
                seq: 0, // TODO
            },
            name_ident: req.name_ident.clone(),
            meta: req.meta.clone(),
        });
        let database = self.build_db_instance(&db_info)?;
        database.init_database(&req.name_ident.tenant).await?;
        Ok(CreateDatabaseReply { db_id: res.db_id })
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<()> {
        self.ctx.meta.drop_database(req).await?;
        Ok(())
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        let res = self.ctx.meta.rename_database(req).await?;
        Ok(res)
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = self.ctx.storage_factory.clone();
        let ctx = StorageContext {
            meta: self.ctx.meta.clone().arc(),
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

        self.load_tables(table_infos)
    }

    async fn list_tables_history(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        // `get_table_history` will not fetch the tables that created before the
        // "metasrv time travel functions" is added.
        // thus, only the table-infos of dropped tables are used.
        let mut dropped = self
            .ctx
            .meta
            .get_table_history(ListTableReq::new(tenant, db_name))
            .await?
            .into_iter()
            .filter(|i| i.meta.drop_on.is_some())
            .collect::<Vec<_>>();

        let mut table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(tenant, db_name))
            .await?;

        table_infos.append(&mut dropped);

        self.load_tables(table_infos)
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<()> {
        self.ctx.meta.create_table(req).await?;
        Ok(())
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply> {
        let res = self.ctx.meta.drop_table(req).await?;
        Ok(res)
    }

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        let res = self.ctx.meta.undrop_table(req).await?;
        Ok(res)
    }

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        let res = self.ctx.meta.undrop_database(req).await?;
        Ok(res)
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        let res = self.ctx.meta.rename_table(req).await?;
        Ok(res)
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let res = self.ctx.meta.upsert_table_option(req).await?;
        Ok(res)
    }

    async fn update_table_meta(&self, req: UpdateTableMetaReq) -> Result<UpdateTableMetaReply> {
        let res = self.ctx.meta.update_table_meta(req).await?;
        Ok(res)
    }

    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply> {
        let res = self.ctx.meta.count_tables(req).await?;
        Ok(res)
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        self.ctx.storage_factory.get_storage_descriptors()
    }
}
