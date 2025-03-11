// Copyright 2021 Datafuse Labs
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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReply;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetMarkedDeletedIndexesReply;
use databend_common_meta_app::schema::GetMarkedDeletedTableIndexesReply;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;
use databend_storages_common_session::SessionState;
use log::info;

use crate::catalogs::default::ImmutableCatalog;
use crate::catalogs::default::MutableCatalog;
use crate::catalogs::default::SessionCatalog;
use crate::storages::Table;
use crate::table_functions::TableFunctionFactory;
/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)
/// - metadata are written to the bottom layer
#[derive(Clone)]
pub struct DatabaseCatalog {
    /// the upper layer, read only
    immutable_catalog: Arc<ImmutableCatalog>,
    /// bottom layer, writing goes here
    mutable_catalog: Arc<SessionCatalog>,
    /// table function engine factories
    table_function_factory: Arc<TableFunctionFactory>,
}

impl Debug for DatabaseCatalog {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("DefaultCatalog").finish_non_exhaustive()
    }
}

impl DatabaseCatalog {
    #[async_backtrace::framed]
    pub async fn try_create_with_config(conf: InnerConfig) -> Result<DatabaseCatalog> {
        let immutable_catalog = ImmutableCatalog::try_create_with_config(&conf).await?;
        let mutable_catalog = MutableCatalog::try_create_with_config(conf).await?;
        let session_catalog = SessionCatalog::create(mutable_catalog, SessionState::default());
        let table_function_factory = TableFunctionFactory::create();
        let res = DatabaseCatalog {
            immutable_catalog: Arc::new(immutable_catalog),
            mutable_catalog: Arc::new(session_catalog),
            table_function_factory: Arc::new(table_function_factory),
        };
        Ok(res)
    }
}

#[async_trait::async_trait]
impl Catalog for DatabaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        "default".to_string()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        Arc::default()
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        let mut me = self.as_ref().clone();
        let mut session_catalog = me.mutable_catalog.as_ref().clone();
        session_catalog.disable_table_info_refresh();
        me.mutable_catalog = Arc::new(session_catalog);
        Ok(Arc::new(me))
    }

    #[async_backtrace::framed]
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let r = self.immutable_catalog.get_database(tenant, db_name).await;
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog.get_database(tenant, db_name).await
                } else {
                    Err(e)
                }
            }
            Ok(db) => Ok(db),
        }
    }

    #[async_backtrace::framed]
    async fn list_databases_history(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self
            .immutable_catalog
            .list_databases_history(tenant)
            .await?;
        let mut other = self.mutable_catalog.list_databases_history(tenant).await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.immutable_catalog.list_databases(tenant).await?;
        let mut other = self.mutable_catalog.list_databases(tenant).await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        info!("Create database from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.name_ident.tenant(), req.name_ident.database_name())
            .await?
        {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                req.name_ident.database_name()
            )));
        }
        // create db in BOTTOM layer only
        self.mutable_catalog.create_database(req).await
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        info!("Drop database from req:{:?}", req);

        // drop db in BOTTOM layer only
        if self
            .immutable_catalog
            .exists_database(req.name_ident.tenant(), req.name_ident.database_name())
            .await?
        {
            return self.immutable_catalog.drop_database(req).await;
        }
        self.mutable_catalog.drop_database(req).await
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        info!("Rename table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.name_ident.tenant(), req.name_ident.database_name())
            .await?
            || self
                .immutable_catalog
                .exists_database(req.name_ident.tenant(), &req.new_db_name)
                .await?
        {
            return self.immutable_catalog.rename_database(req).await;
        }

        self.mutable_catalog.rename_database(req).await
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res = self.immutable_catalog.get_table_by_info(table_info);
        match res {
            Ok(t) => Ok(t),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_TABLE {
                    self.mutable_catalog.get_table_by_info(table_info)
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        let res = self.immutable_catalog.get_table_meta_by_id(table_id).await;

        if let Ok(x) = res {
            Ok(x)
        } else {
            self.mutable_catalog.get_table_meta_by_id(table_id).await
        }
    }

    #[async_backtrace::framed]
    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
        get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        let sys_table_names = self
            .immutable_catalog
            .mget_table_names_by_ids(tenant, table_ids, get_dropped_table)
            .await?;
        let mut_table_names = self
            .mutable_catalog
            .mget_table_names_by_ids(tenant, table_ids, get_dropped_table)
            .await?;

        let mut table_names = Vec::with_capacity(table_ids.len());
        for (mut_table_name, sys_table_name) in
            mut_table_names.into_iter().zip(sys_table_names.into_iter())
        {
            if mut_table_name.is_some() {
                table_names.push(mut_table_name);
            } else if sys_table_name.is_some() {
                table_names.push(sys_table_name);
            } else {
                table_names.push(None);
            }
        }
        Ok(table_names)
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>> {
        let res = self.immutable_catalog.get_table_name_by_id(table_id).await;

        match res {
            Ok(Some(x)) => Ok(Some(x)),
            Ok(None) | Err(_) => self.mutable_catalog.get_table_name_by_id(table_id).await,
        }
    }

    #[async_backtrace::framed]
    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String> {
        let res = self.immutable_catalog.get_db_name_by_id(db_id).await;

        if let Ok(x) = res {
            Ok(x)
        } else {
            self.mutable_catalog.get_db_name_by_id(db_id).await
        }
    }

    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        let sys_dbs = self.immutable_catalog.list_databases(tenant).await?;
        let sys_db_names: Vec<_> = sys_dbs
            .iter()
            .map(|sys_db| sys_db.get_db_info().name_ident.database_name())
            .collect();

        let mut mut_db_names: Vec<_> = Vec::new();
        for db_name in db_names {
            if !sys_db_names.contains(&db_name.database_name()) {
                mut_db_names.push(db_name.clone());
            }
        }

        let mut dbs = self
            .immutable_catalog
            .mget_databases(tenant, db_names)
            .await?;

        let other = self
            .mutable_catalog
            .mget_databases(tenant, &mut_db_names)
            .await?;

        dbs.extend(other);
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        let sys_db_names = self
            .immutable_catalog
            .mget_database_names_by_ids(tenant, db_ids)
            .await?;
        let mut_db_names = self
            .mutable_catalog
            .mget_database_names_by_ids(tenant, db_ids)
            .await?;

        let mut db_names = Vec::with_capacity(db_ids.len());
        for (mut_db_name, sys_db_name) in mut_db_names.into_iter().zip(sys_db_names.into_iter()) {
            if mut_db_name.is_some() {
                db_names.push(mut_db_name);
            } else if sys_db_name.is_some() {
                db_names.push(sys_db_name);
            } else {
                db_names.push(None);
            }
        }
        Ok(db_names)
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let res = self
            .immutable_catalog
            .get_table(tenant, db_name, table_name)
            .await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog
                        .get_table(tenant, db_name, table_name)
                        .await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn get_table_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        let res = self
            .immutable_catalog
            .get_table_history(tenant, db_name, table_name)
            .await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog
                        .get_table_history(tenant, db_name, table_name)
                        .await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let r = self.immutable_catalog.list_tables(tenant, db_name).await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog.list_tables(tenant, db_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    fn list_temporary_tables(&self) -> Result<Vec<TableInfo>> {
        self.mutable_catalog.list_temporary_tables()
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        let r = self
            .immutable_catalog
            .list_tables_history(tenant, db_name)
            .await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog
                        .list_tables_history(tenant, db_name)
                        .await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        info!("Create table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.create_table(req).await;
        }
        self.mutable_catalog.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let res = self.mutable_catalog.drop_table_by_id(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<()> {
        info!("Undrop table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.undrop_table(req).await;
        }
        self.mutable_catalog.undrop_table(req).await
    }

    #[async_backtrace::framed]
    async fn undrop_table_by_id(&self, req: UndropTableByIdReq) -> Result<()> {
        info!("Undrop table by id from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?
        {
            return self.immutable_catalog.undrop_table_by_id(req).await;
        }
        self.mutable_catalog.undrop_table_by_id(req).await
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        info!("Undrop database from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.undrop_database(req).await;
        }
        self.mutable_catalog.undrop_database(req).await
    }

    async fn commit_table_meta(&self, req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        info!("commit_table_meta from req:{:?}", req);

        self.mutable_catalog.commit_table_meta(req).await
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        info!("Rename table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
            || self
                .immutable_catalog
                .exists_database(req.tenant(), &req.new_db_name)
                .await?
        {
            return Err(ErrorCode::Unimplemented(
                "Cannot rename table from(to) system databases",
            ));
        }

        self.mutable_catalog.rename_table(req).await
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<()> {
        self.mutable_catalog.create_table_index(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<()> {
        self.mutable_catalog.drop_table_index(req).await
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        self.mutable_catalog
            .get_table_copied_file_info(tenant, db_name, req)
            .await
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        self.mutable_catalog.truncate_table(table_info, req).await
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        self.mutable_catalog
            .upsert_table_option(tenant, db_name, req)
            .await
    }

    #[async_backtrace::framed]
    async fn retryable_update_multi_table_meta(
        &self,
        reqs: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult> {
        self.mutable_catalog
            .retryable_update_multi_table_meta(reqs)
            .await
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        self.mutable_catalog.set_table_column_mask_policy(req).await
    }

    // Table index

    #[async_backtrace::framed]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        self.mutable_catalog.create_index(req).await
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, req: DropIndexReq) -> Result<()> {
        self.mutable_catalog.drop_index(req).await
    }

    #[async_backtrace::framed]
    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        self.mutable_catalog.get_index(req).await
    }

    #[async_backtrace::framed]
    async fn list_marked_deleted_indexes(
        &self,
        tenant: &Tenant,
        table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedIndexesReply> {
        self.mutable_catalog
            .list_marked_deleted_indexes(tenant, table_id)
            .await
    }

    #[async_backtrace::framed]
    async fn list_marked_deleted_table_indexes(
        &self,
        tenant: &Tenant,
        table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedTableIndexesReply> {
        self.mutable_catalog
            .list_marked_deleted_table_indexes(tenant, table_id)
            .await
    }

    #[async_backtrace::framed]
    async fn remove_marked_deleted_index_ids(
        &self,
        tenant: &Tenant,
        table_id: u64,
        index_ids: &[u64],
    ) -> Result<()> {
        self.mutable_catalog
            .remove_marked_deleted_index_ids(tenant, table_id, index_ids)
            .await
    }

    #[async_backtrace::framed]
    async fn remove_marked_deleted_table_indexes(
        &self,
        tenant: &Tenant,
        table_id: u64,
        indexes: &[(String, String)],
    ) -> Result<()> {
        self.mutable_catalog
            .remove_marked_deleted_table_indexes(tenant, table_id, indexes)
            .await
    }

    #[async_backtrace::framed]
    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        self.mutable_catalog.update_index(req).await
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        self.mutable_catalog.list_indexes(req).await
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        self.mutable_catalog.list_index_ids_by_table_id(req).await
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        self.mutable_catalog.list_indexes_by_table_id(req).await
    }

    // Virtual column

    #[async_backtrace::framed]
    async fn create_virtual_column(&self, req: CreateVirtualColumnReq) -> Result<()> {
        self.mutable_catalog.create_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(&self, req: UpdateVirtualColumnReq) -> Result<()> {
        self.mutable_catalog.update_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(&self, req: DropVirtualColumnReq) -> Result<()> {
        self.mutable_catalog.drop_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        self.mutable_catalog.list_virtual_columns(req).await
    }

    fn get_table_function(
        &self,
        func_name: &str,
        tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        self.table_function_factory.get(func_name, tbl_args)
    }

    fn exists_table_function(&self, func_name: &str) -> bool {
        self.table_function_factory.exists(func_name)
    }

    fn list_table_functions(&self) -> Vec<String> {
        self.table_function_factory.list()
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        // only return mutable_catalog storage table engines
        self.mutable_catalog.get_table_engines()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        self.mutable_catalog.list_lock_revisions(req).await
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        self.mutable_catalog.create_lock_revision(req).await
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        self.mutable_catalog.extend_lock_revision(req).await
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        self.mutable_catalog.delete_lock_revision(req).await
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>> {
        self.mutable_catalog.list_locks(req).await
    }

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        self.mutable_catalog.get_drop_table_infos(req).await
    }

    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<()> {
        self.mutable_catalog.gc_drop_tables(req).await
    }

    async fn create_sequence(&self, req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        self.mutable_catalog.create_sequence(req).await
    }
    async fn get_sequence(&self, req: GetSequenceReq) -> Result<GetSequenceReply> {
        self.mutable_catalog.get_sequence(req).await
    }

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        self.mutable_catalog.get_sequence_next_value(req).await
    }

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply> {
        self.mutable_catalog.drop_sequence(req).await
    }

    fn set_session_state(&self, state: SessionState) -> Arc<dyn Catalog> {
        Arc::new(DatabaseCatalog {
            mutable_catalog: Arc::new(SessionCatalog::create(self.mutable_catalog.inner(), state)),
            immutable_catalog: self.immutable_catalog.clone(),
            table_function_factory: self.table_function_factory.clone(),
        })
    }

    fn get_stream_source_table(
        &self,
        stream_desc: &str,
        max_batch_size: Option<u64>,
    ) -> Result<Option<Arc<dyn Table>>> {
        self.mutable_catalog
            .get_stream_source_table(stream_desc, max_batch_size)
    }

    fn cache_stream_source_table(
        &self,
        stream: TableInfo,
        source: TableInfo,
        max_batch_size: Option<u64>,
    ) {
        self.mutable_catalog
            .cache_stream_source_table(stream, source, max_batch_size)
    }

    /// Dictionary
    #[async_backtrace::framed]
    async fn create_dictionary(&self, req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        self.mutable_catalog.create_dictionary(req).await
    }

    #[async_backtrace::framed]
    async fn update_dictionary(&self, req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        self.mutable_catalog.update_dictionary(req).await
    }

    #[async_backtrace::framed]
    async fn drop_dictionary(
        &self,
        dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        self.mutable_catalog.drop_dictionary(dict_ident).await
    }

    #[async_backtrace::framed]
    async fn get_dictionary(&self, req: DictionaryNameIdent) -> Result<Option<GetDictionaryReply>> {
        self.mutable_catalog.get_dictionary(req).await
    }

    #[async_backtrace::framed]
    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        self.mutable_catalog.list_dictionaries(req).await
    }

    async fn set_table_lvt(
        &self,
        name_ident: &LeastVisibleTimeIdent,
        value: &LeastVisibleTime,
    ) -> Result<LeastVisibleTime> {
        self.mutable_catalog.set_table_lvt(name_ident, value).await
    }

    async fn rename_dictionary(&self, req: RenameDictionaryReq) -> Result<()> {
        self.mutable_catalog.rename_dictionary(req).await
    }
}
