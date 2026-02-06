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

use databend_common_ast::ast::Engine;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::ErrorCodeResultExt;
use databend_common_exception::Result;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::UDTFServer;
use databend_common_meta_app::schema::CatalogInfo;
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
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReply;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListSequencesReply;
use databend_common_meta_app::schema::ListSequencesReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReply;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_meta_app::schema::SwapTableReply;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_iceberg::IcebergMutableCatalog;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_types::MetaId;
use databend_meta_types::SeqV;
use log::info;

use crate::catalogs::default::ImmutableCatalog;
use crate::storages::Table;

#[derive(Debug)]
pub struct IcebergCreator;

impl CatalogCreator for IcebergCreator {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>> {
        let catalog_name = &info.name_ident.catalog_name;
        let res = IcebergCatalog {
            immutable_catalog: Arc::new(ImmutableCatalog::try_create_with_config(
                None,
                Some(catalog_name),
            )?),
            iceberg_catalog: Arc::new(IcebergMutableCatalog::try_create(info)?),
        };
        Ok(Arc::new(res))
    }
}

/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)
/// - metadata are written to the bottom layer
#[derive(Clone)]
pub struct IcebergCatalog {
    /// the upper layer, read only
    immutable_catalog: Arc<ImmutableCatalog>,
    /// bottom layer, writing goes here
    iceberg_catalog: Arc<IcebergMutableCatalog>,
}

impl Debug for IcebergCatalog {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("IcebergCatalog").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Catalog for IcebergCatalog {
    fn name(&self) -> String {
        self.iceberg_catalog.info().name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.iceberg_catalog.info().clone()
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        Ok(self)
    }

    #[async_backtrace::framed]
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let res = self
            .immutable_catalog
            .get_database(tenant, db_name)
            .await
            .or_unknown_database()?;
        if let Some(db) = res {
            return Ok(db);
        }
        self.iceberg_catalog.get_database(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.immutable_catalog.list_databases(tenant).await?;
        let mut other = self.iceberg_catalog.list_databases(tenant).await?;
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
        self.iceberg_catalog.create_database(req).await
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
        self.iceberg_catalog.drop_database(req).await
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, _req: DropIndexReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        unimplemented!()
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res = self
            .immutable_catalog
            .get_table_by_info(table_info)
            .or_unknown_table()?;
        if let Some(table) = res {
            return Ok(table);
        }
        self.iceberg_catalog.get_table_by_info(table_info)
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
        _get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in ICEBERG catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in ICEBERG catalog",
        ))
    }

    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        let res = self
            .immutable_catalog
            .mget_databases(tenant, db_names)
            .await
            .or_unknown_database()?;
        if let Some(tables) = res {
            if !tables.is_empty() {
                return Ok(tables);
            }
        }
        self.iceberg_catalog.mget_databases(tenant, db_names).await
    }

    #[async_backtrace::framed]
    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get dbs name by ids in ICEBERG catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in ICEBERG catalog",
        ))
    }

    fn support_partition(&self) -> bool {
        true
    }

    fn is_external(&self) -> bool {
        true
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        vec![]
    }

    fn default_table_engine(&self) -> Engine {
        Engine::Iceberg
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
            .await
            .or_unknown_database()?;
        if let Some(table) = res {
            return Ok(table);
        }
        self.iceberg_catalog
            .get_table(tenant, db_name, table_name)
            .await
    }

    async fn mget_tables(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        let res = self
            .immutable_catalog
            .mget_tables(tenant, db_name, table_names)
            .await
            .or_unknown_database()?;
        if let Some(tables) = res {
            if !tables.is_empty() {
                return Ok(tables);
            }
        }
        self.iceberg_catalog
            .mget_tables(tenant, db_name, table_names)
            .await
    }

    #[async_backtrace::framed]
    async fn get_table_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let res = self
            .immutable_catalog
            .list_tables(tenant, db_name)
            .await
            .or_unknown_database()?;
        if let Some(tables) = res {
            return Ok(tables);
        }
        self.iceberg_catalog.list_tables(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn list_tables_names(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<String>> {
        let res = self
            .immutable_catalog
            .list_tables_names(tenant, db_name)
            .await
            .or_unknown_database()?;
        if let Some(names) = res {
            return Ok(names);
        }
        self.iceberg_catalog
            .list_tables_names(tenant, db_name)
            .await
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
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
        self.iceberg_catalog.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let res = self.iceberg_catalog.drop_table_by_id(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        unimplemented!()
    }

    // Table index

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

        self.iceberg_catalog.rename_table(req).await
    }

    #[async_backtrace::framed]
    async fn swap_table(&self, _req: SwapTableReq) -> Result<SwapTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot swap table in iceberg catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        unimplemented!()
    }

    async fn set_table_row_access_policy(
        &self,
        _req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    // Virtual column

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(
        &self,
        _req: GetSequenceReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceReply> {
        unimplemented!()
    }
    async fn list_sequences(&self, _req: ListSequencesReq) -> Result<ListSequencesReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }

    /// Dictionary
    #[async_backtrace::framed]
    async fn create_dictionary(&self, _req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_dictionary(&self, _req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_dictionary(
        &self,
        _dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_dictionary(
        &self,
        _req: DictionaryNameIdent,
    ) -> Result<Option<GetDictionaryReply>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_dictionaries(
        &self,
        _req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        unimplemented!()
    }

    async fn rename_dictionary(&self, _req: RenameDictionaryReq) -> Result<()> {
        unimplemented!()
    }

    async fn get_autoincrement_next_value(
        &self,
        _req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply> {
        unimplemented!()
    }

    fn transform_udtf_as_table_function(
        &self,
        _ctx: &dyn TableContext,
        _table_args: &TableArgs,
        _udtf: UDTFServer,
        _func_name: &str,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }
}
