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
use databend_common_catalog::table_function::TableFunction;
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
use databend_common_meta_app::schema::DatabaseId;
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
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_paimon::PaimonCatalog as PaimonStorageCatalog;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_client::types::MetaId;
use databend_meta_client::types::SeqV;

use crate::catalogs::default::ImmutableCatalog;
use crate::sessions::TableContext;
use crate::storages::Table;

#[derive(Debug)]
pub struct PaimonCreator;

impl CatalogCreator for PaimonCreator {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>> {
        let catalog_name = &info.name_ident.catalog_name;
        let res = PaimonCatalog {
            immutable_catalog: Arc::new(ImmutableCatalog::try_create_with_config(
                None,
                Some(catalog_name),
            )?),
            paimon_catalog: Arc::new(PaimonStorageCatalog::try_create(info)?),
        };
        Ok(Arc::new(res))
    }
}

#[derive(Clone)]
pub struct PaimonCatalog {
    immutable_catalog: Arc<ImmutableCatalog>,
    paimon_catalog: Arc<PaimonStorageCatalog>,
}

impl Debug for PaimonCatalog {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("PaimonCatalog").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Catalog for PaimonCatalog {
    fn name(&self) -> String {
        self.paimon_catalog.info().name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.paimon_catalog.info()
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
        self.paimon_catalog.get_database(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn exists_database(&self, tenant: &Tenant, db_name: &str) -> Result<bool> {
        if self
            .immutable_catalog
            .exists_database(tenant, db_name)
            .await?
        {
            return Ok(true);
        }
        self.paimon_catalog.exists_database(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn list_databases_history(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self
            .immutable_catalog
            .list_databases_history(tenant)
            .await?;
        let mut other = self.paimon_catalog.list_databases_history(tenant).await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.immutable_catalog.list_databases(tenant).await?;
        let mut other = self.paimon_catalog.list_databases(tenant).await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        if self
            .immutable_catalog
            .exists_database(req.name_ident.tenant(), req.name_ident.database_name())
            .await?
        {
            return Ok(CreateDatabaseReply {
                db_id: DatabaseId::new(0),
                created: false,
            });
        }
        self.paimon_catalog.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        if self
            .immutable_catalog
            .exists_database(req.name_ident.tenant(), req.name_ident.database_name())
            .await?
        {
            return self.immutable_catalog.drop_database(req).await;
        }
        self.paimon_catalog.drop_database(req).await
    }

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        self.paimon_catalog.undrop_database(req).await
    }

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        self.paimon_catalog.create_index(req).await
    }

    async fn drop_index(&self, req: DropIndexReq) -> Result<()> {
        self.paimon_catalog.drop_index(req).await
    }

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        self.paimon_catalog.get_index(req).await
    }

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        self.paimon_catalog.update_index(req).await
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        self.paimon_catalog.rename_database(req).await
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res = self
            .immutable_catalog
            .get_table_by_info(table_info)
            .or_unknown_table()?;
        if let Some(table) = res {
            return Ok(table);
        }
        self.paimon_catalog.get_table_by_info(table_info)
    }

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        self.paimon_catalog.get_table_meta_by_id(table_id).await
    }

    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
        get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        self.paimon_catalog
            .mget_table_names_by_ids(tenant, table_ids, get_dropped_table)
            .await
    }

    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String> {
        self.paimon_catalog.get_db_name_by_id(db_id).await
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
        if let Some(dbs) = res {
            if !dbs.is_empty() {
                return Ok(dbs);
            }
        }
        self.paimon_catalog.mget_databases(tenant, db_names).await
    }

    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        self.paimon_catalog
            .mget_database_names_by_ids(tenant, db_ids)
            .await
    }

    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>> {
        self.paimon_catalog.get_table_name_by_id(table_id).await
    }

    fn support_partition(&self) -> bool {
        self.paimon_catalog.support_partition()
    }

    fn is_external(&self) -> bool {
        self.paimon_catalog.is_external()
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        self.paimon_catalog.get_table_engines()
    }

    fn default_table_engine(&self) -> Engine {
        self.paimon_catalog.default_table_engine()
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
        self.paimon_catalog
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
        self.paimon_catalog
            .mget_tables(tenant, db_name, table_names)
            .await
    }

    async fn get_table_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.paimon_catalog
            .get_table_history(tenant, db_name, table_name)
            .await
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
        self.paimon_catalog.list_tables(tenant, db_name).await
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
        self.paimon_catalog.list_tables_names(tenant, db_name).await
    }

    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.paimon_catalog
            .list_tables_history(tenant, db_name)
            .await
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        self.paimon_catalog.create_table(req).await
    }

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        self.paimon_catalog.drop_table_by_id(req).await
    }

    async fn undrop_table(&self, req: UndropTableReq) -> Result<()> {
        self.paimon_catalog.undrop_table(req).await
    }

    async fn commit_table_meta(
        &self,
        req: databend_common_meta_app::schema::CommitTableMetaReq,
    ) -> Result<databend_common_meta_app::schema::CommitTableMetaReply> {
        self.paimon_catalog.commit_table_meta(req).await
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        self.paimon_catalog.rename_table(req).await
    }

    async fn swap_table(&self, req: SwapTableReq) -> Result<SwapTableReply> {
        self.paimon_catalog.swap_table(req).await
    }

    async fn upsert_table_option(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        self.paimon_catalog
            .upsert_table_option(tenant, db_name, req)
            .await
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        self.paimon_catalog.set_table_column_mask_policy(req).await
    }

    async fn set_table_row_access_policy(
        &self,
        req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
        self.paimon_catalog.set_table_row_access_policy(req).await
    }

    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<()> {
        self.paimon_catalog.create_table_index(req).await
    }

    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<()> {
        self.paimon_catalog.drop_table_index(req).await
    }

    async fn get_table_copied_file_info(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        self.paimon_catalog
            .get_table_copied_file_info(tenant, db_name, req)
            .await
    }

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        self.paimon_catalog.truncate_table(table_info, req).await
    }

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        self.paimon_catalog.list_lock_revisions(req).await
    }

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        self.paimon_catalog.create_lock_revision(req).await
    }

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        self.paimon_catalog.extend_lock_revision(req).await
    }

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        self.paimon_catalog.delete_lock_revision(req).await
    }

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>> {
        self.paimon_catalog.list_locks(req).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn create_sequence(&self, req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        self.paimon_catalog.create_sequence(req).await
    }

    async fn get_sequence(
        &self,
        req: GetSequenceReq,
        visibility_checker: &Option<Arc<GrantObjectVisibilityChecker>>,
    ) -> Result<GetSequenceReply> {
        self.paimon_catalog
            .get_sequence(req, visibility_checker)
            .await
    }

    async fn list_sequences(&self, req: ListSequencesReq) -> Result<ListSequencesReply> {
        self.paimon_catalog.list_sequences(req).await
    }

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
        visibility_checker: &Option<Arc<GrantObjectVisibilityChecker>>,
    ) -> Result<GetSequenceNextValueReply> {
        self.paimon_catalog
            .get_sequence_next_value(req, visibility_checker)
            .await
    }

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply> {
        self.paimon_catalog.drop_sequence(req).await
    }

    async fn create_dictionary(&self, req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        self.paimon_catalog.create_dictionary(req).await
    }

    async fn drop_dictionary(
        &self,
        dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        self.paimon_catalog.drop_dictionary(dict_ident).await
    }

    async fn get_dictionary(&self, req: DictionaryNameIdent) -> Result<Option<GetDictionaryReply>> {
        self.paimon_catalog.get_dictionary(req).await
    }

    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        self.paimon_catalog.list_dictionaries(req).await
    }

    async fn rename_dictionary(&self, req: RenameDictionaryReq) -> Result<()> {
        self.paimon_catalog.rename_dictionary(req).await
    }

    async fn get_autoincrement_next_value(
        &self,
        req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply> {
        self.paimon_catalog.get_autoincrement_next_value(req).await
    }

    fn transform_udtf_as_table_function(
        &self,
        ctx: &dyn TableContext,
        table_args: &TableArgs,
        udtf: UDTFServer,
        func_name: &str,
    ) -> Result<Arc<dyn TableFunction>> {
        self.paimon_catalog
            .transform_udtf_as_table_function(ctx, table_args, udtf, func_name)
    }
}
