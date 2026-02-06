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
use std::sync::Arc;
use std::unimplemented;

use databend_common_ast::ast::Engine;
use databend_common_exception::ErrorCode;
use databend_common_exception::ErrorCodeResultExt;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UDTFServer;
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
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReply;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
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
use databend_common_meta_app::schema::ListSequencesReply;
use databend_common_meta_app::schema::ListSequencesReq;
use databend_common_meta_app::schema::ListTableCopiedFileReply;
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
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReply;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_types::MetaId;
use databend_meta_types::SeqV;
use databend_storages_common_session::SessionState;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use dyn_clone::DynClone;
use log::info;

use crate::database::Database;
use crate::table::Table;
use crate::table_args::TableArgs;
use crate::table_context::TableContext;
use crate::table_function::TableFunction;

#[derive(Default, Clone)]
pub struct StorageDescription {
    pub engine_name: String,
    pub comment: String,
    pub support_cluster_key: bool,
}

pub trait CatalogCreator: Send + Sync + Debug {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>>;
}

#[async_trait::async_trait]
pub trait Catalog: DynClone + Send + Sync + Debug {
    // Get the name of the catalog.
    fn name(&self) -> String;
    // Get the info of the catalog.
    fn info(&self) -> Arc<CatalogInfo>;

    // Return catalog tables support partition or properties.
    fn support_partition(&self) -> bool {
        false
    }

    fn is_external(&self) -> bool {
        false
    }

    // This is used to return a new catalog; in the new catalog, the table info is not refreshed
    // This is used for attached table, if we attach many tables each is to read from s3, query system.tables it will be very slow.
    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>>;

    // Get the database by name.
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>>;

    // List all database history
    async fn list_databases_history(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>>;

    // Get all the databases.
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>>;

    // Operation with a database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply>;

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply>;

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply>;

    async fn drop_index(&self, req: DropIndexReq) -> Result<()>;

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply>;

    async fn list_marked_deleted_indexes(
        &self,
        _tenant: &Tenant,
        _table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedIndexesReply> {
        Err(ErrorCode::Unimplemented(format!(
            "'list_marked_deleted_indexes' not implemented for catalog {}",
            self.name()
        )))
    }

    async fn list_marked_deleted_table_indexes(
        &self,
        _tenant: &Tenant,
        _table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedTableIndexesReply> {
        Err(ErrorCode::Unimplemented(format!(
            "'list_marked_deleted_table_indexes' not implemented for catalog {}",
            self.name()
        )))
    }

    async fn remove_marked_deleted_index_ids(
        &self,
        _tenant: &Tenant,
        _table_id: u64,
        _index_ids: &[u64],
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "'remove_marked_deleted_index_ids' not implemented for catalog {}",
            self.name()
        )))
    }

    async fn remove_marked_deleted_table_indexes(
        &self,
        _tenant: &Tenant,
        _table_id: u64,
        _indexes: &[(String, String)],
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "'remove_marked_deleted_table_indexes' not implemented for catalog {}",
            self.name()
        )))
    }

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply>;

    async fn list_indexes(&self, _req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        Ok(vec![])
    }

    async fn list_index_ids_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        Ok(vec![])
    }

    async fn list_indexes_by_table_id(
        &self,
        _req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        Ok(vec![])
    }

    #[async_backtrace::framed]
    async fn exists_database(&self, tenant: &Tenant, db_name: &str) -> Result<bool> {
        Ok(self
            .get_database(tenant, db_name)
            .await
            .or_unknown_database()?
            .is_some())
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply>;

    // Build a `Arc<dyn Table>` from `TableInfo`.
    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>>;

    /// Get the table meta by table id.
    async fn get_table_meta_by_id(&self, table_id: u64) -> Result<Option<SeqV<TableMeta>>>;

    /// List the tables name by meta ids. This function should not be used to list temporary tables.
    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
        get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>>;

    // Get the db name by meta id.
    async fn get_db_name_by_id(&self, db_ids: MetaId) -> Result<String>;

    // Mget dbs by DatabaseNameIdent.
    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>>;

    // Mget the dbs name by meta ids.
    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>>;

    /// Get the table name by meta id.
    async fn get_table_name_by_id(&self, table_id: u64) -> Result<Option<String>>;

    // Get one table by db and table name.
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>>;

    async fn get_table_with_branch(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
        branch: Option<&str>,
    ) -> Result<Arc<dyn Table>> {
        let table = self.get_table(tenant, db_name, table_name).await?;
        match branch {
            Some(v) => table.with_branch(v),
            None => Ok(table),
        }
    }

    /// Get multiple tables by db and table names.
    /// Returns tables in the same order as the input table_names.
    /// If a table is not found, it will not be included in the result.
    async fn mget_tables(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(format!(
            "'mget_tables' not implemented for catalog {}",
            self.name()
        )))
    }

    // Get one table identified as dropped by db and table name.
    async fn get_table_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;

    /// List all tables in a database.This will not list temporary tables.
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>>;

    /// List all tables names in a database.This will not list temporary tables.
    async fn list_tables_names(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<String>>;

    fn list_temporary_tables(&self) -> Result<Vec<TableInfo>> {
        Err(ErrorCode::Unimplemented(
            "'list_temporary_tables' not implemented",
        ))
    }

    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;

    async fn get_drop_table_infos(
        &self,
        _req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        Err(ErrorCode::Unimplemented(
            "'get_drop_table_infos' not implemented",
        ))
    }

    /// Garbage collect dropped tables and databases.
    ///
    /// Returns the approximate number of metadata keys removed.
    /// Note: DeleteByPrefix operations count as 1 but may remove multiple keys.
    async fn gc_drop_tables(&self, _req: GcDroppedTableReq) -> Result<usize> {
        Err(ErrorCode::Unimplemented("'gc_drop_tables' not implemented"))
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply>;

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply>;

    async fn undrop_table(&self, req: UndropTableReq) -> Result<()>;

    async fn undrop_table_by_id(&self, _req: UndropTableByIdReq) -> Result<()> {
        unimplemented!("TODO")
    }

    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        unimplemented!("TODO")
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply>;

    async fn swap_table(&self, req: SwapTableReq) -> Result<SwapTableReply>;

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        Ok(self
            .get_table(tenant, db_name, table_name)
            .await
            .or_unknown_table()?
            .is_some())
    }

    // Check a db.dictionary is exists or not.
    #[async_backtrace::framed]
    async fn exists_dictionary(
        &self,
        tenant: &Tenant,
        db_name: &str,
        dict_name: &str,
    ) -> Result<bool> {
        let db_id = self
            .get_database(tenant, db_name)
            .await?
            .get_db_info()
            .database_id
            .db_id;
        let req = DictionaryNameIdent::new(
            tenant,
            DictionaryIdentity::new(db_id, dict_name.to_string()),
        );
        Ok(self
            .get_dictionary(req)
            .await
            .or_unknown_dictionary()?
            .is_some())
    }

    async fn upsert_table_option(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply>;

    async fn retryable_update_multi_table_meta(
        &self,
        _req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult> {
        Err(ErrorCode::Unimplemented(
            "'update_multi_table_meta' not implemented",
        ))
    }

    async fn update_multi_table_meta(
        &self,
        req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        let result = self.retryable_update_multi_table_meta(req).await?;
        match result {
            Ok(reply) => Ok(reply),
            Err(failed_tables) => {
                let err_msg = format!(
                    "Due to concurrent transactions, transaction commit failed. Conflicting table IDs: {:?}",
                    failed_tables
                        .iter()
                        .map(|(tid, _, _)| tid)
                        .collect::<Vec<_>>()
                );
                info!(
                    "Due to concurrent transactions, transaction commit failed. Conflicting tables: {:?}",
                    failed_tables
                );
                Err(ErrorCode::TableVersionMismatched(err_msg))
            }
        }
    }

    // update stream metas, currently used by "copy into location form stream"
    async fn update_stream_metas(
        &self,
        update_stream_metas: Vec<UpdateStreamMetaReq>,
    ) -> Result<()> {
        self.update_multi_table_meta(UpdateMultiTableMetaReq {
            update_stream_metas,
            ..Default::default()
        })
        .await
        .map(|_| ())
    }

    async fn update_single_table_meta(
        &self,
        req: UpdateTableMetaReq,
        table_info: &TableInfo,
    ) -> Result<UpdateTableMetaReply> {
        let mut update_table_metas = vec![];
        let mut update_temp_tables = vec![];
        if table_info.meta.options.contains_key(OPT_KEY_TEMP_PREFIX) {
            let req = UpdateTempTableReq {
                table_id: req.table_id,
                desc: table_info.desc.clone(),
                new_table_meta: req.new_table_meta,
                copied_files: Default::default(),
            };
            update_temp_tables.push(req);
        } else {
            update_table_metas.push((req, table_info.clone()));
        }
        self.update_multi_table_meta(UpdateMultiTableMetaReq {
            update_table_metas,
            update_temp_tables,
            ..Default::default()
        })
        .await
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply>;

    async fn set_table_row_access_policy(
        &self,
        req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply>;

    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<()>;

    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<()>;

    async fn get_table_copied_file_info(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply>;

    async fn list_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_id: u64,
    ) -> Result<ListTableCopiedFileReply> {
        Err(ErrorCode::Unimplemented(format!(
            "'list_table_copied_file_info' not implemented for catalog {}",
            self.name()
        )))
    }

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply>;

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>>;

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply>;

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()>;

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()>;

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>>;

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        Err(ErrorCode::Unimplemented(
            "'get_table_function' not implemented",
        ))
    }

    fn exists_table_function(&self, _func_name: &str) -> bool {
        false
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any;

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    // Get default table engine
    fn default_table_engine(&self) -> Engine {
        Engine::Fuse
    }

    fn get_stream_source_table(
        &self,
        _stream_desc: &str,
        _max_batch_size: Option<u64>,
    ) -> Result<Option<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(
            "'get_stream_source_table' not implemented",
        ))
    }

    fn cache_stream_source_table(
        &self,
        _stream: TableInfo,
        _source: TableInfo,
        _max_batch_size: Option<u64>,
    ) {
        unimplemented!()
    }

    async fn create_sequence(&self, req: CreateSequenceReq) -> Result<CreateSequenceReply>;
    async fn get_sequence(
        &self,
        req: GetSequenceReq,
        visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceReply>;

    async fn list_sequences(&self, req: ListSequencesReq) -> Result<ListSequencesReply>;

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
        visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceNextValueReply>;

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply>;

    async fn get_autoincrement_next_value(
        &self,
        req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply>;

    fn set_session_state(&self, _state: SessionState) -> Arc<dyn Catalog> {
        unimplemented!()
    }

    /// Dictionary
    async fn create_dictionary(&self, req: CreateDictionaryReq) -> Result<CreateDictionaryReply>;

    async fn update_dictionary(&self, req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply>;

    async fn drop_dictionary(
        &self,
        dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>>;

    async fn get_dictionary(&self, req: DictionaryNameIdent) -> Result<Option<GetDictionaryReply>>;

    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>>;

    async fn set_table_lvt(
        &self,
        _name_ident: &LeastVisibleTimeIdent,
        _value: &LeastVisibleTime,
    ) -> Result<LeastVisibleTime> {
        unimplemented!()
    }

    async fn get_table_lvt(
        &self,
        _name_ident: &LeastVisibleTimeIdent,
    ) -> Result<Option<LeastVisibleTime>> {
        unimplemented!()
    }

    async fn rename_dictionary(&self, req: RenameDictionaryReq) -> Result<()>;

    fn transform_udtf_as_table_function(
        &self,
        ctx: &dyn TableContext,
        table_args: &TableArgs,
        udtf: UDTFServer,
        func_name: &str,
    ) -> Result<Arc<dyn TableFunction>>;
}
