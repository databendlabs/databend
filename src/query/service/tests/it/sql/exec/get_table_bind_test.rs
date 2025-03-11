// Copyright 2022 Datafuse Labs.
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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use databend_common_base::base::tokio;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::database::Database;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetrics;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::ContextError;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::ProcessInfo;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
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
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListDictionaryReq;
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
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::MetaId;
use databend_common_pipeline_core::InputError;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::PlanProfile;
use databend_common_settings::Settings;
use databend_common_sql::IndexType;
use databend_common_sql::Planner;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StageFileInfo;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::*;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use parking_lot::Mutex;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

type MetaType = (String, String, String);

#[derive(Clone, Debug)]
struct FakedCatalog {
    cat: Arc<dyn Catalog>,
}

#[async_trait::async_trait]
impl Catalog for FakedCatalog {
    fn name(&self) -> String {
        "FakedCatalog".to_string()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.cat.info()
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        todo!()
    }

    async fn get_database(&self, _tenant: &Tenant, _db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        todo!()
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        todo!()
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        todo!()
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        todo!()
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        self.cat.get_table_by_info(table_info)
    }

    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
        get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        self.cat
            .mget_table_names_by_ids(tenant, table_ids, get_dropped_table)
            .await
    }

    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String> {
        self.cat.get_db_name_by_id(db_id).await
    }

    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        self.cat.mget_databases(tenant, db_names).await
    }

    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        self.cat.mget_database_names_by_ids(tenant, db_ids).await
    }

    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>> {
        self.cat.get_table_name_by_id(table_id).await
    }

    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        self.cat.get_table(tenant, db_name, table_name).await
    }

    async fn get_table_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Ok(Vec::from([self
            .cat
            .get_table(tenant, db_name, table_name)
            .await?]))
    }

    async fn list_tables(&self, _tenant: &Tenant, _db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        todo!()
    }

    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        todo!()
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        todo!()
    }

    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        todo!()
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        todo!()
    }

    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        todo!()
    }

    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        todo!()
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        todo!()
    }

    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        todo!()
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
    async fn list_indexes(&self, _req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        _req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_virtual_column(&self, _req: CreateVirtualColumnReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(&self, _req: UpdateVirtualColumnReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(&self, _req: DropVirtualColumnReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        _req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        unimplemented!()
    }

    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unimplemented!()
    }

    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unimplemented!()
    }

    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unimplemented!()
    }

    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        unimplemented!()
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(&self, _req: GetSequenceReq) -> Result<GetSequenceReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        self.cat.get_table_meta_by_id(table_id).await
    }

    async fn create_dictionary(&self, _req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        todo!()
    }

    async fn update_dictionary(&self, _req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        todo!()
    }

    async fn drop_dictionary(
        &self,
        _dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        todo!()
    }

    async fn get_dictionary(&self, req: DictionaryNameIdent) -> Result<Option<GetDictionaryReply>> {
        self.cat.get_dictionary(req).await
    }

    async fn list_dictionaries(
        &self,
        _req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        todo!()
    }

    async fn rename_dictionary(&self, _req: RenameDictionaryReq) -> Result<()> {
        todo!()
    }
}

struct CtxDelegation {
    ctx: Arc<QueryContext>,
    cat: FakedCatalog,
    cache: Mutex<HashMap<MetaType, Arc<dyn Table>>>,
    table_from_cache: AtomicUsize,
    table_without_cache: AtomicUsize,
}

impl CtxDelegation {
    fn new(ctx: Arc<QueryContext>, faked_cat: FakedCatalog) -> Self {
        Self {
            ctx,
            cat: faked_cat,
            cache: Mutex::new(HashMap::new()),
            table_from_cache: AtomicUsize::new(0),
            table_without_cache: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl TableContext for CtxDelegation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn build_table_from_source_plan(&self, _plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        todo!()
    }

    fn txn_mgr(&self) -> TxnManagerRef {
        self.ctx.txn_mgr()
    }

    fn incr_total_scan_value(&self, _value: ProgressValues) {
        todo!()
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        todo!()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.ctx.get_write_progress()
    }

    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.ctx.get_join_spill_progress()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.ctx.get_aggregate_spill_progress()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.ctx.get_group_by_spill_progress()
    }

    fn get_window_partition_spill_progress(&self) -> Arc<Progress> {
        self.ctx.get_window_partition_spill_progress()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        todo!()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_status_info(&self) -> String {
        "".to_string()
    }

    fn set_status_info(&self, _info: &str) {}

    fn get_partition(&self) -> Option<PartInfoPtr> {
        todo!()
    }

    fn get_partitions(&self, _: usize) -> Vec<PartInfoPtr> {
        todo!()
    }

    fn set_partitions(&self, _partitions: Partitions) -> Result<()> {
        todo!()
    }

    fn add_partitions_sha(&self, _sha: String) {
        todo!()
    }

    fn get_partitions_shas(&self) -> Vec<String> {
        todo!()
    }

    fn get_cacheable(&self) -> bool {
        todo!()
    }

    fn set_cacheable(&self, _: bool) {
        todo!()
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        self.ctx.get_can_scan_from_agg_index()
    }
    fn set_can_scan_from_agg_index(&self, _: bool) {
        todo!()
    }

    fn get_enable_sort_spill(&self) -> bool {
        todo!()
    }
    fn set_enable_sort_spill(&self, _enable: bool) {
        todo!()
    }

    fn attach_query_str(&self, _kind: QueryKind, _query: String) {}
    fn attach_query_hash(&self, _text_hash: String, _parameterized_hash: String) {
        todo!()
    }

    fn get_query_str(&self) -> String {
        todo!()
    }

    fn get_query_text_hash(&self) -> String {
        todo!()
    }

    fn get_query_parameterized_hash(&self) -> String {
        todo!()
    }

    fn get_fragment_id(&self) -> usize {
        todo!()
    }

    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.ctx.get_catalog(catalog_name).await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.ctx.get_default_catalog()
    }

    fn get_id(&self) -> String {
        self.ctx.get_id()
    }

    fn get_current_catalog(&self) -> String {
        "default".to_owned()
    }

    fn check_aborting(&self) -> Result<(), ContextError> {
        todo!()
    }

    fn get_error(&self) -> Option<ErrorCode<ContextError>> {
        todo!()
    }

    fn push_warning(&self, _warn: String) {
        todo!()
    }

    fn get_current_database(&self) -> String {
        self.ctx.get_current_database()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        todo!()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        todo!()
    }
    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        todo!()
    }
    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        todo!()
    }

    async fn validate_privilege(
        &self,
        _object: &GrantObject,
        _privilege: UserPrivilegeType,
        _check_current_role_only: bool,
    ) -> Result<()> {
        todo!()
    }

    async fn get_visibility_checker(
        &self,
        _ignore_ownership: bool,
    ) -> Result<GrantObjectVisibilityChecker> {
        todo!()
    }

    fn get_fuse_version(&self) -> String {
        todo!()
    }

    fn get_format_settings(&self) -> Result<FormatSettings> {
        todo!()
    }

    fn get_tenant(&self) -> Tenant {
        self.ctx.get_tenant()
    }

    fn get_query_kind(&self) -> QueryKind {
        todo!()
    }

    fn get_function_context(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }

    fn get_connection_id(&self) -> String {
        todo!()
    }

    fn get_settings(&self) -> Arc<Settings> {
        Settings::create(Tenant::new_literal("fake_settings"))
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        Settings::create(Tenant::new_literal("fake_shared_settings"))
    }

    fn get_session_settings(&self) -> Arc<Settings> {
        todo!()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        self.ctx.get_cluster()
    }

    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        todo!()
    }

    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        todo!()
    }

    fn get_last_query_id(&self, _index: i32) -> String {
        todo!()
    }
    fn get_query_id_history(&self) -> HashSet<String> {
        todo!()
    }
    fn get_result_cache_key(&self, _query_id: &str) -> Option<String> {
        todo!()
    }
    fn set_query_id_result_cache(&self, _query_id: String, _result_cache_key: String) {
        todo!()
    }

    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        todo!()
    }
    fn set_on_error_map(&self, _map: Arc<DashMap<String, HashMap<u16, InputError>>>) {
        todo!()
    }
    fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        todo!()
    }
    fn set_on_error_mode(&self, _mode: OnErrorMode) {
        todo!()
    }
    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        todo!()
    }

    fn get_application_level_data_operator(&self) -> Result<DataOperator> {
        self.ctx.get_application_level_data_operator()
    }

    async fn get_file_format(&self, _name: &str) -> Result<FileFormatParams> {
        todo!()
    }

    async fn get_connection(&self, _name: &str) -> Result<UserDefinedConnection> {
        todo!()
    }

    async fn get_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        let tenant = self.ctx.get_tenant();
        let db = database.to_string();
        let tbl = table.to_string();
        let table_meta_key = (tenant.tenant_name().to_string(), db, tbl);
        let already_in_cache = { self.cache.lock().contains_key(&table_meta_key) };
        if already_in_cache {
            self.table_from_cache
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self
                .cache
                .lock()
                .get(&table_meta_key)
                .ok_or_else(|| ErrorCode::Internal("Logical error, it's a bug."))?
                .clone())
        } else {
            self.table_without_cache
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let tbl = self
                .cat
                .get_table(&self.ctx.get_tenant(), database, table)
                .await?;
            let tbl2 = tbl.clone();
            let mut guard = self.cache.lock();
            guard.insert(table_meta_key, tbl);
            Ok(tbl2)
        }
    }

    fn evict_table_from_cache(&self, _catalog: &str, _database: &str, _table: &str) -> Result<()> {
        todo!()
    }

    async fn get_table_with_batch(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        _max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        self.get_table(catalog, database, table).await
    }

    async fn filter_out_copied_files(
        &self,
        _catalog_name: &str,
        _database_name: &str,
        _table_name: &str,
        _files: &[StageFileInfo],
        _path_prefix: Option<String>,
        _max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles> {
        todo!()
    }

    fn add_written_segment_location(&self, _segment_loc: Location) -> Result<()> {
        todo!()
    }

    fn clear_written_segment_locations(&self) -> Result<()> {
        todo!()
    }

    fn get_written_segment_locations(&self) -> Result<Vec<Location>> {
        todo!()
    }

    fn add_file_status(&self, _file_path: &str, _file_status: FileStatus) -> Result<()> {
        todo!()
    }

    fn get_copy_status(&self) -> Arc<CopyStatus> {
        todo!()
    }

    fn set_variable(&self, _key: String, _value: Scalar) {}
    fn unset_variable(&self, _key: &str) {}
    fn get_variable(&self, _key: &str) -> Option<Scalar> {
        None
    }

    fn get_all_variables(&self) -> HashMap<String, Scalar> {
        HashMap::new()
    }

    fn get_license_key(&self) -> String {
        self.ctx.get_license_key()
    }

    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>> {
        todo!()
    }
    fn add_mutation_status(&self, _mutation_status: MutationStatus) {
        todo!()
    }

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>> {
        todo!()
    }

    fn update_multi_table_insert_status(&self, _table_id: u64, _num_rows: u64) {
        todo!()
    }

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>> {
        todo!()
    }

    fn add_query_profiles(&self, _: &HashMap<u32, PlanProfile>) {
        todo!()
    }

    fn get_query_profiles(&self) -> Vec<PlanProfile> {
        todo!()
    }
    fn set_merge_into_join(&self, _join: MergeIntoJoin) {
        todo!()
    }

    fn get_merge_into_join(&self) -> MergeIntoJoin {
        todo!()
    }

    fn set_runtime_filter(&self, _filters: (IndexType, RuntimeFilterInfo)) {
        todo!()
    }

    fn set_runtime_filter_ready(&self, _table_index: usize, _ready: Arc<RuntimeFilterReady>) {
        todo!()
    }

    fn get_runtime_filter_ready(&self, _table_index: usize) -> Vec<Arc<RuntimeFilterReady>> {
        todo!()
    }

    fn set_wait_runtime_filter(&self, _table_index: usize, _need_to_wait: bool) {
        todo!()
    }

    fn get_wait_runtime_filter(&self, _table_index: usize) -> bool {
        todo!()
    }

    fn clear_runtime_filter(&self) {
        todo!()
    }

    fn get_bloom_runtime_filter_with_id(&self, _id: usize) -> Vec<(String, BinaryFuse16)> {
        todo!()
    }

    fn get_inlist_runtime_filter_with_id(&self, _id: usize) -> Vec<Expr<String>> {
        todo!()
    }

    fn get_min_max_runtime_filter_with_id(&self, _id: usize) -> Vec<Expr<String>> {
        todo!()
    }

    fn has_bloom_runtime_filters(&self, _id: usize) -> bool {
        todo!()
    }

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics {
        todo!()
    }

    fn get_queued_queries(&self) -> Vec<ProcessInfo> {
        todo!()
    }

    fn get_read_block_thresholds(&self) -> BlockThresholds {
        todo!()
    }

    fn set_read_block_thresholds(&self, _thresholds: BlockThresholds) {
        todo!()
    }

    fn get_query_queued_duration(&self) -> Duration {
        todo!()
    }

    fn set_query_queued_duration(&self, _queued_duration: Duration) {
        todo!()
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        _catalog_name: &str,
        _db_name: &str,
        _tbl_name: &str,
        _lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>> {
        todo!()
    }

    fn get_table_meta_timestamps(
        &self,
        table: &dyn Table,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps> {
        self.ctx.get_table_meta_timestamps(table, previous_snapshot)
    }

    fn get_temp_table_prefix(&self) -> Result<String> {
        todo!()
    }

    fn session_state(&self) -> SessionState {
        todo!()
    }

    fn is_temp_table(&self, _catalog_name: &str, _database_name: &str, _table_name: &str) -> bool {
        false
    }

    fn add_m_cte_temp_table(&self, _database_name: &str, _table_name: &str) {
        todo!()
    }

    async fn drop_m_cte_temp_table(&self) -> Result<()> {
        todo!()
    }

    fn set_cluster(&self, _: Arc<Cluster>) {
        todo!()
    }

    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>> {
        todo!()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_same_table_once() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let query = format!(
        "select * from {}.{} join {}.{} as t2 join {}.{} as t3",
        fixture.default_db_name().as_str(),
        fixture.default_table_name().as_str(),
        fixture.default_db_name().as_str(),
        fixture.default_table_name().as_str(),
        fixture.default_db_name().as_str(),
        fixture.default_table_name().as_str()
    );
    fixture.create_default_table().await?;
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let faked_catalog = FakedCatalog { cat: catalog };

    let ctx = Arc::new(CtxDelegation::new(ctx, faked_catalog));

    let mut planner = Planner::new(ctx.clone());
    let (_, _) = planner.plan_sql(query.as_str()).await?;

    assert_eq!(
        ctx.table_without_cache
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // plan cache need get table
    assert!(
        ctx.table_from_cache
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2
    );

    Ok(())
}
