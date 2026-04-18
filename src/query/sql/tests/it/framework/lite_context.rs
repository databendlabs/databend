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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::Statement;
use databend_common_base::base::BuildInfo;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::GlobalInstance;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::base::Version;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::PerfEvent;
use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::database::Database;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReport;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetrics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::ContextError;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::ProcessInfo;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextBroadcast;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_catalog::table_context::TableContextCopy;
use databend_common_catalog::table_context::TableContextCte;
use databend_common_catalog::table_context::TableContextFragment;
use databend_common_catalog::table_context::TableContextMergeInto;
use databend_common_catalog::table_context::TableContextMutationStatus;
use databend_common_catalog::table_context::TableContextObservability;
use databend_common_catalog::table_context::TableContextOnError;
use databend_common_catalog::table_context::TableContextPartitionStats;
use databend_common_catalog::table_context::TableContextPerf;
use databend_common_catalog::table_context::TableContextProcessInfo;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextQueryInfo;
use databend_common_catalog::table_context::TableContextQueryProfile;
use databend_common_catalog::table_context::TableContextQueryQueue;
use databend_common_catalog::table_context::TableContextQueryState;
use databend_common_catalog::table_context::TableContextReadBlockThresholds;
use databend_common_catalog::table_context::TableContextResultCache;
use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_catalog::table_context::TableContextSegmentLocations;
use databend_common_catalog::table_context::TableContextSession;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextSpillProgress;
use databend_common_catalog::table_context::TableContextStage;
use databend_common_catalog::table_context::TableContextStream;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_catalog::table_context::TableContextTableFactory;
use databend_common_catalog::table_context::TableContextTableManagement;
use databend_common_catalog::table_context::TableContextVariables;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_meta_app::principal::*;
use databend_common_meta_app::schema::*;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_pipeline::core::InputError;
use databend_common_pipeline::core::LockGuard;
use databend_common_pipeline::core::PlanProfile;
use databend_common_settings::Settings;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Planner;
use databend_common_sql::normalize_identifier;
use databend_common_sql::optimize;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::plans::Plan;
use databend_common_sql::resolve_type_name;
use databend_common_sql_test_support::configure_optimizer_settings;
use databend_common_statistics::Datum;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StageFileInfo;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_meta_client::RpcClientConf;
use databend_meta_client::types::MetaId;
use databend_meta_client::types::NodeInfo;
use databend_meta_client::types::SeqV;
use databend_meta_runtime::DatabendRuntime;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManager;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ChangeType;
use parking_lot::Mutex;
use parking_lot::RwLock;

static TEST_BUILD_INFO: BuildInfo = BuildInfo {
    semantic: Version::new(0, 0, 0),
    commit_detail: String::new(),
    embedded_license: String::new(),
};

thread_local! {
    static INIT_TESTING_GLOBALS: std::sync::Once = const { std::sync::Once::new() };
    static THREAD_CATALOG_MANAGER: std::cell::OnceCell<Arc<CatalogManager>> =
        const { std::cell::OnceCell::new() };
    static THREAD_USER_API_PROVIDER: std::cell::OnceCell<Arc<UserApiProvider>> =
        const { std::cell::OnceCell::new() };
}

fn init_testing_globals() {
    #[cfg(debug_assertions)]
    {
        INIT_TESTING_GLOBALS.with(|init| {
            init.call_once(|| {
                let thread_name = std::thread::current().name().unwrap().to_string();
                GlobalInstance::init_testing(&thread_name);
                GlobalConfig::init(&InnerConfig::default(), &TEST_BUILD_INFO)
                    .expect("init global config");
                OssLicenseManager::init("default".to_string()).expect("init oss license manager");
            });
        });
    }

    #[cfg(not(debug_assertions))]
    {
        static INIT_GLOBALS: std::sync::Once = std::sync::Once::new();
        INIT_GLOBALS.call_once(|| {
            GlobalInstance::init_production();
            GlobalConfig::init(&InnerConfig::default(), &TEST_BUILD_INFO)
                .expect("init global config");
            OssLicenseManager::init("default".to_string()).expect("init oss license manager");
        });
    }
}

fn unsupported<T>(name: &str) -> Result<T> {
    Err(ErrorCode::Unimplemented(format!(
        "lite sql harness does not support {name} yet"
    )))
}

type TableKey = (String, String);
type TableMap = HashMap<TableKey, Arc<dyn Table>>;
type ColumnStatsMap = HashMap<String, BasicColumnStatistics>;

#[derive(Clone)]
struct DummyCatalog {
    info: Arc<CatalogInfo>,
    tables: Arc<RwLock<TableMap>>,
}

impl std::fmt::Debug for DummyCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DummyCatalog")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

impl Default for DummyCatalog {
    fn default() -> Self {
        Self {
            info: Arc::new(CatalogInfo {
                meta: CatalogMeta {
                    catalog_option: CatalogOption::Default,
                    created_on: Utc::now(),
                },
                ..Default::default()
            }),
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl DummyCatalog {
    fn insert_table(&self, database: &str, table: Arc<dyn Table>) {
        self.tables
            .write()
            .insert((database.to_string(), table.name().to_string()), table);
    }

    fn clear_tables(&self) {
        self.tables.write().clear();
    }
}

#[derive(Debug, Clone)]
struct FakeTable {
    table_info: TableInfo,
    warehouse_distribution: bool,
    table_stats: Option<TableStatistics>,
    column_stats: HashMap<ColumnId, BasicColumnStatistics>,
}

#[derive(Debug, Clone)]
struct FakeColumnStatisticsProvider {
    column_stats: HashMap<ColumnId, BasicColumnStatistics>,
    num_rows: Option<u64>,
}

impl ColumnStatisticsProvider for FakeColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id)
    }

    fn num_rows(&self) -> Option<u64> {
        self.num_rows
    }

    fn stats_num_rows(&self) -> Option<u64> {
        self.num_rows
    }

    fn average_size(&self, column_id: ColumnId) -> Option<u64> {
        let num_rows = self.num_rows?;
        if num_rows == 0 {
            return None;
        }
        self.column_stats
            .get(&column_id)
            .map(|stats| stats.in_memory_size / num_rows)
    }
}

#[async_trait::async_trait]
impl Table for FakeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn distribution_level(&self) -> DistributionLevel {
        if self.warehouse_distribution {
            DistributionLevel::Cluster
        } else {
            DistributionLevel::Local
        }
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _require_fresh: bool,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        Ok(self.table_stats)
    }

    async fn column_statistics_provider(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        Ok(Box::new(FakeColumnStatisticsProvider {
            column_stats: self.column_stats.clone(),
            num_rows: self.table_stats.and_then(|stats| stats.num_rows),
        }))
    }

    fn support_prewhere(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl Catalog for DummyCatalog {
    fn name(&self) -> String {
        self.info.catalog_name().to_string()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        Ok(self)
    }

    fn set_session_state(&self, _state: SessionState) -> Arc<dyn Catalog> {
        Arc::new(self.clone())
    }

    async fn get_database(&self, _tenant: &Tenant, _db_name: &str) -> Result<Arc<dyn Database>> {
        unsupported("catalog::get_database")
    }

    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![])
    }

    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![])
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        unsupported("catalog::create_database")
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        unsupported("catalog::drop_database")
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        unsupported("catalog::undrop_database")
    }

    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unsupported("catalog::create_index")
    }

    async fn drop_index(&self, _req: DropIndexReq) -> Result<()> {
        unsupported("catalog::drop_index")
    }

    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        unsupported("catalog::get_index")
    }

    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        unsupported("catalog::update_index")
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        unsupported("catalog::rename_database")
    }

    fn get_table_by_info(&self, _table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        unsupported("catalog::get_table_by_info")
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
        _get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        Ok(vec![])
    }

    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        unsupported("catalog::get_db_name_by_id")
    }

    async fn mget_databases(
        &self,
        _tenant: &Tenant,
        _db_names: &[database_name_ident::DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![])
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Ok(vec![])
    }

    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Ok(None)
    }

    async fn get_table(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        self.tables
            .read()
            .get(&(db_name.to_string(), table_name.to_string()))
            .cloned()
            .ok_or_else(|| ErrorCode::UnknownTable(format!("{}.{}", db_name, table_name)))
    }

    async fn mget_tables(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        Ok(vec![])
    }

    async fn get_table_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Ok(vec![])
    }

    async fn list_tables(&self, _tenant: &Tenant, _db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        Ok(vec![])
    }

    async fn list_tables_names(&self, _tenant: &Tenant, _db_name: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Ok(vec![])
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        unsupported("catalog::create_table")
    }

    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        unsupported("catalog::drop_table_by_id")
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        unsupported("catalog::undrop_table")
    }

    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        unsupported("catalog::commit_table_meta")
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        unsupported("catalog::rename_table")
    }

    async fn swap_table(&self, _req: SwapTableReq) -> Result<SwapTableReply> {
        unsupported("catalog::swap_table")
    }

    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        unsupported("catalog::upsert_table_option")
    }

    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        unsupported("catalog::set_table_column_mask_policy")
    }

    async fn set_table_row_access_policy(
        &self,
        _req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
        unsupported("catalog::set_table_row_access_policy")
    }

    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<()> {
        unsupported("catalog::create_table_index")
    }

    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<()> {
        unsupported("catalog::drop_table_index")
    }

    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unsupported("catalog::get_table_copied_file_info")
    }

    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        unsupported("catalog::truncate_table")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        Ok(vec![])
    }

    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unsupported("catalog::create_lock_revision")
    }

    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unsupported("catalog::extend_lock_revision")
    }

    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unsupported("catalog::delete_lock_revision")
    }

    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        Ok(vec![])
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unsupported("catalog::create_sequence")
    }

    async fn get_sequence(
        &self,
        _req: GetSequenceReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceReply> {
        unsupported("catalog::get_sequence")
    }

    async fn list_sequences(&self, _req: ListSequencesReq) -> Result<ListSequencesReply> {
        unsupported("catalog::list_sequences")
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceNextValueReply> {
        unsupported("catalog::get_sequence_next_value")
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unsupported("catalog::drop_sequence")
    }

    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        Ok(None)
    }

    async fn create_dictionary(&self, _req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        unsupported("catalog::create_dictionary")
    }

    async fn update_dictionary(&self, _req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        unsupported("catalog::update_dictionary")
    }

    async fn drop_dictionary(
        &self,
        _dict_ident: dictionary_name_ident::DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        unsupported("catalog::drop_dictionary")
    }

    async fn get_dictionary(
        &self,
        _req: dictionary_name_ident::DictionaryNameIdent,
    ) -> Result<Option<GetDictionaryReply>> {
        Ok(None)
    }

    async fn list_dictionaries(
        &self,
        _req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        Ok(vec![])
    }

    async fn rename_dictionary(&self, _req: RenameDictionaryReq) -> Result<()> {
        unsupported("catalog::rename_dictionary")
    }

    async fn get_autoincrement_next_value(
        &self,
        _req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply> {
        unsupported("catalog::get_autoincrement_next_value")
    }

    fn transform_udtf_as_table_function(
        &self,
        _ctx: &dyn TableContext,
        _table_args: &databend_common_catalog::table_args::TableArgs,
        _udtf: UDTFServer,
        _func_name: &str,
    ) -> Result<Arc<dyn databend_common_catalog::table_function::TableFunction>> {
        unsupported("catalog::transform_udtf_as_table_function")
    }
}

pub struct LiteTableContext {
    catalog_manager: Arc<CatalogManager>,
    default_catalog: Arc<DummyCatalog>,
    settings: Arc<Settings>,
    shared_settings: Arc<Settings>,
    tenant: Tenant,
    current_catalog: String,
    current_database: String,
    cluster: RwLock<Arc<Cluster>>,
    warehouse_distribution: RwLock<bool>,
    abort_notify: Arc<WatchNotify>,
    query: RwLock<String>,
    query_text_hash: RwLock<String>,
    query_parameterized_hash: RwLock<String>,
    scan_progress: Arc<Progress>,
    write_progress: Arc<Progress>,
    join_spill_progress: Arc<Progress>,
    group_by_spill_progress: Arc<Progress>,
    aggregate_spill_progress: Arc<Progress>,
    window_partition_spill_progress: Arc<Progress>,
    result_progress: Arc<Progress>,
    data_cache_metrics: DataCacheMetrics,
    copy_status: Arc<CopyStatus>,
    mutation_status: Arc<RwLock<MutationStatus>>,
    multi_table_insert_status: Arc<Mutex<MultiTableInsertStatus>>,
    merge_into_join: RwLock<MergeIntoJoin>,
    variables: RwLock<HashMap<String, Scalar>>,
    runtime_filter_ready: RwLock<HashMap<usize, Vec<Arc<RuntimeFilterReady>>>>,
    queued_duration: RwLock<Duration>,
    next_table_id: AtomicU64,
}

impl LiteTableContext {
    async fn init_user_api_provider(tenant: &Tenant) -> Result<Arc<UserApiProvider>> {
        if let Some(user_api_provider) = THREAD_USER_API_PROVIDER.with(|cell| cell.get().cloned()) {
            return Ok(user_api_provider);
        }

        let user_api_provider =
            UserApiProvider::try_create_simple(RpcClientConf::empty(), tenant).await?;
        THREAD_USER_API_PROVIDER.with(|cell| {
            assert!(
                cell.set(user_api_provider.clone()).is_ok(),
                "user api provider should be initialized once per thread"
            );
        });
        GlobalInstance::set(user_api_provider.clone());

        Ok(user_api_provider)
    }

    async fn reset_user_api_state(&self) -> Result<()> {
        let user_api_provider = UserApiProvider::instance();
        for udf in user_api_provider.list_udf(&self.tenant).await? {
            user_api_provider
                .drop_udf(&self.tenant, &udf.name, true)
                .await??;
        }

        Ok(())
    }

    pub fn configure_for_optimizer_case(&self, auto_stats: bool) -> Result<()> {
        let settings = self.get_settings();
        settings.set_enable_auto_materialize_cte(0)?;
        configure_optimizer_settings(settings.as_ref(), auto_stats)
    }

    fn build_fake_table(
        &self,
        database: &str,
        table_name: &str,
        fields: Vec<TableField>,
        table_stats: Option<TableStatistics>,
        column_stats: ColumnStatsMap,
    ) -> Result<Arc<dyn Table>> {
        let schema = Arc::new(TableSchema::new(fields));
        let column_stats = column_stats
            .into_iter()
            .map(|(name, stats)| {
                let index = schema.index_of(&name)?;
                let column_id = schema.field(index).column_id();
                Ok((column_id, stats))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let warehouse_distribution = *self.warehouse_distribution.read();
        let table_id = self.next_table_id.fetch_add(1, Ordering::Relaxed);

        Ok(Arc::new(FakeTable {
            table_info: TableInfo {
                ident: TableIdent::new(table_id, 0),
                desc: format!("'{}'.'{}'", database, table_name),
                name: table_name.to_string(),
                meta: TableMeta {
                    schema,
                    ..Default::default()
                },
                catalog_info: self.default_catalog.info(),
                db_type: DatabaseType::NormalDB,
            },
            warehouse_distribution,
            table_stats,
            column_stats,
        }))
    }

    pub async fn create() -> Result<Arc<Self>> {
        init_testing_globals();

        let tenant = Tenant::new_literal("default");
        let settings = Settings::create(tenant.clone());
        let shared_settings = Settings::create(tenant.clone());
        Self::init_user_api_provider(&tenant).await?;
        let catalog_manager = if let Some(catalog_manager) =
            THREAD_CATALOG_MANAGER.with(|cell| cell.get().cloned())
        {
            catalog_manager
        } else {
            let default_catalog = Arc::new(DummyCatalog::default());
            let default_catalog_dyn: Arc<dyn Catalog> = default_catalog.clone();
            let mut rpc_conf = RpcClientConf::empty();
            rpc_conf.endpoints = vec!["http://127.0.0.1:1".to_string()];
            rpc_conf.timeout = Some(Duration::from_millis(1));
            let catalog_manager = Arc::new(CatalogManager {
                meta: MetaStoreProvider::new(rpc_conf)
                    .create_meta_store::<DatabendRuntime>()
                    .await
                    .map_err(|err| ErrorCode::Internal(err.to_string()))?,
                default_catalog: default_catalog_dyn,
                external_catalogs: HashMap::<String, Arc<dyn Catalog>>::new(),
                catalog_creators: HashMap::<CatalogType, Arc<dyn CatalogCreator>>::new(),
                catalog_caches: Default::default(),
            });
            THREAD_CATALOG_MANAGER.with(|cell| {
                assert!(
                    cell.set(catalog_manager.clone()).is_ok(),
                    "catalog manager should be initialized once per thread"
                );
            });
            GlobalInstance::set(catalog_manager.clone());
            catalog_manager
        };
        let default_catalog: Arc<DummyCatalog> = catalog_manager
            .default_catalog
            .as_any()
            .downcast_ref::<DummyCatalog>()
            .ok_or_else(|| ErrorCode::Internal("unexpected default catalog type"))?
            .clone()
            .into();
        default_catalog.clear_tables();

        let ctx = Arc::new(Self {
            catalog_manager,
            default_catalog,
            settings,
            shared_settings,
            tenant,
            current_catalog: "default".to_string(),
            current_database: "default".to_string(),
            cluster: RwLock::new(Arc::new(Cluster {
                unassign: false,
                local_id: "local".to_string(),
                nodes: vec![],
            })),
            warehouse_distribution: RwLock::new(false),
            abort_notify: Arc::new(WatchNotify::new()),
            query: RwLock::new(String::new()),
            query_text_hash: RwLock::new(String::new()),
            query_parameterized_hash: RwLock::new(String::new()),
            scan_progress: Arc::new(Progress::create()),
            write_progress: Arc::new(Progress::create()),
            join_spill_progress: Arc::new(Progress::create()),
            group_by_spill_progress: Arc::new(Progress::create()),
            aggregate_spill_progress: Arc::new(Progress::create()),
            window_partition_spill_progress: Arc::new(Progress::create()),
            result_progress: Arc::new(Progress::create()),
            data_cache_metrics: DataCacheMetrics::new(),
            copy_status: Arc::new(CopyStatus::default()),
            mutation_status: Arc::new(RwLock::new(MutationStatus::default())),
            multi_table_insert_status: Arc::new(Mutex::new(MultiTableInsertStatus::default())),
            merge_into_join: RwLock::new(MergeIntoJoin::default()),
            variables: RwLock::new(HashMap::new()),
            runtime_filter_ready: RwLock::new(HashMap::new()),
            queued_duration: RwLock::new(Duration::default()),
            next_table_id: AtomicU64::new(1),
        });
        ctx.reset_user_api_state().await?;
        Ok(ctx)
    }

    pub fn set_table_warehouse_distribution(&self, enabled: bool) {
        *self.warehouse_distribution.write() = enabled;
    }

    pub fn set_cluster_node_num(&self, nodes: u64) {
        let local_id = if nodes == 0 {
            "local".to_string()
        } else {
            format!("local-{}", nodes)
        };
        let node_infos = (0..nodes.max(1))
            .map(|index| {
                let node_id = if index + 1 == nodes.max(1) {
                    local_id.clone()
                } else {
                    format!("node-{index}")
                };
                let mut node_info = NodeInfo::create(
                    node_id,
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                );
                node_info.cluster_id = "cluster_id".to_string();
                node_info.warehouse_id = "warehouse_id".to_string();
                Arc::new(node_info)
            })
            .collect();

        *self.cluster.write() = Arc::new(Cluster {
            unassign: false,
            local_id,
            nodes: node_infos,
        });
    }

    pub fn register_table_with_stats(
        self: &Arc<Self>,
        database: &str,
        table_name: &str,
        fields: Vec<TableField>,
        table_stats: Option<TableStatistics>,
        column_stats: ColumnStatsMap,
    ) -> Result<()> {
        let table =
            self.build_fake_table(database, table_name, fields, table_stats, column_stats)?;
        self.default_catalog.insert_table(database, table);
        Ok(())
    }

    pub async fn register_table_sql(self: &Arc<Self>, sql: &str) -> Result<()> {
        self.register_table_sql_with_stats(sql, None, HashMap::new())
            .await
    }

    pub async fn register_setup_sql(self: &Arc<Self>, sql: &str) -> Result<()> {
        let planner = Planner::new(self.clone());
        let extras = planner.parse_sql(sql)?;

        match &extras.statement {
            Statement::CreateTable(_) => self.register_table_sql(sql).await,
            Statement::CreateUDF(_) => {
                let metadata = Arc::new(RwLock::new(Metadata::default()));
                let name_resolution_ctx =
                    NameResolutionContext::try_from(self.get_settings().as_ref())?;
                let binder = databend_common_sql::Binder::new(
                    self.clone(),
                    self.catalog_manager.clone(),
                    name_resolution_ctx,
                    metadata,
                );
                match binder.bind(&extras.statement).await? {
                    Plan::CreateUDF(plan) => {
                        UserApiProvider::instance()
                            .add_udf(&self.tenant, plan.udf.clone(), &plan.create_option)
                            .await
                    }
                    _ => unreachable!("create udf statement must bind to create udf plan"),
                }
            }
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => {
                let name_resolution_ctx =
                    NameResolutionContext::try_from(self.get_settings().as_ref())?;
                let udf_name = normalize_identifier(udf_name, &name_resolution_ctx).to_string();
                UserApiProvider::instance()
                    .drop_udf(&self.tenant, &udf_name, *if_exists)
                    .await??;
                Ok(())
            }
            _ => unsupported("lite sql harness setup from unsupported SQL"),
        }
    }

    pub async fn register_table_sql_with_stats(
        self: &Arc<Self>,
        sql: &str,
        table_stats: Option<TableStatistics>,
        column_stats: ColumnStatsMap,
    ) -> Result<()> {
        let planner = Planner::new(self.clone());
        let extras = planner.parse_sql(sql)?;
        let name_resolution_ctx = NameResolutionContext::try_from(self.get_settings().as_ref())?;

        match extras.statement {
            Statement::CreateTable(stmt) => {
                let database = stmt
                    .database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &name_resolution_ctx).name)
                    .unwrap_or_else(|| self.current_database.clone());
                let table_name = normalize_identifier(&stmt.table, &name_resolution_ctx).name;
                let not_null = !self.get_settings().get_ddl_column_type_nullable()?;
                let mut table_stats = table_stats;
                let mut column_stats = column_stats;

                let fields = match stmt.source {
                    Some(CreateTableSource::Columns { columns, .. }) => columns
                        .into_iter()
                        .map(|column| {
                            let name =
                                normalize_identifier(&column.name, &name_resolution_ctx).name;
                            let data_type = resolve_type_name(&column.data_type, not_null)?;
                            Ok(TableField::new(&name, data_type))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    Some(CreateTableSource::Like { .. }) => {
                        return unsupported(
                            "lite sql harness DDL registration from CREATE TABLE LIKE",
                        );
                    }
                    None => {
                        let lowered = sql.to_ascii_lowercase();
                        if lowered.contains(" from numbers(") {
                            let column_name = sql
                                .rsplit_once("tbl(")
                                .and_then(|(_, rest)| rest.split(')').next())
                                .map(str::trim)
                                .filter(|name| !name.is_empty())
                                .unwrap_or("number");

                            if table_stats.is_none() && column_stats.is_empty() {
                                let num_rows = lowered
                                    .split("from numbers(")
                                    .nth(1)
                                    .and_then(|rest| rest.split(')').next())
                                    .and_then(|count| count.trim().parse::<u64>().ok());

                                if let Some(num_rows) = num_rows {
                                    table_stats = Some(TableStatistics {
                                        num_rows: Some(num_rows),
                                        data_size: Some(num_rows.saturating_mul(8)),
                                        data_size_compressed: None,
                                        index_size: None,
                                        bloom_index_size: None,
                                        ngram_index_size: None,
                                        inverted_index_size: None,
                                        vector_index_size: None,
                                        virtual_column_size: None,
                                        number_of_blocks: Some(1),
                                        number_of_segments: Some(1),
                                    });

                                    column_stats.insert(
                                        column_name.to_string(),
                                        BasicColumnStatistics {
                                            min: Some(Datum::UInt(0)),
                                            max: Some(Datum::UInt(num_rows.saturating_sub(1))),
                                            ndv: Some(num_rows),
                                            null_count: 0,
                                            in_memory_size: num_rows.saturating_mul(8),
                                        },
                                    );
                                }
                            }

                            vec![TableField::new(
                                column_name,
                                TableDataType::Number(NumberDataType::UInt64),
                            )]
                        } else {
                            return unsupported(
                                "lite sql harness DDL registration from CREATE TABLE AS",
                            );
                        }
                    }
                };

                self.register_table_with_stats(
                    &database,
                    &table_name,
                    fields,
                    table_stats,
                    column_stats,
                )
            }
            _ => unsupported("lite sql harness table registration from non-DDL SQL"),
        }
    }

    pub async fn bind_sql(self: &Arc<Self>, sql: &str) -> Result<Plan> {
        let planner = Planner::new(self.clone());
        let extras = planner.parse_sql(sql)?;
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let name_resolution_ctx = NameResolutionContext::try_from(self.get_settings().as_ref())?;
        let binder = databend_common_sql::Binder::new(
            self.clone(),
            self.catalog_manager.clone(),
            name_resolution_ctx,
            metadata,
        );
        binder.bind(&extras.statement).await
    }

    pub async fn optimize_plan(self: &Arc<Self>, plan: Plan) -> Result<Plan> {
        let metadata = match &plan {
            Plan::Query { metadata, .. } => metadata.clone(),
            _ => Arc::new(RwLock::new(Metadata::default())),
        };
        let settings = self.get_settings();
        let opt_ctx = OptimizerContext::new(self.clone(), metadata)
            .with_settings(&settings)?
            .set_enable_distributed_optimization(true)
            .clone();
        optimize(opt_ctx, plan).await
    }
}

#[async_trait::async_trait]
impl TableContext for LiteTableContext {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TableContextSession for LiteTableContext {
    fn get_connection_id(&self) -> String {
        "lite-conn".to_string()
    }

    fn txn_mgr(&self) -> TxnManagerRef {
        TxnManager::init()
    }

    fn session_state(&self) -> Result<SessionState> {
        Ok(SessionState::default())
    }

    fn get_session_type(&self) -> SessionType {
        SessionType::HTTPQuery
    }
}

impl TableContextSettings for LiteTableContext {
    fn get_function_context(&self) -> Result<FunctionContext> {
        Ok(FunctionContext::default())
    }

    fn get_settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    fn get_session_settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        self.shared_settings.clone()
    }

    fn get_license_key(&self) -> String {
        String::new()
    }
}

#[async_trait::async_trait]
impl TableContextAuthorization for LiteTableContext {
    fn get_current_user(&self) -> Result<UserInfo> {
        Ok(UserInfo::new_no_auth("root", "%"))
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        None
    }

    fn get_secondary_roles(&self) -> Option<Vec<String>> {
        None
    }

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        Ok(vec![])
    }

    async fn validate_privilege(
        &self,
        _object: &GrantObject,
        _privilege: UserPrivilegeType,
        _check_current_role_only: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        Ok(vec![])
    }

    async fn get_visibility_checker(
        &self,
        _ignore_ownership: bool,
        _object: Object,
    ) -> Result<GrantObjectVisibilityChecker> {
        unsupported("table_ctx::get_visibility_checker")
    }
}

impl TableContextProcessInfo for LiteTableContext {
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        vec![]
    }

    fn get_queued_queries(&self) -> Vec<ProcessInfo> {
        vec![]
    }
}

impl TableContextQueryInfo for LiteTableContext {
    fn get_fuse_version(&self) -> String {
        String::new()
    }

    fn get_version(&self) -> BuildInfoRef {
        &TEST_BUILD_INFO
    }

    fn get_input_format_settings(&self) -> Result<InputFormatSettings> {
        Ok(Default::default())
    }

    fn get_output_format_settings(&self) -> Result<OutputFormatSettings> {
        Ok(Default::default())
    }

    fn get_query_kind(&self) -> QueryKind {
        QueryKind::Query
    }
}

impl TableContextProgress for LiteTableContext {
    fn incr_total_scan_value(&self, value: ProgressValues) {
        self.scan_progress.incr(&value);
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        self.scan_progress.get_values()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        self.scan_progress.clone()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        self.scan_progress.get_values()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.write_progress.clone()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        self.write_progress.get_values()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        self.result_progress.clone()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        self.result_progress.get_values()
    }
}

impl TableContextObservability for LiteTableContext {
    fn get_status_info(&self) -> String {
        String::new()
    }

    fn set_status_info(&self, _info: &str) {}

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics {
        &self.data_cache_metrics
    }
}

#[async_trait::async_trait]
impl TableContextCluster for LiteTableContext {
    fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster.read().clone()
    }

    fn set_cluster(&self, cluster: Arc<Cluster>) {
        *self.cluster.write() = cluster;
    }

    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>> {
        Ok(self.get_cluster())
    }
}

impl TableContextQueryState for LiteTableContext {
    fn check_aborting(&self) -> std::result::Result<(), ErrorCode<ContextError>> {
        Ok(())
    }

    fn get_abort_notify(&self) -> Arc<WatchNotify> {
        self.abort_notify.clone()
    }

    fn get_error(&self) -> Option<ErrorCode<ContextError>> {
        None
    }

    fn push_warning(&self, _warning: String) {}
}

impl TableContextBroadcast for LiteTableContext {
    fn get_next_broadcast_id(&self) -> u32 {
        0
    }
}

#[async_trait::async_trait]
impl TableContextCte for LiteTableContext {
    fn add_m_cte_temp_table(&self, _database_name: &str, _table_name: &str) {}

    async fn drop_m_cte_temp_table(&self) -> Result<()> {
        Ok(())
    }

    fn add_recursive_cte_temp_table(
        &self,
        _catalog_name: &str,
        _database_name: &str,
        _table_name: &str,
    ) {
        unimplemented!()
    }

    async fn drop_recursive_cte_temp_table(&self) -> Result<()> {
        unsupported("table_ctx::drop_recursive_cte_temp_table")
    }
}

impl TableContextMergeInto for LiteTableContext {
    fn set_merge_into_join(&self, join: MergeIntoJoin) {
        *self.merge_into_join.write() = join;
    }

    fn get_merge_into_join(&self) -> MergeIntoJoin {
        let join = self.merge_into_join.read();
        MergeIntoJoin {
            merge_into_join_type: join.merge_into_join_type.clone(),
            is_distributed: join.is_distributed,
            target_tbl_idx: join.target_tbl_idx,
        }
    }
}

impl TableContextOnError for LiteTableContext {
    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        None
    }

    fn set_on_error_map(&self, _map: Arc<DashMap<String, HashMap<u16, InputError>>>) {}

    fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        None
    }

    fn set_on_error_mode(&self, _mode: OnErrorMode) {}

    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        None
    }
}

impl TableContextResultCache for LiteTableContext {
    fn add_partitions_sha(&self, _key: String) {}

    fn get_partitions_shas(&self) -> Vec<String> {
        vec![]
    }

    fn add_cache_key_extra(&self, _extra: String) {}

    fn get_cache_key_extras(&self) -> Vec<String> {
        vec![]
    }

    fn get_cacheable(&self) -> bool {
        false
    }

    fn set_cacheable(&self, _cacheable: bool) {}

    fn get_result_cache_key(&self, _query_id: &str) -> Option<String> {
        None
    }

    fn set_query_id_result_cache(&self, _query_id: String, _result_cache_key: String) {}
}

impl TableContextMutationStatus for LiteTableContext {
    fn add_mutation_status(&self, mutation_status: MutationStatus) {
        self.mutation_status
            .write()
            .merge_mutation_status(mutation_status);
    }

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>> {
        self.mutation_status.clone()
    }

    fn update_multi_table_insert_status(&self, table_id: u64, num_rows: u64) {
        self.multi_table_insert_status
            .lock()
            .insert_rows
            .insert(table_id, num_rows);
    }

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>> {
        self.multi_table_insert_status.clone()
    }
}

impl TableContextQueryIdentity for LiteTableContext {
    fn get_id(&self) -> String {
        "lite-query".to_string()
    }

    fn attach_query_str(&self, _kind: QueryKind, query: String) {
        *self.query.write() = query;
    }

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String) {
        *self.query_text_hash.write() = text_hash;
        *self.query_parameterized_hash.write() = parameterized_hash;
    }

    fn get_query_str(&self) -> String {
        self.query.read().clone()
    }

    fn get_query_parameterized_hash(&self) -> String {
        self.query_parameterized_hash.read().clone()
    }

    fn get_query_text_hash(&self) -> String {
        self.query_text_hash.read().clone()
    }

    fn get_last_query_id(&self, _index: i32) -> Option<String> {
        None
    }

    fn get_query_id_history(&self) -> HashSet<String> {
        HashSet::new()
    }
}

impl TableContextFragment for LiteTableContext {
    fn get_fragment_id(&self) -> usize {
        0
    }
}

impl TableContextQueryProfile for LiteTableContext {
    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>> {
        HashMap::new()
    }

    fn add_query_profiles(&self, _profiles: &HashMap<u32, PlanProfile>) {}

    fn get_query_profiles(&self) -> Vec<PlanProfile> {
        vec![]
    }
}

#[async_trait::async_trait]
impl TableContextStage for LiteTableContext {
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        None
    }

    async fn get_file_format(&self, _name: &str) -> Result<FileFormatParams> {
        unsupported("table_ctx::get_file_format")
    }

    async fn get_connection(&self, _name: &str) -> Result<UserDefinedConnection> {
        unsupported("table_ctx::get_connection")
    }
}

#[async_trait::async_trait]
impl TableContextCopy for LiteTableContext {
    async fn filter_out_copied_files(
        &self,
        _catalog_name: &str,
        _database_name: &str,
        _table_name: &str,
        _files: &[StageFileInfo],
        _path_prefix: Option<String>,
        _max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles> {
        Ok(FilteredCopyFiles::default())
    }

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()> {
        self.copy_status.add_chunk(file_path, file_status);
        Ok(())
    }

    fn get_copy_status(&self) -> Arc<CopyStatus> {
        self.copy_status.clone()
    }
}

impl TableContextTableFactory for LiteTableContext {
    fn build_table_from_source_plan(&self, _plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        unsupported("table_ctx::build_table_from_source_plan")
    }
}

#[async_trait::async_trait]
impl TableContextTableAccess for LiteTableContext {
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.catalog_manager
            .get_catalog(
                self.tenant.tenant_name(),
                catalog_name,
                SessionState::default(),
            )
            .await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.catalog_manager
            .get_default_catalog(SessionState::default())
    }

    fn get_current_catalog(&self) -> String {
        self.current_catalog.clone()
    }

    fn get_current_database(&self) -> String {
        self.current_database.clone()
    }

    fn get_tenant(&self) -> Tenant {
        self.tenant.clone()
    }

    fn get_application_level_data_operator(&self) -> Result<DataOperator> {
        unsupported("table_ctx::get_application_level_data_operator")
    }

    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        self.get_catalog(catalog)
            .await?
            .get_table(&self.tenant, database, table)
            .await
    }

    async fn get_table_with_branch(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        _branch: Option<&str>,
    ) -> Result<Arc<dyn Table>> {
        self.get_table(catalog, database, table).await
    }

    async fn resolve_data_source(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
        _max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        self.get_table_with_branch(catalog, database, table, branch)
            .await
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        _catalog_name: &str,
        _db_name: &str,
        _tbl_name: &str,
        _lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>> {
        Ok(None)
    }

    fn get_temp_table_prefix(&self) -> Result<String> {
        Ok("lite_temp".to_string())
    }

    fn is_temp_table(&self, _catalog_name: &str, _database_name: &str, _table_name: &str) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl TableContextTableManagement for LiteTableContext {
    fn evict_table_from_cache(&self, _catalog: &str, _database: &str, _table: &str) -> Result<()> {
        Ok(())
    }

    fn get_table_meta_timestamps(
        &self,
        _table: &dyn Table,
        _previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps> {
        unsupported("table_ctx::get_table_meta_timestamps")
    }
}

impl TableContextSpillProgress for LiteTableContext {
    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.join_spill_progress.clone()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.group_by_spill_progress.clone()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.aggregate_spill_progress.clone()
    }

    fn get_window_partition_spill_progress(&self) -> Arc<Progress> {
        self.window_partition_spill_progress.clone()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        self.join_spill_progress.get_values()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        self.group_by_spill_progress.get_values()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        self.aggregate_spill_progress.get_values()
    }

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues {
        self.window_partition_spill_progress.get_values()
    }
}

impl TableContextPerf for LiteTableContext {
    fn get_perf_config(&self) -> PerfConfig {
        PerfConfig::default()
    }

    fn set_perf_config(&self, _config: PerfConfig) {}

    fn get_perf_flag(&self) -> bool {
        false
    }

    fn set_perf_flag(&self, _flag: bool) {}

    fn get_nodes_perf(&self) -> Arc<Mutex<HashMap<String, String>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn set_nodes_perf(&self, _node: String, _perf: String) {}

    fn get_perf_events(&self) -> Vec<Vec<PerfEvent>> {
        vec![]
    }

    fn set_perf_events(&self, _event_groups: Vec<Vec<PerfEvent>>) {}

    fn get_running_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        vec![]
    }
}

impl TableContextPartitionStats for LiteTableContext {
    fn get_partition(&self) -> Option<PartInfoPtr> {
        None
    }

    fn get_partitions(&self, _num: usize) -> Vec<PartInfoPtr> {
        vec![]
    }

    fn partition_num(&self) -> usize {
        0
    }

    fn set_partitions(&self, _partitions: Partitions) -> Result<()> {
        Ok(())
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        false
    }

    fn set_can_scan_from_agg_index(&self, _enable: bool) {}

    fn get_enable_sort_spill(&self) -> bool {
        false
    }

    fn set_enable_sort_spill(&self, _enable: bool) {}
}

impl TableContextQueryQueue for LiteTableContext {
    fn get_query_queued_duration(&self) -> Duration {
        *self.queued_duration.read()
    }

    fn set_query_queued_duration(&self, queued_duration: Duration) {
        *self.queued_duration.write() = queued_duration;
    }
}

impl TableContextReadBlockThresholds for LiteTableContext {
    fn get_read_block_thresholds(&self) -> BlockThresholds {
        BlockThresholds::default()
    }

    fn set_read_block_thresholds(&self, _thresholds: BlockThresholds) {}
}

impl TableContextRuntimeFilter for LiteTableContext {
    fn set_runtime_filter_ready(&self, table_index: usize, ready: Arc<RuntimeFilterReady>) {
        self.runtime_filter_ready
            .write()
            .entry(table_index)
            .or_default()
            .push(ready);
    }

    fn get_runtime_filter_ready(&self, table_index: usize) -> Vec<Arc<RuntimeFilterReady>> {
        self.runtime_filter_ready
            .read()
            .get(&table_index)
            .cloned()
            .unwrap_or_default()
    }

    fn clear_runtime_filter(&self) {
        self.runtime_filter_ready.write().clear();
    }

    fn get_runtime_filters(&self, _id: usize) -> Vec<RuntimeFilterEntry> {
        vec![]
    }

    fn get_bloom_runtime_filter_with_id(&self, _id: usize) -> Vec<(String, RuntimeBloomFilter)> {
        vec![]
    }

    fn get_inlist_runtime_filter_with_id(&self, _id: usize) -> Vec<Expr<String>> {
        vec![]
    }

    fn get_min_max_runtime_filter_with_id(&self, _id: usize) -> Vec<Expr<String>> {
        vec![]
    }

    fn runtime_filter_reports(&self) -> HashMap<usize, Vec<RuntimeFilterReport>> {
        HashMap::new()
    }

    fn has_bloom_runtime_filters(&self, _id: usize) -> bool {
        false
    }
}

impl TableContextSegmentLocations for LiteTableContext {
    fn add_written_segment_location(&self, _segment_loc: Location) -> Result<()> {
        Ok(())
    }

    fn clear_written_segment_locations(&self) -> Result<()> {
        Ok(())
    }

    fn get_written_segment_locations(&self) -> Result<Vec<Location>> {
        Ok(vec![])
    }
}

impl TableContextStream for LiteTableContext {
    fn add_streams_ref(&self, _catalog: &str, _database: &str, _stream: &str, _consume: bool) {}

    fn get_consume_streams(&self, _query: bool) -> Result<Vec<Arc<dyn Table>>> {
        Ok(vec![])
    }
}

impl TableContextVariables for LiteTableContext {
    fn set_variable(&self, key: String, value: Scalar) {
        self.variables.write().insert(key, value);
    }

    fn unset_variable(&self, key: &str) {
        self.variables.write().remove(key);
    }

    fn get_variable(&self, key: &str) -> Option<Scalar> {
        self.variables.read().get(key).cloned()
    }

    fn get_all_variables(&self) -> HashMap<String, Scalar> {
        self.variables.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_fields() -> Vec<TableField> {
        vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::UInt64),
        )]
    }

    fn test_udaf_sql() -> &'static str {
        r#"
CREATE OR REPLACE FUNCTION weighted_avg (a INT, b INT) STATE { sum INT, weight INT } RETURNS FLOAT
LANGUAGE javascript AS $$
export function create_state() {
    return {sum: 0, weight: 0};
}
export function accumulate(state, value, weight) {
    state.sum += value * weight;
    state.weight += weight;
    return state;
}
export function retract(state, value, weight) {
    state.sum -= value * weight;
    state.weight -= weight;
    return state;
}
export function merge(state1, state2) {
    state1.sum += state2.sum;
    state1.weight += state2.weight;
    return state1;
}
export function finish(state) {
    return state.sum / state.weight;
}
$$
"#
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_fake_tables_have_unique_ids() -> Result<()> {
        let ctx = LiteTableContext::create().await?;
        ctx.register_table_with_stats("default", "t1", test_fields(), None, HashMap::new())?;
        ctx.register_table_with_stats("default", "t2", test_fields(), None, HashMap::new())?;

        let table1 = ctx
            .default_catalog
            .get_table(&ctx.tenant, "default", "t1")
            .await?;
        let table2 = ctx
            .default_catalog
            .get_table(&ctx.tenant, "default", "t2")
            .await?;

        assert_ne!(
            table1.get_table_info().ident.table_id,
            table2.get_table_info().ident.table_id
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_create_clears_catalog_tables() -> Result<()> {
        let ctx1 = LiteTableContext::create().await?;
        ctx1.register_table_with_stats("default", "t1", test_fields(), None, HashMap::new())?;
        ctx1.default_catalog
            .get_table(&ctx1.tenant, "default", "t1")
            .await?;

        let ctx2 = LiteTableContext::create().await?;
        assert!(
            ctx2.default_catalog
                .get_table(&ctx2.tenant, "default", "t1")
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_register_setup_sql_supports_udaf() -> Result<()> {
        let ctx = LiteTableContext::create().await?;
        ctx.register_table_with_stats(
            "default",
            "t",
            vec![
                TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("b", TableDataType::Number(NumberDataType::UInt64)),
            ],
            None,
            HashMap::new(),
        )?;
        ctx.register_setup_sql(test_udaf_sql()).await?;

        let plan = ctx.bind_sql("SELECT weighted_avg(a, b) FROM t").await?;
        assert!(matches!(plan, Plan::Query { .. }));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_create_clears_registered_udfs() -> Result<()> {
        let ctx1 = LiteTableContext::create().await?;
        ctx1.register_table_with_stats(
            "default",
            "t",
            vec![
                TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("b", TableDataType::Number(NumberDataType::UInt64)),
            ],
            None,
            HashMap::new(),
        )?;
        ctx1.register_setup_sql(test_udaf_sql()).await?;
        ctx1.bind_sql("SELECT weighted_avg(a, b) FROM t").await?;

        let ctx2 = LiteTableContext::create().await?;
        ctx2.register_table_with_stats(
            "default",
            "t",
            vec![
                TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("b", TableDataType::Number(NumberDataType::UInt64)),
            ],
            None,
            HashMap::new(),
        )?;

        assert!(
            ctx2.bind_sql("SELECT weighted_avg(a, b) FROM t")
                .await
                .is_err()
        );
        Ok(())
    }
}
