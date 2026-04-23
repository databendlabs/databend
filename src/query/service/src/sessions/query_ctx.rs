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

// Logs from this module will show up as "[QUERY-CTX] ...".
databend_common_tracing::register_module_tag!("[QUERY-CTX]");

mod context;
mod state;
mod table;

use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_channel::Receiver;
use async_channel::Sender;
use databend_base::uniq_id::GlobalUniq;
#[cfg(feature = "storage-stage")]
use databend_common_ast::ast::CopyIntoTableOptions;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::JoinHandle;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::base::SpillProgress;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::PerfEvent;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::database::Database;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
#[cfg(feature = "storage-stage")]
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReport;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStatsSnapshot;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetrics;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::ContextError;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_catalog::table_context::prelude::*;
use databend_common_component::BroadcastRegistry;
use databend_common_component::CopyState;
use databend_common_component::FragmentId;
use databend_common_component::MutationState;
use databend_common_component::ReadBlockThresholdsState;
use databend_common_component::ResultCacheState;
use databend_common_component::SegmentLocationsState;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
#[cfg(feature = "storage-stage")]
use databend_common_expression::TableDataType;
#[cfg(feature = "storage-stage")]
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::COPY_MAX_FILES_COMMIT_MSG;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::LockGuard;
use databend_common_pipeline::core::PlanProfile;
use databend_common_settings::Settings;
use databend_common_sql::IndexType;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::StorageMetrics;
#[cfg(feature = "storage-stage")]
use databend_common_storage::init_stage_operator;
use databend_common_storages_basic::ResultScan;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_orc::OrcTable;
use databend_common_storages_parquet::ParquetTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
#[cfg(feature = "storage-stage")]
use databend_query_storage_stage_support::StageTable;
use databend_storages_common_blocks::memory::IN_MEMORY_R_CTE_DATA;
use databend_storages_common_blocks::memory::InMemoryDataKey;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_session::drop_table_by_id;
use databend_storages_common_table_meta::meta::SnapshotTimestampValidationContext;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_RECURSIVE_CTE;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use jiff::Zoned;
use jiff::tz::TimeZone;
use log::debug;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::catalogs::Catalog;
use crate::clusters::Cluster;
use crate::clusters::ClusterHelper;
use crate::locks::LockManager;
use crate::pipelines::executor::PipelineExecutor;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::NodePerfCounters;
use crate::servers::http::v1::ClientSessionManager;
use crate::sessions::BuildInfoRef;
use crate::sessions::ProcessInfo;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::query_ctx_shared::MemoryUpdater;
use crate::spillers;
use crate::sql::binder::get_storage_params_from_options;
use crate::storages::Table;

const MYSQL_VERSION: &str = "8.0.90";
const COPIED_FILES_FILTER_BATCH_SIZE: usize = 1000;

pub struct QueryContext {
    version: String,
    mysql_version: String,
    read_block_thresholds: ReadBlockThresholdsState,
    partition_queue: Arc<RwLock<VecDeque<PartInfoPtr>>>,
    shared: Arc<QueryContextShared>,
    query_settings: Arc<Settings>,
    fragment_id: FragmentId,
    // Used by synchronized generate aggregating indexes when new data written.
    written_segment_locs: SegmentLocationsState,
    // Temp table for materialized CTE, first string is the database_name, second string is the table_name
    // All temp tables' catalog is `CATALOG_DEFAULT`, so we don't need to store it.
    m_cte_temp_table: Arc<RwLock<Vec<(String, String)>>>,
}

impl QueryContext {
    // Each table will create a new QueryContext
    // So partition_queue could be independent in each table context
    // see `builder_join.rs` for more details
    pub fn create_from(other: &QueryContext) -> Arc<QueryContext> {
        QueryContext::create_from_shared(other.shared.clone())
    }

    pub fn create_from_shared(shared: Arc<QueryContextShared>) -> Arc<QueryContext> {
        debug!("Creating new QueryContext instance");

        let tenant = GlobalConfig::instance().query.tenant_id.clone();
        let query_settings = Settings::create(tenant);
        Arc::new(QueryContext {
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!("Databend Query {}", shared.version.commit_detail),
            mysql_version: format!("{MYSQL_VERSION}-{}", shared.version.commit_detail),
            shared,
            query_settings,
            fragment_id: Default::default(),
            written_segment_locs: Default::default(),
            read_block_thresholds: Default::default(),
            m_cte_temp_table: Arc::new(RwLock::new(Vec::new())),
        })
    }

    async fn drop_cte_temp_tables(&self, tables: &[(String, String)]) -> Result<()> {
        let registered_tables = tables
            .iter()
            .map(|(db_name, table_name)| {
                (
                    CATALOG_DEFAULT.to_string(),
                    db_name.clone(),
                    table_name.clone(),
                )
            })
            .collect::<Vec<_>>();
        self.drop_registered_cte_temp_tables(&registered_tables)
            .await
    }

    async fn drop_registered_cte_temp_tables(
        &self,
        tables: &[(String, String, String)],
    ) -> Result<()> {
        let temp_tbl_mgr = self.shared.session.session_ctx.temp_tbl_mgr();
        let tenant = self.get_tenant();
        for (catalog_name, db_name, table_name) in tables.iter() {
            let catalog = self.get_catalog(catalog_name).await?;
            let table = self.get_table(catalog_name, db_name, table_name).await?;
            let db = catalog.get_database(&tenant, db_name).await?;
            let temp_prefix = table
                .options()
                .get(OPT_KEY_TEMP_PREFIX)
                .cloned()
                .unwrap_or_default();
            let table_id = table.get_table_info().ident.table_id;
            let is_recursive_cte = table.options().contains_key(OPT_KEY_RECURSIVE_CTE);
            let drop_table_req = DropTableByIdReq {
                if_exists: true,
                tenant: tenant.clone(),
                tb_id: table_id,
                table_name: table_name.to_string(),
                db_id: db.get_db_info().database_id.db_id,
                db_name: db.name().to_string(),
                engine: table.engine().to_string(),
                temp_prefix: temp_prefix.clone(),
            };
            if temp_prefix.is_empty() {
                catalog.drop_table_by_id(drop_table_req).await?;
            } else if drop_table_by_id(temp_tbl_mgr.clone(), drop_table_req)
                .await?
                .is_some()
            {
                ClientSessionManager::instance()
                    .remove_temp_tbl_mgr(temp_prefix.clone(), &temp_tbl_mgr);

                let txn_mgr_ref = self.txn_mgr();
                let mut txn_mgr = txn_mgr_ref.lock();
                txn_mgr.clear_temp_table_by_id(table_id);
            }

            if is_recursive_cte {
                let key = InMemoryDataKey {
                    temp_prefix: if temp_prefix.is_empty() {
                        None
                    } else {
                        Some(temp_prefix.clone())
                    },
                    table_id,
                };
                IN_MEMORY_R_CTE_DATA.write().remove(&key);
            }
        }
        Ok(())
    }

    pub fn get_or_create_logical_recursive_cte_runtime_id(
        &self,
        logical_recursive_cte_id: u32,
    ) -> String {
        let mut ids = self.shared.logical_recursive_cte_runtime_ids.write();
        ids.entry(logical_recursive_cte_id)
            .or_insert_with(GlobalUniq::unique)
            .clone()
    }

    /// Build fuse/system normal table by table info.
    pub fn build_table_by_table_info(
        &self,
        table_info: &TableInfo,
        table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        let catalog_name = table_info.catalog();
        let catalog =
            databend_common_base::runtime::block_on(self.shared.catalog_manager.get_catalog(
                self.get_tenant().tenant_name(),
                catalog_name,
                self.session_state()?,
            ))?;

        let is_default = catalog.info().catalog_type() == CatalogType::Default;
        match (table_args, is_default) {
            (Some(table_args), true) => {
                let default_catalog = self
                    .shared
                    .catalog_manager
                    .get_default_catalog(self.session_state()?)?;
                let udtf_result = databend_common_base::runtime::block_on(async {
                    if let Some(udtf) = UserApiProvider::instance()
                        .get_udf(&self.get_tenant(), &table_info.name)
                        .await?
                        .and_then(|func| func.as_udtf_server())
                    {
                        return default_catalog
                            .transform_udtf_as_table_function(
                                self,
                                &table_args,
                                udtf,
                                &table_info.name,
                            )
                            .map(Some);
                    }
                    Ok(None)
                });
                let table_function = udtf_result.transpose().unwrap_or_else(|| {
                    default_catalog.get_table_function(&table_info.name, table_args)
                })?;
                Ok(table_function.as_table())
            }
            (Some(_), false) => Err(ErrorCode::InvalidArgument(
                "Table args not supported in non-default catalog",
            )),
            // Load table first, if not found, try to load table function.
            (None, true) => {
                let table = catalog.get_table_by_info(table_info);
                if table.is_err() {
                    let Ok(table_function) = catalog
                        .get_table_function(&table_info.name, TableArgs::new_positioned(vec![]))
                    else {
                        // Returns the table error if the table function failed to load.
                        return table;
                    };

                    Ok(table_function.as_table())
                } else {
                    table
                }
            }
            (None, false) => catalog.get_table_by_info(table_info),
        }
    }

    // Build external table by stage info, this is used in:
    // COPY INTO t1 FROM 's3://'
    // 's3://' here is a s3 external stage, and build it to the external table.
    #[cfg(feature = "storage-stage")]
    fn build_external_by_table_info(
        &self,
        table_info: &StageTableInfo,
        _table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        StageTable::try_create(table_info.clone())
    }

    #[cfg(not(feature = "storage-stage"))]
    fn build_external_by_table_info(
        &self,
        _table_info: &StageTableInfo,
        _table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        Err(ErrorCode::Unimplemented(
            "Stage table support is disabled, rebuild with cargo feature 'storage-stage'",
        ))
    }

    #[async_backtrace::framed]
    pub async fn set_current_catalog(&self, new_catalog_name: String) -> Result<()> {
        let _catalog = self.get_catalog(&new_catalog_name).await?;
        self.shared.set_current_catalog(new_catalog_name);

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn set_current_database(
        &self,
        new_database_name: String,
    ) -> Result<Arc<dyn Database>> {
        let tenant_id = self.get_tenant();
        let catalog = self
            .get_catalog(self.get_current_catalog().as_str())
            .await?;
        match catalog.get_database(&tenant_id, &new_database_name).await {
            Ok(db) => {
                self.shared.set_current_database(new_database_name);
                Ok(db)
            }
            Err(_) => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Cannot use database '{}': database does not exist",
                    new_database_name
                )));
            }
        }
    }

    pub fn attach_table(&self, catalog: &str, database: &str, name: &str, table: Arc<dyn Table>) {
        self.shared.attach_table(catalog, database, name, table)
    }

    pub fn broadcast_source_receiver(&self, broadcast_id: u32) -> Receiver<DataBlock> {
        self.shared.broadcast_registry.source_receiver(broadcast_id)
    }

    /// Get a sender to broadcast data
    ///
    /// Note: The channel must be closed by calling close() after data transmission is completed
    pub fn broadcast_source_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        self.shared.broadcast_registry.source_sender(broadcast_id)
    }

    /// A receiver to receive broadcast data
    ///
    /// Note: receive() can be called repeatedly until an Error is returned, indicating
    /// that the upstream channel has been closed
    pub fn broadcast_sink_receiver(&self, broadcast_id: u32) -> Receiver<DataBlock> {
        self.shared.broadcast_registry.sink_receiver(broadcast_id)
    }

    pub fn broadcast_sink_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        self.shared.broadcast_registry.sink_sender(broadcast_id)
    }

    pub fn get_exchange_manager(&self) -> Arc<DataExchangeManager> {
        DataExchangeManager::instance()
    }

    // Get the current session.
    pub fn get_current_session(&self) -> Arc<Session> {
        self.shared.session.clone()
    }

    // Get one session by session id.
    pub fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<Arc<Session>> {
        SessionManager::instance().get_session_by_id(id)
    }

    // Get session id by mysql connection id.
    pub fn get_id_by_mysql_conn_id(self: &Arc<Self>, conn_id: &Option<u32>) -> Option<String> {
        SessionManager::instance().get_id_by_mysql_conn_id(conn_id)
    }

    // Get all the processes list info.
    pub fn get_processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    /// Get the client socket address.
    pub fn get_client_address(&self) -> Option<String> {
        self.shared.session.session_ctx.get_client_host()
    }

    pub fn get_affect(self: &Arc<Self>) -> Option<QueryAffect> {
        self.shared.get_affect()
    }

    pub fn pop_warnings(&self) -> Vec<String> {
        self.shared.pop_warnings()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        self.shared.get_data_metrics()
    }

    pub fn set_affect(self: &Arc<Self>, affect: QueryAffect) {
        self.shared.set_affect(affect)
    }

    pub fn update_init_query_id(&self, id: String) {
        self.shared.spilled_files.write().clear();
        self.shared
            .unload_callbacked
            .store(false, Ordering::Release);
        self.shared.cluster_spill_progress.write().clear();
        *self.shared.init_query_id.write() = id;
    }

    pub fn set_executor(&self, weak_ptr: Arc<PipelineExecutor>) -> Result<()> {
        self.shared.set_executor(weak_ptr)
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        self.shared.attach_stage(attachment);
    }

    pub fn set_ua(&self, ua: String) {
        *self.shared.user_agent.write() = ua;
    }

    pub fn get_ua(&self) -> String {
        let ua = self.shared.user_agent.read();
        ua.clone()
    }

    pub fn get_query_duration_ms(&self) -> i64 {
        let query_start_time = convert_query_log_timestamp(self.shared.created_time);
        let finish_time = *self.shared.finish_time.read();
        let finish_time = finish_time.unwrap_or_else(SystemTime::now);
        let finish_time = convert_query_log_timestamp(finish_time);
        (finish_time - query_start_time) / 1_000
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.shared.created_time
    }

    pub fn set_finish_time(&self, time: SystemTime) {
        *self.shared.finish_time.write() = Some(time)
    }

    pub fn clear_tables_cache(&self) {
        self.shared.clear_tables_cache()
    }

    pub fn incr_spill_progress(&self, file_nums: usize, data_size: usize) {
        let current_id = self.get_cluster().local_id();
        let mut w = self.shared.cluster_spill_progress.write();
        let p = SpillProgress::new(file_nums, data_size);
        w.entry(current_id)
            .and_modify(|stats| {
                stats.incr(&p);
            })
            .or_insert(p);
    }

    pub fn add_spill_file(&self, location: spillers::Location, layout: spillers::Layout) {
        let mut w = self.shared.spilled_files.write();
        w.insert(location, layout);
    }

    pub fn set_cluster_spill_progress(&self, source_target: &str, stats: SpillProgress) {
        if stats.file_nums != 0 {
            let _ = self
                .shared
                .cluster_spill_progress
                .write()
                .insert(source_target.to_string(), stats);
        }
    }

    pub fn get_spill_file_stats(&self, node_id: Option<String>) -> SpillProgress {
        let r = self.shared.cluster_spill_progress.read();
        let node_id = node_id.unwrap_or(self.get_cluster().local_id());

        r.get(&node_id).cloned().unwrap_or(SpillProgress::default())
    }

    pub fn get_total_spill_progress(&self) -> SpillProgress {
        let r = self.shared.cluster_spill_progress.read();
        let mut total = SpillProgress::default();
        for (_, stats) in r.iter() {
            total.incr(stats);
        }
        total
    }

    pub fn get_spill_layout(&self, location: &spillers::Location) -> Option<spillers::Layout> {
        let r = self.shared.spilled_files.read();
        r.get(location).cloned()
    }

    pub fn get_spilled_files(&self) -> Vec<spillers::Location> {
        let r = self.shared.spilled_files.read();
        r.keys().cloned().collect()
    }

    pub fn query_tenant_spill_prefix(&self) -> String {
        let tenant = self.get_tenant();
        format!("_query_spill/{}", tenant.tenant_name())
    }

    pub fn query_id_spill_prefix(&self) -> String {
        let tenant = self.get_tenant();
        let node_index = self.get_cluster().ordered_index();
        format!(
            "_query_spill/{}/{}_{}",
            tenant.tenant_name(),
            self.get_id(),
            node_index
        )
    }

    #[async_backtrace::framed]
    async fn get_table_from_shared(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        if branch.is_some() {
            // Legacy experimental table refs were removed. The parser still accepts
            // `<db>.<table>/<branch>` so the upcoming redesign can reuse the syntax,
            // but any remaining runtime entry point should still return a user-facing
            // error instead of looking like an internal server bug.
            return Err(ErrorCode::Unimplemented(
                "Legacy experimental table refs were removed: table branch references are reserved for a future redesign and are intentionally rejected.",
            ));
        }

        let table = self
            .shared
            .get_table(catalog, database, table, max_batch_size)
            .await?;
        // the better place to do this is in the QueryContextShared::get_table() method,
        // but there is no way to access dyn TableContext.
        let table: Arc<dyn Table> = match table.engine() {
            "ICEBERG" => {
                let sp = get_storage_params_from_options(self, table.options()).await?;
                let mut info = table.get_table_info().to_owned();
                info.meta.storage_params = Some(sp);
                IcebergTable::try_create(info.to_owned())?.into()
            }
            "DELTA" => {
                let sp = get_storage_params_from_options(self, table.options()).await?;
                let mut info = table.get_table_info().to_owned();
                info.meta.storage_params = Some(sp);
                DeltaTable::try_create(info.to_owned())?.into()
            }
            _ => table,
        };
        Ok(table)
    }

    pub fn mark_unload_callbacked(&self) -> bool {
        self.shared
            .unload_callbacked
            .fetch_or(true, Ordering::SeqCst)
    }

    pub fn should_log_runtime_filters(&self) -> bool {
        self.shared.runtime_filter_state.should_log()
    }

    pub fn log_runtime_filter_stats(&self) {
        struct FilterLogEntry {
            filter_id: usize,
            probe_expr: String,
            bloom_column: Option<String>,
            has_bloom: bool,
            has_inlist: bool,
            has_min_max: bool,
            has_spatial: bool,
            stats: RuntimeFilterStatsSnapshot,
            build_rows: usize,
            build_table_rows: Option<u64>,
            enabled: bool,
        }

        let runtime_filters = self.shared.runtime_filter_state.runtime_filter_reports();
        let mut snapshots: Vec<(IndexType, Vec<FilterLogEntry>)> = Vec::new();
        for (scan_id, reports) in runtime_filters {
            if reports.is_empty() {
                continue;
            }

            let filters = self
                .shared
                .runtime_filter_state
                .get_runtime_filters(scan_id)
                .into_iter()
                .map(|entry| FilterLogEntry {
                    filter_id: entry.id,
                    probe_expr: entry.probe_expr.sql_display(),
                    bloom_column: entry.bloom.as_ref().map(|bloom| bloom.column_name.clone()),
                    has_bloom: entry.bloom.is_some(),
                    has_inlist: entry.inlist.is_some(),
                    has_min_max: entry.min_max.is_some(),
                    has_spatial: entry.spatial.is_some(),
                    stats: entry.stats.snapshot(),
                    build_rows: entry.build_rows,
                    build_table_rows: entry.build_table_rows,
                    enabled: entry.enabled,
                })
                .collect::<Vec<_>>();

            if !filters.is_empty() {
                snapshots.push((scan_id, filters));
            }
        }

        if snapshots.is_empty() {
            return;
        }

        if !self.should_log_runtime_filters() {
            return;
        }

        let query_id = self.get_id();

        for (scan_id, filters) in snapshots {
            let mut filter_nodes = Vec::new();
            for filter in filters {
                let FilterLogEntry {
                    filter_id,
                    probe_expr,
                    bloom_column,
                    has_bloom,
                    has_inlist,
                    has_min_max,
                    has_spatial,
                    stats,
                    build_rows,
                    build_table_rows,
                    enabled,
                } = filter;

                let mut types = Vec::new();
                if has_bloom {
                    types.push("bloom");
                }
                if has_inlist {
                    types.push("inlist");
                }
                if has_min_max {
                    types.push("min_max");
                }
                if has_spatial {
                    types.push("spatial");
                }
                let type_text = if types.is_empty() {
                    "none".to_string()
                } else {
                    types.join(",")
                };

                let mut detail_children = vec![
                    FormatTreeNode::new(format!("probe expr: {}", probe_expr)),
                    FormatTreeNode::new(format!("types: [{}]", type_text)),
                    FormatTreeNode::new(format!("enabled: {}", enabled)),
                    FormatTreeNode::new(format!("build rows: {}", build_rows)),
                    FormatTreeNode::new(format!(
                        "build table rows: {}",
                        build_table_rows
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                ];

                if let Some(column) = bloom_column {
                    detail_children.push(FormatTreeNode::new(format!("bloom column: {}", column)));
                }

                if has_bloom {
                    detail_children.push(FormatTreeNode::new(format!(
                        "bloom rows filtered: {}",
                        stats.bloom_rows_filtered
                    )));
                    detail_children.push(FormatTreeNode::new(format!(
                        "bloom time: {:?}",
                        Duration::from_nanos(stats.bloom_time_ns)
                    )));
                }

                if has_inlist || has_min_max {
                    detail_children.push(FormatTreeNode::new(format!(
                        "inlist/min-max time: {:?}",
                        Duration::from_nanos(stats.inlist_min_max_time_ns)
                    )));
                    detail_children.push(FormatTreeNode::new(format!(
                        "min-max rows filtered: {}",
                        stats.min_max_rows_filtered
                    )));
                    detail_children.push(FormatTreeNode::new(format!(
                        "min-max partitions pruned: {}",
                        stats.min_max_partitions_pruned
                    )));
                }
                if has_spatial {
                    detail_children.push(FormatTreeNode::new(format!(
                        "spatial time: {:?}",
                        Duration::from_nanos(stats.spatial_time_ns)
                    )));
                    detail_children.push(FormatTreeNode::new(format!(
                        "spatial rows filtered: {}",
                        stats.spatial_rows_filtered
                    )));
                    detail_children.push(FormatTreeNode::new(format!(
                        "spatial partitions pruned: {}",
                        stats.spatial_partitions_pruned
                    )));
                }

                filter_nodes.push(FormatTreeNode::with_children(
                    format!("filter id:{}", filter_id),
                    detail_children,
                ));
            }

            if filter_nodes.is_empty() {
                continue;
            }

            let root = FormatTreeNode::with_children(format!("Scan {}", scan_id), vec![
                FormatTreeNode::with_children("runtime filters".to_string(), filter_nodes),
            ]);

            match root.format_pretty() {
                Ok(text) => info!(
                    "runtime filter stats (query_id={}, scan_id={}):\n{}",
                    query_id, scan_id, text
                ),
                Err(err) => info!(
                    "runtime filter stats (query_id={}, scan_id={}): failed to format: {}",
                    query_id, scan_id, err
                ),
            }
        }
    }

    pub fn unload_spill_meta(&self) {
        const SPILL_META_SUFFIX: &str = ".list";
        let r = self.shared.spilled_files.read();
        let mut remote_spill_files = r
            .iter()
            .map(|(k, _)| k)
            .filter_map(|l| match l {
                spillers::Location::Remote(r) => Some(r),
                _ => None,
            })
            .cloned()
            .collect::<Vec<_>>();

        drop(r);

        if remote_spill_files.is_empty() {
            return;
        }

        {
            let mut w = self.shared.spilled_files.write();
            w.clear();
        }

        let location_prefix = self.query_tenant_spill_prefix();
        let node_idx = self.get_cluster().ordered_index();
        let meta_path = format!(
            "{}/{}_{}{}",
            location_prefix,
            self.get_id(),
            node_idx,
            SPILL_META_SUFFIX
        );
        let op = DataOperator::instance().spill_operator();
        // append dir and current meta
        remote_spill_files.push(meta_path.clone());
        remote_spill_files.push(format!(
            "{}/{}_{}/",
            location_prefix,
            self.get_id(),
            node_idx
        ));
        let joined_contents = remote_spill_files.join("\n");

        if let Err(e) = GlobalIORuntime::instance().block_on::<(), (), _>(async move {
            let _ = op.write(&meta_path, joined_contents).await?;
            Ok(())
        }) {
            log::error!("Failed to create spill meta file: {}", e);
        }
    }

    pub fn get_query_memory_tracking(&self) -> Option<Arc<MemStat>> {
        self.shared.get_query_memory_tracking()
    }

    pub fn set_query_memory_tracking(&self, mem_stat: Option<Arc<MemStat>>) {
        self.shared.set_query_memory_tracking(mem_stat)
    }

    pub fn get_node_memory_updater(&self, node: &str) -> Arc<MemoryUpdater> {
        self.shared.get_node_memory_updater(node)
    }

    pub fn get_node_peek_memory_usage(&self) -> HashMap<String, usize> {
        self.shared.get_nodes_peek_memory_usage()
    }

    pub fn set_nodes_perf_counters(&self, node: String, counters: NodePerfCounters) {
        self.shared.set_nodes_perf_counters(node, counters);
    }

    pub fn get_nodes_perf_counters(&self) -> HashMap<String, NodePerfCounters> {
        self.shared.get_nodes_perf_counters()
    }

    pub fn collect_local_perf_counters(&self, node_id: String) {
        self.shared.collect_local_perf_counters(node_id);
    }

    pub fn clear_table_meta_timestamps_cache(&self) {
        self.shared.table_meta_timestamps.lock().clear();
    }

    pub fn get_materialized_cte_senders(
        &self,
        cte_name: &str,
        cte_ref_count: usize,
        channel_size: Option<usize>,
    ) -> Vec<Sender<DataBlock>> {
        let mut senders = vec![];
        let mut receivers = vec![];
        for _ in 0..cte_ref_count {
            let (sender, receiver) = if let Some(channel_size) = channel_size {
                async_channel::bounded(channel_size)
            } else {
                async_channel::unbounded()
            };
            senders.push(sender);
            receivers.push(receiver);
        }
        self.shared
            .materialized_cte_receivers
            .lock()
            .insert(cte_name.to_string(), receivers);
        senders
    }

    pub fn get_materialized_cte_receiver(&self, cte_name: &str) -> Receiver<DataBlock> {
        let mut receivers = self.shared.materialized_cte_receivers.lock();
        let receivers = receivers.get_mut(cte_name).unwrap();
        receivers.pop().unwrap()
    }
}

impl QueryContext {
    /// Tries to spawn a new asynchronous task, returning a JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    #[track_caller]
    pub fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let runtime = self.shared.try_get_runtime()?;
        Ok(runtime.spawn(task))
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.get_current_user())
    }
}

pub fn convert_query_log_timestamp(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 0))
        .as_micros() as i64
}
