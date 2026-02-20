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

use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_channel::Receiver;
use async_channel::Sender;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
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
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::database::Database;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
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
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
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
use databend_common_meta_app::schema::BranchInfo;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::InputError;
use databend_common_pipeline::core::LockGuard;
use databend_common_pipeline::core::PlanProfile;
use databend_common_settings::Settings;
use databend_common_sql::IndexType;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::StorageMetrics;
use databend_common_storage::init_stage_operator;
use databend_common_storages_basic::ResultScan;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_orc::OrcTable;
use databend_common_storages_parquet::ParquetTable;
use databend_common_storages_stage::StageTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_session::drop_table_by_id;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SnapshotTimestampValidationContext;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
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
const CLICKHOUSE_VERSION: &str = "8.12.14";
const COPIED_FILES_FILTER_BATCH_SIZE: usize = 1000;

pub struct QueryContext {
    version: String,
    mysql_version: String,
    clickhouse_version: String,
    block_threshold: Arc<RwLock<BlockThresholds>>,
    partition_queue: Arc<RwLock<VecDeque<PartInfoPtr>>>,
    shared: Arc<QueryContextShared>,
    query_settings: Arc<Settings>,
    fragment_id: Arc<AtomicUsize>,
    // Used by synchronized generate aggregating indexes when new data written.
    written_segment_locs: Arc<RwLock<HashSet<Location>>>,
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
            clickhouse_version: CLICKHOUSE_VERSION.to_string(),
            shared,
            query_settings,
            fragment_id: Arc::new(AtomicUsize::new(0)),
            written_segment_locs: Default::default(),
            block_threshold: Default::default(),
            m_cte_temp_table: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Build fuse/system normal table by table info.
    pub fn build_table_by_table_info(
        &self,
        table_info: &TableInfo,
        branch_info: &Option<BranchInfo>,
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
        let mut table = match (table_args, is_default) {
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
        }?;

        if let Some(branch_info) = branch_info {
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            table = fuse_table.with_branch_info(branch_info.clone())?;
        }
        Ok(table)
    }

    // Build external table by stage info, this is used in:
    // COPY INTO t1 FROM 's3://'
    // 's3://' here is a s3 external stage, and build it to the external table.
    fn build_external_by_table_info(
        &self,
        table_info: &StageTableInfo,
        _table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        StageTable::try_create(table_info.clone())
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
        self.shared.broadcast_source_receiver(broadcast_id)
    }

    /// Get a sender to broadcast data
    ///
    /// Note: The channel must be closed by calling close() after data transmission is completed
    pub fn broadcast_source_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        self.shared.broadcast_source_sender(broadcast_id)
    }

    /// A receiver to receive broadcast data
    ///
    /// Note: receive() can be called repeatedly until an Error is returned, indicating
    /// that the upstream channel has been closed
    pub fn broadcast_sink_receiver(&self, broadcast_id: u32) -> Receiver<DataBlock> {
        self.shared.broadcast_sink_receiver(broadcast_id)
    }

    pub fn broadcast_sink_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        self.shared.broadcast_sink_sender(broadcast_id)
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

        if let Some(branch) = branch {
            if !self
                .get_settings()
                .get_enable_experimental_table_ref()
                .unwrap_or_default()
            {
                return Err(ErrorCode::Unimplemented(
                    "Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature",
                ));
            }
            // TODO(zhyass): Branch are currently not allowed inside a transaction.
            if self.txn_mgr().lock().is_active() {
                return Err(ErrorCode::StorageUnsupported(
                    "Branch operations are not supported within an active transaction",
                ));
            }
            table.with_branch(branch)
        } else {
            Ok(table)
        }
    }

    pub fn mark_unload_callbacked(&self) -> bool {
        self.shared
            .unload_callbacked
            .fetch_or(true, Ordering::SeqCst)
    }

    pub fn should_log_runtime_filters(&self) -> bool {
        !self
            .shared
            .runtime_filter_logged
            .swap(true, Ordering::SeqCst)
    }

    pub fn log_runtime_filter_stats(&self) {
        struct FilterLogEntry {
            filter_id: usize,
            probe_expr: String,
            bloom_column: Option<String>,
            has_bloom: bool,
            has_inlist: bool,
            has_min_max: bool,
            stats: RuntimeFilterStatsSnapshot,
            build_rows: usize,
            build_table_rows: Option<u64>,
            enabled: bool,
        }

        let runtime_filters = self.shared.runtime_filters.read();
        let mut snapshots: Vec<(IndexType, Vec<FilterLogEntry>)> = Vec::new();
        for (scan_id, info) in runtime_filters.iter() {
            if info.filters.is_empty() {
                continue;
            }

            let mut filters = Vec::with_capacity(info.filters.len());
            for entry in &info.filters {
                filters.push(FilterLogEntry {
                    filter_id: entry.id,
                    probe_expr: entry.probe_expr.sql_display(),
                    bloom_column: entry.bloom.as_ref().map(|bloom| bloom.column_name.clone()),
                    has_bloom: entry.bloom.is_some(),
                    has_inlist: entry.inlist.is_some(),
                    has_min_max: entry.min_max.is_some(),
                    stats: entry.stats.snapshot(),
                    build_rows: entry.build_rows,
                    build_table_rows: entry.build_table_rows,
                    enabled: entry.enabled,
                });
            }

            if !filters.is_empty() {
                snapshots.push((*scan_id, filters));
            }
        }
        drop(runtime_filters);

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

#[async_trait::async_trait]
impl TableContext for QueryContext {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        match &plan.source_info {
            DataSourceInfo::TableSource(extend_info) => self.build_table_by_table_info(
                &extend_info.table_info,
                &extend_info.branch_info,
                plan.tbl_args.clone(),
            ),
            DataSourceInfo::StageSource(stage_info) => {
                self.build_external_by_table_info(stage_info, plan.tbl_args.clone())
            }
            DataSourceInfo::ParquetSource(table_info) => ParquetTable::from_info(table_info),
            DataSourceInfo::ResultScanSource(table_info) => ResultScan::from_info(table_info),
            DataSourceInfo::ORCSource(table_info) => OrcTable::from_info(table_info),
        }
    }

    fn incr_total_scan_value(&self, value: ProgressValues) {
        self.shared.total_scan_values.as_ref().incr(&value);
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        self.shared.total_scan_values.as_ref().get_values()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.shared.write_progress.clone()
    }

    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.shared.join_spill_progress.clone()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.shared.agg_spill_progress.clone()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.shared.group_by_spill_progress.clone()
    }

    fn get_window_partition_spill_progress(&self) -> Arc<Progress> {
        self.shared.window_partition_spill_progress.clone()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        self.shared.write_progress.as_ref().get_values()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        self.shared.join_spill_progress.as_ref().get_values()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        self.shared.agg_spill_progress.as_ref().get_values()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        self.shared.group_by_spill_progress.as_ref().get_values()
    }

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues {
        self.shared
            .window_partition_spill_progress
            .as_ref()
            .get_values()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }

    fn get_status_info(&self) -> String {
        let status = self.shared.status.read();
        status.clone()
    }

    fn set_status_info(&self, info: &str) {
        // set_status_info is not called frequently, so we can use info! here.
        // make it easier to match the status to the log.
        info!("Status update: {}", info);
        let mut status = self.shared.status.write();
        *status = info.to_string();
    }

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics {
        self.shared.get_query_cache_metrics()
    }

    fn get_partition(&self) -> Option<PartInfoPtr> {
        if let Some(part) = self.partition_queue.write().pop_front() {
            Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, 1);
            return Some(part);
        }

        None
    }

    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr> {
        let mut res = Vec::with_capacity(num);
        let mut queue_guard = self.partition_queue.write();

        for _index in 0..num {
            match queue_guard.pop_front() {
                None => {
                    break;
                }
                Some(part) => {
                    res.push(part);
                }
            };
        }

        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, res.len());

        res
    }

    // Update the context partition pool from the pipeline builder.
    fn set_partitions(&self, partitions: Partitions) -> Result<()> {
        let mut partition_queue = self.partition_queue.write();

        partition_queue.clear();
        for part in partitions.partitions {
            partition_queue.push_back(part);
        }
        Ok(())
    }

    fn partition_num(&self) -> usize {
        self.partition_queue.read().len()
    }

    fn add_partitions_sha(&self, s: String) {
        let mut shas = self.shared.partitions_shas.write();
        // Avoid duplicate invalidation keys when the same table is scanned multiple times.
        // Example: `SELECT * FROM t WHERE a > (SELECT MIN(a) FROM t)`
        // In this query, table `t` appears twice:
        // 1. The main TableScan adds SHA via table_read_plan.rs
        // 2. The scalar subquery is optimized to DummyTableScan, which also
        //    adds an invalidation key for its source table `t` via build_dummy_table_scan
        // Without deduplication, the same key would appear twice in the list.
        if !shas.contains(&s) {
            shas.push(s);
        }
    }

    fn get_partitions_shas(&self) -> Vec<String> {
        let mut sha = self.shared.partitions_shas.read().clone();
        // Sort to make sure the keys are stable for the same query.
        sha.sort();
        sha
    }

    fn add_cache_key_extra(&self, extra: String) {
        let mut extras = self.shared.cache_key_extras.write();
        if !extras.contains(&extra) {
            extras.push(extra);
        }
    }

    fn get_cache_key_extras(&self) -> Vec<String> {
        let mut extras = self.shared.cache_key_extras.read().clone();
        extras.sort();
        extras
    }

    fn get_cacheable(&self) -> bool {
        self.shared.cacheable.load(Ordering::Acquire)
    }

    fn set_cacheable(&self, cacheable: bool) {
        self.shared.cacheable.store(cacheable, Ordering::Release);
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        self.shared.can_scan_from_agg_index.load(Ordering::Acquire)
    }

    fn set_can_scan_from_agg_index(&self, enable: bool) {
        self.shared
            .can_scan_from_agg_index
            .store(enable, Ordering::Release);
    }

    fn get_enable_sort_spill(&self) -> bool {
        self.shared.enable_sort_spill.load(Ordering::Acquire)
    }

    fn get_enable_auto_analyze(&self) -> bool {
        self.shared.enable_auto_analyze.load(Ordering::Acquire)
    }

    fn set_enable_auto_analyze(&self, enable: bool) {
        self.shared
            .enable_auto_analyze
            .store(enable, Ordering::Release);
    }

    fn set_enable_sort_spill(&self, enable: bool) {
        self.shared
            .enable_sort_spill
            .store(enable, Ordering::Release);
    }

    // get a hint at the number of blocks that need to be compacted.
    fn get_compaction_num_block_hint(&self, table_name: &str) -> u64 {
        self.shared
            .num_fragmented_block_hint
            .lock()
            .get(table_name)
            .copied()
            .unwrap_or_default()
    }

    // set a hint at the number of blocks that need to be compacted.
    fn set_compaction_num_block_hint(&self, table_name: &str, hint: u64) {
        let old = self
            .shared
            .num_fragmented_block_hint
            .lock()
            .insert(table_name.to_string(), hint);
        info!(
            "Set compaction hint for table '{}': old={:?}, new={}",
            table_name, old, hint
        );
    }

    fn attach_query_str(&self, kind: QueryKind, query: String) {
        self.shared.attach_query_str(kind, query);
    }

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String) {
        self.shared.attach_query_hash(text_hash, parameterized_hash);
    }

    /// Get the session running query.
    fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    fn get_query_parameterized_hash(&self) -> String {
        self.shared.get_query_parameterized_hash()
    }

    fn get_query_text_hash(&self) -> String {
        self.shared.get_query_text_hash()
    }

    fn get_fragment_id(&self) -> usize {
        self.fragment_id.fetch_add(1, Ordering::Release)
    }

    #[async_backtrace::framed]
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_catalog(
                self.get_tenant().tenant_name(),
                catalog_name.as_ref(),
                self.session_state()?,
            )
            .await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_default_catalog(self.session_state()?)
    }

    fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }

    fn get_current_catalog(&self) -> String {
        self.shared.get_current_catalog()
    }

    fn check_aborting(&self) -> Result<(), ContextError> {
        self.shared.check_aborting()
    }

    fn get_abort_notify(&self) -> Arc<WatchNotify> {
        self.shared.abort_notify.clone()
    }

    fn get_error(&self) -> Option<ErrorCode<ContextError>> {
        self.shared.get_error()
    }

    fn push_warning(&self, warn: String) {
        self.shared.push_warning(warn)
    }

    fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        self.shared.get_current_role()
    }

    fn get_secondary_roles(&self) -> Option<Vec<String>> {
        self.shared.get_secondary_roles()
    }

    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_available_roles().await
    }

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_effective_roles().await
    }

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()> {
        self.get_current_session()
            .validate_privilege(object, privilege, check_current_role_only)
            .await
    }

    fn get_current_session_id(&self) -> String {
        self.get_current_session().get_id()
    }

    fn get_current_client_session_id(&self) -> Option<String> {
        self.get_current_session().get_client_session_id()
    }

    async fn get_visibility_checker(
        &self,
        ignore_ownership: bool,
        object: Object,
    ) -> Result<GrantObjectVisibilityChecker> {
        self.shared
            .session
            .get_visibility_checker(ignore_ownership, object)
            .await
    }

    fn get_fuse_version(&self) -> String {
        let session = self.get_current_session();
        match session.get_type() {
            SessionType::ClickHouseHttpHandler => self.clickhouse_version.clone(),
            SessionType::MySQL => self.mysql_version.clone(),
            _ => self.version.clone(),
        }
    }

    fn get_version(&self) -> BuildInfoRef {
        self.shared.version
    }

    fn get_input_format_settings(&self) -> Result<InputFormatSettings> {
        self.get_settings().get_input_format_settings()
    }

    fn get_output_format_settings(&self) -> Result<OutputFormatSettings> {
        self.get_settings().get_output_format_settings()
    }

    fn get_tenant(&self) -> Tenant {
        self.shared.get_tenant()
    }

    fn get_query_kind(&self) -> QueryKind {
        self.shared.get_query_kind()
    }

    fn get_function_context(&self) -> Result<FunctionContext> {
        let settings = self.get_settings();

        let tz_string = settings.get_timezone()?;
        let tz = TimeZone::get(&tz_string).map_err(|e| {
            ErrorCode::InvalidTimezone(format!("Timezone validation failed: {}", e))
        })?;
        let now = Zoned::now().with_time_zone(TimeZone::UTC);
        let numeric_cast_option = settings.get_numeric_cast_option()?;
        let rounding_mode = numeric_cast_option.as_str() == "rounding";
        let disable_variant_check = settings.get_disable_variant_check()?;
        let geometry_output_format = settings.get_geometry_output_format()?;
        let binary_input_format = settings.get_binary_input_format()?;
        let binary_output_format = settings.get_binary_output_format()?;
        let parse_datetime_ignore_remainder = settings.get_parse_datetime_ignore_remainder()?;
        let enable_strict_datetime_parser = settings.get_enable_strict_datetime_parser()?;
        let week_start = settings.get_week_start()? as u8;
        let date_format_style = settings.get_date_format_style()?;
        let random_function_seed = settings.get_random_function_seed()?;

        Ok(FunctionContext {
            now,
            tz,
            rounding_mode,
            disable_variant_check,
            enable_selector_executor: settings.get_enable_selector_executor()?,

            geometry_output_format,
            binary_input_format,
            binary_output_format,
            parse_datetime_ignore_remainder,
            enable_strict_datetime_parser,
            random_function_seed,
            week_start,
            date_format_style,
        })
    }

    fn get_connection_id(&self) -> String {
        self.shared.get_connection_id()
    }

    // subquery level
    fn get_settings(&self) -> Arc<Settings> {
        // query level change
        if self.shared.query_settings.is_changed()
            && self.shared.query_settings.query_level_change()
        {
            let shared_settings = self.shared.query_settings.changes();
            // if has session level change, should not cover query level change
            if self.get_session_settings().is_changed() {
                for r in self.get_session_settings().changes().iter() {
                    if !self.shared.query_settings.changes().contains_key(r.key()) {
                        shared_settings.insert(r.key().clone(), r.value().clone());
                    }
                }
                unsafe {
                    self.query_settings.unchecked_apply_changes(shared_settings);
                }
            } else {
                unsafe {
                    self.query_settings.unchecked_apply_changes(shared_settings);
                }
            }
        } else {
            unsafe {
                // apply session level changes
                self.query_settings
                    .unchecked_apply_changes(self.get_session_settings().changes())
            }
        }

        self.query_settings.clone()
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        self.shared.query_settings.clone()
    }

    fn get_session_settings(&self) -> Arc<Settings> {
        // get session settings from query shared
        self.shared.get_settings()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    fn set_cluster(&self, cluster: Arc<Cluster>) {
        self.shared.set_cluster(cluster)
    }

    // Get all the processes list info.
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    fn get_running_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        let mut all = SessionManager::instance().get_query_execution_stats();
        all.extend(DataExchangeManager::instance().get_query_execution_stats());
        all
    }

    fn get_queued_queries(&self) -> Vec<ProcessInfo> {
        let queries = QueriesQueueManager::instance()
            .list()
            .iter()
            .map(|x| x.query_id.clone())
            .collect::<HashSet<_>>();

        SessionManager::instance()
            .processes_info()
            .into_iter()
            .filter(|x| match &x.current_query_id {
                None => false,
                Some(query_id) => queries.contains(query_id),
            })
            .collect::<Vec<_>>()
    }

    // Get Stage Attachment.
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.shared.get_stage_attachment()
    }

    fn get_last_query_id(&self, index: i32) -> Option<String> {
        self.shared.session.session_ctx.get_last_query_id(index)
    }

    fn get_query_id_history(&self) -> HashSet<String> {
        self.shared.session.session_ctx.get_query_id_history()
    }

    fn get_result_cache_key(&self, query_id: &str) -> Option<String> {
        self.shared
            .session
            .session_ctx
            .get_query_result_cache_key(query_id)
    }

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String) {
        self.shared
            .session
            .session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }

    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        self.shared.get_on_error_map()
    }

    fn set_on_error_map(&self, map: Arc<DashMap<String, HashMap<u16, InputError>>>) {
        self.shared.set_on_error_map(map);
    }

    fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.shared.get_on_error_mode()
    }
    fn set_on_error_mode(&self, mode: OnErrorMode) {
        self.shared.set_on_error_mode(mode)
    }

    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        if let Some(on_error_map) = self.get_on_error_map() {
            if on_error_map.is_empty() {
                return None;
            }
            let mut m = HashMap::<String, ErrorCode>::new();
            on_error_map
                .iter()
                .for_each(|x: RefMulti<String, HashMap<u16, InputError>>| {
                    if let Some(max_v) = x.value().iter().max_by_key(|entry| entry.1.num) {
                        m.insert(x.key().to_string(), max_v.1.err.clone());
                    }
                });
            return Some(m);
        }
        None
    }

    /// Get the storage data accessor operator from the session manager.
    /// Note that this is the application level data accessor, which may be different from
    /// the table level data accessor (e.g., table with customized storage parameters).
    fn get_application_level_data_operator(&self) -> Result<DataOperator> {
        Ok(self.shared.data_operator.clone())
    }

    #[async_backtrace::framed]
    async fn get_file_format(&self, name: &str) -> Result<FileFormatParams> {
        match StageFileFormatType::from_str(name) {
            Ok(typ) => FileFormatParams::default_by_type(typ),
            Err(_) => {
                let user_mgr = UserApiProvider::instance();
                let tenant = self.get_tenant();
                Ok(user_mgr
                    .get_file_format(&tenant, name)
                    .await?
                    .file_format_params)
            }
        }
    }
    async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        if self
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            let visibility_checker = self
                .get_visibility_checker(false, Object::Connection)
                .await?;
            if !visibility_checker.check_connection_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege AccessConnection is required on connection {name} for user {}",
                    &self.get_current_user()?.identity().display(),
                )));
            }
        }
        self.shared.get_connection(name).await
    }

    /// Fetch a Table by db and table name.
    ///
    /// It guaranteed to return a consistent result for multiple calls, in a same query.
    /// E.g.:
    /// ```sql
    /// SELECT * FROM (SELECT * FROM db.table_name) as subquery_1, (SELECT * FROM db.table_name) AS subquery_2
    /// ```
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        // Queries to non-internal system_history databases require license checks to be enabled.
        if database.eq_ignore_ascii_case("system_history")
            && ThreadTracker::capture_log_settings().is_none()
        {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.get_license_key(), Feature::SystemHistory)?;

            if GlobalConfig::instance().log.history.is_invisible(table) {
                return Err(ErrorCode::InvalidArgument(format!(
                    "history table `{}` is configured as invisible",
                    table
                )));
            }
        }

        let batch_size = self.get_settings().get_stream_consume_batch_size_hint()?;
        self.get_table_from_shared(catalog, database, table, None, batch_size)
            .await
    }

    fn evict_table_from_cache(&self, catalog: &str, database: &str, table: &str) -> Result<()> {
        self.shared.evict_table_from_cache(catalog, database, table)
    }

    #[async_backtrace::framed]
    async fn get_table_with_batch(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        let final_batch_size = match max_batch_size {
            Some(v) => {
                // use the batch size specified in the statement
                Some(v)
            }
            None => {
                if let Some(v) = self.get_settings().get_stream_consume_batch_size_hint()? {
                    info!("Overriding stream max_batch_size with setting value: {}", v);
                    Some(v)
                } else {
                    None
                }
            }
        };

        let table = self
            .get_table_from_shared(catalog, database, table, branch, final_batch_size)
            .await?;

        if table.is_stream() {
            let stream = StreamTable::try_from_table(table.as_ref())?;
            let actual_batch_limit = stream.max_batch_size();
            if actual_batch_limit != final_batch_size {
                return Err(ErrorCode::StorageUnsupported(format!(
                    "Stream batch size must be consistent within transaction: actual={:?}, requested={:?}",
                    actual_batch_limit, final_batch_size
                )));
            }
        } else if max_batch_size.is_some() {
            return Err(ErrorCode::StorageUnsupported(
                "MAX_BATCH_SIZE parameter only supported for STREAM tables",
            ));
        }
        Ok(table)
    }

    #[async_backtrace::framed]
    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[StageFileInfo],
        path_prefix: Option<String>,
        max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles> {
        if files.is_empty() {
            info!("No files to filter for copy operation");
            return Ok(FilteredCopyFiles::default());
        }

        let collect_duplicated_files = self
            .get_settings()
            .get_enable_purge_duplicated_files_in_copy()?;

        let tenant = self.get_tenant();
        let catalog = self.get_catalog(catalog_name).await?;
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();

        let mut result_size: usize = 0;
        let max_files = max_files.unwrap_or(usize::MAX);
        let batch_size = min(COPIED_FILES_FILTER_BATCH_SIZE, max_files);

        let mut files_to_copy = Vec::with_capacity(files.len());
        let mut duplicated_files = Vec::with_capacity(files.len());

        for chunk in files.chunks(batch_size) {
            let files = chunk
                .iter()
                .map(|v| {
                    if let Some(p) = &path_prefix {
                        format!("{}{}", p, v.path)
                    } else {
                        v.path.clone()
                    }
                })
                .collect::<Vec<_>>();
            let req = GetTableCopiedFileReq {
                table_id,
                files: files.clone(),
            };
            let start_request = Instant::now();
            let copied_files = catalog
                .get_table_copied_file_info(&tenant, database_name, req)
                .await?
                .file_info;

            metrics_inc_copy_filter_out_copied_files_request_milliseconds(
                Instant::now().duration_since(start_request).as_millis() as u64,
            );
            // Colored
            for (file, key) in chunk.iter().zip(files.iter()) {
                if !copied_files.contains_key(key) {
                    files_to_copy.push(file.clone());
                    result_size += 1;
                    if result_size == max_files {
                        return Ok(FilteredCopyFiles {
                            files_to_copy,
                            duplicated_files,
                        });
                    }
                    if result_size > COPY_MAX_FILES_PER_COMMIT {
                        return Err(ErrorCode::Internal(format!(
                            "{}",
                            COPY_MAX_FILES_COMMIT_MSG
                        )));
                    }
                } else if collect_duplicated_files && duplicated_files.len() < max_files {
                    duplicated_files.push(file.path.clone());
                }
            }
        }
        Ok(FilteredCopyFiles {
            files_to_copy,
            duplicated_files,
        })
    }

    fn add_written_segment_location(&self, segment_loc: Location) -> Result<()> {
        let mut segment_locations = self.written_segment_locs.write();
        segment_locations.insert(segment_loc);
        Ok(())
    }

    fn clear_written_segment_locations(&self) -> Result<()> {
        let mut segment_locations = self.written_segment_locs.write();
        segment_locations.clear();
        Ok(())
    }

    fn get_written_segment_locations(&self) -> Result<Vec<Location>> {
        Ok(self
            .written_segment_locs
            .read()
            .iter()
            .cloned()
            .collect::<Vec<_>>())
    }

    fn add_selected_segment_location(&self, segment_loc: Location) {
        let mut segment_locations = self.shared.selected_segment_locs.write();
        segment_locations.insert(segment_loc);
    }

    fn get_selected_segment_locations(&self) -> Vec<Location> {
        self.shared
            .selected_segment_locs
            .read()
            .iter()
            .cloned()
            .collect()
    }

    fn clear_selected_segment_locations(&self) {
        let mut segment_locations = self.shared.selected_segment_locs.write();
        segment_locations.clear();
    }

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()> {
        if matches!(self.get_query_kind(), QueryKind::CopyIntoTable) {
            self.shared.copy_status.add_chunk(file_path, file_status);
        }
        Ok(())
    }

    fn get_copy_status(&self) -> Arc<CopyStatus> {
        self.shared.copy_status.clone()
    }

    fn add_mutation_status(&self, mutation_status: MutationStatus) {
        self.shared
            .mutation_status
            .write()
            .merge_mutation_status(mutation_status)
    }

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>> {
        self.shared.mutation_status.clone()
    }

    fn update_multi_table_insert_status(&self, table_id: u64, num_rows: u64) {
        let mut multi_table_insert_status = self.shared.multi_table_insert_status.lock();
        match multi_table_insert_status.insert_rows.get_mut(&table_id) {
            Some(v) => {
                *v += num_rows;
            }
            None => {
                multi_table_insert_status
                    .insert_rows
                    .insert(table_id, num_rows);
            }
        }
    }

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>> {
        self.shared.multi_table_insert_status.clone()
    }

    fn get_license_key(&self) -> String {
        self.get_settings()
            .get_enterprise_license(self.get_version())
    }

    fn get_query_profiles(&self) -> Vec<PlanProfile> {
        self.shared.get_query_profiles()
    }

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>) {
        self.shared.add_query_profiles(profiles)
    }

    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>> {
        SessionManager::instance().get_queries_profiles()
    }

    fn set_merge_into_join(&self, join: MergeIntoJoin) {
        let mut merge_into_join = self.shared.merge_into_join.write();
        *merge_into_join = join;
    }

    fn clear_runtime_filter(&self) {
        let mut runtime_filters = self.shared.runtime_filters.write();
        runtime_filters.clear();
        self.shared.runtime_filter_ready.write().clear();
        self.shared
            .runtime_filter_logged
            .store(false, Ordering::SeqCst);
    }

    fn assert_no_runtime_filter_state(&self) -> Result<()> {
        let query_id = self.get_id();
        if !self.shared.runtime_filters.read().is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Runtime filters should be empty for query {query_id}"
            )));
        }
        if !self.shared.runtime_filter_ready.read().is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Runtime filter ready set should be empty for query {query_id}"
            )));
        }
        if self.shared.runtime_filter_logged.load(Ordering::Relaxed) {
            return Err(ErrorCode::Internal(format!(
                "Runtime filter logged flag should be reset for query {query_id}"
            )));
        }
        Ok(())
    }

    fn set_runtime_filter(&self, filters: HashMap<usize, RuntimeFilterInfo>) {
        let mut runtime_filters = self.shared.runtime_filters.write();
        for (scan_id, filter) in filters {
            let entry = runtime_filters.entry(scan_id).or_default();
            for new_filter in filter.filters {
                entry.filters.push(new_filter);
            }
        }
    }

    fn set_runtime_filter_ready(&self, table_index: usize, ready: Arc<RuntimeFilterReady>) {
        let mut runtime_filter_ready = self.shared.runtime_filter_ready.write();
        match runtime_filter_ready.entry(table_index) {
            Entry::Vacant(v) => {
                v.insert(vec![ready]);
            }
            Entry::Occupied(mut v) => {
                v.get_mut().push(ready);
            }
        }
    }

    fn get_runtime_filter_ready(&self, scan_id: usize) -> Vec<Arc<RuntimeFilterReady>> {
        let runtime_filter_ready = self.shared.runtime_filter_ready.read();
        match runtime_filter_ready.get(&scan_id) {
            Some(v) => v.to_vec(),
            None => vec![],
        }
    }

    fn get_merge_into_join(&self) -> MergeIntoJoin {
        let merge_into_join = self.shared.merge_into_join.read();
        MergeIntoJoin {
            merge_into_join_type: merge_into_join.merge_into_join_type.clone(),
            is_distributed: merge_into_join.is_distributed,
            target_tbl_idx: merge_into_join.target_tbl_idx,
        }
    }

    fn get_runtime_filters(&self, id: IndexType) -> Vec<RuntimeFilterEntry> {
        let runtime_filters = self.shared.runtime_filters.read();
        runtime_filters
            .get(&id)
            .map(|v| v.filters.clone())
            .unwrap_or_default()
    }

    fn get_bloom_runtime_filter_with_id(&self, id: IndexType) -> Vec<(String, RuntimeBloomFilter)> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.bloom.map(|bloom| (bloom.column_name, bloom.filter)))
            .collect()
    }

    fn get_inlist_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.inlist)
            .collect()
    }

    fn get_min_max_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.min_max)
            .collect()
    }

    fn runtime_filter_reports(&self) -> HashMap<IndexType, Vec<RuntimeFilterReport>> {
        let runtime_filters = self.shared.runtime_filters.read();
        runtime_filters
            .iter()
            .map(|(scan_id, info)| {
                let reports = info
                    .filters
                    .iter()
                    .map(|entry| RuntimeFilterReport {
                        filter_id: entry.id,
                        has_bloom: entry.bloom.is_some(),
                        has_inlist: entry.inlist.is_some(),
                        has_min_max: entry.min_max.is_some(),
                        stats: entry.stats.snapshot(),
                    })
                    .collect();
                (*scan_id, reports)
            })
            .collect()
    }

    fn has_bloom_runtime_filters(&self, id: usize) -> bool {
        if let Some(runtime_filter) = self.shared.runtime_filters.read().get(&id) {
            return runtime_filter
                .filters
                .iter()
                .any(|entry| entry.bloom.is_some());
        }
        false
    }

    fn txn_mgr(&self) -> TxnManagerRef {
        self.shared.session.session_ctx.txn_mgr()
    }

    fn session_state(&self) -> Result<SessionState> {
        self.shared.session.session_ctx.session_state()
    }

    fn get_table_meta_timestamps(
        &self,
        table: &dyn Table,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps> {
        let table_id = table.get_id();
        let table_unique_id = table.get_unique_id();

        let cached_table_timestamps = {
            self.shared
                .table_meta_timestamps
                .lock()
                .get(&table_unique_id)
                .copied()
        };

        if let Some(ts) = cached_table_timestamps {
            return Ok(ts);
        }

        let fuse_table = FuseTable::try_from_table(table)?;
        let is_transient = fuse_table.is_transient();
        let delta = {
            let duration = if is_transient {
                Duration::from_secs(0)
            } else {
                let settings = self.get_settings();
                let max_exec_time_secs = settings.get_max_execute_time_in_seconds()?;
                if max_exec_time_secs != 0 {
                    Duration::from_secs(max_exec_time_secs)
                } else {
                    // no limit, use retention period as delta
                    // prefer table-level retention setting.
                    match fuse_table.get_table_retention_period() {
                        None => Duration::from_days(settings.get_data_retention_time_in_days()?),
                        Some(v) => v,
                    }
                }
            };

            chrono::Duration::from_std(duration).map_err(|e| {
                ErrorCode::Internal(format!(
                    "Unable to construct delta duration of table meta timestamp: {e}",
                ))
            })?
        };

        let validation_context = SnapshotTimestampValidationContext {
            table_id,
            is_transient,
        };

        let table_meta_timestamps = TableMetaTimestamps::with_snapshot_timestamp_validation_context(
            previous_snapshot,
            delta,
            Some(validation_context),
        );

        {
            let txn_mgr_ref = self.txn_mgr();
            let mut txn_mgr = txn_mgr_ref.lock();

            if txn_mgr.is_active() {
                // Transaction Timestamp Tracking:
                let existing_timestamp = txn_mgr.get_table_txn_begin_timestamp(table_unique_id);

                if let Some(existing_ts) = existing_timestamp {
                    // Defensively check that:
                    // Inside an active transaction, if we already have a transaction timestamp for this table,
                    // ensure the new segment_block_timestamp is greater than or equal to it.
                    // This maintains timestamp monotonicity within the transaction, which is crucial for
                    // the safety of vacuum operation.
                    if table_meta_timestamps.segment_block_timestamp < existing_ts {
                        return Err(ErrorCode::Internal(format!(
                            "Transaction timestamp violation: table_id = {}, new segment timestamp {:?} is lesser than existing transaction timestamp {:?}",
                            table_id, table_meta_timestamps.segment_block_timestamp, existing_ts
                        )));
                    }
                } else {
                    // When a table is first mutated within an active transaction, record its
                    // segment_block_timestamp as the transaction's begin timestamp for this table.
                    txn_mgr.set_table_txn_begin_timestamp(
                        table_unique_id,
                        table_meta_timestamps.segment_block_timestamp,
                    );
                }
            }
        }

        {
            let mut cache = self.shared.table_meta_timestamps.lock();
            cache.insert(table_unique_id, table_meta_timestamps);
        }

        Ok(table_meta_timestamps)
    }

    fn get_read_block_thresholds(&self) -> BlockThresholds {
        *self.block_threshold.read()
    }

    fn set_read_block_thresholds(&self, thresholds: BlockThresholds) {
        *self.block_threshold.write() = thresholds;
    }

    fn get_query_queued_duration(&self) -> std::time::Duration {
        *self.shared.query_queued_duration.read()
    }

    fn set_query_queued_duration(&self, queued_duration: std::time::Duration) {
        *self.shared.query_queued_duration.write() = queued_duration;
    }

    fn set_variable(&self, key: String, value: Scalar) {
        self.shared.session.session_ctx.set_variable(key, value)
    }

    fn unset_variable(&self, key: &str) {
        self.shared.session.session_ctx.unset_variable(key)
    }

    fn get_variable(&self, key: &str) -> Option<Scalar> {
        self.shared.session.session_ctx.get_variable(key)
    }

    fn get_all_variables(&self) -> HashMap<String, Scalar> {
        self.shared.session.session_ctx.get_all_variables()
    }

    #[async_backtrace::framed]
    async fn load_datalake_schema(
        &self,
        kind: &str,
        sp: &StorageParams,
    ) -> Result<(TableSchema, String)> {
        match kind {
            "delta" => {
                let table = DeltaTable::load(sp).await?;
                DeltaTable::get_meta(&table).await
            }
            // TODO: iceberg doesn't support load from storage directly.
            _ => Err(ErrorCode::Internal(
                "Unsupported datalake type for schema loading",
            )),
        }
    }

    async fn create_stage_table(
        &self,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        files_to_copy: Option<Vec<StageFileInfo>>,
        max_column_position: usize,
        case_sensitive: bool,
        on_error_mode: Option<OnErrorMode>,
    ) -> Result<Arc<dyn Table>> {
        let copy_options = CopyIntoTableOptions {
            on_error: on_error_mode.unwrap_or_default(),
            ..Default::default()
        };
        let operator = init_stage_operator(&stage_info)?;
        let info = operator.info();
        let stage_root = format!("{}{}", info.name(), info.root());
        let stage_root = if stage_root.ends_with('/') {
            stage_root
        } else {
            format!("{}/", stage_root)
        };
        match &stage_info.file_format_params {
            FileFormatParams::Parquet(fmt) => {
                if max_column_position > 1 {
                    Err(ErrorCode::SemanticError(
                        "Query from parquet file only support $1 as column position",
                    ))
                } else if max_column_position == 0 {
                    let settings = self.get_settings();
                    let mut read_options = ParquetReadOptions::default();

                    if !settings.get_enable_parquet_page_index()? {
                        read_options = read_options.with_prune_pages(false);
                    }

                    if !settings.get_enable_parquet_rowgroup_pruning()? {
                        read_options = read_options.with_prune_row_groups(false);
                    }

                    if !settings.get_enable_parquet_prewhere()? {
                        read_options = read_options.with_do_prewhere(false);
                    }
                    ParquetTable::create(
                        self,
                        stage_info.clone(),
                        files_info,
                        read_options,
                        files_to_copy,
                        self.get_settings(),
                        self.get_query_kind(),
                        case_sensitive,
                        fmt,
                    )
                    .await
                } else {
                    let schema = Arc::new(TableSchema::new(vec![TableField::new(
                        "_$1",
                        TableDataType::Variant,
                    )]));
                    let info = StageTableInfo {
                        schema,
                        stage_info,
                        files_info,
                        files_to_copy,
                        duplicated_files_detected: vec![],
                        is_select: true,
                        default_exprs: None,
                        copy_into_table_options: copy_options.clone(),
                        stage_root,
                        is_variant: true,
                        parquet_metas: None,
                    };
                    StageTable::try_create(info)
                }
            }
            FileFormatParams::Orc(..) => {
                let is_variant = match max_column_position {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(ErrorCode::SemanticError(
                            "Query from ORC file only support $1 as column position",
                        ));
                    }
                };
                let schema = Arc::new(TableSchema::empty());
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    stage_root,
                    is_variant,
                    is_select: true,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                OrcTable::try_create(self, info).await
            }
            FileFormatParams::NdJson(..) | FileFormatParams::Avro(..) => {
                let schema = Arc::new(TableSchema::new(vec![TableField::new(
                    "_$1", // TODO: this name should be in visible
                    TableDataType::Variant,
                )]));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    is_select: true,
                    is_variant: true,
                    stage_root,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                StageTable::try_create(info)
            }
            FileFormatParams::Csv(..) | FileFormatParams::Tsv(..) => {
                if max_column_position == 0 {
                    let file_type = match stage_info.file_format_params {
                        FileFormatParams::Csv(..) => "CSV",
                        FileFormatParams::Tsv(..) => "TSV",
                        _ => unreachable!(), // This branch should never be reached
                    };

                    return Err(ErrorCode::SemanticError(format!(
                        "Query from {} file lacks column positions. Specify as $1, $2, etc.",
                        file_type
                    )));
                }

                let mut fields = vec![];
                for i in 1..(max_column_position + 1) {
                    fields.push(TableField::new(
                        &format!("_${}", i),
                        TableDataType::Nullable(Box::new(TableDataType::String)),
                    ));
                }

                let schema = Arc::new(TableSchema::new(fields));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    is_select: true,
                    stage_root,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                StageTable::try_create(info)
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported file format in query stage. Supported formats: Parquet, NDJson, AVRO, CSV, TSV. Provided: '{}'",
                    stage_info.file_format_params
                )));
            }
        }
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        catalog_name: &str,
        db_name: &str,
        tbl_name: &str,
        lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>> {
        let enabled_table_lock = self.get_settings().get_enable_table_lock().unwrap_or(false);
        if !enabled_table_lock {
            return Ok(None);
        }

        let catalog = self.get_catalog(catalog_name).await?;
        let tbl = catalog
            .get_table(&self.get_tenant(), db_name, tbl_name)
            .await?;
        if tbl.engine() != "FUSE" || tbl.is_read_only() || tbl.is_temp() {
            return Ok(None);
        }

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = match lock_opt {
            LockTableOption::LockNoRetry => table_lock.try_lock(self.clone(), false).await?,
            LockTableOption::LockWithRetry => table_lock.try_lock(self.clone(), true).await?,
            LockTableOption::NoLock => None,
        };
        if lock_guard.is_some() {
            self.evict_table_from_cache(catalog_name, db_name, tbl_name)?;
        }
        Ok(lock_guard)
    }

    fn get_temp_table_prefix(&self) -> Result<String> {
        self.shared.session.get_temp_table_prefix()
    }

    fn is_temp_table(&self, catalog_name: &str, database_name: &str, table_name: &str) -> bool {
        catalog_name == CATALOG_DEFAULT
            && self
                .shared
                .session
                .session_ctx
                .temp_tbl_mgr()
                .lock()
                .is_temp_table(database_name, table_name)
    }

    fn add_streams_ref(&self, catalog: &str, database: &str, stream: &str, consume: bool) {
        let mut streams = self.shared.streams_refs.write();
        let stream_key = (
            catalog.to_string(),
            database.to_string(),
            stream.to_string(),
        );
        streams
            .entry(stream_key)
            .and_modify(|v| {
                if consume {
                    *v = true;
                }
            })
            .or_insert(consume);
    }

    fn add_m_cte_temp_table(&self, database_name: &str, table_name: &str) {
        self.m_cte_temp_table
            .write()
            .push((database_name.to_string(), table_name.to_string()));
    }

    async fn drop_m_cte_temp_table(&self) -> Result<()> {
        let temp_tbl_mgr = self.shared.session.session_ctx.temp_tbl_mgr();
        let m_cte_temp_table = self.m_cte_temp_table.read().clone();
        let tenant = self.get_tenant();
        for (db_name, table_name) in m_cte_temp_table.iter() {
            let table = self.get_table(CATALOG_DEFAULT, db_name, table_name).await?;
            let db = self
                .get_catalog(CATALOG_DEFAULT)
                .await?
                .get_database(&tenant, db_name)
                .await?;
            let temp_prefix = table
                .options()
                .get(OPT_KEY_TEMP_PREFIX)
                .cloned()
                .unwrap_or_default();
            let table_id = table.get_table_info().ident.table_id;
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
            if drop_table_by_id(temp_tbl_mgr.clone(), drop_table_req)
                .await?
                .is_some()
            {
                ClientSessionManager::instance().remove_temp_tbl_mgr(temp_prefix, &temp_tbl_mgr);

                // Clear the temp table state from TxnBuffer
                let txn_mgr_ref = self.txn_mgr();
                let mut txn_mgr = txn_mgr_ref.lock();
                txn_mgr.clear_temp_table_by_id(table_id);
            }
        }
        let mut m_cte_temp_table = self.m_cte_temp_table.write();
        m_cte_temp_table.clear();
        Ok(())
    }

    fn get_consume_streams(&self, query: bool) -> Result<Vec<Arc<dyn Table>>> {
        let streams_refs = self.shared.streams_refs.read();
        let tables = self.shared.tables_refs.lock();
        let mut streams_meta = Vec::with_capacity(streams_refs.len());
        for (stream_key, consume) in streams_refs.iter() {
            if query && !consume {
                continue;
            }
            let stream = tables
                .get(stream_key)
                .ok_or_else(|| ErrorCode::Internal("Stream reference not found in tables cache"))?;
            streams_meta.push(stream.clone());
        }
        Ok(streams_meta)
    }

    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>> {
        self.shared.get_warehouse_clusters().await
    }

    fn get_pruned_partitions_stats(&self) -> HashMap<u32, PartStatistics> {
        self.shared.get_pruned_partitions_stats()
    }

    fn set_pruned_partitions_stats(&self, plan_id: u32, stats: PartStatistics) {
        self.shared.set_pruned_partitions_stats(plan_id, stats);
    }

    fn merge_pruned_partitions_stats(&self, other: &HashMap<u32, PartStatistics>) {
        self.shared.merge_pruned_partitions_stats(other);
    }

    fn get_next_broadcast_id(&self) -> u32 {
        self.shared
            .next_broadcast_id
            .fetch_add(1, Ordering::Acquire)
    }

    fn reset_broadcast_id(&self) {
        self.shared.next_broadcast_id.store(0, Ordering::Release);
    }

    fn get_session_type(&self) -> SessionType {
        self.shared.session.get_type()
    }

    fn get_perf_flag(&self) -> bool {
        self.shared.get_perf_flag()
    }

    fn set_perf_flag(&self, flag: bool) {
        self.shared.set_perf_flag(flag);
    }

    fn get_nodes_perf(&self) -> Arc<Mutex<HashMap<String, String>>> {
        self.shared.get_nodes_perf()
    }

    fn set_nodes_perf(&self, node: String, perf: String) {
        self.shared.set_nodes_perf(node, perf);
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
