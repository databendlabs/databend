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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::SystemTime;

use dashmap::DashMap;
use databend_common_base::base::short_sql;
use databend_common_base::base::Progress;
use databend_common_base::base::SpillProgress;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetrics;
use databend_common_catalog::table_context::ContextError;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_pipeline_core::InputError;
use databend_common_settings::Settings;
use databend_common_sql::IndexType;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StorageMetrics;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use parking_lot::Mutex;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::clusters::Cluster;
use crate::clusters::ClusterDiscovery;
use crate::pipelines::executor::PipelineExecutor;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::Session;
use crate::storages::Table;

pub struct MemoryUpdater {
    pub memory_usage: AtomicUsize,
    pub peek_memory_usage: AtomicUsize,
}

type DatabaseAndTable = (String, String, String);

/// Data that needs to be shared in a query context.
pub struct QueryContextShared {
    // Query level
    pub(crate) query_settings: Arc<Settings>,
    /// total_scan_values for scan stats
    pub(in crate::sessions) total_scan_values: Arc<Progress>,
    /// scan_progress for scan metrics of datablocks (uncompressed)
    pub(in crate::sessions) scan_progress: Arc<Progress>,
    /// write_progress for write/commit metrics of datablocks (uncompressed)
    pub(in crate::sessions) write_progress: Arc<Progress>,
    /// Record how many bytes/rows have been spilled in join.
    pub(in crate::sessions) join_spill_progress: Arc<Progress>,
    /// Record how many bytes/rows have been spilled in agg.
    pub(in crate::sessions) agg_spill_progress: Arc<Progress>,
    /// Record how many bytes/rows have been spilled in group by
    pub(in crate::sessions) group_by_spill_progress: Arc<Progress>,
    /// Record how many bytes/rows have been spilled in window partition
    pub(in crate::sessions) window_partition_spill_progress: Arc<Progress>,
    /// result_progress for metrics of result datablocks (uncompressed)
    pub(in crate::sessions) result_progress: Arc<Progress>,
    pub(in crate::sessions) error: Arc<Mutex<Option<ErrorCode<ContextError>>>>,
    pub(in crate::sessions) warnings: Arc<Mutex<Vec<String>>>,
    pub(in crate::sessions) session: Arc<Session>,
    pub(in crate::sessions) runtime: Arc<RwLock<Option<Arc<Runtime>>>>,
    pub(in crate::sessions) init_query_id: Arc<RwLock<String>>,
    pub(in crate::sessions) cluster_cache: Arc<RwLock<Arc<Cluster>>>,
    pub(in crate::sessions) warehouse_cache: Arc<RwLock<Option<Arc<Cluster>>>>,
    pub(in crate::sessions) running_query: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) running_query_kind: Arc<RwLock<Option<QueryKind>>>,
    pub(in crate::sessions) running_query_text_hash: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) running_query_parameterized_hash: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) aborting: Arc<AtomicBool>,
    pub(in crate::sessions) tables_refs: Arc<Mutex<HashMap<DatabaseAndTable, Arc<dyn Table>>>>,
    pub(in crate::sessions) streams_refs: Arc<RwLock<HashMap<DatabaseAndTable, bool>>>,
    pub(in crate::sessions) affect: Arc<Mutex<Option<QueryAffect>>>,
    pub(in crate::sessions) catalog_manager: Arc<CatalogManager>,
    pub(in crate::sessions) data_operator: DataOperator,
    pub(in crate::sessions) executor: Arc<RwLock<Weak<PipelineExecutor>>>,
    pub(in crate::sessions) stage_attachment: Arc<RwLock<Option<StageAttachment>>>,
    pub(in crate::sessions) created_time: SystemTime,
    // now it is only set in query_log::log_query_finished
    pub(in crate::sessions) finish_time: RwLock<Option<SystemTime>>,
    // DashMap<file_path, HashMap<ErrorCode::code, (ErrorCode, Number of occurrences)>>
    // We use this field to count maximum of one error found per data file.
    #[allow(clippy::type_complexity)]
    pub(in crate::sessions) on_error_map:
        Arc<RwLock<Option<Arc<DashMap<String, HashMap<u16, InputError>>>>>>,
    pub(in crate::sessions) on_error_mode: Arc<RwLock<Option<OnErrorMode>>>,
    pub(in crate::sessions) copy_status: Arc<CopyStatus>,
    pub(in crate::sessions) mutation_status: Arc<RwLock<MutationStatus>>,
    pub(in crate::sessions) multi_table_insert_status: Arc<Mutex<MultiTableInsertStatus>>,
    /// partitions_sha for each table in the query. Not empty only when enabling query result cache.
    pub(in crate::sessions) partitions_shas: Arc<RwLock<Vec<String>>>,
    pub(in crate::sessions) cacheable: Arc<AtomicBool>,
    pub(in crate::sessions) can_scan_from_agg_index: Arc<AtomicBool>,
    pub(in crate::sessions) num_fragmented_block_hint: Arc<Mutex<HashMap<String, u64>>>,
    pub(in crate::sessions) enable_sort_spill: Arc<AtomicBool>,
    // Status info.
    pub(in crate::sessions) status: Arc<RwLock<String>>,

    // Client User-Agent
    pub(in crate::sessions) user_agent: Arc<RwLock<String>>,

    pub(in crate::sessions) query_profiles: Arc<RwLock<HashMap<Option<u32>, PlanProfile>>>,

    pub(in crate::sessions) runtime_filters: Arc<RwLock<HashMap<IndexType, RuntimeFilterInfo>>>,

    pub(in crate::sessions) runtime_filter_ready:
        Arc<RwLock<HashMap<IndexType, Vec<Arc<RuntimeFilterReady>>>>>,

    pub(in crate::sessions) wait_runtime_filter: Arc<RwLock<HashMap<IndexType, bool>>>,

    pub(in crate::sessions) merge_into_join: Arc<RwLock<MergeIntoJoin>>,

    // Records query level data cache metrics
    pub(in crate::sessions) query_cache_metrics: DataCacheMetrics,

    pub(in crate::sessions) query_queued_duration: Arc<RwLock<Duration>>,
    pub(in crate::sessions) table_meta_timestamps: Arc<Mutex<HashMap<u64, TableMetaTimestamps>>>,

    pub(in crate::sessions) cluster_spill_progress: Arc<RwLock<HashMap<String, SpillProgress>>>,
    pub(in crate::sessions) spilled_files:
        Arc<RwLock<HashMap<crate::spillers::Location, crate::spillers::Layout>>>,
    pub(in crate::sessions) unload_callbacked: AtomicBool,
    pub(in crate::sessions) mem_stat: Arc<RwLock<Option<Arc<MemStat>>>>,
    pub(in crate::sessions) node_memory_usage: Arc<RwLock<HashMap<String, Arc<MemoryUpdater>>>>,
}

impl QueryContextShared {
    pub fn try_create(
        session: Arc<Session>,
        cluster_cache: Arc<Cluster>,
    ) -> Result<Arc<QueryContextShared>> {
        Ok(Arc::new(QueryContextShared {
            query_settings: Settings::create(session.get_current_tenant()),
            catalog_manager: CatalogManager::instance(),
            session,
            cluster_cache: Arc::new(RwLock::new(cluster_cache)),
            data_operator: DataOperator::instance(),
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            total_scan_values: Arc::new(Progress::create()),
            scan_progress: Arc::new(Progress::create()),
            result_progress: Arc::new(Progress::create()),
            write_progress: Arc::new(Progress::create()),
            error: Arc::new(Mutex::new(None)),
            warnings: Arc::new(Mutex::new(vec![])),
            runtime: Arc::new(RwLock::new(None)),
            running_query: Arc::new(RwLock::new(None)),
            running_query_kind: Arc::new(RwLock::new(None)),
            running_query_text_hash: Arc::new(RwLock::new(None)),
            running_query_parameterized_hash: Arc::new(RwLock::new(None)),
            aborting: Arc::new(AtomicBool::new(false)),
            tables_refs: Arc::new(Mutex::new(HashMap::new())),
            streams_refs: Default::default(),
            affect: Arc::new(Mutex::new(None)),
            executor: Arc::new(RwLock::new(Weak::new())),
            stage_attachment: Arc::new(RwLock::new(None)),
            created_time: SystemTime::now(),
            finish_time: Default::default(),
            on_error_map: Arc::new(RwLock::new(None)),
            on_error_mode: Arc::new(RwLock::new(None)),
            copy_status: Default::default(),
            mutation_status: Default::default(),
            partitions_shas: Arc::new(RwLock::new(vec![])),
            cacheable: Arc::new(AtomicBool::new(true)),
            can_scan_from_agg_index: Arc::new(AtomicBool::new(true)),
            num_fragmented_block_hint: Default::default(),
            enable_sort_spill: Arc::new(AtomicBool::new(true)),
            status: Arc::new(RwLock::new("null".to_string())),
            user_agent: Arc::new(RwLock::new("null".to_string())),
            join_spill_progress: Arc::new(Progress::create()),
            agg_spill_progress: Arc::new(Progress::create()),
            group_by_spill_progress: Arc::new(Progress::create()),
            window_partition_spill_progress: Arc::new(Progress::create()),
            query_cache_metrics: DataCacheMetrics::new(),
            query_profiles: Arc::new(RwLock::new(HashMap::new())),
            runtime_filters: Default::default(),
            runtime_filter_ready: Default::default(),
            wait_runtime_filter: Default::default(),
            merge_into_join: Default::default(),
            multi_table_insert_status: Default::default(),
            query_queued_duration: Arc::new(RwLock::new(Duration::from_secs(0))),
            table_meta_timestamps: Arc::new(Mutex::new(HashMap::new())),

            cluster_spill_progress: Default::default(),
            spilled_files: Default::default(),
            unload_callbacked: AtomicBool::new(false),
            warehouse_cache: Arc::new(RwLock::new(None)),
            mem_stat: Arc::new(RwLock::new(None)),
            node_memory_usage: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    pub fn set_error<C>(&self, err: ErrorCode<C>) {
        let err = err.with_context("query context error");

        let mut guard = self.error.lock();
        *guard = Some(err);
    }

    pub fn get_error(&self) -> Option<ErrorCode<ContextError>> {
        let guard = self.error.lock();
        (*guard).clone()
    }

    pub fn push_warning(&self, warn: String) {
        let mut guard = self.warnings.lock();
        (*guard).push(warn);
    }

    pub fn pop_warnings(&self) -> Vec<String> {
        let mut guard = self.warnings.lock();
        let warnings = (*guard).clone();
        (*guard).clear();
        warnings
    }

    pub fn set_on_error_map(&self, map: Arc<DashMap<String, HashMap<u16, InputError>>>) {
        let mut guard = self.on_error_map.write();
        *guard = Some(map);
    }

    pub fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        self.on_error_map.read().as_ref().cloned()
    }

    pub fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.on_error_mode.read().clone()
    }

    pub fn set_on_error_mode(&self, mode: OnErrorMode) {
        let mut guard = self.on_error_mode.write();
        *guard = Some(mode);
    }

    pub fn kill<C>(&self, cause: ErrorCode<C>) {
        self.set_error(cause.clone());

        if let Some(executor) = self.executor.read().upgrade() {
            executor.finish(Some(cause));
        }

        self.aborting.store(true, Ordering::Release);

        // TODO: Wait for the query to be processed (write out the last error)
    }

    pub fn set_cluster(&self, cluster: Arc<Cluster>) {
        let mut cluster_cache = self.cluster_cache.write();
        *cluster_cache = cluster;
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster_cache.read().clone()
    }

    pub async fn get_warehouse_clusters(&self) -> Result<Arc<Cluster>> {
        if let Some(warehouse) = self.warehouse_cache.read().as_ref() {
            return Ok(warehouse.clone());
        }

        let config = GlobalConfig::instance();
        let discovery = ClusterDiscovery::instance();
        let warehouse = discovery.discover_warehouse_nodes(&config).await?;

        let mut write_guard = self.warehouse_cache.write();

        if write_guard.is_none() {
            *write_guard = Some(warehouse.clone());
        }

        Ok(write_guard.as_ref().cloned().expect("expect cluster."))
    }

    pub fn get_current_catalog(&self) -> String {
        self.session.get_current_catalog()
    }

    pub fn set_current_catalog(&self, catalog_name: String) {
        self.session.set_current_catalog(catalog_name)
    }

    pub fn get_aborting(&self) -> Arc<AtomicBool> {
        self.aborting.clone()
    }

    pub fn check_aborting(&self) -> Result<(), ContextError> {
        if self.aborting.load(Ordering::Acquire) {
            Err(self.get_error().unwrap_or_else(|| {
                ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                )
                .with_context("query aborted")
            }))
        } else {
            Ok(())
        }
    }

    pub fn get_current_database(&self) -> String {
        self.session.get_current_database()
    }

    pub fn set_current_database(&self, new_database_name: String) {
        self.session.set_current_database(new_database_name);
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.session.get_current_user()
    }

    pub fn get_current_role(&self) -> Option<RoleInfo> {
        self.session.get_current_role()
    }

    /// Get all tables that already attached in this query.
    pub fn get_tables_refs(&self) -> Vec<Arc<dyn Table>> {
        let tables = self.tables_refs.lock();
        tables.values().cloned().collect()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        let tables = self.get_tables_refs();
        let metrics: Vec<Arc<StorageMetrics>> =
            tables.iter().filter_map(|v| v.get_data_metrics()).collect();
        StorageMetrics::merge(&metrics)
    }

    pub fn get_tenant(&self) -> Tenant {
        self.session.get_current_tenant()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session.get_settings()
    }

    pub fn attach_table(&self, catalog: &str, database: &str, name: &str, table: Arc<dyn Table>) {
        let mut tables_refs = self.tables_refs.lock();
        let table_meta_key = (catalog.to_string(), database.to_string(), name.to_string());

        if let Entry::Vacant(v) = tables_refs.entry(table_meta_key) {
            v.insert(table);
        };
    }

    #[async_backtrace::framed]
    pub async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        // Always get same table metadata in the same query
        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());

        let already_in_cache = { self.tables_refs.lock().contains_key(&table_meta_key) };
        let res = match already_in_cache {
            false => {
                self.get_table_to_cache(catalog, database, table, max_batch_size)
                    .await?
            }
            true => self
                .tables_refs
                .lock()
                .get(&table_meta_key)
                .ok_or_else(|| ErrorCode::Internal("Logical error, it's a bug."))?
                .clone(),
        };

        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table_to_cache(
        &self,
        catalog_name: &str,
        database: &str,
        table: &str,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        let tenant = self.get_tenant();
        let table_meta_key = (
            catalog_name.to_string(),
            database.to_string(),
            table.to_string(),
        );
        let catalog = self
            .catalog_manager
            .get_catalog(
                tenant.tenant_name(),
                catalog_name,
                self.session.session_ctx.session_state(),
            )
            .await?;
        let cache_table = catalog.get_table(&tenant, database, table).await?;
        let cache_table = self
            .cache_stream_source_table(catalog, cache_table, max_batch_size)
            .await?;

        let mut tables_refs = self.tables_refs.lock();

        match tables_refs.entry(table_meta_key) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => Ok(v.insert(cache_table).clone()),
        }
    }

    // Cache the source table of a stream table to ensure can get the same table metadata.
    #[async_backtrace::framed]
    async fn cache_stream_source_table(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        if !table.is_stream() {
            return Ok(table);
        }

        let stream = StreamTable::try_from_table(table.as_ref())?;
        let source_database_name = stream.source_database_name(catalog.as_ref()).await?;
        let source_table_name = stream.source_table_name(catalog.as_ref()).await?;
        let meta_key = (
            catalog.name(),
            source_database_name.to_string(),
            source_table_name.to_string(),
        );
        let already_in_cache = { self.tables_refs.lock().contains_key(&meta_key) };
        let source_table = match already_in_cache {
            false => {
                let stream_desc = &stream.get_table_info().desc;
                let source_table =
                    match catalog.get_stream_source_table(stream_desc, max_batch_size)? {
                        Some(source_table) => source_table,
                        None => {
                            let source_table = stream
                                .navigate_within_batch_limit(
                                    catalog.as_ref(),
                                    &self.get_tenant(),
                                    &source_database_name,
                                    &source_table_name,
                                    max_batch_size,
                                )
                                .await?;
                            catalog.cache_stream_source_table(
                                stream.get_table_info().clone(),
                                source_table.get_table_info().clone(),
                                max_batch_size,
                            );
                            source_table
                        }
                    };

                let mut tables_refs = self.tables_refs.lock();
                tables_refs.entry(meta_key).or_insert(source_table.clone());
                source_table
            }
            true => self
                .tables_refs
                .lock()
                .get(&meta_key)
                .ok_or_else(|| ErrorCode::Internal("Logical error, it's a bug."))?
                .clone(),
        };

        let mut stream_info = stream.get_table_info().to_owned();
        stream_info.meta.schema = source_table.schema();

        Ok(StreamTable::create(
            stream_info,
            max_batch_size,
            Some(source_table),
        ))
    }

    pub fn evict_table_from_cache(&self, catalog: &str, database: &str, table: &str) -> Result<()> {
        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());
        let mut tables_refs = self.tables_refs.lock();
        tables_refs.remove(&table_meta_key);
        Ok(())
    }

    pub fn clear_tables_cache(&self) {
        let mut tables_refs = self.tables_refs.lock();
        tables_refs.clear();
    }

    /// Init runtime when first get
    pub fn try_get_runtime(&self) -> Result<Arc<Runtime>> {
        let mut query_runtime = self.runtime.write();

        match &*query_runtime {
            Some(query_runtime) => Ok(query_runtime.clone()),
            None => {
                // To avoid possible deadlock, we should keep at least two threads.
                let runtime = Arc::new(Runtime::with_worker_threads(
                    2,
                    Some("query-ctx".to_string()),
                )?);
                *query_runtime = Some(runtime.clone());
                Ok(runtime)
            }
        }
    }

    pub fn get_runtime(&self) -> Option<Arc<Runtime>> {
        let query_runtime = self.runtime.read();
        (*query_runtime).clone()
    }

    pub fn attach_query_str(&self, kind: QueryKind, query: String) {
        {
            let mut running_query = self.running_query.write();
            *running_query = Some(short_sql(
                query,
                self.get_settings()
                    .get_short_sql_max_length()
                    .unwrap_or(1000),
            ));
        }

        {
            let mut running_query_kind = self.running_query_kind.write();
            *running_query_kind = Some(kind);
        }
    }

    pub fn attach_query_hash(&self, text_hash: String, parameterized_hash: String) {
        {
            let mut running_query_hash = self.running_query_text_hash.write();
            *running_query_hash = Some(text_hash);
        }

        {
            let mut running_query_parameterized_hash =
                self.running_query_parameterized_hash.write();
            *running_query_parameterized_hash = Some(parameterized_hash);
        }
    }

    pub fn get_query_str(&self) -> String {
        let running_query = self.running_query.read();
        running_query.as_ref().unwrap_or(&"".to_string()).clone()
    }

    pub fn get_query_parameterized_hash(&self) -> String {
        let running_query_parameterized_hash = self.running_query_parameterized_hash.read();
        running_query_parameterized_hash
            .as_ref()
            .unwrap_or(&"".to_string())
            .clone()
    }

    pub fn get_query_text_hash(&self) -> String {
        let running_query_text_hash = self.running_query_text_hash.read();
        running_query_text_hash
            .as_ref()
            .unwrap_or(&"".to_string())
            .clone()
    }

    pub fn get_query_kind(&self) -> QueryKind {
        let running_query_kind = self.running_query_kind.read();
        running_query_kind
            .as_ref()
            .cloned()
            .unwrap_or(QueryKind::Unknown)
    }

    pub fn get_connection_id(&self) -> String {
        self.session.get_id()
    }

    pub fn get_affect(&self) -> Option<QueryAffect> {
        let guard = self.affect.lock();
        (*guard).clone()
    }

    pub fn set_affect(&self, affect: QueryAffect) {
        let mut guard = self.affect.lock();
        *guard = Some(affect);
    }

    pub fn set_executor(&self, executor: Arc<PipelineExecutor>) -> Result<()> {
        let mut guard = self.executor.write();
        match self.check_aborting() {
            Ok(_) => {
                *guard = Arc::downgrade(&executor);
                Ok(())
            }
            Err(err) => {
                executor.finish(Some(err.clone()));
                Err(err.with_context("failed to set executor"))
            }
        }
    }

    pub fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.stage_attachment.read().clone()
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        let mut stage_attachment = self.stage_attachment.write();
        *stage_attachment = Some(attachment);
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.created_time
    }

    pub fn get_status_info(&self) -> String {
        let status = self.status.read();
        status.clone()
    }

    pub async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        let user_mgr = UserApiProvider::instance();
        let tenant = self.get_tenant();
        user_mgr.get_connection(&tenant, name).await
    }

    pub fn get_query_cache_metrics(&self) -> &DataCacheMetrics {
        &self.query_cache_metrics
    }

    pub fn set_priority(&self, priority: u8) {
        if let Some(executor) = self.executor.read().upgrade() {
            executor.change_priority(priority)
        }
    }

    pub fn get_query_profiles(&self) -> Vec<PlanProfile> {
        if let Some(executor) = self.executor.read().upgrade() {
            self.add_query_profiles(&executor.fetch_profiling(false));
        }

        self.query_profiles.read().values().cloned().collect()
    }

    pub fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>) {
        let mut merged_profiles = self.query_profiles.write();

        for query_profile in profiles.values() {
            match merged_profiles.entry(query_profile.id) {
                Entry::Vacant(v) => {
                    v.insert(query_profile.clone());
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().merge(query_profile);
                }
            };
        }
    }

    pub fn set_query_memory_tracking(&self, mem_stat: Option<Arc<MemStat>>) {
        let mut mem_stat_guard = self.mem_stat.write();
        *mem_stat_guard = mem_stat;
    }

    pub fn get_query_memory_tracking(&self) -> Option<Arc<MemStat>> {
        self.mem_stat.read().clone()
    }

    pub fn get_node_memory_updater(&self, node: &str) -> Arc<MemoryUpdater> {
        {
            if let Some(v) = self.node_memory_usage.read().get(node) {
                return v.clone();
            }
        }

        let key = node.to_string();
        let node_memory_updater = Arc::new(MemoryUpdater {
            memory_usage: AtomicUsize::new(0),
            peek_memory_usage: AtomicUsize::new(0),
        });

        let mut guard = self.node_memory_usage.write();
        guard.insert(key, node_memory_updater.clone());
        node_memory_updater
    }

    pub fn get_nodes_memory_usage(&self) -> usize {
        let mut memory_usage = {
            match self.mem_stat.read().as_ref() {
                None => 0,
                Some(mem_stat) => mem_stat.get_memory_usage(),
            }
        };

        for (_, node_memory_updater) in self.node_memory_usage.read().iter() {
            memory_usage += node_memory_updater.memory_usage.load(Ordering::Relaxed);
        }

        memory_usage
    }

    pub fn get_nodes_peek_memory_usage(&self) -> HashMap<String, usize> {
        let memory_usage = {
            match self.mem_stat.read().as_ref() {
                None => 0,
                Some(mem_stat) => std::cmp::max(0, mem_stat.get_peek_memory_usage()) as usize,
            }
        };

        let mut nodes_peek_memory_usage = HashMap::new();

        nodes_peek_memory_usage
            .insert(GlobalConfig::instance().query.node_id.clone(), memory_usage);

        for (node, node_memory_updater) in self.node_memory_usage.read().iter() {
            let peek_memory_usage = node_memory_updater
                .peek_memory_usage
                .load(Ordering::Relaxed);
            nodes_peek_memory_usage.insert(node.clone(), peek_memory_usage);
        }

        nodes_peek_memory_usage
    }

    pub fn get_table_meta_timestamps(&self) -> Arc<Mutex<HashMap<u64, TableMetaTimestamps>>> {
        self.table_meta_timestamps.clone()
    }
}

impl Drop for QueryContextShared {
    fn drop(&mut self) {
        drop_guard(move || {
            // last_query_id() should return the query_id of the last executed statement,
            // so we set it when the current context drops
            // to avoid returning the query_id of the current statement.
            self.session
                .session_ctx
                .update_query_ids_results(self.init_query_id.read().clone(), None)
        })
    }
}
