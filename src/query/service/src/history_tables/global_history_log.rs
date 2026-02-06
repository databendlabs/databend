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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::spawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::storage::StorageParams;
use databend_common_sql::Planner;
use databend_common_storage::DataOperator;
use databend_common_storage::init_operator;
use databend_common_tracing::GlobalLogger;
use databend_common_tracing::HistoryTable;
use databend_common_tracing::get_all_history_table_names;
use databend_common_tracing::init_history_tables;
use databend_meta_client::MetaGrpcClient;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use log::debug;
use log::error;
use log::info;
use log::warn;
use opendal::raw::normalize_root;
use parking_lot::Mutex;
use rand::random;
use tokio::time::Instant;
use tokio::time::sleep;
use uuid::Uuid;

use crate::clusters::ClusterDiscovery;
use crate::history_tables::alter_table::get_alter_table_sql;
use crate::history_tables::alter_table::get_log_table;
use crate::history_tables::alter_table::should_reset;
use crate::history_tables::error_handling::ErrorCounters;
use crate::history_tables::error_handling::is_temp_error;
use crate::history_tables::external::ExternalStorageConnection;
use crate::history_tables::external::get_external_storage_connection;
use crate::history_tables::meta::HistoryMetaHandle;
use crate::history_tables::session::create_session;
use crate::interpreters::InterpreterFactory;
use crate::sessions::BuildInfoRef;
use crate::sessions::QueryContext;

const DEAD_IN_SECS: u64 = 60;

pub struct GlobalHistoryLog {
    meta_handle: HistoryMetaHandle,
    interval: u64,
    tenant_id: String,
    stage_name: String,
    initialized: AtomicBool,
    retention_interval: usize,
    tables: Vec<Arc<HistoryTable>>,
    connection: Option<ExternalStorageConnection>,
    current_params: Mutex<Option<StorageParams>>,
    version: BuildInfoRef,
    _runtime: Arc<Runtime>,
}

impl GlobalHistoryLog {
    pub async fn init(cfg: &InnerConfig, version: BuildInfoRef) -> Result<()> {
        let connection = if let Some(params) = &cfg.log.history.storage_params {
            let connection = get_external_storage_connection(params);
            Some(connection)
        } else {
            None
        };
        let meta_client = MetaGrpcClient::try_new(&cfg.meta.to_meta_grpc_client_conf())
            .map_err(|_e| ErrorCode::Internal("Create MetaClient failed for SystemHistory"))?;
        let meta_handle = HistoryMetaHandle::new(meta_client, cfg.query.node_id.clone());
        let stage_name = cfg.log.history.stage_name.clone();
        let runtime = Arc::new(Runtime::with_worker_threads(
            2,
            Some("log-transform-worker".to_owned()),
        )?);

        let instance = Arc::new(Self {
            meta_handle,
            interval: cfg.log.history.interval as u64,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            stage_name,
            initialized: AtomicBool::new(false),
            retention_interval: cfg.log.history.retention_interval,
            tables: init_history_tables(&cfg.log.history)?,
            connection,
            current_params: Mutex::new(None),
            version,
            _runtime: runtime.clone(),
        });
        GlobalInstance::set(instance);
        if cfg.log.history.log_only {
            info!("History tables transform is disabled, only logging is enabled.");
            return Ok(());
        }
        runtime.spawn(async move {
            if let Err(e) = GlobalHistoryLog::instance().work().await {
                error!("System history tables exit with {}", e);
            }
        });
        Ok(())
    }

    pub fn instance() -> Arc<GlobalHistoryLog> {
        GlobalInstance::get()
    }

    /// Mark the system history log as initialized.
    ///
    /// GlobalHistoryLog rely on other services, so we need to wait
    /// for all services to be initialized before starting the work.
    pub async fn initialized(&self, log_only: bool) -> Result<()> {
        let _ = should_reset(self.create_context().await?, &self.connection).await?;
        self.initialized.store(true, Ordering::SeqCst);
        // if log_only is true, this node will not have reset operation,
        // so we can set up operator here.
        if log_only {
            self.update_operator(true).await?;
            return Ok(());
        }
        Ok(())
    }

    pub async fn work(&self) -> Result<()> {
        self.wait_for_initialization().await;
        self.prepare().await?;
        self.update_operator(true).await?;
        info!("System history prepared successfully");

        let mut handles = vec![];

        self.spawn_transform_tasks(&mut handles).await;
        self.spawn_clean_tasks(&mut handles).await;

        join_all(handles).await;
        Ok(())
    }

    async fn wait_for_initialization(&self) {
        loop {
            if !self.initialized.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
    }

    async fn spawn_transform_tasks(&self, handles: &mut Vec<tokio::task::JoinHandle<()>>) {
        for table in self.tables.iter() {
            let table_clone = table.clone();
            let meta_key = format!("{}/history_log_transform", self.tenant_id);
            let log = GlobalHistoryLog::instance();

            let handle = spawn(async move {
                log.run_table_transform_loop(table_clone, meta_key).await;
            });
            handles.push(handle);
        }
    }

    async fn spawn_clean_tasks(&self, handles: &mut Vec<tokio::task::JoinHandle<()>>) {
        let sleep_time = Duration::from_mins(30 + random::<u64>() % 60);

        for table in self.tables.iter() {
            let table_clone = table.clone();
            let meta_key = format!("{}/history_log_clean", self.tenant_id);
            let log = GlobalHistoryLog::instance();

            let handle = spawn(async move {
                log.run_table_clean_loop(table_clone, meta_key, sleep_time)
                    .await;
            });
            handles.push(handle);
        }
    }

    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        let query_id = Uuid::new_v4().to_string();
        tracking_payload.query_id = Some(query_id.clone());
        tracking_payload.mem_stat = Some(MemStat::create(format!("Query-{}", query_id)));
        // prevent log table from logging its own logs
        tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
        tracking_payload
            .tracking(self.do_execute(sql, query_id))
            .await?;
        Ok(())
    }

    async fn do_execute(&self, sql: &str, query_id: String) -> Result<()> {
        let context = self.create_context().await?;
        context.update_init_query_id(query_id);
        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor.execute(context).await?;
        let _: Vec<DataBlock> = stream.try_collect::<Vec<_>>().await?;
        Ok(())
    }

    /// Create the stage, database and table if not exists
    pub async fn prepare(&self) -> Result<()> {
        let prepare_key = format!("{}/history_log_prepare/lock", self.tenant_id);
        let _guard = self.meta_handle.acquire(&prepare_key, 0).await?;

        if should_reset(self.create_context().await?, &self.connection).await? {
            self.reset().await?;
        }

        let create_stage = if let Some(connection) = &self.connection {
            connection.to_create_stage_sql(&self.stage_name)
        } else {
            format!("CREATE STAGE IF NOT EXISTS {}", &self.stage_name)
        };
        self.execute_sql(&create_stage).await?;

        let create_db = "CREATE DATABASE IF NOT EXISTS system_history";
        self.execute_sql(create_db).await?;

        for table in self.tables.iter() {
            let mut create_table = table.create.clone();
            if let Some(connection) = &self.connection {
                create_table = connection.to_create_table_sql(&create_table, &table.name)
            };
            self.execute_sql(&create_table).await?;

            let get_alter_sql =
                get_alter_table_sql(self.create_context().await?, &create_table, &table.name)
                    .await?;
            for alter_sql in get_alter_sql {
                info!("executing alter table: {}", alter_sql);
                self.execute_sql(&alter_sql).await?;
            }
        }
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        info!("Resetting system history tables");
        let drop_stage = format!("DROP STAGE IF EXISTS {}", self.stage_name);
        self.execute_sql(&drop_stage).await?;

        // drop all pre-defined history tables to keep the system_history's tables consistency
        let drop_tables = get_all_history_table_names()
            .iter()
            .map(|name| format!("DROP TABLE IF EXISTS system_history.{}", name))
            .collect::<Vec<_>>();
        for sql in drop_tables {
            self.execute_sql(&sql).await?;
        }
        Ok(())
    }

    pub async fn transform(&self, table: &HistoryTable, meta_key: &str) -> Result<()> {
        // stage operator need to update when the params changed
        // this will happen when another node has updated the configuration
        if table.name == "log_history" {
            self.update_operator(false).await?;
        }

        let mut batch_number_end = 0;
        let batch_number_begin = self
            .meta_handle
            .get_u64_from_meta(&format!("{}/{}/batch_number", meta_key, table.name))
            .await?
            .unwrap_or(0);
        let sql = if table.name == "log_history" {
            table.assemble_log_history_transform(&self.stage_name, batch_number_begin)
        } else {
            batch_number_end = self
                .meta_handle
                .get_u64_from_meta(&format!("{}/{}/batch_number", meta_key, "log_history"))
                .await?
                .unwrap_or(0);
            if batch_number_begin >= batch_number_end {
                return Ok(());
            }
            table.assemble_normal_transform(batch_number_begin, batch_number_end)
        };
        self.execute_sql(&sql).await?;
        if table.name == "log_history" {
            self.meta_handle
                .set_u64_to_meta(
                    &format!("{}/{}/batch_number", meta_key, table.name),
                    batch_number_begin + 1,
                )
                .await?;
        } else {
            self.meta_handle
                .set_u64_to_meta(
                    &format!("{}/{}/batch_number", meta_key, table.name),
                    batch_number_end,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn update_operator(&self, force: bool) -> Result<()> {
        let log_history_table = get_log_table(self.create_context().await?).await?;
        if let Some(t) = log_history_table {
            let params_from_meta = t.get_table_info().meta.storage_params.clone();
            {
                let mut params = self.current_params.lock();
                if params.as_ref() != params_from_meta.as_ref() || force {
                    info!("log_history table storage params changed, update log operator");
                    *params = params_from_meta.clone();
                } else {
                    return Ok(());
                }
            }
            setup_operator(&params_from_meta).await?;
        }
        Ok(())
    }

    pub async fn clean(&self, table: &HistoryTable, meta_key: &str) -> Result<bool> {
        let got_permit = self
            .meta_handle
            .check_should_perform_clean(
                &format!("{}/{}/lock", meta_key, table.name),
                Duration::from_hours(self.retention_interval as u64).as_secs(),
            )
            .await?;
        if got_permit {
            let start = Instant::now();
            let sql = &table.delete;
            self.execute_sql(sql).await?;
            let context = self.create_context().await?;
            let delete_elapsed = start.elapsed().as_secs();
            if LicenseManagerSwitch::instance()
                .check_enterprise_enabled(context.get_license_key(), Feature::Vacuum)
                .is_ok()
            {
                let vacuum = format!("VACUUM TABLE system_history.{}", table.name);
                self.execute_sql(&vacuum).await?;
            }
            info!(
                "periodic retention operation on history log table '{}' completed successfully (delete {} secs, vacuum {} secs)",
                table.name,
                delete_elapsed,
                start.elapsed().as_secs() - delete_elapsed
            );
            return Ok(true);
        }
        Ok(false)
    }

    fn transform_sleep_duration(&self) -> Duration {
        Duration::from_millis(self.interval * 500 + random::<u64>() % (self.interval * 1000))
    }

    async fn run_table_transform_loop(&self, table: Arc<HistoryTable>, meta_key: String) {
        let mut error_counters = ErrorCounters::new();
        loop {
            // 1. Acquire heartbeat
            let heartbeat_key = format!("{}/{}", meta_key, table.name);
            let heartbeat_guard = match self
                .meta_handle
                .create_heartbeat_task(&heartbeat_key, DEAD_IN_SECS)
                .await
            {
                Ok(Some(guard)) => guard,
                Ok(None) => {
                    sleep(self.transform_sleep_duration()).await;
                    continue;
                }
                Err(e) => {
                    error!("{} failed to create heartbeat, retry: {}", table.name, e);
                    sleep(self.transform_sleep_duration()).await;
                    continue;
                }
            };

            debug!("{} acquired heartbeat, starting work loop", table.name);

            // 2. Start to work on the task, hold the heartbeat guard during the work
            let mut transform_cnt = 0;
            loop {
                // Check if heartbeat is lost or cancelled, indicating another instance took over
                // or the task should be terminated
                if heartbeat_guard.exited() {
                    info!("{} lost heartbeat, releasing and retrying", table.name);
                    break;
                }
                match self.transform(&table, &meta_key).await {
                    Ok(()) => {
                        error_counters.reset();
                        transform_cnt += 1;
                        sleep(self.transform_sleep_duration()).await;
                    }
                    Err(e) => {
                        if is_temp_error(&e) {
                            let temp_count = error_counters.increment_temporary();
                            let backoff_second = error_counters.calculate_temp_backoff();
                            warn!(
                                "{} log transform failed with temporary error {}, count {}, next retry in {} seconds",
                                table.name, e, temp_count, backoff_second
                            );
                            sleep(Duration::from_secs(backoff_second)).await;
                        } else {
                            let persistent_count = error_counters.increment_persistent();
                            error!(
                                "{} log transform failed with persistent error {}, retry count {}",
                                table.name, e, persistent_count
                            );
                            if error_counters.persistent_exceeded_limit() {
                                error!(
                                    "{} log transform failed too many times, giving up",
                                    table.name
                                );
                                break;
                            }
                            sleep(self.transform_sleep_duration()).await;
                        }

                        // On error(e.g. DUPLICATED_UPSERT_FILES), verify that our heartbeat is still valid (from this node).
                        // Purpose: avoid two nodes performing the same work concurrently.
                        // The periodic heartbeat loop would also detect the conflict, but it runs
                        // only around every 30 seconds; this check enables faster failover.
                        if e.code() == ErrorCode::DUPLICATED_UPSERT_FILES
                            || e.code() == ErrorCode::TABLE_ALREADY_LOCKED
                            || e.code() == ErrorCode::TABLE_LOCK_EXPIRED
                            || e.code() == ErrorCode::TABLE_VERSION_MISMATCHED
                        {
                            if let Ok(valid) =
                                self.meta_handle.is_heartbeat_valid(&heartbeat_key).await
                            {
                                if !valid {
                                    info!("{} heartbeat lost during transform", table.name);
                                    break;
                                }
                            }
                        }
                    }
                }
                // Release heartbeat periodically to allow other nodes in the cluster
                // to take over and ensure even task distribution across the cluster
                if transform_cnt % 200 == 0 {
                    break;
                }
            }
            debug!("{} released heartbeat", table.name);

            if error_counters.persistent_exceeded_limit() {
                return;
            }
        }
    }

    async fn run_table_clean_loop(
        &self,
        table: Arc<HistoryTable>,
        meta_key: String,
        sleep_time: Duration,
    ) {
        loop {
            if let Err(e) = self.clean(&table, &meta_key).await {
                error!("{} log clean failed {}", table.name, e);
            }
            sleep(sleep_time).await;
        }
    }

    pub async fn create_context(&self) -> Result<Arc<QueryContext>> {
        // only need run the sql on the current node
        let cluster_discovery = ClusterDiscovery::instance();
        let dummy_cluster = cluster_discovery
            .single_node_cluster(&GlobalConfig::instance())
            .await?;

        let cluster_id = dummy_cluster.get_cluster_id()?;
        let session = create_session(&self.tenant_id, &cluster_id).await?;
        session.create_query_context_with_cluster(dummy_cluster, self.version)
    }
}

/// GlobalLogger initialization before the operator
/// This is a workaround for the current architecture.
pub async fn setup_operator(params: &Option<StorageParams>) -> Result<()> {
    // If it has specified storage_params, use it to initialize the operator.
    // Otherwise, use the default operator from global DataOperator.
    let op = match params {
        None => DataOperator::instance().operator(),
        Some(p) => {
            let mut new_p = p.clone();
            new_p = new_p.map_root(|root| {
                // replace `log_history` with empty string
                let new_root = root.replace("log_history/", "");
                normalize_root(new_root.as_str())
            });
            // Special handling for history tables.
            // Since history tables storage params are fully generated from config,
            // we can safely allow credential chain.
            if let StorageParams::S3(cfg) = &mut new_p {
                if cfg.allow_credential_chain.is_none() {
                    cfg.allow_credential_chain = Some(true);
                }
            }
            init_operator(&new_p)?
        }
    };
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
