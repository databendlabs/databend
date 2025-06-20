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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::spawn;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_sql::Planner;
use databend_common_storage::init_operator;
use databend_common_storage::DataOperator;
use databend_common_tracing::init_history_tables;
use databend_common_tracing::GlobalLogger;
use databend_common_tracing::HistoryTable;
use futures_util::future::join_all;
use futures_util::TryStreamExt;
use log::error;
use log::info;
use opendal::raw::normalize_root;
use parking_lot::Mutex;
use rand::random;
use tokio::time::sleep;
use uuid::Uuid;

use crate::history_tables::alter_table::get_alter_table_sql;
use crate::history_tables::alter_table::get_log_table;
use crate::history_tables::alter_table::should_reset;
use crate::history_tables::external::get_external_storage_connection;
use crate::history_tables::external::ExternalStorageConnection;
use crate::history_tables::meta::HistoryMetaHandle;
use crate::history_tables::session::create_session;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

pub struct GlobalHistoryLog {
    meta_handle: HistoryMetaHandle,
    interval: u64,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
    retention_interval: usize,
    tables: Vec<Arc<HistoryTable>>,
    connection: Option<ExternalStorageConnection>,
    current_params: Mutex<Option<StorageParams>>,
    _runtime: Arc<Runtime>,
}

impl GlobalHistoryLog {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        let connection = if let Some(params) = &cfg.log.history.storage_params {
            let connection = get_external_storage_connection(params);
            Some(connection)
        } else {
            None
        };

        setup_operator(&cfg.log.history.storage_params).await?;
        if cfg.log.history.log_only {
            info!(
                "[HISTORY-TABLES] History tables transform is disabled, only logging is enabled."
            );
            return Ok(());
        }
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
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name,
            initialized: AtomicBool::new(false),
            retention_interval: cfg.log.history.retention_interval,
            tables: init_history_tables(&cfg.log.history)?,
            connection,
            current_params: Mutex::new(None),
            _runtime: runtime.clone(),
        });
        GlobalInstance::set(instance);
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
    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub async fn work(&self) -> Result<()> {
        // Wait all services to be initialized
        loop {
            if !self.initialized.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        self.prepare().await?;
        info!("[HISTORY-TABLES] System history prepared successfully");
        // add a random sleep time (from 0.5*interval to 1.5*interval) to avoid always one node doing the work
        let sleep_time =
            Duration::from_millis(self.interval * 500 + random::<u64>() % (self.interval * 1000));
        let mut handles = vec![];
        for table in self.tables.iter() {
            let table_clone = table.clone();
            let meta_key = format!("{}/history_log_transform", self.tenant_id).clone();
            let log = GlobalHistoryLog::instance();
            let handle = spawn(async move {
                let mut consecutive_error = 0;
                loop {
                    match log.transform(&table_clone, &meta_key).await {
                        Ok(acquired_lock) => {
                            if acquired_lock {
                                consecutive_error = 0;
                            }
                        }
                        Err(e) => {
                            error!(
                                "[HISTORY-TABLES] {} log transform failed due to {}, retry {}",
                                table_clone.name, e, consecutive_error
                            );
                            consecutive_error += 1;
                            if consecutive_error > 3 {
                                error!(
                                    "[HISTORY-TABLES] {} log transform failed too many times, exit",
                                    table_clone.name
                                );
                                break;
                            }
                        }
                    }
                    sleep(sleep_time).await;
                }
            });
            handles.push(handle);
        }

        let sleep_time = Duration::from_mins(30 + random::<u64>() % 60);
        for table in self.tables.iter() {
            let table_clone = table.clone();
            let meta_key = format!("{}/history_log_clean", self.tenant_id);
            let log = GlobalHistoryLog::instance();
            let handle = spawn(async move {
                loop {
                    if let Err(e) = log.clean(&table_clone, &meta_key).await {
                        error!(
                            "[HISTORY-TABLES] {} log clean failed {}",
                            table_clone.name, e
                        );
                    }
                    sleep(sleep_time).await;
                }
            });
            handles.push(handle);
        }
        join_all(handles).await;

        Ok(())
    }

    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        let query_id = Uuid::new_v4().to_string();
        tracking_payload.query_id = Some(query_id.clone());
        tracking_payload.mem_stat = Some(MemStat::create(format!("Query-{}", query_id)));
        // prevent log table from logging its own logs
        tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
        let _guard = ThreadTracker::tracking(tracking_payload);
        ThreadTracker::tracking_future(self.do_execute(sql, query_id)).await?;
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
        let transform_keys = self
            .tables
            .iter()
            .map(|t| format!("{}/history_log_transform/{}/lock", self.tenant_id, t.name))
            .collect::<Vec<_>>();
        let prepare_key = format!("{}/history_log_prepare/lock", self.tenant_id);
        let _guard = self.meta_handle.acquire_all(&prepare_key, &transform_keys);

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
                info!("[HISTORY-TABLES] executing alter table: {}", alter_sql);
                self.execute_sql(&alter_sql).await?;
            }
        }

        let log_history_table = get_log_table(self.create_context().await?).await?;
        if let Some(t) = log_history_table {
            let mut params = self.current_params.lock();
            *params = t.get_table_info().meta.storage_params.clone();
        }
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        info!("[HISTORY-TABLES] Resetting system history tables");
        let drop_stage = format!("DROP STAGE IF EXISTS {}", self.stage_name);
        self.execute_sql(&drop_stage).await?;

        let drop_tables = self
            .tables
            .iter()
            .map(|t| format!("DROP TABLE system_history.{}", t.name))
            .collect::<Vec<_>>();
        for sql in drop_tables {
            self.execute_sql(&sql).await?;
        }
        Ok(())
    }

    pub async fn transform(&self, table: &HistoryTable, meta_key: &str) -> Result<bool> {
        let may_permit = self
            .meta_handle
            .acquire_with_guard(&format!("{}/{}/lock", meta_key, table.name), self.interval)
            .await?;

        // stage operator need to update when the params changed
        // this will happen when another node has updated the configuration
        if table.name == "log_history" {
            self.check_params().await?;
        }

        if let Some(_guard) = may_permit {
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
                    return Ok(true);
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
            drop(_guard);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn check_params(&self) -> Result<()> {
        let log_history_table = get_log_table(self.create_context().await?).await?;
        if let Some(t) = log_history_table {
            let params_from_meta = t.get_table_info().meta.storage_params.clone();
            {
                let mut params = self.current_params.lock();
                if params.as_ref() != params_from_meta.as_ref() {
                    info!(
                            "[HISTORY-TABLES] log_history table storage params changed, update log operator"
                        );
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
        let may_permit = self
            .meta_handle
            .acquire_with_guard(
                &format!("{}/{}/lock", meta_key, table.name),
                Duration::from_hours(self.retention_interval as u64).as_secs(),
            )
            .await?;
        if let Some(_guard) = may_permit {
            let sql = &table.delete;
            self.execute_sql(sql).await?;
            let session = create_session(&self.tenant_id, &self.cluster_id).await?;
            let context = session.create_query_context().await?;
            if LicenseManagerSwitch::instance()
                .check_enterprise_enabled(context.get_license_key(), Feature::Vacuum)
                .is_ok()
            {
                let vacuum = format!("VACUUM TABLE system_history.{}", table.name);
                self.execute_sql(&vacuum).await?;
                info!(
                    "[HISTORY-TABLES] periodic VACUUM operation on history log table '{}' completed successfully.",
                    table.name
                );
            }
            drop(_guard);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn create_context(&self) -> Result<Arc<QueryContext>> {
        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        // only need run the sql on the current node
        session.create_query_context_with_cluster(Arc::new(Cluster {
            unassign: false,
            local_id: self.node_id.clone(),
            nodes: vec![],
        }))
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
            let new_p = p.clone();
            let new_p = new_p.map_root(|root| {
                // replace `log_history` with empty string
                let new_root = root.replace("log_history/", "");
                normalize_root(new_root.as_str())
            });
            init_operator(&new_p)?
        }
    };
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
