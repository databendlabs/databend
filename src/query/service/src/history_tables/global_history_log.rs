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
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::Semaphore;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_meta_types::UpsertKV;
use databend_common_sql::Planner;
use databend_common_storage::DataOperator;
use databend_common_tracing::init_history_tables;
use databend_common_tracing::GlobalLogger;
use databend_common_tracing::HistoryTable;
use futures_util::future::join_all;
use futures_util::TryStreamExt;
use log::error;
use log::info;
use rand::random;
use tokio::time::sleep;
use uuid::Uuid;

use crate::history_tables::alter_table::get_alter_table_sql;
use crate::history_tables::session::create_session;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

pub struct GlobalHistoryLog {
    meta_client: Arc<ClientHandle>,
    interval: u64,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
    retention_interval: usize,
    tables: Vec<Arc<HistoryTable>>,
    _runtime: Arc<Runtime>,
}

impl GlobalHistoryLog {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        setup_operator().await?;
        let meta_client = MetaGrpcClient::try_new(&cfg.meta.to_meta_grpc_client_conf())
            .map_err(|_e| ErrorCode::Internal("Create MetaClient failed for SystemHistory"))?;
        let stage_name = cfg.log.history.stage_name.clone();
        let runtime = Arc::new(Runtime::with_worker_threads(
            2,
            Some("log-transform-worker".to_owned()),
        )?);

        let instance = Arc::new(Self {
            meta_client,
            interval: cfg.log.history.interval as u64,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name,
            initialized: AtomicBool::new(false),
            retention_interval: cfg.log.history.retention_interval,
            tables: init_history_tables(&cfg.log.history)?,
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
        info!("System history prepared successfully");
        // add a random sleep time (from 0.5*interval to 1.5*interval) to avoid always one node doing the work
        let sleep_time =
            Duration::from_millis(self.interval * 500 + random::<u64>() % (self.interval * 1000));
        let mut handles = vec![];
        for table in self.tables.iter() {
            let table_clone = table.clone();
            let meta_key = format!("{}/history_log_transform", self.tenant_id).clone();
            let log = GlobalHistoryLog::instance();
            let handle = spawn(async move {
                loop {
                    match log.transform(&table_clone, &meta_key).await {
                        Ok(acquired_lock) => {
                            if acquired_lock {
                                let _ = log
                                    .finish_hook(&format!("{}/{}/lock", meta_key, table_clone.name))
                                    .await;
                            }
                        }
                        Err(e) => {
                            let _ = log
                                .finish_hook(&format!("{}/{}/lock", meta_key, table_clone.name))
                                .await;

                            // BadArguments(1006), if the table schema is changed
                            // means this node is older version then exit
                            if e.code() == 1006 {
                                info!(
                                    "system history {} log transform exit due to schema changed",
                                    table_clone.name
                                );
                                break;
                            }

                            error!(
                                "system history {} log transform exit {}",
                                table_clone.name, e
                            );
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
                    match log.clean(&table_clone, &meta_key).await {
                        Ok(acquired_lock) => {
                            if acquired_lock {
                                let _ = log
                                    .finish_hook(&format!("{}/{}/lock", meta_key, table_clone.name))
                                    .await;
                            }
                        }
                        Err(e) => {
                            let _ = log
                                .finish_hook(&format!("{}/{}/lock", meta_key, table_clone.name))
                                .await;
                            error!("{} log clean exit {}", table_clone.name, e);
                        }
                    }
                    sleep(sleep_time).await;
                }
            });
            handles.push(handle);
        }
        join_all(handles).await;

        Ok(())
    }

    /// Acquires a permit from a distributed semaphore with timestamp-based rate limiting.
    ///
    /// This function attempts to acquire a permit from a distributed semaphore identified by `meta_key`.
    /// It also implements a rate limiting mechanism based on the last execution timestamp.g
    pub async fn acquire(&self, meta_key: &str, interval: u64) -> Result<Option<Permit>> {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        // prevent log table from logging its own logs
        tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
        let _guard = ThreadTracker::tracking(tracking_payload);
        let acquired_guard = ThreadTracker::tracking_future(Semaphore::new_acquired(
            self.meta_client.clone(),
            meta_key,
            1,
            self.node_id.clone(),
            Duration::from_secs(3),
        ))
        .await
        .map_err(|_e| "acquire semaphore failed from GlobalHistoryLog")?;
        if interval == 0 {
            return Ok(Some(acquired_guard));
        }
        if match ThreadTracker::tracking_future(
            self.meta_client
                .get_kv(&format!("{}/last_timestamp", meta_key)),
        )
        .await?
        {
            Some(v) => {
                let last: u64 = serde_json::from_slice(&v.data)?;
                chrono::Utc::now().timestamp_millis() as u64
                    - Duration::from_secs(interval).as_millis() as u64
                    > last
            }
            None => true,
        } {
            Ok(Some(acquired_guard))
        } else {
            drop(acquired_guard);
            Ok(None)
        }
    }

    /// Updating the last execution timestamp in the metadata.
    pub async fn finish_hook(&self, meta_key: &str) -> Result<()> {
        self.meta_client
            .upsert_kv(UpsertKV::new(
                format!("{}/last_timestamp", meta_key),
                MatchSeq::Any,
                Operation::Update(serde_json::to_vec(&chrono::Utc::now().timestamp_millis())?),
                None,
            ))
            .await?;
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
        let stage_name = self.stage_name.clone();
        let create_stage = format!("CREATE STAGE IF NOT EXISTS {}", stage_name);
        self.execute_sql(&create_stage).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS system_history";
        self.execute_sql(create_db).await?;
        for table in self.tables.iter() {
            let create_table = &table.create;
            self.execute_sql(create_table).await?;
            let get_alter_sql =
                get_alter_table_sql(self.create_context().await?, create_table, &table.name)
                    .await?;
            for alter_sql in get_alter_sql {
                self.execute_sql(&alter_sql).await?;
            }
        }
        Ok(())
    }

    pub async fn transform(&self, table: &HistoryTable, meta_key: &str) -> Result<bool> {
        let may_permit = self
            .acquire(&format!("{}/{}/lock", meta_key, table.name), self.interval)
            .await?;
        if let Some(_guard) = may_permit {
            let mut batch_number_end = 0;
            let batch_number_begin = self
                .get_u64_from_meta(&format!("{}/{}/batch_number", meta_key, table.name))
                .await?
                .unwrap_or(0);
            let sql = if table.name == "log_history" {
                table.assemble_log_history_transform(&self.stage_name, batch_number_begin)
            } else {
                batch_number_end = self
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
                self.set_u64_to_meta(
                    &format!("{}/{}/batch_number", meta_key, table.name),
                    batch_number_begin + 1,
                )
                .await?;
            } else {
                self.set_u64_to_meta(
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

    pub async fn clean(&self, table: &HistoryTable, meta_key: &str) -> Result<bool> {
        let may_permit = self
            .acquire(
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
                    "Periodic VACUUM operation on history log table '{}' completed successfully.",
                    table.name
                );
            }
            drop(_guard);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn get_u64_from_meta(&self, meta_key: &str) -> Result<Option<u64>> {
        match self.meta_client.get_kv(meta_key).await? {
            Some(v) => {
                let num: u64 = serde_json::from_slice(&v.data)?;
                Ok(Some(num))
            }
            None => Ok(None),
        }
    }

    pub async fn set_u64_to_meta(&self, meta_key: &str, value: u64) -> Result<()> {
        self.meta_client
            .upsert_kv(UpsertKV::new(
                meta_key,
                MatchSeq::Any,
                Operation::Update(serde_json::to_vec(&value)?),
                None,
            ))
            .await?;
        Ok(())
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
pub async fn setup_operator() -> Result<()> {
    let op = DataOperator::instance().operator();
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
