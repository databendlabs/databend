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
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
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
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_meta_types::UpsertKV;
use databend_common_sql::Planner;
use databend_common_storage::DataOperator;
use databend_common_tracing::GlobalLogger;
use futures_util::TryStreamExt;
use log::error;
use log::info;
use rand::random;
use tokio::time::sleep;
use uuid::Uuid;

use crate::interpreters::InterpreterFactory;
use crate::persistent_log::session::create_session;
use crate::persistent_log::tables::HistoryTable;
use crate::persistent_log::tables::HistoryTables;

pub struct GlobalPersistentLog {
    meta_client: Arc<ClientHandle>,
    interval: u64,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
    retention_interval: usize,
    tables: HistoryTables,
}

impl GlobalPersistentLog {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        setup_operator().await?;

        let meta_client =
            MetaGrpcClient::try_new(&cfg.meta.to_meta_grpc_client_conf()).map_err(|_e| {
                ErrorCode::Internal("Create MetaClient failed for GlobalPersistentLog")
            })?;

        let stage_name = cfg.log.history.stage_name.clone();

        let instance = Arc::new(Self {
            meta_client,
            interval: cfg.log.history.interval as u64,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name,
            initialized: AtomicBool::new(false),
            retention_interval: cfg.log.history.retention_interval,
            tables: HistoryTables::init(cfg)?,
        });
        GlobalInstance::set(instance);
        GlobalIORuntime::instance().try_spawn(
            async move {
                if let Err(e) = GlobalPersistentLog::instance().work().await {
                    error!("persistent log exit {}", e);
                }
            },
            Some("persistent-log-worker".to_string()),
        )?;
        Ok(())
    }

    pub fn instance() -> Arc<GlobalPersistentLog> {
        GlobalInstance::get()
    }

    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub async fn work(&self) -> Result<()> {
        let meta_key = format!("{}/persistent_log_work", self.tenant_id);
        // Wait all services to be initialized
        loop {
            if !self.initialized.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        self.prepare().await?;
        info!("Persistent log prepared successfully");

        spawn(async move {
            if let Err(e) = GlobalPersistentLog::instance().clean_work().await {
                error!("Persistent log clean_work exit {}", e);
            }
        });

        loop {
            // Periodically performs persistent log COPY INTO/ INSERT INTO  every `interval` seconds (around)
            let may_permit = self.acquire(&meta_key, self.interval).await?;
            if let Some(guard) = may_permit {
                if let Err(e) = self.do_copy_into().await {
                    error!("Persistent log copy into failed: {:?}", e);
                }
                self.finish_hook(&meta_key).await?;
                drop(guard);
            }
            // add a random sleep time (from 0.5*interval to 1.5*interval) to avoid always one node doing the work
            let sleep_time = self.interval * 500 + random::<u64>() % (self.interval * 1000);
            tokio::time::sleep(Duration::from_millis(sleep_time)).await;
        }
    }

    /// Acquires a permit from a distributed semaphore with timestamp-based rate limiting.
    ///
    /// This function attempts to acquire a permit from a distributed semaphore identified by `meta_key`.
    /// It also implements a rate limiting mechanism based on the last execution timestamp.
    pub async fn acquire(&self, meta_key: &str, interval: u64) -> Result<Option<Permit>> {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.should_log = false;
        let _guard = ThreadTracker::tracking(tracking_payload);
        let acquired_guard = ThreadTracker::tracking_future(Semaphore::new_acquired(
            self.meta_client.clone(),
            meta_key,
            1,
            self.node_id.clone(),
            Duration::from_secs(3),
        ))
        .await
        .map_err(|_e| "acquire semaphore failed from GlobalPersistentLog")?;
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
                chrono::Local::now().timestamp_millis() as u64
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

    pub async fn finish_hook(&self, meta_key: &str) -> Result<()> {
        self.meta_client
            .upsert_kv(UpsertKV::new(
                format!("{}/last_timestamp", meta_key),
                MatchSeq::Any,
                Operation::Update(serde_json::to_vec(
                    &chrono::Local::now().timestamp_millis(),
                )?),
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
        tracking_payload.should_log = false;
        let _guard = ThreadTracker::tracking(tracking_payload);
        ThreadTracker::tracking_future(self.do_execute(sql, query_id)).await?;
        Ok(())
    }

    async fn do_execute(&self, sql: &str, query_id: String) -> Result<()> {
        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        // only need run the sql on the current node
        let context = session.create_query_context_with_cluster(
            Arc::new(Cluster {
                unassign: false,
                local_id: self.node_id.clone(),
                nodes: vec![],
            }),
            ThreadTracker::mem_stat().cloned(),
        )?;
        context.update_init_query_id(query_id);
        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor.execute(context).await?;
        let _: Vec<DataBlock> = stream.try_collect::<Vec<_>>().await?;
        Ok(())
    }

    /// Create the stage, database and table if not exists
    /// Alter the table if schema is changed
    pub async fn prepare(&self) -> Result<()> {
        let stage_name = self.stage_name.clone();
        let create_stage = format!("CREATE STAGE IF NOT EXISTS {}", stage_name);
        self.execute_sql(&create_stage).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS system_history";
        self.execute_sql(create_db).await?;
        let create_seq = "CREATE SEQUENCE IF NOT EXISTS log_history_seq";
        self.execute_sql(create_seq).await?;
        for (_, table) in self.tables.tables.iter() {
            let create_table = table.create_sql();
            self.execute_sql(&create_table).await?;
        }

        Ok(())
    }

    pub async fn do_copy_into(&self) -> Result<()> {
        let stage_name = self.stage_name.clone();
        let operator = GlobalLogger::instance().get_operator().await;
        if let Some(op) = operator {
            let path = format!("stage/internal/{}/", stage_name);
        }
        Ok(())
    }

    /// Periodically performs persistent log cleaning every `retention_interval` hours (around)
    async fn clean_work(&self) -> Result<()> {
        loop {
            // will check if it is already `retention_interval` hours every sleep_time
            let sleep_time = Duration::from_mins(30 + random::<u64>() % 60);
            let meta_key = format!("{}/persistent_log_clean", self.tenant_id);
            let may_permit = self.acquire(&meta_key, 60).await?;
            if let Some(guard) = may_permit {
                if let Err(e) = self.do_clean().await {
                    error!("persistent log clean failed: {}", e);
                }
                self.finish_hook(&meta_key).await?;
                drop(guard);
            }
            sleep(sleep_time).await;
        }
    }

    pub async fn do_clean(&self) -> Result<()> {
        // for table in &self.tables {
        //     let clean_sql = table.clean_sql(self.retention);
        //     self.execute_sql(&clean_sql).await?;
        //     info!(
        //         "Periodic retention operation on persistent log table '{}' completed successfully.",
        //         table.table_name()
        //     );
        // }

        // let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        // let context = session.create_query_context().await?;
        // if LicenseManagerSwitch::instance()
        //     .check_enterprise_enabled(context.get_license_key(), Feature::Vacuum)
        //     .is_ok()
        // {
        //     for table in &self.tables {
        //         let vacuum = format!("VACUUM TABLE persistent_system.{}", table.table_name());
        //         self.execute_sql(&vacuum).await?;
        //         info!(
        //         "Periodic VACUUM operation on persistent log table '{}' completed successfully.",
        //         table.table_name()
        //     );
        //     }
        // }
        Ok(())
    }
}

/// GlobalLogger initialization before the operator
/// This is a workaround for the current architecture.
pub async fn setup_operator() -> Result<()> {
    let op = DataOperator::instance().operator();
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
