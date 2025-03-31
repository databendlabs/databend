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
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::txn_condition;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_sql::Planner;
use databend_common_storage::DataOperator;
use databend_common_tracing::GlobalLogger;
use log::error;
use log::info;
use rand::random;

use crate::interpreters::InterpreterFactory;
use crate::persistent_log::session::create_session;
use crate::persistent_log::table_schemas::PersistentLogTable;
use crate::persistent_log::table_schemas::QueryLogTable;
use crate::sessions::QueryContext;

pub struct GlobalPersistentLog {
    meta_store: MetaStore,
    interval: usize,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
    stopped: AtomicBool,
    #[allow(dead_code)]
    retention: usize,
}

impl GlobalPersistentLog {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        setup_operator().await?;

        let provider = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf());
        let meta_store = provider.create_meta_store().await?;

        let instance = Arc::new(Self {
            meta_store,
            interval: cfg.log.persistentlog.interval,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name: cfg.log.persistentlog.stage_name.clone(),
            initialized: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            retention: cfg.log.persistentlog.retention,
        });
        GlobalInstance::set(instance);
        GlobalIORuntime::instance().spawn(async move {
            if let Err(e) = GlobalPersistentLog::instance().work().await {
                error!("persistent log exit {}", e);
            }
        });
        Ok(())
    }

    pub fn instance() -> Arc<GlobalPersistentLog> {
        GlobalInstance::get()
    }

    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub async fn work(&self) -> Result<()> {
        let mut prepared = false;

        // // Use a counter rather than a time interval to trigger cleanup operations.
        // // because in cluster environment, a time-based interval would cause cleanup frequency
        // // to scale with the number of nodes in the cluster, whereas this count-based
        // // approach ensures consistent cleanup frequency regardless of cluster size.
        // let thirty_minutes_in_seconds = 30 * 60;
        // let copy_into_threshold = thirty_minutes_in_seconds / self.interval;
        // let mut copy_into_count = 0;

        loop {
            // add a random sleep time to avoid always one node doing the work
            let sleep_time = self.interval as u64 * 1000 + random::<u64>() % 1000;

            tokio::time::sleep(Duration::from_millis(sleep_time)).await;
            if self.stopped.load(Ordering::SeqCst) {
                return Ok(());
            }
            // Wait all services to be initialized
            if !self.initialized.load(Ordering::SeqCst) {
                continue;
            }
            // create the stage, database and table if not exists
            // only execute once, it is ok to do this in multiple nodes without lock
            if !prepared {
                match self.prepare().await {
                    Ok(_) => {
                        info!("Persistent log prepared successfully");
                        prepared = true;
                    }
                    Err(e) => {
                        error!("Persistent log prepare failed: {:?}", e);
                    }
                }
            }
            if let Ok(acquired_lock) = self.try_acquire().await {
                if acquired_lock {
                    if let Err(e) = self.do_copy_into().await {
                        error!("Persistent log copy into failed: {:?}", e);
                    }
                    // copy_into_count += 1;
                    // if copy_into_count > copy_into_threshold {
                    //     if let Err(e) = self.clean().await {
                    //         error!("Persistent log delete failed: {:?}", e);
                    //     }
                    //     copy_into_count = 0;
                    // }
                }
            }
        }
    }

    /// Multiple nodes doing the work may make commit conflict.
    pub async fn try_acquire(&self) -> Result<bool> {
        let meta_key = format!("{}/persistent_log_lock", self.tenant_id);
        let condition = vec![TxnCondition {
            key: meta_key.clone(),
            expected: ConditionResult::Eq as i32,
            target: Some(txn_condition::Target::Seq(0)),
        }];

        let if_then = vec![TxnOp::put_with_ttl(
            &meta_key,
            self.node_id.clone().into(),
            Some(Duration::from_secs(self.interval as u64)),
        )];

        let txn = TxnRequest::new(condition, if_then);
        let resp = self.meta_store.transaction(txn).await?;

        Ok(resp.success)
    }

    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        let context = session.create_query_context().await?;
        let query_id = context.get_id();
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(query_id.clone());
        tracking_payload.mem_stat = Some(MemStat::create(format!("Query-{}", query_id)));
        let _guard = ThreadTracker::tracking(tracking_payload);
        ThreadTracker::tracking_future(self.do_execute(context, sql)).await?;
        Ok(())
    }

    async fn do_execute(&self, context: Arc<QueryContext>, sql: &str) -> Result<()> {
        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let _ = executor.execute(context).await?;
        Ok(())
    }

    pub async fn prepare(&self) -> Result<()> {
        let stage_name = self.stage_name.clone();
        let create_stage = format!("CREATE STAGE IF NOT EXISTS {}", stage_name);
        self.execute_sql(&create_stage).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS persistent_system";
        self.execute_sql(create_db).await?;

        let tables: Vec<Box<dyn PersistentLogTable>> = vec![Box::new(QueryLogTable {})];
        for table in tables {
            let session = create_session(&self.tenant_id, &self.cluster_id).await?;
            let context = session.create_query_context().await?;
            let table_name = table.table_name();
            let old_table = context
                .get_table(CATALOG_DEFAULT, "persistent_system", table_name)
                .await;
            if old_table.is_ok() {
                let old_schema = old_table?.schema();
                if !table.schema_equal(old_schema) {
                    let rename_target =
                        format!("`{}_old_{}`", table_name, chrono::Utc::now().timestamp());
                    let rename = format!(
                        "ALTER TABLE persistent_system.{} RENAME TO {}",
                        table_name, rename_target
                    );
                    info!("Persistent log table already exists, but schema is different, renaming to {}", rename_target);
                    self.execute_sql(&rename).await?;
                }
            } else {
                info!("Persistent log table {} not exists, creating", table_name);
            }
            let create_table = table.create_table_sql();
            self.execute_sql(&create_table).await?;
        }
        Ok(())
    }

    async fn do_copy_into(&self) -> Result<()> {
        let stage_name = GlobalPersistentLog::instance().stage_name.clone();
        let sql = format!(
            "COPY INTO persistent_system.query_log
             FROM @{} PATTERN = '.*[.]parquet' file_format = (TYPE = PARQUET)
             PURGE = TRUE",
            stage_name
        );
        self.execute_sql(&sql).await
    }

    /// Do retention and vacuum
    #[allow(dead_code)]
    async fn clean(&self) -> Result<()> {
        let delete = format!(
            "DELETE FROM persistent_system.query_log WHERE timestamp < subtract_hours(NOW(), {})",
            self.retention
        );
        self.execute_sql(&delete).await?;

        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        let context = session.create_query_context().await?;
        if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(context.get_license_key(), Feature::Vacuum)
            .is_ok()
        {
            let vacuum = "VACUUM TABLE persistent_system.query_log";
            self.execute_sql(vacuum).await?
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

/// GlobalLogger initialization before the operator
/// This is a workaround for the current architecture.
pub async fn setup_operator() -> Result<()> {
    let op = DataOperator::instance().operator();
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
