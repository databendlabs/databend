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
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::Semaphore;
use databend_common_sql::Planner;
use databend_common_storage::DataOperator;
use databend_common_tracing::GlobalLogger;
use log::error;
use log::info;
use rand::random;
use tokio::time::sleep;

use crate::interpreters::InterpreterFactory;
use crate::persistent_log::session::create_session;
use crate::persistent_log::table_schemas::PersistentLogTable;
use crate::persistent_log::table_schemas::QueryDetailsTable;
use crate::persistent_log::table_schemas::QueryLogTable;
use crate::persistent_log::table_schemas::QueryProfileTable;
use crate::sessions::QueryContext;

pub struct GlobalPersistentLog {
    meta_client: Option<Arc<ClientHandle>>,
    interval: usize,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
    stopped: AtomicBool,
    tables: Vec<Box<dyn PersistentLogTable>>,
    retention: usize,
}

impl GlobalPersistentLog {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        setup_operator().await?;

        let meta_client =
            MetaGrpcClient::try_new(&cfg.meta.to_meta_grpc_client_conf()).map_err(|_e| {
                ErrorCode::Internal("Create MetaClient failed for GlobalPersistentLog")
            })?;

        let mut tables: Vec<Box<dyn PersistentLogTable>> = vec![];

        if cfg.log.query.on {
            let query_details = QueryDetailsTable;
            info!(
                "Persistent query details table is enabled, persistent_system.{}",
                query_details.table_name()
            );
            tables.push(Box::new(query_details));
        }

        if cfg.log.profile.on {
            let profile = QueryProfileTable;
            info!(
                "Persistent query profile table is enabled, persistent_system.{}",
                profile.table_name()
            );
            tables.push(Box::new(profile));
        }

        let query_log = QueryLogTable;
        info!(
            "Persistent query log table is enabled, persistent_system.{}",
            query_log.table_name()
        );
        tables.push(Box::new(query_log));

        let instance = Arc::new(Self {
            meta_client: Some(meta_client),
            interval: cfg.log.persistentlog.interval,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name: cfg.log.persistentlog.stage_name.clone(),
            initialized: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            tables,
            retention: cfg.log.persistentlog.retention,
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

    pub async fn create_dummy(cfg: &InnerConfig) -> Result<Self> {
        setup_operator().await?;
        Ok(Self {
            meta_client: None,
            interval: cfg.log.persistentlog.interval,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name: cfg.log.persistentlog.stage_name.clone(),
            initialized: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            tables: vec![
                Box::new(QueryDetailsTable),
                Box::new(QueryProfileTable),
                Box::new(QueryLogTable),
            ],
            retention: cfg.log.persistentlog.retention,
        })
    }

    pub fn instance() -> Arc<GlobalPersistentLog> {
        GlobalInstance::get()
    }

    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub async fn work(&self) -> Result<()> {
        let mut prepared = false;
        // Wait all services to be initialized
        loop {
            if !self.initialized.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        spawn(async move {
            if let Err(e) = GlobalPersistentLog::instance().clean_work().await {
                error!("Persistent log clean_work exit {}", e);
            }
        });
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return Ok(());
            }
            // create the stage, database and table if not exists
            // alter the table if schema is changed
            if !prepared {
                let prepare_guard = self
                    .acquire(
                        format!("{}/persistent_log_prepare", self.tenant_id),
                        self.interval as u64,
                    )
                    .await?;
                match self.prepare().await {
                    Ok(_) => {
                        info!("Persistent log prepared successfully");
                        prepared = true;
                    }
                    Err(e) => {
                        error!("Persistent log prepare failed: {:?}", e);
                    }
                }
                drop(prepare_guard);
            }

            let guard = self
                .acquire(
                    format!("{}/persistent_log_work", self.tenant_id),
                    self.interval as u64,
                )
                .await?;
            // add a random sleep time to avoid always one node doing the work
            let sleep_time = self.interval as u64 * 1000 + random::<u64>() % 1000;
            tokio::time::sleep(Duration::from_millis(sleep_time)).await;

            if let Err(e) = self.do_copy_into().await {
                error!("Persistent log copy into failed: {:?}", e);
            }

            drop(guard)
        }
    }

    /// Multiple nodes doing the work may make commit conflict.
    /// acquire the semaphore to avoid this.
    pub async fn acquire(&self, meta_key: String, lease: u64) -> Result<Permit> {
        let acquired_guard = Semaphore::new_acquired(
            self.meta_client.clone().unwrap(),
            meta_key,
            1,
            self.node_id.clone(),
            Duration::from_secs(lease),
        )
        .await
        .map_err(|_e| "acquire semaphore failed from GlobalPersistentLog")?;

        Ok(acquired_guard)
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

        for table in &self.tables {
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

    pub async fn do_copy_into(&self) -> Result<()> {
        let stage_name = self.stage_name.clone();
        let operator = GlobalLogger::instance().get_operator().await;
        if let Some(op) = operator {
            let path = format!("stage/internal/{}/", stage_name);
            // Why we need to list the files first?
            // Consider this case:
            // After executing the two insert statements, a new file is created in the stage.
            // Copy into enable the `PURGE` option, which will delete all files in the stage.
            // the new file will be deleted and not inserted into the tables.
            let files: Vec<String> = op
                .list(&path)
                .await?
                .into_iter()
                .filter(|f| f.name().ends_with(".parquet"))
                .map(|f| f.name().to_string())
                .collect();
            if files.is_empty() {
                return Ok(());
            }
            for table in &self.tables {
                self.execute_sql(&table.copy_into_sql(&stage_name, &files))
                    .await?;
            }
        }
        Ok(())
    }

    async fn clean_work(&self) -> Result<()> {
        loop {
            let guard = self
                .acquire(format!("{}/persistent_log_clean", self.tenant_id), 60)
                .await?;
            sleep(Duration::from_mins(60)).await;
            if let Err(e) = self.do_clean().await {
                error!("persistent log clean failed: {}", e);
            }
            drop(guard);
        }
    }

    pub async fn do_clean(&self) -> Result<()> {
        for table in &self.tables {
            let clean_sql = table.clean_sql(self.retention);
            self.execute_sql(&clean_sql).await?;
        }

        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        let context = session.create_query_context().await?;
        if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(context.get_license_key(), Feature::Vacuum)
            .is_ok()
        {
            for table in &self.tables {
                let vacuum = format!("VACUUM TABLE persistent_system.{}", table.table_name());
                self.execute_sql(&vacuum).await?
            }
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
