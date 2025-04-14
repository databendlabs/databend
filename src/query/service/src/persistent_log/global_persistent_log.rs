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
use databend_common_tracing::PERSISTENT_LOG_SCHEMA_VERSION;
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
    meta_store: MetaStore,
    interval: u64,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
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
            let query_details = QueryDetailsTable::new();
            info!(
                "Persistent query details table is enabled, persistent_system.{}",
                query_details.table_name()
            );
            tables.push(Box::new(query_details));
        }

        if cfg.log.profile.on {
            let profile = QueryProfileTable::new();
            info!(
                "Persistent query profile table is enabled, persistent_system.{}",
                profile.table_name()
            );
            tables.push(Box::new(profile));
        }

        let query_log = QueryLogTable::new();
        info!(
            "Persistent query log table is enabled, persistent_system.{}",
            query_log.table_name()
        );
        tables.push(Box::new(query_log));

        let stage_name = format!(
            "{}_v{}",
            cfg.log.persistentlog.stage_name.clone(),
            PERSISTENT_LOG_SCHEMA_VERSION
        );

        let instance = Arc::new(Self {
            meta_store: MetaStore::R(meta_client),
            interval: cfg.log.persistentlog.interval as u64,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name,
            initialized: AtomicBool::new(false),
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

    /// Create a dummy instance of GlobalPersistentLog for testing purposes.
    pub async fn create_dummy(cfg: &InnerConfig) -> Result<Self> {
        setup_operator().await?;
        let meta_store = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf())
            .create_meta_store()
            .await?;
        Ok(Self {
            meta_store,
            interval: cfg.log.persistentlog.interval as u64,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            stage_name: cfg.log.persistentlog.stage_name.clone(),
            initialized: AtomicBool::new(false),
            tables: vec![
                Box::new(QueryDetailsTable::new()),
                Box::new(QueryProfileTable::new()),
                Box::new(QueryLogTable::new()),
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
        let meta_key = format!("{}/persistent_log_work", self.tenant_id);
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
            // create the stage, database and table if not exists
            // alter the table if schema is changed
            if !prepared {
                let prepare_guard = self.acquire(&meta_key, self.interval, 0).await?;
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
            let may_permit = self
                .acquire(&meta_key, self.interval, self.interval)
                .await?;
            if let Some(guard) = may_permit {
                if let Err(e) = self.do_copy_into().await {
                    error!("Persistent log copy into failed: {:?}", e);
                    let latest_version = self.get_version_from_meta().await?;
                    if let Some(version) = latest_version {
                        if version > PERSISTENT_LOG_SCHEMA_VERSION as u64 {
                            info!("Persistent log tables enable version suffix");
                            for table in &self.tables {
                                table.enable_version_suffix();
                            }
                        }
                    }
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
    pub async fn acquire(
        &self,
        meta_key: &str,
        lease: u64,
        interval: u64,
    ) -> Result<Option<Permit>> {
        let meta_client = match &self.meta_store {
            MetaStore::R(handle) => handle.clone(),
            _ => unreachable!("Metastore::L should only used for testing"),
        };
        let acquired_guard = Semaphore::new_acquired(
            meta_client,
            meta_key,
            1,
            self.node_id.clone(),
            Duration::from_secs(lease),
        )
        .await
        .map_err(|_e| "acquire semaphore failed from GlobalPersistentLog")?;
        if interval == 0 {
            return Ok(Some(acquired_guard));
        }
        if match self
            .meta_store
            .get_kv(&format!("{}/last_timestamp", meta_key))
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
        self.meta_store
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

        let session = create_session(&self.tenant_id, &self.cluster_id).await?;
        let context = session.create_query_context().await?;
        if let Some(version) = self.get_version_from_meta().await? {
            if version > PERSISTENT_LOG_SCHEMA_VERSION as u64 {
                // older version node need put the logs into the table has version suffix
                for table in &self.tables {
                    table.enable_version_suffix();
                }
                return Ok(());
            }
            let mut need_rename = false;
            for table in &self.tables {
                let old_table = context
                    .get_table(CATALOG_DEFAULT, "persistent_system", &table.table_name())
                    .await;
                if old_table.is_ok() {
                    let old_schema = old_table?.schema();
                    if !table.schema_equal(old_schema) {
                        need_rename = true;
                    }
                }
            }
            if need_rename {
                for table in &self.tables {
                    let old_table_name = format!("`{}_v{}`", table.table_name(), version);
                    let rename_sql = format!(
                        "ALTER TABLE IF EXISTS persistent_system.{} RENAME TO {}",
                        table.table_name(),
                        old_table_name
                    );
                    self.execute_sql(&rename_sql).await?;
                }
            }
        }
        self.set_version_to_meta(PERSISTENT_LOG_SCHEMA_VERSION)
            .await?;
        for table in &self.tables {
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
            let meta_key = format!("{}/persistent_log_clean", self.tenant_id);
            let may_permit = self.acquire(&meta_key, 60, 60 * 60).await?;
            if let Some(guard) = may_permit {
                if let Err(e) = self.do_clean().await {
                    error!("persistent log clean failed: {}", e);
                }
                self.finish_hook(&meta_key).await?;
                drop(guard);
            }

            // sleep for a random time between 30 and 90 minutes
            sleep(Duration::from_mins(30 + random::<u64>() % 60)).await;
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

    pub async fn get_version_from_meta(&self) -> Result<Option<u64>> {
        match self
            .meta_store
            .get_kv(&format!("{}/persistent_log_work/version", self.tenant_id))
            .await?
        {
            Some(v) => {
                let version: u64 = serde_json::from_slice(&v.data)?;
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    pub async fn set_version_to_meta(&self, version: usize) -> Result<()> {
        self.meta_store
            .upsert_kv(UpsertKV::new(
                &format!("{}/persistent_log_work/version", self.tenant_id),
                MatchSeq::Any,
                Operation::Update(serde_json::to_vec(&version)?),
                None,
            ))
            .await?;
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
