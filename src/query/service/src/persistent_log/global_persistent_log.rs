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
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::TrySpawn;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
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
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use log::error;
use log::info;
use tokio::time::interval;

use crate::interpreters::InterpreterFactory;
use crate::persistent_log::session::get_persistent_log_user;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub struct GlobalPersistentLog {
    meta_store: MetaStore,
    interval: usize,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    stage_name: String,
    initialized: AtomicBool,
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

    async fn work(&self) -> Result<()> {
        let mut prepared = false;
        let mut copy_into_count = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(self.interval as u64)).await;
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
                    copy_into_count += 1;
                    if copy_into_count > 20 {
                        //@TODO: do vacuum  and delete
                    }
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
        let session_manager = SessionManager::instance();
        let dummy_session = session_manager.create_session(SessionType::Dummy).await?;
        let session = session_manager.register_session(dummy_session)?;
        let user = get_persistent_log_user(&self.tenant_id, &self.cluster_id);
        session
            .set_authed_user(user.clone(), Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()))
            .await?;
        let context = session.create_query_context().await?;
        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let _ = executor.execute(context).await?;
        Ok(())
    }

    async fn prepare(&self) -> Result<()> {
        let stage_name = self.stage_name.clone();
        let create_stage = format!("CREATE STAGE IF NOT EXISTS {}", stage_name);
        self.execute_sql(&create_stage).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS persistent_system";
        self.execute_sql(create_db).await?;
        let create_table = "
        CREATE TABLE IF NOT EXISTS persistent_system.text_log (
            timestamp TIMESTAMP,
            path VARCHAR,
            log_level VARCHAR,
            cluster_id VARCHAR,
            node_id VARCHAR,
            query_id VARCHAR,
            messages VARIANT
        ) CLUSTER BY (timestamp, query_id)";
        self.execute_sql(create_table).await?;
        Ok(())
    }

    async fn do_copy_into(&self) -> Result<()> {
        let stage_name = GlobalPersistentLog::instance().stage_name.clone();
        let sql = format!(
            "COPY INTO persistent_system.text_log
             FROM @{} PATTERN = '.*[.]parquet' file_format = (TYPE = PARQUET)
             PURGE = TRUE",
            stage_name
        );
        self.execute_sql(&sql).await
    }
}

/// GlobalLogger initialization before the operator
/// This is a workaround for the current architecture.
pub async fn setup_operator() -> Result<()> {
    let op = DataOperator::instance().operator();
    GlobalLogger::instance().set_operator(op).await;
    Ok(())
}
