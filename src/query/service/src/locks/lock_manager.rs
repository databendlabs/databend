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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::Lock;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::lock::metrics_inc_shutdown_lock_holder_nums;
use databend_common_metrics::lock::metrics_inc_start_lock_holder_nums;
use databend_common_pipeline::core::LockGuard;
use databend_common_pipeline::core::UnlockApi;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::locks::lock_holder::LockHolder;
use crate::locks::table_lock::TableLock;

pub struct LockManager {
    active_locks: Arc<RwLock<HashMap<u64, Arc<LockHolder>>>>,
    tx: mpsc::UnboundedSender<u64>,
}

impl LockManager {
    pub fn init() -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let active_locks = Arc::new(RwLock::new(HashMap::new()));
        let lock_manager = Self { active_locks, tx };
        let active_locks_clone = lock_manager.active_locks.clone();
        GlobalIORuntime::instance().spawn(async move {
            while let Some(revision) = rx.recv().await {
                metrics_inc_shutdown_lock_holder_nums();
                if let Some(lock) = active_locks_clone.write().remove(&revision) {
                    lock.shutdown();
                }
            }
        });
        GlobalInstance::set(Arc::new(lock_manager));
        Ok(())
    }

    pub fn instance() -> Arc<LockManager> {
        GlobalInstance::get()
    }

    pub fn create_table_lock(table_info: TableInfo) -> Result<Arc<dyn Lock>> {
        let lock_mgr = LockManager::instance();
        Ok(TableLock::create(lock_mgr, table_info))
    }

    /// The requested lock returns a global incremental revision, listing all existing revisions,
    /// and if the current revision is the smallest, the lock is acquired successfully.
    /// Otherwise, listen to the deletion event of the previous revision in a loop until get lock success.
    ///
    /// NOTICE: the lock holder is not 100% reliable.
    /// E.g., there is a very small probability of failure in extending or deleting the lock.
    #[async_backtrace::framed]
    pub async fn try_lock(
        self: &Arc<Self>,
        ctx: Arc<dyn TableContext>,
        lock_key: LockKey,
        catalog_name: &str,
        should_retry: bool,
    ) -> Result<Option<Arc<LockGuard>>> {
        let acquire_timeout = Duration::from_secs(ctx.get_settings().get_acquire_lock_timeout()?);

        let ttl = Duration::from_secs(ctx.get_settings().get_table_lock_expire_secs()?);
        let req = CreateLockRevReq::new(
            lock_key,
            ctx.get_current_user()?.name,       // user
            ctx.get_cluster().local_id.clone(), // node
            ctx.get_id(),                       // query_id
            ttl,
        );

        let catalog = ctx.get_catalog(catalog_name).await?;

        let lock_holder = Arc::new(LockHolder::default());
        match lock_holder
            .try_acquire_lock(catalog, req, should_retry, acquire_timeout)
            .await
        {
            Ok(revision) => {
                self.insert_lock(revision, lock_holder);
                let guard = LockGuard::new(self.clone(), revision);

                Ok(Some(Arc::new(guard)))
            }
            Err(err) => {
                lock_holder.shutdown();
                Err(err)
            }
        }
    }

    fn insert_lock(&self, revision: u64, lock_holder: Arc<LockHolder>) {
        let mut active_locks = self.active_locks.write();
        let prev = active_locks.insert(revision, lock_holder);
        assert!(prev.is_none());

        // metrics.
        metrics_inc_start_lock_holder_nums();
    }
}

impl UnlockApi for LockManager {
    fn unlock(&self, revision: u64) {
        let _ = self.tx.send(revision);
    }
}
