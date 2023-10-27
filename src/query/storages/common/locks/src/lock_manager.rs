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

use async_channel::Sender;
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::time::timeout;
use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::lock_api::LockApi;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchRequest;
use common_pipeline_core::LockGuard;
use common_pipeline_core::UnlockApi;
use common_users::UserApiProvider;
use futures_util::StreamExt;
use parking_lot::RwLock;

use crate::record_table_lock_nums;
use crate::table_lock::TableLock;
use crate::LockHolder;

pub struct LockManager {
    active_locks: Arc<RwLock<HashMap<u64, Arc<Mutex<LockHolder>>>>>,
    tx: Sender<u64>,
}

impl LockManager {
    pub fn init() -> Result<()> {
        let (tx, rx) = async_channel::unbounded();
        let active_locks = Arc::new(RwLock::new(HashMap::new()));
        let lock_manager = Self { active_locks, tx };
        GlobalIORuntime::instance().spawn({
            let active_locks = lock_manager.active_locks.clone();
            async move {
                while let Ok(revision) = rx.recv().await {
                    let lock = active_locks.write().remove(&revision);
                    if let Some(lock) = lock {
                        let mut guard = lock.lock().await;
                        if let Err(cause) = guard.shutdown().await {
                            log::warn!("Cannot release table lock, cause {:?}", cause);
                        }
                    }
                }
            }
        });
        GlobalInstance::set(Arc::new(lock_manager));
        Ok(())
    }

    pub fn instance() -> Arc<LockManager> {
        GlobalInstance::get()
    }

    pub fn create_table_lock(table_info: TableInfo) -> TableLock {
        TableLock::create(LockManager::instance(), table_info)
    }

    #[async_backtrace::framed]
    pub async fn try_lock<T: LockApi + ?Sized>(
        self: &Arc<Self>,
        ctx: Arc<dyn TableContext>,
        lock: &T,
    ) -> Result<Option<LockGuard>> {
        let expire_secs = ctx.get_settings().get_table_lock_expire_secs()?;
        let catalog = ctx.get_catalog(lock.catalog()).await?;

        // get a new table lock revision.
        let res = catalog
            .create_table_lock_rev(lock.create_table_lock_req(expire_secs))
            .await?;
        let revision = res.revision;

        let duration = Duration::from_secs(expire_secs);
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let list_table_lock_req = lock.list_table_lock_req();
        let delete_table_lock_req = lock.delete_table_lock_req(revision);
        loop {
            // List all revisions and check if the current is the minimum.
            let reply = catalog
                .list_table_lock_revs(list_table_lock_req.clone())
                .await?;
            let position = reply.iter().position(|x| *x == revision).ok_or(
                // If the current is not found in list,  it means that the current has expired.
                ErrorCode::TableLockExpired("the acquired table lock has expired".to_string()),
            )?;

            if position == 0 {
                // The lock is acquired by current session.
                break;
            }

            // Get the previous revision, watch the delete event.
            let req = WatchRequest {
                key: lock.watch_delete_key(reply[position - 1]),
                key_end: None,
                filter_type: FilterType::Delete.into(),
            };
            let mut watch_stream = meta_api.watch(req).await?;
            // Add a timeout period for watch.
            match timeout(duration, async move {
                while let Some(Ok(resp)) = watch_stream.next().await {
                    if let Some(event) = resp.event {
                        if event.current.is_none() {
                            break;
                        }
                    }
                }
            })
            .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    catalog
                        .delete_table_lock_rev(delete_table_lock_req.clone())
                        .await?;
                    Err(ErrorCode::TableAlreadyLocked(
                        "table is locked by other session, please retry later".to_string(),
                    ))
                }
            }?;
        }

        let mut lock_holder = LockHolder::create();
        lock_holder
            .start(catalog, lock, expire_secs, revision)
            .await?;

        // metrics.
        record_table_lock_nums(lock.level(), lock.table_id(), 1);

        let mut active_locks = self.active_locks.write();
        let prev = active_locks.insert(revision, Arc::new(Mutex::new(lock_holder)));
        assert!(prev.is_none());

        let guard = LockGuard::new(self.clone(), revision);
        Ok(Some(guard))
    }
}

impl UnlockApi for LockManager {
    fn unlock(&self, revision: u64) {
        let _ = self.tx.send_blocking(revision);
    }
}
