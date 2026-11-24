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
use std::time::Duration;

use databend_common_base::runtime::block_on;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_meta_client::ClientHandle;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::Operation;
use databend_meta_client::types::UpsertKV;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;
use futures_util::StreamExt;

use crate::meta_service_error;

/// refer to [crate::history_tables::meta::PermitGuard]
pub struct PermitGuard {
    _permit: Permit,
    meta_handle: Arc<TaskMetaHandle>,
    meta_key: String,
}

impl PermitGuard {
    pub fn new(permit: Permit, meta_handle: Arc<TaskMetaHandle>, meta_key: String) -> Self {
        Self {
            _permit: permit,
            meta_handle,
            meta_key,
        }
    }
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        let meta_handle = self.meta_handle.clone();
        let meta_key = self.meta_key.clone();

        block_on(async move {
            let _ = meta_handle.update_last_execution_timestamp(&meta_key).await;
        });
    }
}

/// refer to [crate::history_tables::meta::HistoryMetaHandle]
pub struct TaskMetaHandle {
    meta_client: Arc<ClientHandle<DatabendRuntime>>,
    node_id: String,
}

impl TaskMetaHandle {
    pub fn new(meta_client: Arc<ClientHandle<DatabendRuntime>>, node_id: String) -> Self {
        Self {
            meta_client,
            node_id,
        }
    }

    pub fn meta_client(&self) -> &Arc<ClientHandle<DatabendRuntime>> {
        &self.meta_client
    }

    pub async fn acquire(&self, meta_key: &str, interval_millis: u64) -> Result<Option<Permit>> {
        let acquired_guard = Semaphore::new_acquired(
            self.meta_client.clone(),
            meta_key,
            1,
            self.node_id.clone(),
            Duration::from_secs(3),
        )
        .await
        .map_err(|_e| "acquire semaphore failed from TaskService")?;
        if interval_millis == 0 {
            return Ok(Some(acquired_guard));
        }
        if match self
            .meta_client
            .get_kv(&format!("{}/last_timestamp", meta_key))
            .await
            .map_err(meta_service_error)?
        {
            Some(v) => {
                let last: u64 = serde_json::from_slice(&v.data)?;
                chrono::Utc::now().timestamp_millis() as u64
                    - Duration::from_millis(interval_millis).as_millis() as u64
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

    /// Take a lease of `meta_key` that lasts while the returned permit is held, and ends within
    /// `ttl` after the process holding it stops.
    pub async fn acquire_lease(&self, meta_key: &str, ttl: Duration) -> Result<Permit> {
        // Unbounded: the semaphore only keeps the lease of each permit, and admits every one.
        Semaphore::new_acquired(
            self.meta_client.clone(),
            meta_key,
            u64::MAX,
            self.node_id.clone(),
            ttl,
        )
        .await
        .map_err(|e| ErrorCode::MetaServiceError(format!("acquire the lease {meta_key}: {e}")))
    }

    /// Whether a process holds a lease of `meta_key` taken by [`Self::acquire_lease`].
    pub async fn is_leased(&self, meta_key: &str) -> Result<bool> {
        // A semaphore stores each permit at `{prefix}/queue/{seq}`, see `PermitKey::format_key`.
        // meta-service lists no expired one.
        let mut permits = self
            .meta_client
            .list(&format!("{meta_key}/queue/"))
            .await
            .map_err(meta_service_error)?;
        match permits.next().await {
            Some(Ok(_)) => Ok(true),
            Some(Err(status)) => Err(ErrorCode::MetaServiceError(format!(
                "list the lease {meta_key}: {status}"
            ))),
            None => Ok(false),
        }
    }

    pub async fn acquire_with_guard(
        &self,
        meta_key: &str,
        interval_millis: u64,
    ) -> Result<Option<PermitGuard>> {
        if let Some(permit) = self.acquire(meta_key, interval_millis).await? {
            Ok(Some(PermitGuard::new(
                permit,
                Arc::new(TaskMetaHandle {
                    meta_client: self.meta_client.clone(),
                    node_id: self.node_id.clone(),
                }),
                meta_key.to_string(),
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn update_last_execution_timestamp(&self, meta_key: &str) -> Result<()> {
        self.meta_client
            .upsert_kv(UpsertKV::new(
                format!("{}/last_timestamp", meta_key),
                MatchSeq::Any,
                Operation::Update(serde_json::to_vec(&chrono::Utc::now().timestamp_millis())?),
                None,
            ))
            .await
            .map_err(meta_service_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use databend_common_exception::Result;
    use databend_common_meta_store::MetaStore;
    use databend_meta_runtime::DatabendRuntime;

    use crate::task::meta::TaskMetaHandle;

    /// A lease is seen while its permit is held, and no longer once the permit is dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lease_is_held_until_dropped() -> Result<()> {
        let store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let handle = TaskMetaHandle::new(store.inner().clone(), "node-1".to_string());

        assert!(!handle.is_leased("tenant/task_run_lease/a").await?);

        let lease = handle
            .acquire_lease("tenant/task_run_lease/a", Duration::from_secs(30))
            .await?;
        assert!(handle.is_leased("tenant/task_run_lease/a").await?);
        assert!(!handle.is_leased("tenant/task_run_lease/b").await?);

        // A background task deletes the permit entry of a dropped permit.
        drop(lease);
        let mut leased = true;
        for _ in 0..50 {
            leased = handle.is_leased("tenant/task_run_lease/a").await?;
            if !leased {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            !leased,
            "the lease is still held after its permit is dropped"
        );
        Ok(())
    }
}
