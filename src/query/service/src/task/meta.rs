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
use databend_meta_plugin_semaphore::PermitKey;
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

    /// Acquire a hold on the shared lease `prefix`, kept until the returned permit is dropped.
    ///
    /// Any number of holds exist at once, so this waits for no other holder. A hold is one permit
    /// entry under [`PermitKey::queue_dir`] of `prefix`, recording this node's id, with a TTL of
    /// `ttl`: while the permit is held, a background task extends the TTL every `ttl / 3`, or
    /// every 2 s if that is shorter. Dropping the permit deletes the entry. If the deletion fails,
    /// or this process stops, the entry expires `ttl` after its last extension.
    pub async fn acquire_shared_lease(&self, prefix: &str, ttl: Duration) -> Result<Permit> {
        // A semaphore that admits every acquirer at once: each permit takes 1 of its capacity.
        Semaphore::new_acquired(
            self.meta_client.clone(),
            prefix,
            u64::MAX,
            self.node_id.clone(),
            ttl,
        )
        .await
        .map_err(|e| {
            ErrorCode::MetaServiceError(format!("acquire a hold on the shared lease {prefix}: {e}"))
        })
    }

    /// Whether the shared lease `prefix` is held: whether meta-service lists the entry of at
    /// least one hold acquired with [`Self::acquire_shared_lease`].
    ///
    /// A hold's entry is listed from when its acquisition returns until its permit is dropped, or
    /// until the entry expires if its holder stops or fails to delete it. meta-service lists no
    /// expired entry. Any listed entry answers the question, whoever holds it, so only the first
    /// one is read.
    pub async fn is_shared_lease_held(&self, prefix: &str) -> Result<bool> {
        let mut entries = self
            .meta_client
            .list(&PermitKey::queue_dir(prefix))
            .await
            .map_err(meta_service_error)?;

        match entries.next().await {
            Some(Ok(_entry)) => Ok(true),
            Some(Err(status)) => Err(ErrorCode::MetaServiceError(format!(
                "list the holds on the shared lease {prefix}: {status}"
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
    use databend_meta_plugin_semaphore::PermitKey;
    use databend_meta_runtime::DatabendRuntime;
    use futures_util::StreamExt;
    use tokio::time::timeout;

    use crate::meta_service_error;
    use crate::task::meta::TaskMetaHandle;

    /// A shared lease is held while any of its holders keeps its permit, and a hold does not wait
    /// for another one.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_shared_lease_is_held_while_any_hold_is_kept() -> Result<()> {
        let store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let handle = TaskMetaHandle::new(store.inner().clone(), "node-1".to_string());
        let ttl = Duration::from_secs(30);

        assert!(!handle.is_shared_lease_held("tenant/lease/a").await?);

        let first = handle.acquire_shared_lease("tenant/lease/a", ttl).await?;
        let second = timeout(
            Duration::from_secs(5),
            handle.acquire_shared_lease("tenant/lease/a", ttl),
        )
        .await
        .expect("a hold must not wait for another one")?;
        assert_eq!(count_holds(&store, "tenant/lease/a", 2).await?, 2);
        assert!(handle.is_shared_lease_held("tenant/lease/a").await?);
        assert!(!handle.is_shared_lease_held("tenant/lease/b").await?);

        // A background task deletes the entry of a dropped permit.
        drop(first);
        assert_eq!(count_holds(&store, "tenant/lease/a", 1).await?, 1);
        assert!(handle.is_shared_lease_held("tenant/lease/a").await?);

        drop(second);
        assert_eq!(count_holds(&store, "tenant/lease/a", 0).await?, 0);
        assert!(!handle.is_shared_lease_held("tenant/lease/a").await?);
        Ok(())
    }

    /// The number of hold entries of the shared lease `prefix` that meta-service lists, read
    /// again for up to 5 s until it is `expected`.
    async fn count_holds(store: &MetaStore, prefix: &str, expected: usize) -> Result<usize> {
        let mut count = 0;
        for _ in 0..50 {
            let entries = store
                .inner()
                .list(&PermitKey::queue_dir(prefix))
                .await
                .map_err(meta_service_error)?;
            count = entries.count().await;
            if count == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(count)
    }
}
