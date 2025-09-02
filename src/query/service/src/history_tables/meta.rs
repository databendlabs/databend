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

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime::block_on;
use databend_common_base::runtime::spawn_named;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::Semaphore;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::FutureExt;
use log::debug;
use log::warn;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

/// RAII wrapper for Permit that automatically updates timestamp on drop
pub struct PermitGuard {
    _permit: Permit,
    meta_handle: Arc<HistoryMetaHandle>,
    meta_key: String,
}

impl PermitGuard {
    pub fn new(permit: Permit, meta_handle: Arc<HistoryMetaHandle>, meta_key: String) -> Self {
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

pub struct HeartbeatTaskGuard {
    _cancel: oneshot::Sender<()>,
    exited: Arc<AtomicBool>,
}

impl HeartbeatTaskGuard {
    pub fn exited(&self) -> bool {
        self.exited.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeartbeatMessage {
    from_node_id: String,
}

pub struct HeartbeatTask;

impl HeartbeatTask {
    pub async fn new_task(
        meta_client: Arc<ClientHandle>,
        meta_key: &str,
        node_id: &str,
        dead_in_secs: u64,
    ) -> Result<Option<HeartbeatTaskGuard>> {
        let heartbeat_key = format!("{}/heartbeat", meta_key);

        let txn_req = {
            let heartbeat_message = serde_json::to_vec(&HeartbeatMessage {
                from_node_id: node_id.to_string(),
            })?;
            // Condition: Check if the key does not exist
            let condition = vec![TxnCondition::eq_seq(&heartbeat_key, 0)];

            // If-Then: If the key does not exist, put the new value.
            let if_then = vec![TxnOp::put_with_ttl(
                &heartbeat_key,
                heartbeat_message,
                Some(Duration::from_secs(dead_in_secs)),
            )];

            // Else: If the key already exists, do nothing.
            TxnRequest::new(condition, if_then)
        };

        let resp = meta_client.transaction(txn_req).await?;

        // key already exits
        if !resp.success {
            return Ok(None);
        }

        debug!("[HISTORY-TABLES] Heartbeat key created: {}", &heartbeat_key);

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let loop_fut = HeartbeatTask::heartbeat_loop(
            meta_client,
            heartbeat_key,
            node_id.to_string(),
            dead_in_secs,
            cancel_rx.map(|_| ()),
        );

        let exited = Arc::new(AtomicBool::new(false));

        let task_name = format!("heartbeat_{}", &meta_key);
        let exited_clone = exited.clone();
        spawn_named(
            async move {
                let result = loop_fut.await;
                if result.is_err() {
                    warn!(
                        "[HISTORY-TABLES] Heartbeat loop exited with error: {:?}",
                        result
                    );
                }
                exited_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            },
            task_name,
        );

        Ok(Some(HeartbeatTaskGuard {
            _cancel: cancel_tx,
            exited,
        }))
    }

    pub async fn heartbeat_loop(
        meta_client: Arc<ClientHandle>,
        heartbeat_key: String,
        node_id: String,
        dead_in_secs: u64,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        // sleep from dead_in_secs/3 ~ dead_in_secs*2/3
        let jitter = rand::random::<u64>() % (dead_in_secs / 3);
        let sleep_time = Duration::from_secs(dead_in_secs / 3 + jitter);

        let mut c = pin!(cancel);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(sleep_time) => {
                    let txn_req = {
                        let heartbeat_message = serde_json::to_vec(&HeartbeatMessage {
                            from_node_id: node_id.to_string(),
                        })?;
                        // Condition: Check if the heartbeat is from this node
                        let condition = vec![TxnCondition::eq_value(&heartbeat_key, heartbeat_message.clone())];

                        // If-Then: if the heartbeat is from this node, refresh ttl
                        let if_then = vec![TxnOp::put_with_ttl(
                            &heartbeat_key,
                            heartbeat_message,
                            Some(Duration::from_secs(dead_in_secs)),
                        )];

                        TxnRequest::new(condition, if_then)
                    };

                    let resp = meta_client.transaction(txn_req).await?;
                    if !resp.success {
                        return Err(ErrorCode::Internal("Heartbeat is from other node, stopping"))
                    }

                }
                _ = &mut c =>{
                    let txn_req = {
                        let heartbeat_message = serde_json::to_vec(&HeartbeatMessage {
                            from_node_id: node_id.to_string(),
                        })?;
                        // Condition: Check if the heartbeat is from this node
                        let condition = vec![TxnCondition::eq_value(&heartbeat_key, heartbeat_message)];

                        // If-Then: if the heartbeat is from this node, delete it
                        let if_then = vec![TxnOp::delete(&heartbeat_key)];

                        TxnRequest::new(condition, if_then)
                    };
                    let _resp = meta_client.transaction(txn_req).await?;

                    debug!("[HISTORY-TABLES] Heartbeat key delete: {}", &heartbeat_key);

                    return Ok(())
                }
            }
        }
    }
}

/// Service for interacting with the meta store for persistent meta information
pub struct HistoryMetaHandle {
    meta_client: Arc<ClientHandle>,
    node_id: String,
}

impl HistoryMetaHandle {
    /// Creates a new instance of `HistoryMetaHandle`.
    pub fn new(meta_client: Arc<ClientHandle>, node_id: String) -> Self {
        Self {
            meta_client,
            node_id,
        }
    }

    /// Acquires a permit from a distributed semaphore with timestamp-based rate limiting.
    /// If `interval` is 0, it will not apply rate limiting and will always return a permit.
    ///
    /// This function attempts to acquire a permit from a distributed semaphore identified by `meta_key`.
    /// It also implements a rate limiting mechanism based on the last execution timestamp
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
        .map_err(|e| format!("acquire semaphore failed from GlobalHistoryLog {}", e))?;
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

    /// Acquires a permit with automatic timestamp update on drop using RAII pattern.
    /// Returns a PermitGuard that will automatically update the timestamp when dropped.
    pub async fn acquire_with_guard(
        &self,
        meta_key: &str,
        interval: u64,
    ) -> Result<Option<PermitGuard>> {
        if let Some(permit) = self.acquire(meta_key, interval).await? {
            Ok(Some(PermitGuard::new(
                permit,
                Arc::new(HistoryMetaHandle {
                    meta_client: self.meta_client.clone(),
                    node_id: self.node_id.clone(),
                }),
                meta_key.to_string(),
            )))
        } else {
            Ok(None)
        }
    }

    /// Updating the last execution timestamp in the metadata.
    pub async fn update_last_execution_timestamp(&self, meta_key: &str) -> Result<()> {
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

    pub async fn create_heartbeat_task(
        &self,
        meta_key: &str,
        dead_in_secs: u64,
    ) -> Result<Option<HeartbeatTaskGuard>> {
        HeartbeatTask::new_task(
            self.meta_client.clone(),
            meta_key,
            &self.node_id,
            dead_in_secs,
        )
        .await
    }

    // Check if heartbeat key exists and is from this node
    pub async fn is_heartbeat_valid(&self, meta_key: &str) -> Result<bool> {
        let heartbeat_key = format!("{}/heartbeat", meta_key);

        match self.meta_client.get_kv(&heartbeat_key).await? {
            Some(v) => {
                let msg: HeartbeatMessage = serde_json::from_slice(&v.data)?;
                Ok(msg.from_node_id == self.node_id)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use databend_common_base::runtime::spawn;
    use databend_common_exception::Result;
    use databend_common_meta_kvapi::kvapi::KVApi;
    use databend_common_meta_store::MetaStore;
    use databend_common_meta_types::UpsertKV;

    use crate::history_tables::meta::HeartbeatMessage;
    use crate::history_tables::meta::HeartbeatTask;
    use crate::history_tables::meta::HistoryMetaHandle;

    pub async fn setup_meta_client() -> MetaStore {
        MetaStore::new_local_testing(&databend_common_version::BUILD_INFO).await
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_permit_guard() -> databend_common_exception::Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let node_id = "test_node_123".to_string();
        let meta_handle = HistoryMetaHandle::new(meta_client, node_id);

        // Test 1: Basic permit acquisition with interval 0 (no rate limiting)
        let meta_key = "test/history_table/permit_guard";
        let guard_result = meta_handle.acquire_with_guard(meta_key, 0).await?;
        assert!(
            guard_result.is_some(),
            "Should acquire permit when interval is 0"
        );

        if let Some(guard) = guard_result {
            // Verify that the guard contains the correct meta key
            assert_eq!(guard.meta_key, meta_key);
        }

        // Same meta key, because we set the interval to 0, it should not block
        let guard_result2 = meta_handle.acquire_with_guard(meta_key, 0).await?;

        assert!(
            guard_result2.is_some(),
            "Should acquire permit again when interval is 0"
        );

        if let Some(guard) = guard_result2 {
            // Verify that the guard contains the correct meta key
            assert_eq!(guard.meta_key, meta_key);
        }

        // Test 2: Permit acquisition with interval > 0 (rate limiting)
        let meta_key_rate_limited = "test/history_table/permit_guard_rate_limited";
        let interval_seconds = 2;

        // First acquisition should succeed
        let first_guard_result = meta_handle
            .acquire_with_guard(meta_key_rate_limited, interval_seconds)
            .await?;
        assert!(
            first_guard_result.is_some(),
            "First permit acquisition should succeed"
        );

        // Drop the first guard to trigger timestamp update
        drop(first_guard_result);

        // Immediate second acquisition should fail due to rate limiting
        let second_guard_result = meta_handle
            .acquire_with_guard(meta_key_rate_limited, interval_seconds)
            .await?;
        assert!(
            second_guard_result.is_none(),
            "Second permit acquisition should fail due to rate limiting"
        );

        // Test 3: Verify permit guard automatically updates timestamp on drop
        let meta_key_timestamp = "test/history_table/permit_guard_timestamp";

        // Get initial timestamp (should be None)
        let initial_timestamp = meta_handle
            .get_u64_from_meta(&format!("{}/last_timestamp", meta_key_timestamp))
            .await?;
        assert!(
            initial_timestamp.is_none(),
            "Initial timestamp should be None"
        );

        // Acquire permit with guard
        let guard = meta_handle
            .acquire_with_guard(meta_key_timestamp, 0)
            .await?;
        assert!(guard.is_some(), "Should acquire permit");

        // Drop guard to trigger timestamp update
        drop(guard);

        // Verify timestamp was updated
        let updated_timestamp = meta_handle
            .get_u64_from_meta(&format!("{}/last_timestamp", meta_key_timestamp))
            .await?;
        assert!(
            updated_timestamp.is_some(),
            "Timestamp should be updated after guard drop"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_basic() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let heartbeat_guard =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id_1", 4)
                .await?;

        assert!(heartbeat_guard.is_some());

        let heartbeat_guard_none =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id_2", 4)
                .await?;

        assert!(heartbeat_guard_none.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_extend() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let heartbeat_guard =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id1", 4)
                .await?;

        assert!(heartbeat_guard.is_some());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let heartbeat_guard_none =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id1", 4)
                .await?;

        assert!(heartbeat_guard_none.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_drop() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let heartbeat_guard =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id1", 4)
                .await?;

        assert!(heartbeat_guard.is_some());

        drop(heartbeat_guard);

        tokio::time::sleep(Duration::from_secs(1)).await;

        let heartbeat_guard_new =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id1", 4)
                .await?;

        assert!(heartbeat_guard_new.is_some());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_from_others() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let heartbeat_guard =
            HeartbeatTask::new_task(meta_client.clone(), "test_heartbeat_key", "node_id1", 4)
                .await?;

        assert!(heartbeat_guard.is_some());

        // other node override heartbeat
        let heartbeat_message = serde_json::to_vec(&HeartbeatMessage {
            from_node_id: "node_id2".to_string(),
        })?;
        let upsert = UpsertKV::update("test_heartbeat_key/heartbeat", &heartbeat_message)
            .with_ttl(Duration::from_secs(2));

        meta_client.upsert_kv(upsert).await?;

        tokio::time::sleep(Duration::from_secs(3)).await;

        let is_exited = heartbeat_guard.unwrap().exited.load(Ordering::SeqCst);
        assert!(is_exited);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_concurrent() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.deref().clone();

        let barrier = Arc::new(tokio::sync::Barrier::new(5));
        let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let mut handles = Vec::new();

        for i in 0..5 {
            let meta_client_clone = meta_client.clone();
            let barrier_clone = barrier.clone();
            let results_clone = results.clone();

            let handle = spawn(async move {
                // Wait for all tasks to reach this point
                barrier_clone.wait().await;

                // All tasks try to create heartbeat for the same key simultaneously
                let heartbeat_guard =
                    HeartbeatTask::new_task(meta_client_clone, "test_heartbeat_key", "node_id1", 4)
                        .await
                        .unwrap();

                // Store result
                let mut results_guard = results_clone.lock().await;
                results_guard.push((i, heartbeat_guard.is_some()));

                heartbeat_guard
            });

            handles.push(handle);
        }

        let mut guards = Vec::new();
        for handle in handles {
            guards.push(handle.await.unwrap());
        }

        // Verify results: exactly one should succeed, four should fail
        let results_guard = results.lock().await;
        let successful_count = results_guard.iter().filter(|(_, success)| *success).count();
        let failed_count = results_guard
            .iter()
            .filter(|(_, success)| !*success)
            .count();

        assert_eq!(
            successful_count, 1,
            "Exactly one heartbeat creation should succeed"
        );
        assert_eq!(
            failed_count, 4,
            "Four heartbeat creation attempts should fail"
        );

        // Clean up: drop the successful guard to allow cleanup
        drop(guards);
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }
}
