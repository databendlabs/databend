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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::spawn_named;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_meta_client::ClientHandle;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;
use databend_meta_types::Operation;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use databend_meta_types::UpsertKV;
use futures::FutureExt;
use log::debug;
use log::warn;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::meta_service_error;

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
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
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

        let resp = meta_client
            .transaction(txn_req)
            .await
            .map_err(meta_service_error)?;

        // key already exits
        if !resp.success {
            return Ok(None);
        }

        debug!("Heartbeat key created: {}", &heartbeat_key);

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let loop_fut = HeartbeatTask::heartbeat_loop(
            meta_client,
            heartbeat_key,
            node_id.to_string(),
            dead_in_secs,
            cancel_rx.map(|_| ()),
        );

        let exited = Arc::new(AtomicBool::new(false));

        let task_name = format!("heartbeat_{}", meta_key);
        let exited_clone = exited.clone();
        let meta_key = meta_key.to_string();
        spawn_named(
            async move {
                let result = loop_fut.await;
                if let Err(e) = result {
                    warn!("{} loop exited with error: {}", meta_key, e);
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
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
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

                    let resp = meta_client
                        .transaction(txn_req)
                        .await
                        .map_err(meta_service_error)?;
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
                    let _resp = meta_client
                        .transaction(txn_req)
                        .await
                        .map_err(meta_service_error)?;

                    debug!("Heartbeat key delete: {}", &heartbeat_key);

                    return Ok(())
                }
            }
        }
    }
}

/// Service for interacting with the meta store for persistent meta information
pub struct HistoryMetaHandle {
    meta_client: Arc<ClientHandle<DatabendRuntime>>,
    node_id: String,
}

impl HistoryMetaHandle {
    /// Creates a new instance of `HistoryMetaHandle`.
    pub fn new(meta_client: Arc<ClientHandle<DatabendRuntime>>, node_id: String) -> Self {
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

        let acquired_guard = tracking_payload
            .clone()
            .tracking(Semaphore::new_acquired(
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
        if match tracking_payload
            .tracking(
                self.meta_client
                    .get_kv(&format!("{}/last_timestamp", meta_key)),
            )
            .await
            .map_err(meta_service_error)?
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

    /// Checks if enough time has passed since the last execution to perform a clean operation.
    /// If enough time has passed, it updates the last execution timestamp atomically.
    /// Returns `Ok(true)` if the clean operation should be performed, `Ok(false)` otherwise.
    ///
    /// Note: This function should only be used for clean operations, cleaning tasks is idempotence
    /// and not critical if multiple nodes perform cleaning simultaneously.
    pub async fn check_should_perform_clean(
        &self,
        meta_key: &str,
        interval_secs: u64,
    ) -> Result<bool> {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let last_ts_key = format!("{}/last_timestamp", meta_key);

        let current = self
            .meta_client
            .get_kv(&last_ts_key)
            .await
            .map_err(meta_service_error)?;

        if let Some(v) = &current {
            let last_ts: u64 = serde_json::from_slice(&v.data)?;
            let enough_time =
                now_ms - Duration::from_secs(interval_secs).as_millis() as u64 > last_ts;
            if !enough_time {
                // Not enough time has passed since last execution
                return Ok(false);
            }
        }

        let last_seq = current.map_or(0, |v| v.seq);

        let condition = TxnCondition::eq_seq(last_ts_key.clone(), last_seq);
        let operation = TxnOp::put(last_ts_key, serde_json::to_vec(&now_ms)?);
        let txn_req = TxnRequest::new(vec![condition], vec![operation]);
        let resp = self
            .meta_client
            .transaction(txn_req)
            .await
            .map_err(meta_service_error)?;

        // we don't retry on failure
        // some other node has updated the timestamp and do the clean work
        Ok(resp.success)
    }

    pub async fn get_u64_from_meta(&self, meta_key: &str) -> Result<Option<u64>> {
        match self
            .meta_client
            .get_kv(meta_key)
            .await
            .map_err(meta_service_error)?
        {
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
            .await
            .map_err(meta_service_error)?;
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

        match self
            .meta_client
            .get_kv(&heartbeat_key)
            .await
            .map_err(meta_service_error)?
        {
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
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use databend_common_base::runtime::spawn;
    use databend_common_exception::Result;
    use databend_common_meta_store::MetaStore;
    use databend_meta_runtime::DatabendRuntime;
    use databend_meta_types::UpsertKV;

    use crate::history_tables::meta::HeartbeatMessage;
    use crate::history_tables::meta::HeartbeatTask;
    use crate::history_tables::meta::HistoryMetaHandle;
    use crate::meta_service_error;

    pub async fn setup_meta_client() -> MetaStore {
        MetaStore::new_local_testing::<DatabendRuntime>().await
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_basic() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.inner().clone();

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
        let meta_client = meta_store.inner().clone();

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
        let meta_client = meta_store.inner().clone();

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
        let meta_client = meta_store.inner().clone();

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

        meta_client
            .upsert_kv(upsert)
            .await
            .map_err(meta_service_error)?;

        tokio::time::sleep(Duration::from_secs(3)).await;

        let is_exited = heartbeat_guard.unwrap().exited.load(Ordering::SeqCst);
        assert!(is_exited);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_heartbeat_concurrent() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.inner().clone();

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

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_check_and_update_last_execution_timestamp() -> Result<()> {
        let meta_store = setup_meta_client().await;
        let meta_client = meta_store.inner().clone();

        let meta_handle = HistoryMetaHandle::new(meta_client, "test_node".to_string());
        let meta_key = "test/history_table/check_update";
        let interval_secs = 5;

        // First time: key missing, should insert.
        let first = meta_handle
            .check_should_perform_clean(meta_key, interval_secs)
            .await?;
        assert!(first);
        let ts_after_first = meta_handle
            .get_u64_from_meta(&format!("{}/last_timestamp", meta_key))
            .await?
            .expect("timestamp should exist");

        // Immediately again: should not update.
        let second = meta_handle
            .check_should_perform_clean(meta_key, interval_secs)
            .await?;
        assert!(!second);
        let ts_after_second = meta_handle
            .get_u64_from_meta(&format!("{}/last_timestamp", meta_key))
            .await?
            .expect("timestamp should still exist");
        assert_eq!(ts_after_first, ts_after_second);

        // Make the stored timestamp old enough, then it should update.
        let old_ts = ts_after_first.saturating_sub((interval_secs + 1) * 1000);
        meta_handle
            .set_u64_to_meta(&format!("{}/last_timestamp", meta_key), old_ts)
            .await?;

        let third = meta_handle
            .check_should_perform_clean(meta_key, interval_secs)
            .await?;
        assert!(third);
        let ts_after_third = meta_handle
            .get_u64_from_meta(&format!("{}/last_timestamp", meta_key))
            .await?
            .expect("timestamp should exist");
        assert!(ts_after_third > ts_after_first);

        Ok(())
    }
}
