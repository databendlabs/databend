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
use databend_common_meta_types::UpsertKV;
use futures_util::future;

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
        .map_err(|_e| "acquire semaphore failed from GlobalHistoryLog")?;
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

    pub async fn acquire_all(
        &self,
        prepare_key: &str,
        transform_keys: &[String],
    ) -> Result<Vec<Permit>> {
        // To avoid deadlock of acquiring multiple semaphores,
        // we first acquire the prepare semaphore, then acquire others
        let prepare_permit = self.acquire(prepare_key, 0).await?;
        if let Some(prepare_permit) = prepare_permit {
            let futures = transform_keys
                .iter()
                .map(|meta_key| self.acquire(meta_key, 0));

            let results = future::join_all(futures).await;

            let mut permits = Vec::with_capacity(transform_keys.len());
            for result in results {
                if let Some(permit) = result? {
                    permits.push(permit);
                }
            }
            permits.push(prepare_permit);
            return Ok(permits);
        }
        Err(ErrorCode::Internal("Cannot acquire prepare semaphore"))
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
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::time::Duration;

    use databend_common_meta_store::MetaStoreProvider;
    use tokio::time::timeout;

    use crate::history_tables::meta::HistoryMetaHandle;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_history_table_permit_guard() -> databend_common_exception::Result<()> {
        let meta_store = MetaStoreProvider::new(Default::default())
            .create_meta_store()
            .await
            .unwrap();
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
        let guard_result2 = timeout(
            Duration::from_secs(1),
            meta_handle.acquire_with_guard(meta_key, 0),
        )
        .await;
        assert!(
            guard_result2.is_ok(),
            "Should acquire permit again when interval is 0"
        );
        if let Some(guard) = guard_result2.unwrap()? {
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
}
