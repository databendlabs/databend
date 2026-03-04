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
use databend_common_exception::Result;
use databend_meta_client::ClientHandle;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;
use databend_meta_types::Operation;
use databend_meta_types::UpsertKV;

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
