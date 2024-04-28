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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_license::license::StorageQuota;

/// StorageQuotaHandler is a trait that defines whether we support storage quota or not.
#[async_trait::async_trait]
pub trait StorageQuotaHandler: Sync + Send {
    /// Fetch if storage quota is enabled.
    async fn check_license(&self) -> Result<StorageQuota>;
}

#[async_trait::async_trait]
impl StorageQuotaHandler for () {
    async fn check_license(&self) -> Result<StorageQuota> {
        Ok(StorageQuota::default())
    }
}

/// The wrapper for StorageQuotaHandler.
pub struct StorageQuotaHandlerWrapper {
    handler: Box<dyn StorageQuotaHandler>,
}

impl StorageQuotaHandlerWrapper {
    /// Create a new StorageQuotaHandlerWrapper.
    pub fn new(handler: Box<dyn StorageQuotaHandler>) -> Self {
        Self { handler }
    }

    /// Check if storage quota is enabled.
    pub async fn check_license(&self) -> Result<StorageQuota> {
        self.handler.check_license().await
    }
}

/// Fetch the StorageQuotaHandlerWrapper from the global instance.
pub fn get_storage_quota_handler() -> Arc<StorageQuotaHandlerWrapper> {
    GlobalInstance::try_get()
        .unwrap_or_else(|| Arc::new(StorageQuotaHandlerWrapper::new(Box::new(()))))
}
