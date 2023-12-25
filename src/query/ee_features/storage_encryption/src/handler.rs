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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// StorageEncryptionHandler is a trait that defines whether we support storage encryption or not.
#[async_trait::async_trait]
pub trait StorageEncryptionHandler: Sync + Send {
    /// Check if storage encryption is enabled.
    async fn check_license(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl StorageEncryptionHandler for () {
    async fn check_license(&self) -> Result<()> {
        Err(ErrorCode::LicenseKeyInvalid(
            "Storage encryption feature needs commercial license".to_string(),
        ))
    }
}

/// The wrapper for StorageEncryptionHandler.
pub struct StorageEncryptionHandlerWrapper {
    handler: Box<dyn StorageEncryptionHandler>,
}

impl StorageEncryptionHandlerWrapper {
    /// Create a new StorageEncryptionHandlerWrapper.
    pub fn new(handler: Box<dyn StorageEncryptionHandler>) -> Self {
        Self { handler }
    }

    /// Check if storage encryption is enabled.
    pub async fn check_license(&self) -> Result<()> {
        self.handler.check_license().await
    }
}

/// Fetch the StorageEncryptionHandlerWrapper from the global instance.
pub fn get_storage_encryption_handler() -> Arc<StorageEncryptionHandlerWrapper> {
    GlobalInstance::try_get()
        .unwrap_or_else(|| Arc::new(StorageEncryptionHandlerWrapper::new(Box::new(()))))
}
