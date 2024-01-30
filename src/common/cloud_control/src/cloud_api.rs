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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::task_client::TaskClient;

pub const CLOUD_REQUEST_TIMEOUT_SEC: u64 = 5; // 5 seconds

// commonly used metadata
pub const TENANT_ID: &str = "X-DATABEND-TENANT";
pub const REQUESTER: &str = "X-DATABEND-USER";
pub const QUERY_ID: &str = "X-DATABEND-QUERY-ID";

pub struct CloudControlApiProvider {
    pub task_client: Arc<TaskClient>,
    pub timeout: Duration,
}

impl CloudControlApiProvider {
    pub async fn new(endpoint: String, timeout: u64) -> Result<Arc<CloudControlApiProvider>> {
        let timeout = if timeout == 0 {
            Duration::from_secs(CLOUD_REQUEST_TIMEOUT_SEC)
        } else {
            Duration::from_secs(timeout)
        };

        let endpoint = Self::get_endpoint(endpoint, timeout).await?;
        let task_client = TaskClient::new(endpoint).await?;

        Ok(Arc::new(CloudControlApiProvider {
            task_client,
            timeout,
        }))
    }

    async fn get_endpoint(
        endpoint: String,
        timeout: Duration,
    ) -> Result<tonic::transport::Endpoint> {
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint)
            .map_err(|err| {
                ErrorCode::CloudControlConnectError(format!(
                    "Invalid cloud control Server address: {err}"
                ))
            })?
            .connect_timeout(timeout);

        Ok(endpoint)
    }

    #[async_backtrace::framed]
    pub async fn init(addr: String, timeout: u64) -> Result<()> {
        let provider = Self::new(addr, timeout).await?;
        GlobalInstance::set(provider);
        Ok(())
    }

    pub fn instance() -> Arc<CloudControlApiProvider> {
        GlobalInstance::get()
    }

    pub fn get_task_client(&self) -> Arc<TaskClient> {
        self.task_client.clone()
    }
    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }
}
