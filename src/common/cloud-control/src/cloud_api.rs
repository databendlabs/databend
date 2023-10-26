use std::sync::Arc;
use std::time::Duration;

use common_base::base::GlobalInstance;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::task_client::TaskClient;

pub const CLOUD_REQUEST_TIMEOUT_SEC: u64 = 180; // 180 seconds

// commonly used metadata
pub const TENANT_ID: &str = "X-DATABEND-TENANT-ID";
pub const REQUESTER: &str = "X-DATABEND-USER";
pub const QUERY_ID: &str = "X-DATABEND-QUERY-ID";

pub struct CloudControlApiProvider {
    pub task_client: Arc<TaskClient>,
}

impl CloudControlApiProvider {
    pub async fn new(endpoint: String) -> Result<Arc<CloudControlApiProvider>> {
        let endpoint = Self::get_endpoint(endpoint).await?;
        let task_client = TaskClient::new(endpoint).await?;
        Ok(Arc::new(CloudControlApiProvider { task_client }))
    }

    async fn get_endpoint(endpoint: String) -> Result<tonic::transport::Endpoint> {
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint)
            .map_err(|err| {
                ErrorCode::CloudControlConnectError(format!(
                    "Invalid cloud control Server address: {err}"
                ))
            })?
            .connect_timeout(Duration::from_secs(CLOUD_REQUEST_TIMEOUT_SEC));

        Ok(endpoint)
    }

    #[async_backtrace::framed]
    pub async fn init(addr: String) -> Result<()> {
        let provider = Self::new(addr).await?;
        GlobalInstance::set(provider);
        Ok(())
    }

    pub fn instance() -> Arc<CloudControlApiProvider> {
        GlobalInstance::get()
    }

    pub fn get_task_client(&self) -> Arc<TaskClient> {
        self.task_client.clone()
    }
}
