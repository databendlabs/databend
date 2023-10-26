use std::sync::Arc;

use common_exception::Result;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Request;

use crate::client_config::ClientConfig;
use crate::pb::task_service_client::TaskServiceClient;
use crate::pb::AlterTaskRequest;
use crate::pb::AlterTaskResponse;
use crate::pb::CreateTaskRequest;
use crate::pb::CreateTaskResponse;
use crate::pb::DescribeTaskRequest;
use crate::pb::DescribeTaskResponse;
use crate::pb::DropTaskRequest;
use crate::pb::DropTaskResponse;
use crate::pb::ShowTasksRequest;
use crate::pb::ShowTasksResponse;

const TASK_CLIENT_VERSION: &str = "v1";
const TASK_CLIENT_VERSION_NAME: &str = "TASK_CLIENT_VERSION";

pub struct TaskClient {
    pub task_client: TaskServiceClient<Channel>,
}

// add necessary metadata and client request setup for auditing and tracing purpose
pub fn make_request<T>(t: T, config: ClientConfig) -> Request<T> {
    let mut request = Request::new(t);
    request.set_timeout(config.get_timeout());
    let metadata = request.metadata_mut();
    let config_meta = config.get_metadata().clone();
    for (k, v) in config_meta {
        let key = k
            .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            .unwrap();
        metadata.insert(key, v.parse().unwrap());
    }
    metadata.insert(
        TASK_CLIENT_VERSION_NAME,
        TASK_CLIENT_VERSION.to_string().parse().unwrap(),
    );
    request
}

impl TaskClient {
    // TODO: add auth interceptor
    pub async fn new(endpoint: Endpoint) -> Result<Arc<TaskClient>> {
        let task_client = TaskServiceClient::connect(endpoint).await.map_err(|err| {
            common_exception::ErrorCode::CloudControlConnectError(format!(
                "Cannot connect to task client: {err}"
            ))
        })?;
        Ok(Arc::new(TaskClient { task_client }))
    }

    // TODO: richer error handling on Task Error
    pub async fn create_task(&self, req: Request<CreateTaskRequest>) -> Result<CreateTaskResponse> {
        let mut client = self.task_client.clone();
        let resp = client.create_task(req).await?;
        Ok(resp.into_inner())
    }

    // TODO: richer error handling on Task Error
    pub async fn describe_task(
        &self,
        req: Request<DescribeTaskRequest>,
    ) -> Result<DescribeTaskResponse> {
        let mut client = self.task_client.clone();
        let resp = client.describe_task(req).await?;
        Ok(resp.into_inner())
    }

    // TODO: richer error handling on Task Error
    pub async fn drop_task(&self, req: Request<DropTaskRequest>) -> Result<DropTaskResponse> {
        let mut client = self.task_client.clone();
        let resp = client.drop_task(req).await?;
        Ok(resp.into_inner())
    }

    // TODO: richer error handling on Task Error
    pub async fn alter_task(&self, req: Request<AlterTaskRequest>) -> Result<AlterTaskResponse> {
        let mut client = self.task_client.clone();
        let resp = client.alter_task(req).await?;
        Ok(resp.into_inner())
    }

    // TODO: richer error handling on Task Error
    pub async fn show_tasks(&self, req: Request<ShowTasksRequest>) -> Result<ShowTasksResponse> {
        let mut client = self.task_client.clone();
        let resp = client.show_tasks(req).await?;
        Ok(resp.into_inner())
    }

    // TODO: richer error handling on Task Error
    pub async fn show_task_runs(
        &self,
        req: Request<crate::pb::ShowTaskRunsRequest>,
    ) -> Result<crate::pb::ShowTaskRunsResponse> {
        let mut client = self.task_client.clone();
        let resp = client.show_task_runs(req).await?;
        Ok(resp.into_inner())
    }
}
