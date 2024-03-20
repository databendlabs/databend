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

use databend_common_exception::Result;
use tonic::transport::Channel;
use tonic::Request;

use crate::client_config::make_request;
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
use crate::pb::ExecuteTaskRequest;
use crate::pb::ExecuteTaskResponse;
use crate::pb::ShowTasksRequest;
use crate::pb::ShowTasksResponse;

pub(crate) const TASK_CLIENT_VERSION: &str = "v1";
pub(crate) const TASK_CLIENT_VERSION_NAME: &str = "TASK_CLIENT_VERSION";

pub struct TaskClient {
    pub task_client: TaskServiceClient<Channel>,
}

impl TaskClient {
    // TODO: add auth interceptor
    pub async fn new(channel: Channel) -> Result<Arc<TaskClient>> {
        let task_client = TaskServiceClient::new(channel);
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
    pub async fn execute_task(
        &self,
        req: Request<ExecuteTaskRequest>,
    ) -> Result<ExecuteTaskResponse> {
        let mut client = self.task_client.clone();
        let resp = client.execute_task(req).await?;
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

    pub async fn show_task_runs_full(
        &self,
        config: ClientConfig,
        req: crate::pb::ShowTaskRunsRequest,
    ) -> Result<Vec<crate::pb::ShowTaskRunsResponse>> {
        let mut client = self.task_client.clone();
        let request = make_request(req.clone(), config.clone());
        let resp = client.show_task_runs(request).await?;
        let mut has_next = resp.get_ref().next_page_token.is_some();
        // it is a pagination request, so we need to handle the response
        let mut result = vec![resp.into_inner()];
        while has_next {
            let mut req = req.clone();
            req.next_page_token = result.last().unwrap().next_page_token;
            let resp = client
                .show_task_runs(make_request(req.clone(), config.clone()))
                .await?;
            let resp = resp.into_inner();
            has_next = resp.next_page_token.is_some();
            result.push(resp);
        }
        Ok(result)
    }

    pub async fn get_task_dependents(
        &self,
        req: Request<crate::pb::GetTaskDependentsRequest>,
    ) -> Result<crate::pb::GetTaskDependentsResponse> {
        let mut client = self.task_client.clone();
        let resp = client.get_task_dependents(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn enable_task_dependents(
        &self,
        req: Request<crate::pb::EnableTaskDependentsRequest>,
    ) -> Result<crate::pb::EnableTaskDependentsResponse> {
        let mut client = self.task_client.clone();
        let resp = client.enable_task_dependents(req).await?;
        Ok(resp.into_inner())
    }
}
