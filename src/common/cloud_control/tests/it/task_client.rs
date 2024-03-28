// Copyright 2022 Datafuse Labs.
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

use databend_common_base::base::tokio;
use databend_common_base::runtime;
use databend_common_cloud_control::pb::task::Status::Suspended;
use databend_common_cloud_control::pb::task_service_client::TaskServiceClient;
use databend_common_cloud_control::pb::task_service_server::TaskService;
use databend_common_cloud_control::pb::task_service_server::TaskServiceServer;
use databend_common_cloud_control::pb::AlterTaskRequest;
use databend_common_cloud_control::pb::AlterTaskResponse;
use databend_common_cloud_control::pb::CreateTaskRequest;
use databend_common_cloud_control::pb::CreateTaskResponse;
use databend_common_cloud_control::pb::DescribeTaskRequest;
use databend_common_cloud_control::pb::DescribeTaskResponse;
use databend_common_cloud_control::pb::DropTaskRequest;
use databend_common_cloud_control::pb::DropTaskResponse;
use databend_common_cloud_control::pb::EnableTaskDependentsRequest;
use databend_common_cloud_control::pb::EnableTaskDependentsResponse;
use databend_common_cloud_control::pb::ExecuteTaskRequest;
use databend_common_cloud_control::pb::ExecuteTaskResponse;
use databend_common_cloud_control::pb::GetTaskDependentsRequest;
use databend_common_cloud_control::pb::GetTaskDependentsResponse;
use databend_common_cloud_control::pb::ShowTaskRunsRequest;
use databend_common_cloud_control::pb::ShowTaskRunsResponse;
use databend_common_cloud_control::pb::ShowTasksRequest;
use databend_common_cloud_control::pb::ShowTasksResponse;
use databend_common_cloud_control::pb::Task;
use databend_common_exception::Result;
use tonic::codegen::tokio_stream;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tonic::transport::Uri;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tower::service_fn;

#[derive(Default)]
pub struct MockTaskService {}

#[tonic::async_trait]
impl TaskService for MockTaskService {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let task_id = request.into_inner().task_name.parse::<u64>();
        Ok(Response::new(CreateTaskResponse {
            error: None,
            task_id: task_id.unwrap(),
        }))
    }
    async fn describe_task(
        &self,
        request: Request<DescribeTaskRequest>,
    ) -> Result<Response<DescribeTaskResponse>, Status> {
        Ok(Response::new(DescribeTaskResponse {
            task: Some(Task {
                task_id: 0,
                task_name: request.into_inner().task_name,
                query_text: "".to_string(),
                comment: None,
                owner: "".to_string(),
                schedule_options: None,
                warehouse_options: None,
                next_scheduled_at: Default::default(),
                suspend_task_after_num_failures: None,
                status: i32::from(Suspended),
                created_at: Default::default(),
                updated_at: Default::default(),
                last_suspended_at: None,
                error_integration: None,
                after: vec![],
                when_condition: None,
                session_parameters: Default::default(),
            }),
            error: None,
        }))
    }

    async fn execute_task(
        &self,
        _request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        Ok(Response::new(ExecuteTaskResponse { error: None }))
    }

    async fn drop_task(
        &self,
        _request: Request<DropTaskRequest>,
    ) -> Result<Response<DropTaskResponse>, Status> {
        Ok(Response::new(DropTaskResponse { error: None }))
    }

    async fn alter_task(
        &self,
        _request: Request<AlterTaskRequest>,
    ) -> Result<Response<AlterTaskResponse>, Status> {
        Ok(Response::new(AlterTaskResponse {
            error: None,
            task: None,
        }))
    }

    async fn show_tasks(
        &self,
        _request: Request<ShowTasksRequest>,
    ) -> Result<Response<ShowTasksResponse>, Status> {
        Ok(Response::new(ShowTasksResponse {
            tasks: vec![],
            error: None,
        }))
    }

    async fn show_task_runs(
        &self,
        _request: Request<ShowTaskRunsRequest>,
    ) -> Result<Response<ShowTaskRunsResponse>, Status> {
        Ok(Response::new(ShowTaskRunsResponse {
            task_runs: vec![],
            error: None,
            next_page_token: None,
            previous_page_token: None,
        }))
    }

    async fn get_task_dependents(
        &self,
        _request: Request<GetTaskDependentsRequest>,
    ) -> std::result::Result<Response<GetTaskDependentsResponse>, Status> {
        Ok(Response::new(GetTaskDependentsResponse {
            task: vec![],
            error: None,
        }))
    }

    async fn enable_task_dependents(
        &self,
        _request: Request<EnableTaskDependentsRequest>,
    ) -> std::result::Result<Response<EnableTaskDependentsResponse>, Status> {
        Ok(Response::new(EnableTaskDependentsResponse { error: None }))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_task_client_success_cases() -> Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let mock = MockTaskService::default();

    runtime::spawn(async move {
        Server::builder()
            .add_service(TaskServiceServer::new(mock))
            .serve_with_incoming(tokio_stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    let mut client = Some(client);
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let client = client.take();

            async move {
                if let Some(client) = client {
                    Ok(client)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Client already taken",
                    ))
                }
            }
        }))
        .await
        .unwrap();

    let mut client = TaskServiceClient::new(channel);

    let request = tonic::Request::new(CreateTaskRequest {
        task_name: "123".to_string(),
        tenant_id: "".to_string(),
        query_text: "".to_string(),
        owner: "".to_string(),
        comment: None,
        schedule_options: None,
        error_integration: None,
        task_sql_type: 0,
        warehouse_options: None,
        suspend_task_after_num_failures: None,
        if_not_exist: false,
        after: vec![],
        when_condition: None,
        session_parameters: Default::default(),
        script_sql: None,
    });

    let response = client.create_task(request).await?;

    assert_eq!(response.into_inner().task_id, 123);

    Ok(())
}
