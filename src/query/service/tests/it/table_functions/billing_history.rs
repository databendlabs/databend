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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_base::runtime;
use databend_common_cloud_control::pb::AlterTaskRequest;
use databend_common_cloud_control::pb::AlterTaskResponse;
use databend_common_cloud_control::pb::BillingHistoryDailyCloudServiceUsage;
use databend_common_cloud_control::pb::BillingHistoryDailyRow;
use databend_common_cloud_control::pb::BillingHistoryDailyStorageUsage;
use databend_common_cloud_control::pb::BillingHistoryWarehouseDailyRow;
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
use databend_common_cloud_control::pb::GetBillingHistoryDailyRequest;
use databend_common_cloud_control::pb::GetBillingHistoryDailyResponse;
use databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyRequest;
use databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyResponse;
use databend_common_cloud_control::pb::GetTaskDependentsRequest;
use databend_common_cloud_control::pb::GetTaskDependentsResponse;
use databend_common_cloud_control::pb::ShowTaskRunsRequest;
use databend_common_cloud_control::pb::ShowTaskRunsResponse;
use databend_common_cloud_control::pb::ShowTasksRequest;
use databend_common_cloud_control::pb::ShowTasksResponse;
use databend_common_cloud_control::pb::TaskError as PbTaskError;
use databend_common_cloud_control::pb::task_service_server::TaskService;
use databend_common_cloud_control::pb::task_service_server::TaskServiceServer;
use databend_common_config::InnerConfig;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Server;

#[derive(Clone, Default)]
struct BillingRequests {
    daily: Arc<Mutex<Vec<GetBillingHistoryDailyRequest>>>,
    warehouse_daily: Arc<Mutex<Vec<GetBillingHistoryWarehouseDailyRequest>>>,
}

#[derive(Clone, Default)]
struct MockBillingTaskService {
    requests: BillingRequests,
    daily_error: Option<PbTaskError>,
    warehouse_daily_error: Option<PbTaskError>,
}

#[tonic::async_trait]
impl TaskService for MockBillingTaskService {
    async fn create_task(
        &self,
        _request: Request<CreateTaskRequest>,
    ) -> std::result::Result<Response<CreateTaskResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn describe_task(
        &self,
        _request: Request<DescribeTaskRequest>,
    ) -> std::result::Result<Response<DescribeTaskResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn execute_task(
        &self,
        _request: Request<ExecuteTaskRequest>,
    ) -> std::result::Result<Response<ExecuteTaskResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn drop_task(
        &self,
        _request: Request<DropTaskRequest>,
    ) -> std::result::Result<Response<DropTaskResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn alter_task(
        &self,
        _request: Request<AlterTaskRequest>,
    ) -> std::result::Result<Response<AlterTaskResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn show_tasks(
        &self,
        _request: Request<ShowTasksRequest>,
    ) -> std::result::Result<Response<ShowTasksResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn show_task_runs(
        &self,
        _request: Request<ShowTaskRunsRequest>,
    ) -> std::result::Result<Response<ShowTaskRunsResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn get_billing_history_daily(
        &self,
        request: Request<GetBillingHistoryDailyRequest>,
    ) -> std::result::Result<Response<GetBillingHistoryDailyResponse>, Status> {
        self.requests
            .daily
            .lock()
            .unwrap()
            .push(request.into_inner());

        if let Some(error) = self.daily_error.clone() {
            return Ok(Response::new(GetBillingHistoryDailyResponse {
                rows: vec![],
                error: Some(error),
            }));
        }

        Ok(Response::new(GetBillingHistoryDailyResponse {
            rows: vec![BillingHistoryDailyRow {
                date: "2026-03-02".to_string(),
                total_amount: "2.465".to_string(),
                warehouse_amount: "1.390".to_string(),
                storage_amount: "1.067".to_string(),
                cloud_service_amount: "0.008".to_string(),
                storage_usage: Some(BillingHistoryDailyStorageUsage {
                    total_bytes: "1580580967006".to_string(),
                    day_price_per_tb: "0.7419354838709677".to_string(),
                    read_requests: 10,
                    kilo_read_request_price: "0.0004".to_string(),
                    write_requests: 20,
                    kilo_write_request_price: "0.005".to_string(),
                }),
                cloud_service_usage: Some(BillingHistoryDailyCloudServiceUsage {
                    api_requests: "78".to_string(),
                    kilo_api_requests_price: "0.1".to_string(),
                }),
            }],
            error: None,
        }))
    }

    async fn get_billing_history_warehouse_daily(
        &self,
        request: Request<GetBillingHistoryWarehouseDailyRequest>,
    ) -> std::result::Result<Response<GetBillingHistoryWarehouseDailyResponse>, Status> {
        self.requests
            .warehouse_daily
            .lock()
            .unwrap()
            .push(request.into_inner());

        if let Some(error) = self.warehouse_daily_error.clone() {
            return Ok(Response::new(GetBillingHistoryWarehouseDailyResponse {
                rows: vec![],
                error: Some(error),
            }));
        }

        Ok(Response::new(GetBillingHistoryWarehouseDailyResponse {
            rows: vec![BillingHistoryWarehouseDailyRow {
                date: "2026-03-02".to_string(),
                warehouse_name: "default".to_string(),
                cluster_name: "cl-00000".to_string(),
                max_clusters: 1,
                size: "XSmall".to_string(),
                seconds: 2653,
                price_per_second: "0.0002777777777778".to_string(),
                credits: "0.737".to_string(),
                tags: BTreeMap::from([("env".to_string(), "test".to_string())]),
            }],
            error: None,
        }))
    }

    async fn get_task_dependents(
        &self,
        _request: Request<GetTaskDependentsRequest>,
    ) -> std::result::Result<Response<GetTaskDependentsResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }

    async fn enable_task_dependents(
        &self,
        _request: Request<EnableTaskDependentsRequest>,
    ) -> std::result::Result<Response<EnableTaskDependentsResponse>, Status> {
        Err(Status::unimplemented("not used in billing tests"))
    }
}

fn extract_single_block(blocks: Vec<DataBlock>) -> DataBlock {
    let block = DataBlock::concat(&blocks).expect("concat blocks");
    assert_eq!(block.num_rows(), 1);
    block
}

#[tokio::test(flavor = "multi_thread")]
async fn test_billing_history_table_functions_via_mock_grpc() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let requests = BillingRequests::default();
    let mock = MockBillingTaskService {
        requests: requests.clone(),
        daily_error: None,
        warehouse_daily_error: None,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = runtime::spawn(async move {
        Server::builder()
            .add_service(TaskServiceServer::new(mock))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address = Some(format!("http://{addr}"));
    config.query.common.cloud_control_grpc_timeout = 5;

    let fixture = TestFixture::setup_with_config(&config).await?;

    let daily_blocks = fixture
        .execute_query(
            "select date, total_amount, storage_read_requests, cloud_service_api_requests \
             from billing_history_daily(month => '2026-03')",
        )
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let daily_block = extract_single_block(daily_blocks);
    assert_eq!(
        daily_block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::String("2026-03-02")
    );
    assert_eq!(
        daily_block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::String("2.465")
    );
    assert_eq!(
        daily_block.get_by_offset(2).index(0).unwrap(),
        ScalarRef::Number(NumberScalar::UInt64(10))
    );
    assert_eq!(
        daily_block.get_by_offset(3).index(0).unwrap(),
        ScalarRef::String("78")
    );

    let warehouse_blocks = fixture
        .execute_query(
            "select date, warehouse_name, tags \
             from billing_history_warehouse_daily(month => '2026-03')",
        )
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let warehouse_block = extract_single_block(warehouse_blocks);
    assert_eq!(
        warehouse_block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::String("2026-03-02")
    );
    assert_eq!(
        warehouse_block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::String("default")
    );
    let expected_tags =
        serde_json::to_vec(&HashMap::from([("env".to_string(), "test".to_string())]))?;
    match warehouse_block.get_by_offset(2).index(0).unwrap() {
        ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_tags.as_slice()),
        other => panic!("unexpected scalar type for tags: {other:?}"),
    }

    {
        let daily_requests = requests.daily.lock().unwrap();
        assert_eq!(daily_requests.len(), 1);
        assert_eq!(daily_requests[0].billing_month, "2026-03");
        assert_eq!(daily_requests[0].sql_user, "root");
        assert!(!daily_requests[0].tenant_id.is_empty());
        assert!(!daily_requests[0].query_id.is_empty());
    }

    {
        let warehouse_requests = requests.warehouse_daily.lock().unwrap();
        assert_eq!(warehouse_requests.len(), 1);
        assert_eq!(warehouse_requests[0].billing_month, "2026-03");
        assert_eq!(warehouse_requests[0].sql_user, "root");
        assert!(!warehouse_requests[0].tenant_id.is_empty());
        assert!(!warehouse_requests[0].query_id.is_empty());
    }

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_billing_history_table_functions_surface_task_error() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let mock = MockBillingTaskService {
        requests: BillingRequests::default(),
        daily_error: Some(PbTaskError {
            kind: "AuthFailed".to_string(),
            message: "daily billing access denied".to_string(),
            code: 403,
        }),
        warehouse_daily_error: Some(PbTaskError {
            kind: "Internal".to_string(),
            message: "warehouse billing unavailable".to_string(),
            code: 500,
        }),
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = runtime::spawn(async move {
        Server::builder()
            .add_service(TaskServiceServer::new(mock))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address = Some(format!("http://{addr}"));
    config.query.common.cloud_control_grpc_timeout = 5;

    let fixture = TestFixture::setup_with_config(&config).await?;

    let daily_stream = fixture
        .execute_query("select * from billing_history_daily(month => '2026-03')")
        .await?;
    let daily_err = daily_stream
        .try_collect::<Vec<DataBlock>>()
        .await
        .expect_err("daily billing task error should fail query");
    assert!(daily_err.message().contains("daily billing access denied"));
    assert!(daily_err.message().contains("AuthFailed"));

    let warehouse_stream = fixture
        .execute_query("select * from billing_history_warehouse_daily(month => '2026-03')")
        .await?;
    let warehouse_err = warehouse_stream
        .try_collect::<Vec<DataBlock>>()
        .await
        .expect_err("warehouse billing task error should fail query");
    assert!(
        warehouse_err
            .message()
            .contains("warehouse billing unavailable")
    );
    assert!(warehouse_err.message().contains("Internal"));

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}
