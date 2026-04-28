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

use databend_common_base::runtime;
use databend_common_cloud_control::billing_client::BillingClient;
use databend_common_cloud_control::pb::BillingHistoryDailyRow;
use databend_common_cloud_control::pb::BillingHistoryWarehouseDailyRow;
use databend_common_cloud_control::pb::GetBillingHistoryDailyRequest;
use databend_common_cloud_control::pb::GetBillingHistoryDailyResponse;
use databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyRequest;
use databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyResponse;
use databend_common_cloud_control::pb::billing_service_server::BillingService;
use databend_common_cloud_control::pb::billing_service_server::BillingServiceServer;
use hyper_util::rt::TokioIo;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::codegen::tokio_stream;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tonic::transport::Uri;
use tower::service_fn;

#[derive(Default)]
pub struct MockBillingService {}

#[tonic::async_trait]
impl BillingService for MockBillingService {
    async fn get_billing_history_daily(
        &self,
        request: Request<GetBillingHistoryDailyRequest>,
    ) -> std::result::Result<Response<GetBillingHistoryDailyResponse>, Status> {
        Ok(Response::new(GetBillingHistoryDailyResponse {
            rows: vec![BillingHistoryDailyRow {
                date: request.into_inner().billing_month,
                ..Default::default()
            }],
            error: None,
        }))
    }

    async fn get_billing_history_warehouse_daily(
        &self,
        request: Request<GetBillingHistoryWarehouseDailyRequest>,
    ) -> std::result::Result<Response<GetBillingHistoryWarehouseDailyResponse>, Status> {
        Ok(Response::new(GetBillingHistoryWarehouseDailyResponse {
            rows: vec![BillingHistoryWarehouseDailyRow {
                warehouse_name: request.into_inner().billing_month,
                ..Default::default()
            }],
            error: None,
        }))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_billing_client_success_cases() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);
    let client = TokioIo::new(client);

    runtime::spawn(async move {
        Server::builder()
            .add_service(BillingServiceServer::new(MockBillingService::default()))
            .serve_with_incoming(tokio_stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    let mut client_io = Some(client);
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let client = client_io.take();

            async move {
                if let Some(client) = client {
                    Ok(client)
                } else {
                    Err(std::io::Error::other("Client already taken"))
                }
            }
        }))
        .await
        .unwrap();

    let client = BillingClient::new(channel).await?;

    let daily_resp = client
        .get_billing_history_daily(Request::new(GetBillingHistoryDailyRequest {
            tenant_id: "tenant".to_string(),
            billing_month: "2026-03".to_string(),
            sql_user: "root".to_string(),
            query_id: "query-1".to_string(),
        }))
        .await?;
    assert_eq!(daily_resp.rows.len(), 1);
    assert_eq!(daily_resp.rows[0].date, "2026-03");

    let warehouse_resp = client
        .get_billing_history_warehouse_daily(Request::new(GetBillingHistoryWarehouseDailyRequest {
            tenant_id: "tenant".to_string(),
            billing_month: "2026-04".to_string(),
            sql_user: "root".to_string(),
            query_id: "query-2".to_string(),
        }))
        .await?;
    assert_eq!(warehouse_resp.rows.len(), 1);
    assert_eq!(warehouse_resp.rows[0].warehouse_name, "2026-04");

    Ok(())
}
