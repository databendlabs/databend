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

use std::collections::BTreeMap;

use databend_common_base::runtime;
use databend_common_cloud_control::billing_client::BillingClient;
use databend_common_cloud_control::pb::BillingUsageDailyRow;
use databend_common_cloud_control::pb::GetBillingUsageDailyRequest;
use databend_common_cloud_control::pb::GetBillingUsageDailyResponse;
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
    async fn get_billing_usage_daily(
        &self,
        request: Request<GetBillingUsageDailyRequest>,
    ) -> std::result::Result<Response<GetBillingUsageDailyResponse>, Status> {
        Ok(Response::new(GetBillingUsageDailyResponse {
            rows: vec![BillingUsageDailyRow {
                usage_date: request.into_inner().billing_month,
                usage_type: "compute".to_string(),
                service_type: "WAREHOUSE_METERING".to_string(),
                resource_name: "default".to_string(),
                usage: "2653".to_string(),
                usage_unit: "second".to_string(),
                rate: "".to_string(),
                rate_unit: "second".to_string(),
                usage_in_currency: "0.737".to_string(),
                currency: "USD".to_string(),
                tags: BTreeMap::from([("env".to_string(), "test".to_string())]),
                details: "{\"cluster_name\":\"cl-00000\"}".to_string(),
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

    let resp = client
        .get_billing_usage_daily(Request::new(GetBillingUsageDailyRequest {
            tenant_id: "tenant".to_string(),
            billing_month: "2026-03".to_string(),
            sql_user: "root".to_string(),
            query_id: "query-1".to_string(),
        }))
        .await?;
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.rows[0].usage_date, "2026-03");
    assert_eq!(resp.rows[0].usage_type, "compute");
    assert_eq!(resp.rows[0].resource_name, "default");

    Ok(())
}
