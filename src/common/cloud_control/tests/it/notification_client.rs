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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_cloud_control::notification_client::NotificationClient;
use databend_common_cloud_control::pb::notification_service_server::NotificationService;
use databend_common_cloud_control::pb::notification_service_server::NotificationServiceServer;
use databend_common_cloud_control::pb::*;
use hyper_util::rt::TokioIo;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::codegen::tokio_stream;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tower::service_fn;

#[derive(Default)]
struct MockNotificationService {
    requests: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl NotificationService for MockNotificationService {
    async fn create_notification(
        &self,
        _: Request<CreateNotificationRequest>,
    ) -> Result<Response<CreateNotificationResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn drop_notification(
        &self,
        _: Request<DropNotificationRequest>,
    ) -> Result<Response<DropNotificationResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_notification(
        &self,
        _: Request<ListNotificationRequest>,
    ) -> Result<Response<ListNotificationResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_notification(
        &self,
        _: Request<GetNotificationRequest>,
    ) -> Result<Response<GetNotificationResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn alter_notification(
        &self,
        _: Request<AlterNotificationRequest>,
    ) -> Result<Response<AlterNotificationResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_notification_history(
        &self,
        _: Request<ListNotificationHistoryRequest>,
    ) -> Result<Response<ListNotificationHistoryResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn enqueue_notification(
        &self,
        request: Request<EnqueueNotificationRequest>,
    ) -> Result<Response<EnqueueNotificationResponse>, Status> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        assert_eq!(request.tenant_id, "tenant");
        assert_eq!(request.message, "alert");
        assert_eq!(request.query_id.as_deref(), Some("query-id"));
        match request.name.as_str() {
            "old-server" => Err(Status::unimplemented("old server")),
            "failed" => Err(Status::internal("persistence failed")),
            "lost-response" => Err(Status::unavailable("response lost after commit")),
            "rejected" => Ok(Response::new(EnqueueNotificationResponse {
                error: Some(NotificationError {
                    kind: "INTERNAL".into(),
                    message: "rejected".into(),
                    code: 13,
                }),
                delivery_id: String::new(),
            })),
            _ => Ok(Response::new(EnqueueNotificationResponse {
                error: None,
                delivery_id: "delivery-id".into(),
            })),
        }
    }
}

#[tokio::test]
async fn enqueue_acceptance_errors_and_no_retry() -> anyhow::Result<()> {
    let requests = Arc::new(AtomicUsize::new(0));
    let service = MockNotificationService {
        requests: requests.clone(),
    };
    let (client_io, server_io) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(NotificationServiceServer::new(service))
            .serve_with_incoming(tokio_stream::iter(vec![Ok::<_, std::io::Error>(server_io)]))
            .await
    });
    let mut client_io = Some(client_io);
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_| {
            let io = client_io.take().unwrap();
            async move { Ok::<_, std::io::Error>(TokioIo::new(io)) }
        }))
        .await?;
    let client = NotificationClient::new(channel).await?;
    for name in [
        "accepted",
        "old-server",
        "failed",
        "lost-response",
        "rejected",
    ] {
        let before = requests.load(Ordering::SeqCst);
        let result = client
            .enqueue_notification(Request::new(EnqueueNotificationRequest {
                tenant_id: "tenant".into(),
                name: name.into(),
                message: "alert".into(),
                query_id: Some("query-id".into()),
            }))
            .await;
        if name == "accepted" {
            assert_eq!(result?.delivery_id, "delivery-id");
        } else {
            let error = result.unwrap_err();
            if name == "old-server" {
                assert!(
                    error
                        .message()
                        .contains("does not support active notifications")
                );
            }
        }
        assert_eq!(requests.load(Ordering::SeqCst), before + 1);
    }
    server.abort();
    Ok(())
}
