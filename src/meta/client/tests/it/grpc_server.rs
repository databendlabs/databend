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

use std::pin::Pin;
use std::thread::sleep;
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_meta_client::to_digit_ver;
use databend_common_meta_client::MIN_METASRV_SEMVER;
use databend_common_meta_types::protobuf::meta_service_server::MetaService;
use databend_common_meta_types::protobuf::meta_service_server::MetaServiceServer;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::Empty;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::HandshakeResponse;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::TxnReply;
use databend_common_meta_types::protobuf::TxnRequest;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use futures::Stream;
use rand::Rng;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

/// A service that times out a kv_api() call, without impl other API.
pub struct GrpcServiceForTestImpl {}

#[tonic::async_trait]
impl MetaService for GrpcServiceForTestImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<databend_common_meta_types::protobuf::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let output = futures::stream::once(async {
            Ok(HandshakeResponse {
                protocol_version: to_digit_ver(&MIN_METASRV_SEMVER),
                payload: vec![],
            })
        });
        Ok(Response::new(Box::pin(output)))
    }

    async fn kv_api(&self, _request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        // for timeout test
        tokio::time::sleep(Duration::from_secs(60)).await;
        Err(Status::unimplemented("Not yet implemented"))
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        _request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        let itm = StreamItem::new("kv_read_v1".to_string(), None);
        let output = futures::stream::once(async { Ok(itm) });
        Ok(Response::new(Box::pin(output)))
    }

    type ExportStream =
        Pin<Box<dyn Stream<Item = Result<ExportedChunk, tonic::Status>> + Send + 'static>>;

    async fn export(
        &self,
        _request: Request<databend_common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        unimplemented!()
    }

    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, tonic::Status>> + Send + 'static>>;

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        unimplemented!()
    }

    async fn transaction(
        &self,
        _request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        unimplemented!()
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        unimplemented!()
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        unimplemented!()
    }

    async fn get_client_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        unimplemented!()
    }
}

/// Start a grpc server and return its address and task control handle.
pub fn start_grpc_server() -> (String, oneshot::Sender<()>, JoinHandle<()>) {
    let addr = rand_local_addr();
    let (shutdown, task_handle) = start_grpc_server_addr(&addr);
    (addr, shutdown, task_handle)
}

/// Returns a shutdown tx and a task handle.
pub fn start_grpc_server_addr(addr: impl ToString) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let addr = addr.to_string().parse().unwrap();

    let service = GrpcServiceForTestImpl {};
    let svc = MetaServiceServer::new(service);

    let (tx, rx) = oneshot::channel::<()>();

    let h = tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    // Wait for server to be ready
    sleep(Duration::from_secs(1));

    (tx, h)
}

pub fn rand_local_addr() -> String {
    let mut rng = rand::thread_rng();
    let port = rng.gen_range(10000..20000);
    format!("127.0.0.1:{}", port)
}
