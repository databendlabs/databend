// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_meta_client::to_digit_ver;
use common_meta_client::MIN_METASRV_SEMVER;
use common_meta_types::protobuf::meta_service_server::MetaService;
use common_meta_types::protobuf::meta_service_server::MetaServiceServer;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ClusterStatus;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeResponse;
use common_meta_types::protobuf::MemberListReply;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::StreamItem;
use common_meta_types::protobuf::TxnReply;
use common_meta_types::protobuf::TxnRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
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
        _request: Request<Streaming<common_meta_types::protobuf::HandshakeRequest>>,
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
        todo!()
    }

    type ExportStream =
        Pin<Box<dyn Stream<Item = Result<ExportedChunk, tonic::Status>> + Send + 'static>>;

    async fn export(
        &self,
        _request: Request<common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        todo!()
    }

    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, tonic::Status>> + Send + 'static>>;

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        todo!()
    }

    async fn transaction(
        &self,
        _request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        todo!()
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        todo!()
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        todo!()
    }

    async fn get_client_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        todo!()
    }
}

pub fn start_grpc_server() -> String {
    let service = GrpcServiceForTestImpl {};
    start_grpc_server_with_service(service)
}

pub fn start_grpc_server_with_service(svc: impl MetaService) -> String {
    let mut rng = rand::thread_rng();
    let port = rng.gen_range(10000..20000);
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    let svc = MetaServiceServer::new(svc);

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .unwrap();
    });
    sleep(Duration::from_secs(1));
    addr.to_string()
}
