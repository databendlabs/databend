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

//! A node that claims to lead but refuses the watch, as a leader cut off from a quorum does
//! since databend-meta v260629.8.0, while it still accepts gRPC connections.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MIN_SERVER_VERSION;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::runtime_api::SpawnApi;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::ClientInfo;
use databend_meta_client::types::protobuf::ClusterStatus;
use databend_meta_client::types::protobuf::Empty;
use databend_meta_client::types::protobuf::ExportRequest;
use databend_meta_client::types::protobuf::ExportedChunk;
use databend_meta_client::types::protobuf::HandshakeRequest;
use databend_meta_client::types::protobuf::HandshakeResponse;
use databend_meta_client::types::protobuf::KeysCount;
use databend_meta_client::types::protobuf::KeysLayoutRequest;
use databend_meta_client::types::protobuf::KvGetManyRequest;
use databend_meta_client::types::protobuf::KvListRequest;
use databend_meta_client::types::protobuf::KvTransactionReply;
use databend_meta_client::types::protobuf::KvTransactionRequest;
use databend_meta_client::types::protobuf::MemberListReply;
use databend_meta_client::types::protobuf::MemberListRequest;
use databend_meta_client::types::protobuf::RaftReply;
use databend_meta_client::types::protobuf::RaftRequest;
use databend_meta_client::types::protobuf::StreamItem;
use databend_meta_client::types::protobuf::TxnReply;
use databend_meta_client::types::protobuf::TxnRequest;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::WatchResponse;
use databend_meta_client::types::protobuf::meta_service_server::MetaService;
use databend_meta_client::types::protobuf::meta_service_server::MetaServiceServer;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use futures::stream;
use test_harness::test;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

/// A meta node that answers that it leads, and refuses every watch.
#[derive(Default)]
struct RefusingLeader {
    refused: Arc<AtomicUsize>,
}

fn unimplemented<T>() -> Result<T, Status> {
    Err(Status::unimplemented("not used by the cache"))
}

#[tonic::async_trait]
impl MetaService for RefusingLeader {
    type HandshakeStream = BoxStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: MIN_SERVER_VERSION.to_digit(),
            payload: b"token".to_vec(),
        };
        Ok(Response::new(Box::pin(stream::iter([Ok(response)]))))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        Ok(Response::new(ClusterStatus {
            state: "Leader".to_string(),
            is_leader: true,
            current_term: 1,
            ..Default::default()
        }))
    }

    type WatchStream = BoxStream<WatchResponse>;

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        self.refused.fetch_add(1, Ordering::Relaxed);
        Err(Status::unavailable(
            "this node cannot tell that its state machine is current: watch another node",
        ))
    }

    async fn kv_api(&self, _request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        unimplemented()
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        _request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        unimplemented()
    }

    type KvListStream = BoxStream<StreamItem>;

    async fn kv_list(
        &self,
        _request: Request<KvListRequest>,
    ) -> Result<Response<Self::KvListStream>, Status> {
        unimplemented()
    }

    type KvGetManyStream = BoxStream<StreamItem>;

    async fn kv_get_many(
        &self,
        _request: Request<Streaming<KvGetManyRequest>>,
    ) -> Result<Response<Self::KvGetManyStream>, Status> {
        unimplemented()
    }

    type ExportStream = BoxStream<ExportedChunk>;

    async fn export(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        unimplemented()
    }

    type ExportV1Stream = BoxStream<ExportedChunk>;

    async fn export_v1(
        &self,
        _request: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportV1Stream>, Status> {
        unimplemented()
    }

    type SnapshotKeysLayoutStream = BoxStream<KeysCount>;

    async fn snapshot_keys_layout(
        &self,
        _request: Request<KeysLayoutRequest>,
    ) -> Result<Response<Self::SnapshotKeysLayoutStream>, Status> {
        unimplemented()
    }

    async fn kv_transaction(
        &self,
        _request: Request<KvTransactionRequest>,
    ) -> Result<Response<KvTransactionReply>, Status> {
        unimplemented()
    }

    async fn transaction(
        &self,
        _request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        unimplemented()
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        unimplemented()
    }

    async fn get_client_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        unimplemented()
    }
}

/// Serve `service` on a local port, and return its address.
async fn serve(service: RefusingLeader) -> anyhow::Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let incoming = TcpIncoming::from(listener);
    let server = Server::builder()
        .add_service(MetaServiceServer::new(service))
        .serve_with_incoming(incoming);
    DatabendRuntime::spawn(
        async move {
            server.await.unwrap();
        },
        None,
    );
    Ok(address)
}

fn new_client(addresses: Vec<String>) -> anyhow::Result<Arc<ClientHandle<DatabendRuntime>>> {
    // No endpoint sync: it would ask the fake node for the member list.
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        addresses,
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        None,
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    Ok(client)
}

/// A node that claims to lead and refuses the watch must not hold the cache: it watches the leader
/// on another endpoint, while the refusing node still accepts connections.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_cache_moves_off_a_leader_that_refuses_the_watch() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;
    let healthy = tcs[0].config.grpc.api_address().unwrap();
    let writer = new_client(vec![healthy.clone()])?;
    writer.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;

    let refusing = RefusingLeader::default();
    let refused = refusing.refused.clone();
    let refusing = serve(refusing).await?;

    // Start on the refusing node, as a client that last used it does.
    let client = new_client(vec![refusing.clone(), healthy])?;
    let established = client.make_established_client().await?;
    let set = established.endpoints().lock().set_current(Some(&refusing));
    set.map_err(anyhow::Error::msg)?;

    let cache = Cache::new(client, "pp", "test-refused-watch").await;
    let got = tokio::time::timeout(Duration::from_secs(10), cache.try_get("pp/a")).await;
    let got = got.map_err(|_| anyhow::anyhow!("the cache is not ready after 10s"))??;
    assert_eq!(got.map(|v| v.data), Some(b"a".to_vec()));
    assert!(
        refused.load(Ordering::Relaxed) > 0,
        "the refusing node was not asked"
    );
    Ok(())
}
