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
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_grpc::ConnectionFactory;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaChannelManager;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_client::Streamed;
use databend_common_meta_client::MIN_METASRV_SEMVER;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::UpsertKV;
use futures::StreamExt;
use log::info;
use tonic::codegen::BoxStream;

use crate::grpc_server::rand_local_addr;
use crate::grpc_server::start_grpc_server;
use crate::grpc_server::start_grpc_server_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_client_timeout() -> anyhow::Result<()> {
    // start_grpc_server will sleep 1 second.
    let (srv_addr, _shutdown, _task_handle) = start_grpc_server();

    // use `timeout=3secs` here cause our mock grpc
    // server's handshake impl will sleep 2secs.
    let timeout = Duration::from_secs(3);
    let client = new_client(&srv_addr, Some(timeout))?;

    let res = client.request(UpsertKV::insert("foo", b"foo")).await;

    if let Err(err) = res {
        let got = err.to_string();
        assert!(got.starts_with(
            "ConnectionError: failed to connect to meta-service source: failed after 4 retries:"
        ));
    } else {
        panic!("expect error, but got ok");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_client_handshake_timeout() {
    let (srv_addr, _shutdown, _task_handle) = start_grpc_server();

    // timeout will happen
    {
        // our mock grpc server's handshake impl will sleep 2secs.
        // see: GrpcServiceForTestImp.handshake
        let timeout = Duration::from_secs(1);
        let c = ConnectionFactory::create_rpc_channel(srv_addr.clone(), Some(timeout), None)
            .await
            .unwrap();

        let (mut client, _once) = MetaChannelManager::new_real_client(c);

        let res = MetaGrpcClient::handshake(
            &mut client,
            &MIN_METASRV_SEMVER,
            &MIN_METASRV_SEMVER,
            "root",
            "xxx",
        )
        .await;

        let got = res.unwrap_err();
        let got =
            ErrorCode::from(MetaError::ClientError(MetaClientError::HandshakeError(got))).message();
        let expect = "failed to handshake with meta-service: when sending handshake rpc, cause: tonic::status::Status: status: Cancelled, message: \"Timeout expired\", details: [], metadata: MetadataMap { headers: {} } source: transport error source: Timeout expired";
        assert_eq!(got, expect);
    }

    // handshake success
    {
        let timeout = Duration::from_secs(3);
        let c = ConnectionFactory::create_rpc_channel(srv_addr, Some(timeout), None)
            .await
            .unwrap();

        let (mut client, _once) = MetaChannelManager::new_real_client(c);

        let res = MetaGrpcClient::handshake(
            &mut client,
            &MIN_METASRV_SEMVER,
            &MIN_METASRV_SEMVER,
            "root",
            "xxx",
        )
        .await;

        assert!(res.is_ok());
    }
}

/// When server is down, client should try to reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_client_reconnect() -> anyhow::Result<()> {
    let srv_addr = rand_local_addr();
    let client = new_client(&srv_addr, Some(Duration::from_secs(3)))?;

    // Send a Get request and return the first item.
    async fn send_req(client: &Arc<ClientHandle>) -> Result<StreamItem, MetaError> {
        let res: Result<BoxStream<StreamItem>, MetaError> =
            client.request(Streamed(MGetKVReq::new(["foo"]))).await;

        let mut strm = res?;
        let first = strm.next().await.unwrap();
        let first = first?;
        Ok(first)
    }

    info!("--- no server, client does not work");
    {
        let res = send_req(&client).await;
        info!("--- expected error: {:?}", res.unwrap_err());
    }

    info!("--- start server, client works as expected");
    let (shutdown, task_handle) = start_grpc_server_addr(&srv_addr);
    {
        let got = send_req(&client).await?;
        info!("--- rpc got: {:?}", got);
    }

    info!("--- shutdown server, client can not connect");
    {
        shutdown.send(()).unwrap();
        let r = task_handle.await;
        info!("--- task_handle.await: {:?}", r);

        let res = send_req(&client).await;
        info!("--- expected error: {:?}", res.unwrap_err());
    }

    info!("--- restart server, client auto reconnect");
    let (_shutdown, _task_handle) = start_grpc_server_addr(&srv_addr);
    {
        let got = send_req(&client).await?;
        info!("--- rpc got: {:?}", got);
    }

    Ok(())
}

fn new_client(addr: impl ToString, timeout: Option<Duration>) -> anyhow::Result<Arc<ClientHandle>> {
    let client = MetaGrpcClient::try_create(vec![addr.to_string()], "", "", timeout, None, None)?;

    Ok(client)
}
