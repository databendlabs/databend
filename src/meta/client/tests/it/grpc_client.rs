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

use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_grpc::ConnectionFactory;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_client::MIN_METASRV_SEMVER;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;

use crate::grpc_server::start_grpc_server;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_client_action_timeout() {
    // start_grpc_server will sleep 1 second.
    let srv_addr = start_grpc_server();

    // use `timeout=3secs` here cause our mock grpc
    // server's handshake impl will sleep 2secs.
    let timeout = Duration::from_secs(3);

    let client = MetaGrpcClient::try_create(
        vec![srv_addr],
        "",
        "",
        Some(timeout),
        None,
        Duration::from_secs(10),
        None,
    )
    .unwrap();

    let res = client
        .get_database(GetDatabaseReq::new("tenant1", "xx"))
        .await;
    let got = res.unwrap_err();
    let got = ErrorCode::from(got).message();
    let expect = "ConnectionError:  source: tonic::status::Status: status: Cancelled, message: \"Timeout expired\", details: [], metadata: MetadataMap { headers: {} } source: transport error source: Timeout expired";
    assert_eq!(got, expect);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grpc_client_handshake_timeout() {
    let srv_addr = start_grpc_server();

    // timeout will happen
    {
        // our mock grpc server's handshake impl will sleep 2secs.
        // see: GrpcServiceForTestImp.handshake
        let timeout = Duration::from_secs(2);
        let c = ConnectionFactory::create_rpc_channel(srv_addr.clone(), Some(timeout), None)
            .await
            .unwrap();

        let (mut client, _once) = MetaGrpcClient::new_real_client(c);

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

        let (mut client, _once) = MetaGrpcClient::new_real_client(c);

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
