// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Empty;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpListener;
use common_runtime::tokio::sync::Notify;
use common_store_api_sdk::ConnectionFactory;
use common_store_api_sdk::RpcClientTlsConfig;
use tokio_stream::wrappers::TcpListenerStream;

use crate::api::rpc::DatabendQueryFlightDispatcher;
use crate::api::RpcService;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::SessionManager;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server() -> Result<()> {
    // setup
    let mut conf = Config::default();
    conf.query.rpc_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.query.rpc_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut srv = RpcService {
        sessions: session_manager.clone(),
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(DatabendQueryFlightDispatcher::create()),
    };
    let addr_str = addr.to_string();
    let stream = TcpListenerStream::new(listener);
    srv.start_with_incoming(stream).await?;

    let client_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    // normal case
    let conn =
        ConnectionFactory::create_flight_channel(addr_str.clone(), None, Some(client_conf)).await;
    assert!(conn.is_ok());
    let channel = conn.unwrap();
    let mut f_client = FlightServiceClient::new(channel);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_ok());

    // client access without tls enabled will be failed
    // - channel can still be created, but communication will be failed
    let channel = ConnectionFactory::create_flight_channel(addr_str, None, None).await?;
    let mut f_client = FlightServiceClient::new(channel);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_server_config() -> Result<()> {
    // setup, invalid cert locations
    let mut conf = Config::default();
    conf.query.rpc_tls_server_key = "../tests/data/certs/none.key".to_owned();
    conf.query.rpc_tls_server_cert = "../tests/data/certs/none.pem".to_owned();

    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut srv = RpcService {
        sessions: session_manager.clone(),
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(DatabendQueryFlightDispatcher::create()),
    };
    let stream = TcpListenerStream::new(listener);
    let r = srv.start_with_incoming(stream).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_client_config() -> Result<()> {
    // setup, invalid cert locations
    let client_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/nowhere.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = ConnectionFactory::create_flight_channel("fake:1234", None, Some(client_conf)).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    Ok(())
}
