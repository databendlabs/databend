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

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Empty;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::tokio;
use common_base::tokio::net::TcpListener;
use common_base::tokio::sync::Notify;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_grpc::GrpcConnectionError;
use common_grpc::RpcClientTlsConfig;
use databend_query::api::DatabendQueryFlightDispatcher;
use databend_query::api::RpcService;
use databend_query::servers::Server;
use tokio_stream::wrappers::TcpListenerStream;

use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;
use crate::tests::SessionManagerBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server() -> Result<()> {
    let mut rpc_service = RpcService {
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(DatabendQueryFlightDispatcher::create()),
        sessions: SessionManagerBuilder::create()
            .rpc_tls_server_key(TEST_SERVER_KEY)
            .rpc_tls_server_cert(TEST_SERVER_CERT)
            .build()?,
    };

    let mut listener_address = SocketAddr::from_str("127.0.0.1:0")?;
    listener_address = rpc_service.start(listener_address).await?;

    let tls_conf = Some(RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    });

    // normal case
    let conn = ConnectionFactory::create_rpc_channel(listener_address, None, tls_conf)?;
    let mut f_client = FlightServiceClient::new(conn);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_ok());

    // client access without tls enabled will be failed
    // - channel can still be created, but communication will be failed
    let channel = ConnectionFactory::create_rpc_channel(listener_address, None, None)?;
    let mut f_client = FlightServiceClient::new(channel);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_server_config() -> Result<()> {
    // setup, invalid cert locations
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut srv = RpcService {
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(DatabendQueryFlightDispatcher::create()),
        sessions: SessionManagerBuilder::create()
            .rpc_tls_server_key("../tests/data/certs/none.key")
            .rpc_tls_server_cert("../tests/data/certs/none.pem")
            .build()?,
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

    let r = ConnectionFactory::create_rpc_channel("fake:1234", None, Some(client_conf));
    assert!(r.is_err());
    let e = r.unwrap_err();
    match e {
        GrpcConnectionError::TLSConfigError { action, .. } => {
            assert_eq!("loading", action);
        }
        _ => unimplemented!("expect TLSConfigError"),
    }
    Ok(())
}
