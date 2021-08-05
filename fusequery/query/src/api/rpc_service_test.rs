// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Empty;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::ConnectionFactory;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpListener;
use common_runtime::tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;

use crate::api::rpc::FuseQueryFlightDispatcher;
use crate::api::RpcService;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::configs::RpcClientTlsConfig;
use crate::sessions::SessionManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server() -> Result<()> {
    // setup
    let mut conf = Config::default();
    conf.rpc_tls_server_key = "../../tests/data/certs/server.key".to_owned();
    conf.rpc_tls_server_cert = "../../tests/data/certs/server.pem".to_owned();

    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut srv = RpcService {
        sessions: session_manager.clone(),
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(FuseQueryFlightDispatcher::create()),
        conf,
    };
    let addr_str = addr.to_string();
    let stream = TcpListenerStream::new(listener);
    srv.start_with_incoming(stream).await?;

    let client_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../../tests/data/certs/ca.pem".to_string(),
        domain_name: "localhost".to_string(),
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
    conf.rpc_tls_server_key = "../../tests/data/certs/none.key".to_owned();
    conf.rpc_tls_server_cert = "../../tests/data/certs/none.pem".to_owned();

    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut srv = RpcService {
        sessions: session_manager.clone(),
        abort_notify: Arc::new(Notify::new()),
        dispatcher: Arc::new(FuseQueryFlightDispatcher::create()),
        conf,
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
        rpc_tls_server_root_ca_cert: "../../tests/data/certs/nowhere.pem".to_string(),
        domain_name: "localhost".to_string(),
    };

    let r = ConnectionFactory::create_flight_channel("fake:1234", None, Some(client_conf)).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    Ok(())
}
