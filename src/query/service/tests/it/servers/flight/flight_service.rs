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

use std::net::SocketAddr;
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;

use arrow_flight::Empty;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::GrpcConnectionError;
use databend_common_grpc::RpcClientTlsConfig;
use databend_common_settings::FlightKeepAliveParams;
use databend_meta_client::types::NodeInfo;
use databend_query::clusters::Cluster;
use databend_query::clusters::ClusterHelper;
use databend_query::clusters::FlightParams;
use databend_query::servers::flight::FlightService;
use databend_query::servers::flight::v1::DatabendQueryFlightService;
use databend_query::servers::flight::v1::exchange::DataExchangeManager;
use databend_query::servers::flight::v1::packets::DataflowDiagramBuilder;
use databend_query::servers::flight::v1::packets::QueryEnv;
use databend_query::sessions::TableContextAuthorization;
use databend_query::sessions::TableContextCluster;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextQueryInfo;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::*;
use tokio_stream::wrappers::TcpListenerStream;
use uuid::Uuid;

use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server() -> anyhow::Result<()> {
    // Set default crypto provider to use
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html#using-the-per-process-default-cryptoprovider
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut rpc_service = FlightService::create(
        ConfigBuilder::create()
            .rpc_tls_server_key(TEST_SERVER_KEY)
            .rpc_tls_server_cert(TEST_SERVER_CERT)
            .build(),
    )?;

    let mut listener_address = SocketAddr::from_str("127.0.0.1:9991")?;
    listener_address = rpc_service.start(listener_address).await?;

    let tls_conf = Some(RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    });

    // normal case
    let conn =
        ConnectionFactory::create_rpc_channel(listener_address, None, tls_conf, None).await?;
    let mut f_client = FlightServiceClient::new(conn);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_ok());

    // client access without tls enabled will be failed
    // - channel can still be created, but communication will be failed
    let channel = ConnectionFactory::create_rpc_channel(listener_address, None, None, None).await?;
    let mut f_client = FlightServiceClient::new(channel);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_server_config() -> anyhow::Result<()> {
    // setup, invalid cert locations
    let mut srv = FlightService {
        config: ConfigBuilder::create()
            .rpc_tls_server_key("../tests/data/certs/none.key")
            .rpc_tls_server_cert("../tests/data/certs/none.pem")
            .build(),
        abort_notify: Default::default(),
    };
    let r = srv
        .start_with_incoming("127.0.0.1:0".parse().unwrap())
        .await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::T_L_S_CONFIGURATION_FAILURE);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_client_config() -> anyhow::Result<()> {
    // setup, invalid cert locations
    let client_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/nowhere.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = ConnectionFactory::create_rpc_channel("fake:1234", None, Some(client_conf), None).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rpc_server_port_used() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let local_socket = listener.local_addr().unwrap();

    let mut srv = FlightService {
        config: ConfigBuilder::create().build(),
        abort_notify: Default::default(),
    };

    let r = srv.start_with_incoming(local_socket).await;

    assert!(r.is_err());
    Ok(())
}

#[tokio::test]
async fn test_maintenance_init_failure_releases_remote_grant() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.query.common.table_hook_async_max_concurrency = 1;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let config = GlobalConfig::instance();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = databend_common_base::runtime::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(
                DatabendQueryFlightService::create(),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    // Route two logical workers to one server. The first INIT owns its only
    // maintenance grant; the next INIT must fail before touching that context,
    // regardless of the coordinator's iteration order.
    let node = |id| {
        Arc::new(NodeInfo::create(
            id,
            config.query.node_secret.clone(),
            String::new(),
            addr.to_string(),
            String::new(),
            String::new(),
            String::new(),
        ))
    };
    let worker = node(config.query.node_id.clone());
    let workers = vec![worker.clone(), node("second-worker".to_string())];
    let request_server_id = "maintenance-coordinator".to_string();
    let cluster = Cluster::create(workers.clone(), request_server_id.clone());
    let ctx = fixture.new_query_ctx().await?;
    ctx.set_cluster(cluster.clone());
    let settings = ctx.get_settings();
    settings.set_max_query_memory_usage(256 * 1024 * 1024)?;
    let mut env = QueryEnv {
        automatic_maintenance: true,
        query_id: ctx.get_id(),
        query_created_time: ctx.get_query_created_time(),
        cluster,
        settings,
        query_kind: ctx.get_query_kind(),
        dataflow_diagram: Arc::new(DataflowDiagramBuilder::create(workers).build()),
        request_server_id,
        workload_group: None,
        create_rpc_clint_with_current_rt: false,
        perf_config: Default::default(),
        user: ctx.get_current_user()?,
        audit_role_name: None,
        cache_policy: Default::default(),
    };
    let flight_params = FlightParams {
        timeout: 5,
        retry_times: 0,
        retry_interval: 0,
        keep_alive: FlightKeepAliveParams::default(),
        reuse_control_channel: true,
    };
    let error = env.init(&ctx, flight_params).await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::NO_RESOURCES_AVAILABLE, "{error}");
    let exchange = DataExchangeManager::instance();
    assert!(
        exchange.get_query_ctx(&env.query_id).is_err(),
        "the admitted worker must be cleaned up before returning the error"
    );

    // Remove the rejecting alias and retry immediately with a fresh query ID.
    // If the first grant was retained, admission would fail again.
    env.query_id = Uuid::new_v4().to_string();
    env.dataflow_diagram = Arc::new(DataflowDiagramBuilder::create(vec![worker]).build());
    env.init(&ctx, flight_params).await?;
    assert!(exchange.get_query_ctx(&env.query_id).is_ok());
    exchange.on_finished_query(&env.query_id, None);
    server.abort();
    Ok(())
}
