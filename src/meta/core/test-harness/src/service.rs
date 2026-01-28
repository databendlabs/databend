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

use std::fmt;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use databend_base::testutil::next_port;
use databend_base::uniq_id::GlobalUniq;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_client::errors::CreationError;
use databend_common_meta_runtime_api::RuntimeApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_version::BUILD_INFO;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::meta_service::MetaNode;
use databend_meta_runtime::DatabendRuntime;
use log::info;
use log::warn;

/// Start one random service and get the session manager.
#[fastrace::trace]
pub async fn start_metasrv() -> Result<(MetaSrvTestContext, String)> {
    let mut tc = MetaSrvTestContext::new(0);

    start_metasrv_with_context(&mut tc).await?;

    let addr = tc
        .config
        .grpc
        .api_address()
        .expect("gRPC port should be assigned after server start");

    Ok((tc, addr))
}

pub async fn start_metasrv_with_context(tc: &mut MetaSrvTestContext) -> Result<()> {
    // In tests, listen_port must be None so the OS assigns an ephemeral port.
    // This prevents port conflicts between concurrent tests.
    assert!(
        tc.config.grpc.listen_port.is_none(),
        "listen_port must be None in tests; OS will assign an ephemeral port"
    );

    let runtime = TokioRuntime::new_testing("meta-io-rt-ut");
    let mh = MetaWorker::create_meta_worker(tc.config.clone(), Arc::new(runtime)).await?;
    let mh = Arc::new(mh);

    let mut srv = GrpcServer::create(&tc.config, BUILD_INFO.semver(), mh.clone());

    // Bind first to get the actual port, then update tc.config
    let incoming = srv.bind()?;
    tc.config.grpc = srv.grpc_config().clone();

    // Join cluster with the updated config that has the actual port
    let c = tc.config.clone();
    let _ = mh
        .request(move |mn| {
            let fu = async move { mn.join_cluster(&c).await };
            Box::pin(fu)
        })
        .await??;

    srv.do_start_with_incoming(incoming).await?;
    tc.grpc_srv = Some(Box::new(srv));

    Ok(())
}

/// Bring up a cluster of metasrv, the first one is the leader.
///
/// It returns a vec of test-context.
pub async fn start_metasrv_cluster(node_ids: &[NodeId]) -> anyhow::Result<Vec<MetaSrvTestContext>> {
    let mut res = vec![];

    let leader_id = node_ids[0];

    let mut tc0 = MetaSrvTestContext::new(leader_id);
    start_metasrv_with_context(&mut tc0).await?;

    let leader_addr = tc0.config.raft_config.raft_api_addr().await?;
    res.push(tc0);

    for node_id in node_ids.iter().skip(1) {
        let mut tc = MetaSrvTestContext::new(*node_id);
        tc.config.raft_config.single = false;
        tc.config.raft_config.join = vec![leader_addr.to_string()];
        start_metasrv_with_context(&mut tc).await?;

        res.push(tc);
    }

    Ok(res)
}

pub fn make_grpc_client(
    addresses: Vec<String>,
) -> Result<Arc<ClientHandle<DatabendRuntime>>, CreationError> {
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        addresses,
        BUILD_INFO.semver(),
        "root",
        "xxx",
        Some(Duration::from_secs(2)), // timeout
        Some(Duration::from_secs(10)),
        None,
    )?;

    Ok(client)
}

/// It holds a reference to a MetaNode or a GrpcServer, for testing MetaNode or GrpcServer.
pub struct MetaSrvTestContext {
    pub _temp_dir: tempfile::TempDir,

    pub config: configs::MetaServiceConfig,

    /// Admin API configuration for HTTP service tests.
    /// This is separate from the core `MetaServiceConfig` because admin config
    /// is a CLI-level concern, not a service-level concern.
    pub admin: configs::AdminConfig,

    pub meta_node: Option<Arc<MetaNode<TokioRuntime>>>,

    pub grpc_srv: Option<Box<GrpcServer<TokioRuntime>>>,
}

impl Drop for MetaSrvTestContext {
    fn drop(&mut self) {
        self.rm_raft_dir("Drop MetaSrvTestContext");
    }
}

impl MetaSrvTestContext {
    /// Create a new Config for test, with unique port assigned
    pub fn new(id: u64) -> MetaSrvTestContext {
        let temp_dir = tempfile::tempdir().unwrap();

        let config_id = GlobalUniq::unique();

        let mut config = configs::MetaServiceConfig::default();

        config.raft_config.id = id;

        config.raft_config.config_id = config_id.clone();

        // Use a unique dir for each test case.
        config.raft_config.raft_dir =
            format!("{}/{}/raft_dir", temp_dir.path().display(), config_id);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        config.raft_config.raft_api_port = next_port();

        // when running unit tests, set raft_listen_host to "127.0.0.1" and raft_advertise_host to localhost,
        // so if something wrong in raft meta nodes communication we will catch bug in unit tests.
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "localhost".to_string();

        // gRPC port will be assigned by OS when server starts
        config.grpc = configs::GrpcConfig::new_local("127.0.0.1");

        let admin = {
            let http_port = next_port();
            configs::AdminConfig {
                api_address: format!("127.0.0.1:{}", http_port),
                tls: configs::TlsConfig::default(),
            }
        };

        info!("new test context config: {:?}", config);

        let c = MetaSrvTestContext {
            config,
            admin,
            meta_node: None,
            grpc_srv: None,
            _temp_dir: temp_dir,
        };

        c.rm_raft_dir("new MetaSrvTestContext");

        c
    }

    pub fn rm_raft_dir(&self, msg: impl fmt::Display + Copy) {
        let raft_dir = &self.config.raft_config.raft_dir;

        info!("{}: about to remove raft_dir: {:?}", msg, raft_dir);

        let res = fs::remove_dir_all(raft_dir);
        if let Err(e) = res {
            warn!("{}: can not remove raft_dir {:?}, {:?}", msg, raft_dir, e);
        } else {
            info!("{}: OK removed raft_dir {:?}", msg, raft_dir)
        }
    }

    pub fn meta_node(&self) -> Arc<MetaNode<TokioRuntime>> {
        self.meta_node.clone().unwrap()
    }

    pub async fn grpc_client(&self) -> anyhow::Result<Arc<ClientHandle<DatabendRuntime>>> {
        let addr = self
            .config
            .grpc
            .api_address()
            .ok_or_else(|| anyhow::anyhow!("gRPC port not assigned yet"))?;

        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr],
            BUILD_INFO.semver(),
            "root",
            "xxx",
            None,
            Some(Duration::from_secs(10)),
            None,
        )?;
        Ok(client)
    }

    pub async fn raft_client(
        &self,
    ) -> anyhow::Result<RaftServiceClient<tonic::transport::Channel>> {
        let addr = self.config.raft_config.raft_api_addr().await?;

        // retry 3 times until server starts listening.
        for _ in 0..3 {
            let client = RaftServiceClient::connect(format!("http://{}", addr)).await;
            match client {
                Ok(x) => return Ok(x),
                Err(err) => {
                    info!("can not yet connect to {}, {}, sleep a while", addr, err);
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            }
        }

        panic!("can not connect to raft server: {:?}", self.config);
    }

    pub async fn assert_raft_server_connection(&self) -> anyhow::Result<()> {
        let mut client = self.raft_client().await?;

        let req = ForwardRequest {
            forward_to_leader: 0,
            body: ForwardRequestBody::Ping,
        };

        client.forward(req).await?;
        Ok(())
    }

    pub fn drop_meta_node(&mut self) {
        self.meta_node.take();
        self.grpc_srv.take();
    }
}
