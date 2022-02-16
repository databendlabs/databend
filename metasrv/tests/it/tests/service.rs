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

use std::sync::Arc;

use anyhow::Result;
use common_base::tokio;
use common_base::GlobalSequence;
use common_base::Stoppable;
use common_meta_grpc::MetaGrpcClient;
use common_meta_sled_store::openraft::NodeId;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::protobuf::GetRequest;
use common_tracing::tracing;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::meta_service::MetaNode;

// Start one random service and get the session manager.
#[tracing::instrument(level = "info")]
pub async fn start_metasrv() -> Result<(MetaSrvTestContext, String)> {
    let mut tc = MetaSrvTestContext::new(0);

    start_metasrv_with_context(&mut tc).await?;

    let addr = tc.config.grpc_api_address.clone();

    Ok((tc, addr))
}

pub async fn start_metasrv_with_context(tc: &mut MetaSrvTestContext) -> Result<()> {
    let mn = MetaNode::start(&tc.config.raft_config).await?;
    let mut srv = GrpcServer::create(tc.config.clone(), mn);
    srv.start().await?;
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
        tc.config.raft_config.join = vec![leader_addr.clone()];
        start_metasrv_with_context(&mut tc).await?;

        res.push(tc);
    }

    Ok(res)
}

pub fn next_port() -> u32 {
    29000u32 + (GlobalSequence::next() as u32)
}

pub struct MetaSrvTestContext {
    // /// To hold a per-case logging guard
    // logging_guard: (WorkerGuard, DefaultGuard),
    pub config: configs::Config,

    pub meta_node: Option<Arc<MetaNode>>,

    pub grpc_srv: Option<Box<GrpcServer>>,
}

impl MetaSrvTestContext {
    /// Create a new Config for test, with unique port assigned
    pub fn new(id: u64) -> MetaSrvTestContext {
        let config_id = next_port();

        let mut config = configs::Config::empty();

        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.raft_config.no_sync = true;
        }

        config.raft_config.id = id;

        config.raft_config.config_id = format!("{}", config_id);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        config.raft_config.raft_api_port = config_id;

        // when running unit tests, both raft_{listen|advertise}_host is "127.0.0.1"
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "127.0.0.1".to_string();

        let host = "127.0.0.1";

        // We use a single sled db for all unit test. Every unit test need a unique prefix so that it opens different tree.
        config.raft_config.sled_tree_prefix = format!("test-{}-", config_id);

        {
            let grpc_port = next_port();
            config.grpc_api_address = format!("{}:{}", host, grpc_port);
        }

        {
            let http_port = next_port();
            config.admin_api_address = format!("{}:{}", host, http_port);
        }

        {
            let metric_port = next_port();
            config.metric_api_address = format!("{}:{}", host, metric_port);
        }

        tracing::info!("new test context config: {:?}", config);

        MetaSrvTestContext {
            config,
            meta_node: None,
            grpc_srv: None,
        }
    }

    pub fn meta_node(&self) -> Arc<MetaNode> {
        self.meta_node.clone().unwrap()
    }

    pub async fn grpc_client(&self) -> anyhow::Result<MetaGrpcClient> {
        let addr = self.config.grpc_api_address.clone();

        let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;
        Ok(client)
    }

    pub async fn raft_client(
        &self,
    ) -> anyhow::Result<RaftServiceClient<tonic::transport::Channel>> {
        let addr = self.config.raft_config.raft_api_addr().await?;

        // retry 3 times until server starts listening.
        for _ in 0..4 {
            let client = RaftServiceClient::connect(format!("http://{}", addr)).await;
            match client {
                Ok(x) => return Ok(x),
                Err(err) => {
                    tracing::info!("can not yet connect to {}, {}, sleep a while", addr, err);
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            }
        }

        panic!("can not connect to raft server: {:?}", self.config);
    }

    pub async fn assert_raft_server_connection(&self) -> anyhow::Result<()> {
        let mut client = self.raft_client().await?;

        let req = tonic::Request::new(GetRequest {
            key: "ensure-connection".into(),
        });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("", rst.value, "connected");
        Ok(())
    }
}

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()` and keeps the guard held.
#[macro_export]
macro_rules! init_meta_ut {
    () => {{
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        common_meta_sled_store::init_temp_sled_db(t);

        // common_tracing::init_tracing(&format!("ut-{}", name), "./_logs")
        common_tracing::init_meta_ut_tracing();

        let name = common_tracing::func_name!();
        let span =
            common_tracing::tracing::debug_span!("ut", "{}", name.split("::").last().unwrap());
        ((), span)
    }};
}
