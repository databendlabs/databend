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
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use databend_common_base::base::tokio;
use databend_common_base::base::GlobalSequence;
use databend_common_base::base::Stoppable;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::NodeId;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::meta_service::MetaNode;
use log::info;
use log::warn;

// Start one random service and get the session manager.
#[fastrace::trace]
pub async fn start_metasrv() -> Result<(MetaSrvTestContext, String)> {
    let mut tc = MetaSrvTestContext::new(0);

    start_metasrv_with_context(&mut tc).await?;

    let addr = tc.config.grpc_api_address.clone();

    Ok((tc, addr))
}

pub async fn start_metasrv_with_context(tc: &mut MetaSrvTestContext) -> Result<()> {
    let mn = MetaNode::start(&tc.config).await?;
    let _ = mn
        .join_cluster(
            &tc.config.raft_config,
            tc.config.grpc_api_advertise_address(),
        )
        .await?;

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
        tc.config.raft_config.join = vec![leader_addr.to_string()];
        start_metasrv_with_context(&mut tc).await?;

        res.push(tc);
    }

    Ok(res)
}

pub fn make_grpc_client(addresses: Vec<String>) -> Result<Arc<ClientHandle>, MetaClientError> {
    let client = MetaGrpcClient::try_create(
        addresses,
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    Ok(client)
}

pub fn next_port() -> u16 {
    29000u16 + (GlobalSequence::next() as u16)
}

/// It holds a reference to a MetaNode or a GrpcServer, for testing MetaNode or GrpcServer.
pub struct MetaSrvTestContext {
    pub config: configs::Config,

    pub meta_node: Option<Arc<MetaNode>>,

    pub grpc_srv: Option<Box<GrpcServer>>,
}

impl Drop for MetaSrvTestContext {
    fn drop(&mut self) {
        self.rm_raft_dir("Drop MetaSrvTestContext");
    }
}

impl MetaSrvTestContext {
    /// Create a new Config for test, with unique port assigned
    pub fn new(id: u64) -> MetaSrvTestContext {
        let config_id = next_port();

        let mut config = configs::Config::default();

        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.raft_config.no_sync = true;
        }

        config.raft_config.id = id;

        config.raft_config.config_id = config_id.to_string();

        // Use a unique dir for each test case.
        config.raft_config.raft_dir = format!("{}-{}", config.raft_config.raft_dir, config_id);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        config.raft_config.raft_api_port = config_id;

        // when running unit tests, set raft_listen_host to "127.0.0.1" and raft_advertise_host to localhost,
        // so if something wrong in raft meta nodes communication we will catch bug in unit tests.
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "localhost".to_string();

        let host = "127.0.0.1";

        // We use a single sled db for all unit test. Every unit test need a unique prefix so that it opens different tree.
        config.raft_config.sled_tree_prefix = format!("test-{}-", config_id);

        // For testing, we use a fake license to enable clustering functions
        config.raft_config.fake_ee_license = true;

        {
            let grpc_port = next_port();
            config.grpc_api_address = format!("{}:{}", host, grpc_port);
            config.grpc_api_advertise_host = Some(host.to_string());
        }

        {
            let http_port = next_port();
            config.admin_api_address = format!("{}:{}", host, http_port);
        }

        info!("new test context config: {:?}", config);

        let c = MetaSrvTestContext {
            config,
            meta_node: None,
            grpc_srv: None,
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

    pub fn meta_node(&self) -> Arc<MetaNode> {
        self.meta_node.clone().unwrap()
    }

    pub async fn grpc_client(&self) -> anyhow::Result<Arc<ClientHandle>> {
        let addr = self.config.grpc_api_address.clone();

        let client = MetaGrpcClient::try_create(
            vec![addr],
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
}

/// Build metasrv or metasrv cluster, returns the clients
#[derive(Clone)]
pub struct MetaSrvBuilder {
    pub test_contexts: Arc<Mutex<Vec<MetaSrvTestContext>>>,
}

#[async_trait]
impl kvapi::ApiBuilder<Arc<ClientHandle>> for MetaSrvBuilder {
    async fn build(&self) -> Arc<ClientHandle> {
        let (tc, addr) = start_metasrv().await.unwrap();

        let client =
            MetaGrpcClient::try_create(vec![addr], "root", "xxx", None, None, None).unwrap();

        {
            let mut tcs = self.test_contexts.lock().unwrap();
            tcs.push(tc);
        }

        client
    }

    async fn build_cluster(&self) -> Vec<Arc<ClientHandle>> {
        let tcs = start_metasrv_cluster(&[0, 1, 2]).await.unwrap();

        let cluster = vec![
            tcs[0].grpc_client().await.unwrap(),
            tcs[1].grpc_client().await.unwrap(),
            tcs[2].grpc_client().await.unwrap(),
        ];

        {
            let mut test_contexts = self.test_contexts.lock().unwrap();
            test_contexts.extend(tcs);
        }

        cluster
    }
}
