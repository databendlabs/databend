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

use anyhow::Result;
use common_runtime::tokio;
use common_runtime::tokio::sync::oneshot;
use common_sled_store::get_sled_db;
use common_tracing::tracing;
// use common_tracing::tracing::Span;
use tempfile::tempdir;
use tempfile::TempDir;

// use tracing_appender::non_blocking::WorkerGuard;
use crate::api::FlightServer;
use crate::configs;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::raft::config::RaftConfig;

// Start one random service and get the session manager.
#[tracing::instrument(level = "info")]
pub async fn start_metasrv() -> Result<(MetaSrvTestContext, String)> {
    let mut tc = new_test_context();

    start_metasrv_with_context(&mut tc).await?;

    let addr = tc.config.flight_api_address.clone();

    Ok((tc, addr))
}

pub async fn start_metasrv_with_context(tc: &mut MetaSrvTestContext) -> Result<()> {
    let srv = FlightServer::create(tc.config.clone());
    let (stop_tx, fin_rx) = srv.start().await?;

    tc.channels = Some((stop_tx, fin_rx));

    // TODO(xp): some times the MetaNode takes more than 200 ms to startup, with disk-backed store.
    //           Find out why and using some kind of waiting routine to ensure service is on.
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    Ok(())
}

pub fn next_port() -> u32 {
    29000u32 + (common_uniq_id::uniq_usize() as u32)
}

pub struct MetaSrvTestContext {
    #[allow(dead_code)]
    temp_raft_dir: TempDir,

    // /// To hold a per-case logging guard
    // #[allow(dead_code)]
    // logging_guard: (WorkerGuard, DefaultGuard),
    pub config: configs::Config,

    pub meta_nodes: Vec<Arc<MetaNode>>,

    /// channel to send to stop Metasrv, and channel for waiting for shutdown to finish.
    pub channels: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}

/// Create a new Config for test, with unique port assigned
pub fn new_test_context() -> MetaSrvTestContext {
    let config_id = next_port();

    let mut config = configs::Config::empty();

    // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
    if cfg!(target_os = "macos") {
        tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
        config.raft_config.no_sync = true;
    }

    config.raft_config.config_id = format!("{}", config_id);

    // By default, create a meta node instead of open an existent one.
    config.raft_config.single = true;

    config.raft_config.raft_api_port = config_id;

    let host = "127.0.0.1";

    // We use a single sled db for all unit test. Every unit test need a unique prefix so that it opens different tree.
    config.raft_config.sled_tree_prefix = format!("test-{}-", config_id);

    {
        let flight_port = next_port();
        config.flight_api_address = format!("{}:{}", host, flight_port);
    }

    {
        let http_port = next_port();
        config.admin_api_address = format!("{}:{}", host, http_port);
    }

    {
        let metric_port = next_port();
        config.metric_api_address = format!("{}:{}", host, metric_port);
    }

    let temp_raft_dir = tempdir().expect("create temp dir to store meta");
    config.raft_config.raft_dir = temp_raft_dir.path().to_str().unwrap().to_string();

    tracing::info!("new test context config: {:?}", config);

    MetaSrvTestContext {
        // The TempDir type creates a directory on the file system that is deleted once it goes out of scope
        // So hold the tmp_meta_dir and tmp_local_fs_dir until being dropped.
        temp_raft_dir,
        config,
        meta_nodes: vec![],

        channels: None,
    }
}

pub struct RaftTestContext {
    pub raft_config: RaftConfig,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_raft_test_context() -> RaftTestContext {
    // config for unit test of sled db, meta_sync() is true by default.
    let mut config = RaftConfig::empty();

    config.sled_tree_prefix = format!("test-{}-", next_port());

    RaftTestContext {
        raft_config: config,
        db: get_sled_db(),
    }
}

pub async fn assert_meta_connection(addr: &str) -> anyhow::Result<()> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
    let req = tonic::Request::new(GetReq {
        key: "ensure-connection".into(),
    });
    let rst = client.get(req).await?.into_inner();
    assert_eq!("", rst.value, "connected");
    Ok(())
}

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()` and keeps the guard held.
#[macro_export]
macro_rules! init_meta_ut {
    () => {{
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        common_sled_store::init_temp_sled_db(t);

        // common_tracing::init_tracing(&format!("ut-{}", name), "./_logs")
        common_tracing::init_default_ut_tracing();

        let name = common_tracing::func_name!();
        let span =
            common_tracing::tracing::debug_span!("ut", "{}", name.split("::").last().unwrap());
        ((), span)
    }};
}
