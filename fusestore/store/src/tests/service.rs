// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_runtime::tokio;
use tempfile::tempdir;
use tempfile::TempDir;

use crate::api::StoreServer;
use crate::configs;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::tests::Seq;

// Start one random service and get the session manager.
pub async fn start_store_server() -> Result<(StoreTestContext, String)> {
    let mut tc = new_test_context();

    let addr = next_local_addr();

    // TODO(xp): when testing, new_test_context() should build a random addr for flight
    //           and fs storage dir
    tc.config.flight_api_address = addr.clone();

    let srv = StoreServer::create(tc.config.clone());
    tokio::spawn(async move {
        srv.serve().await?;
        Ok::<(), anyhow::Error>(())
    });

    // TODO(xp): some times the MetaNode takes more than 200 ms to startup, with disk-backed store.
    //           Find out why and using some kind of waiting routine to ensure service is on.
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    Ok((tc, addr))
}

pub fn next_port() -> u32 {
    19000u32 + (*Seq::default() as u32)
}

pub fn next_local_addr() -> String {
    let port: u32 = next_port();
    format!("127.0.0.1:{}", port)
}

pub struct StoreTestContext {
    #[allow(dead_code)]
    meta_temp_dir: TempDir,
    pub config: configs::Config,
    pub meta_nodes: Vec<Arc<MetaNode>>,
}

/// Create a new Config for test, with unique port assigned
pub fn new_test_context() -> StoreTestContext {
    let mut config = configs::Config::empty();

    config.meta_api_port = next_port();

    let t = tempdir().expect("create temp dir to store meta");
    config.meta_dir = t.path().to_str().unwrap().to_string();

    StoreTestContext {
        // hold the TempDir until being dropped.
        meta_temp_dir: t,
        config,
        meta_nodes: vec![],
    }
}

pub struct SledTestContext {
    #[allow(dead_code)]
    temp_dir: TempDir,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_sled_test_context() -> SledTestContext {
    let t = tempdir().expect("create temp dir to store meta");
    let tmpdir = t.path().to_str().unwrap().to_string();

    SledTestContext {
        // hold the TempDir until being dropped.
        temp_dir: t,
        db: sled::open(tmpdir).expect("open sled db"),
    }
}

pub async fn assert_meta_connection(addr: &str) -> anyhow::Result<()> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
    let req = tonic::Request::new(GetReq { key: "foo".into() });
    let rst = client.get(req).await?.into_inner();
    assert_eq!("", rst.value, "connected");
    Ok(())
}
