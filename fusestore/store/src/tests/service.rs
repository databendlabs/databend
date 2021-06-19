// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_runtime::tokio;
use rand::Rng;

use crate::api::StoreServer;
use crate::configs::Config;
use crate::meta_service::GetReq;
use crate::meta_service::MetaServiceClient;

// Start one random service and get the session manager.
pub async fn start_store_server() -> Result<String> {
    let addr = rand_local_addr();

    let mut conf = Config::default();
    conf.flight_api_address = addr.clone();

    let srv = StoreServer::create(conf);
    tokio::spawn(async move {
        srv.serve().await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok(addr)
}

pub fn rand_local_addr() -> String {
    let mut rng = rand::thread_rng();
    let port: u32 = rng.gen_range(10000..11000);
    let addr = format!("127.0.0.1:{}", port);
    return addr;
}

pub async fn assert_meta_connection(addr: &str) -> anyhow::Result<()> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
    let req = tonic::Request::new(GetReq { key: "foo".into() });
    let rst = client.get(req).await?.into_inner();
    assert_eq!("", rst.value, "connected");
    Ok(())
}
