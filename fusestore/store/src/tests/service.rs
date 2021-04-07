// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use rand::Rng;

use crate::api::RpcService;
use crate::configs::Config;

// Start one random service and get the session manager.
pub async fn start_one_service() -> Result<String> {
    let mut rng = rand::thread_rng();
    let port: u32 = rng.gen_range(10000..11000);
    let addr = format!("127.0.0.1:{}", port);

    let mut conf = Config::default();
    conf.rpc_api_address = addr.clone();

    let srv = RpcService::create(conf);
    tokio::spawn(async move {
        srv.make_server().await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok(addr)
}
