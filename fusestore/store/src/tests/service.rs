// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use rand::Rng;

use crate::api::StoreServer;
use crate::configs::Config;

// Start one random service and get the session manager.
pub async fn start_store_server() -> Result<String> {
    let addr = rand_local_addr();

    let mut conf = Config::default();
    conf.rpc_api_address = addr.clone();

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

macro_rules! serve_grpc {
    ($addr:expr, $srv:expr) => {
        let addr = $addr.parse::<std::net::SocketAddr>()?;

        let srv = tonic::transport::Server::builder().add_service($srv);

        tokio::spawn(async move {
            srv.serve(addr)
                .await
                .map_err(|e| anyhow::anyhow!("Flight service error: {:?}", e))?;
            Ok::<(), anyhow::Error>(())
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    };
}
