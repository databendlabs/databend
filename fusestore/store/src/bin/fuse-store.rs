// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_store::api::RpcService;
use fuse_store::configs::Config;
use fuse_store::metrics::MetricService;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::create_from_args();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str())
    )
    .init();

    info!("{:?}", conf.clone());
    info!("FuseStore v-{}", conf.version);

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tokio::spawn(async move {
            srv.make_server().expect("Metrics service error");
        });
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // RPC API service.
    {
        let srv = RpcService::create(conf.clone());
        info!("RPC API server listening on {}", conf.rpc_api_address);
        srv.make_server().await.expect("RPC service error");
    }

    Ok(())
}
