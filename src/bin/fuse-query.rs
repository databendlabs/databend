// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use log::info;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

use tokio::signal::unix::{signal, SignalKind};

use fuse_query::admins::AdminService;
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::rpcs::RpcService;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::Session;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::create();

    // Log level.
    match cfg.log_level.to_lowercase().as_str() {
        "debug" => SimpleLogger::init(LevelFilter::Debug, LogConfig::default())?,
        "info" => SimpleLogger::init(LevelFilter::Info, LogConfig::default())?,
        _ => SimpleLogger::init(LevelFilter::Error, LogConfig::default())?,
    }
    info!("{:?}", cfg.clone());
    info!("FuseQuery v-{}", cfg.version);

    let cluster = Cluster::create(cfg.clone());

    // MySQL handler.
    {
        let session_mgr = Session::create();
        let mysql_handler = MySQLHandler::create(cfg.clone(), session_mgr, cluster.clone());
        tokio::spawn(async move { mysql_handler.start() });

        info!(
            "MySQL handler listening on {}:{}, Usage: mysql -h{} -P{}",
            cfg.mysql_handler_host,
            cfg.mysql_handler_port,
            cfg.mysql_handler_host,
            cfg.mysql_handler_port
        );
    }

    // RPC API service.
    {
        RpcService::create(cfg.clone()).make_server().await?;
        info!("RPC API server listening on {}", cfg.rpc_api_address);
    }

    // Admin API service.
    {
        AdminService::create(cfg.clone(), cluster)
            .make_server()
            .await?;
        info!("HTTP API server listening on {}", cfg.metric_api_address);
    }

    // Metric API service.
    {
        MetricService::create(cfg.clone()).make_server()?;
        info!("Metric API server listening on {}", cfg.metric_api_address);
    }

    // Wait.
    signal(SignalKind::hangup())?.recv().await;
    Ok(())
}
