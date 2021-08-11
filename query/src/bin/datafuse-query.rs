// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use common_runtime::tokio;
use common_tracing::init_tracing_with_file;
use datafuse_query::api::HttpService;
use datafuse_query::api::RpcService;
use datafuse_query::clusters::Cluster;
use datafuse_query::configs::Config;
use datafuse_query::metrics::MetricService;
use datafuse_query::servers::ClickHouseHandler;
use datafuse_query::servers::MySQLHandler;
use datafuse_query::servers::Server;
use datafuse_query::servers::ShutdownHandle;
use datafuse_query::sessions::SessionManager;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use customize malloc.
    let malloc = common_allocators::init();

    // First load configs from args.
    let mut conf = Config::load_from_args();

    // If config file is not empty: -c xx.toml
    // Reload configs from the file.
    if !conf.config_file.is_empty() {
        info!("Config reload from {:?}", conf.config_file);
        conf = Config::load_from_toml(conf.config_file.as_str())?;
    }

    // Prefer to use env variable in cloud native deployment
    // Override configs based on env variables
    conf = Config::load_from_env(&conf)?;

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str()),
    )
    .init();
    let _guards = init_tracing_with_file(
        "datafuse-query",
        conf.log_dir.as_str(),
        conf.log_level.as_str(),
    );

    info!("{:?}", conf);
    info!(
        "DatafuseQuery v-{}, Allocator: {}",
        *datafuse_query::configs::config::FUSE_COMMIT_VERSION,
        malloc
    );

    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;
    let mut shutdown_handle = ShutdownHandle::create(session_manager.clone());

    // MySQL handler.
    {
        let listening = format!(
            "{}:{}",
            conf.mysql_handler_host.clone(),
            conf.mysql_handler_port
        );
        let listening = listening.parse::<SocketAddr>()?;

        let mut handler = MySQLHandler::create(session_manager.clone());
        let listening = handler.start(listening).await?;
        shutdown_handle.add_service(handler);

        info!(
            "MySQL handler listening on {}, Usage: mysql -h{} -P{}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // ClickHouse handler.
    {
        let hostname = conf.clickhouse_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.clickhouse_handler_port);
        let listening = listening.parse::<SocketAddr>()?;

        let mut srv = ClickHouseHandler::create(session_manager.clone());
        let listening = srv.start(listening).await?;
        shutdown_handle.add_service(srv);

        info!(
            "ClickHouse handler listening on {}, Usage: clickhouse-client --host {} --port {}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // Metric API service.
    {
        let listening = conf.metric_api_address.parse::<std::net::SocketAddr>()?;
        let mut srv = MetricService::create();
        let listening = srv.start(listening).await?;
        shutdown_handle.add_service(srv);
        info!("Metric API server listening on {}", listening);
    }

    // HTTP API service.
    {
        let listening = conf.http_api_address.parse::<std::net::SocketAddr>()?;
        let mut srv = HttpService::create(conf.clone(), cluster.clone());
        let listening = srv.start(listening).await?;
        shutdown_handle.add_service(srv);
        info!("HTTP API server listening on {}", listening);
    }

    // RPC API service.
    {
        let addr = conf.flight_api_address.parse::<std::net::SocketAddr>()?;
        let mut srv = RpcService::create(session_manager.clone());
        let listening = srv.start(addr).await?;
        shutdown_handle.add_service(srv);
        info!("RPC API server listening on {}", listening);
    }

    log::info!("Ready for connections.");
    shutdown_handle.wait_for_termination_request().await;
    log::info!("Shutdown server.");
    Ok(())
}
