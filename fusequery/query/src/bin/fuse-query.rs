// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query::api::HttpService;
use fuse_query::api::RpcService;
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::servers::ClickHouseHandler;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::SessionManager;
use fuse_query::servers::Abortable;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First load configs from args.
    let mut conf = Config::load_from_args();

    // If config file is not empty: -c xx.toml
    // Reload configs from the file.
    if !conf.config_file.is_empty() {
        info!("Config reload from {:?}", conf.config_file);
        conf = Config::load_from_toml(conf.config_file.as_str())?;
    }

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str()),
    )
    .init();

    info!("{:?}", conf);
    info!(
        "FuseQuery v-{}",
        fuse_query::configs::config::FUSE_COMMIT_VERSION
    );

    let mut tasks = vec![];
    let mut runnable_servers = vec![];
    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::create(conf.mysql_handler_thread_num);

    // MySQL handler.
    {
        let handler = MySQLHandler::create(conf.clone(), cluster.clone(), session_manager.clone());
        runnable_servers.push(handler.start(&conf.mysql_handler_host, conf.mysql_handler_port).await?);

        info!(
            "MySQL handler listening on {}:{}, Usage: mysql -h{} -P{}",
            conf.mysql_handler_host,
            conf.mysql_handler_port,
            conf.mysql_handler_host,
            conf.mysql_handler_port
        );
    }

    // ClickHouse handler.
    {
        let handler =
            ClickHouseHandler::create(conf.clone(), cluster.clone(), session_manager.clone());

        tasks.push(tokio::spawn(async move {
            handler.start().await.expect("ClickHouse handler error");
        }));

        info!(
            "ClickHouse handler listening on {}:{}, Usage: clickhouse-client --host {} --port {}",
            conf.clickhouse_handler_host,
            conf.clickhouse_handler_port,
            conf.clickhouse_handler_host,
            conf.clickhouse_handler_port
        );
    }

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tasks.push(tokio::spawn(async move {
            srv.make_server().expect("Metrics service error");
        }));
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // HTTP API service.
    {
        let srv = HttpService::create(conf.clone(), cluster.clone());
        tasks.push(tokio::spawn(async move {
            srv.make_server().await.expect("HTTP service error");
        }));
        info!("HTTP API server listening on {}", conf.http_api_address);
    }

    // RPC API service.
    {
        let srv = RpcService::create(conf.clone(), cluster.clone(), session_manager.clone());
        info!("RPC API server listening on {}", conf.flight_api_address);
        tasks.push(tokio::spawn(async move {
            srv.make_server().await.expect("RPC service error");
        }));
    }

    // Process exit when error.
    if let Err(_e) = futures::future::try_join_all(tasks).await {
        std::process::exit(1);
    }

    for mut runnable_server in runnable_servers {
        runnable_server.wait_server_terminal().await;
    }

    Ok(())
}
