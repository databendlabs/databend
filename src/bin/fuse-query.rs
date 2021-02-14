// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::info;
use metrics_exporter_prometheus::PrometheusBuilder;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

use tokio::signal::unix::{signal, SignalKind};

use fuse_query::configs::Config;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::SessionManager;

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

    // Run a Prometheus scrape endpoint on 127.0.0.1:9000.
    let _ = PrometheusBuilder::new()
        .listen_address(
            cfg.prometheus_exporter_address
                .parse::<std::net::SocketAddr>()
                .expect("Failed to parse prometheus exporter address"),
        )
        .install()
        .expect("Failed to install prometheus exporter");

    info!(
        "Listening for Prometheus exporter {}",
        cfg.prometheus_exporter_address
    );

    // MySQL handler.
    let session_mgr = SessionManager::create();
    let mysql_handler = MySQLHandler::create(cfg.clone(), session_mgr.clone());
    tokio::spawn(async move { mysql_handler.start() });

    info!(
        "Listening for MySQL handler {}:{}, Usage: mysql -h{} -P{}",
        cfg.mysql_listen_host,
        cfg.mysql_handler_port,
        cfg.mysql_listen_host,
        cfg.mysql_handler_port
    );

    // Wait.
    signal(SignalKind::hangup())?.recv().await;
    Ok(())
}
