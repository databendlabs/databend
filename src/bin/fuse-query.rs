// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::info;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

use tokio::signal::unix::{signal, SignalKind};

use fuse_query::configs::Config;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::SessionManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::create();
    let session_mgr = SessionManager::create();

    match cfg.log_level.to_lowercase().as_str() {
        "debug" => SimpleLogger::init(LevelFilter::Debug, LogConfig::default())?,
        "info" => SimpleLogger::init(LevelFilter::Info, LogConfig::default())?,
        _ => SimpleLogger::init(LevelFilter::Error, LogConfig::default())?,
    }
    info!("{:?}", cfg.clone());

    let mysql_handler = MySQLHandler::create(cfg.clone(), session_mgr.clone());
    tokio::spawn(async move { mysql_handler.start() });

    info!("FuseQuery v-{} Cloud Compute Starts...", cfg.version);
    info!(
        "Usage: mysql -h{} -P{:?}",
        cfg.mysql_listen_host, cfg.mysql_handler_port
    );
    signal(SignalKind::hangup())?.recv().await;
    Ok(())
}
