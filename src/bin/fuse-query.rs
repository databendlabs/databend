// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::info;
use simplelog::{Config, LevelFilter, SimpleLogger};

use tokio::signal::unix::{signal, SignalKind};

use fuse_query::contexts::Options;
use fuse_query::servers::MySQLHandler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::try_create()?;

    match opts.get_log_level()?.to_lowercase().as_str() {
        "debug" => SimpleLogger::init(LevelFilter::Debug, Config::default())?,
        "info" => SimpleLogger::init(LevelFilter::Info, Config::default())?,
        _ => SimpleLogger::init(LevelFilter::Error, Config::default())?,
    }
    info!("{:?}", opts.clone());

    let mysql_handler = MySQLHandler::create(opts.clone());
    tokio::spawn(async move { mysql_handler.start() });

    info!("Fuse-Query Cloud Compute Starts...");
    info!(
        "Usage: mysql -h127.0.0.1 -P{:?}",
        opts.get_mysql_handler_port()?
    );
    signal(SignalKind::hangup())?.recv().await;
    Ok(())
}
