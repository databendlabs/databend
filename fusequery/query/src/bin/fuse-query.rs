// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Sub;
use std::time::Duration;

use common_exception::ErrorCode;
use common_runtime::tokio;
use common_tracing::init_tracing_with_file;
use fuse_query::api::HttpService;
use fuse_query::api::RpcService;
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::servers::AbortableServer;
use fuse_query::servers::AbortableService;
use fuse_query::servers::ClickHouseHandler;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::SessionManager;
use log::info;
use num::ToPrimitive;

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
    let _guards =
        init_tracing_with_file("fuse-query", conf.log_dir.as_str(), conf.log_level.as_str());

    info!("{:?}", conf);
    info!(
        "FuseQuery v-{}, Allocator: {}",
        *fuse_query::configs::config::FUSE_COMMIT_VERSION,
        malloc
    );

    let mut services: Vec<AbortableServer> = vec![];
    let cluster = Cluster::create_global(conf.clone())?;
    let session_manager = SessionManager::from_conf(conf.clone(), cluster.clone())?;

    // MySQL handler.
    {
        let handler = MySQLHandler::create(session_manager.clone());
        let listening = handler
            .start((conf.mysql_handler_host.clone(), conf.mysql_handler_port))
            .await?;
        services.push(handler);

        info!(
            "MySQL handler listening on {}, Usage: mysql -h{} -P{}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // ClickHouse handler.
    {
        let addr = (conf.clickhouse_handler_host.clone(), conf.clickhouse_handler_port);
        let srv = ClickHouseHandler::create(session_manager.clone());
        let listening = srv.start(addr).await?;
        services.push(srv);

        info!(
            "ClickHouse handler listening on {}, Usage: clickhouse-client --host {} --port {}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // Metric API service.
    {
        let addr = conf.metric_api_address.parse::<std::net::SocketAddr>()?;
        let srv = MetricService::create();
        let addr = srv.start((addr.ip().to_string(), addr.port())).await?;
        services.push(srv);
        info!("Metric API server listening on {}", addr);
    }

    // HTTP API service.
    {
        let addr = conf.http_api_address.parse::<std::net::SocketAddr>()?;
        let srv = HttpService::create(conf.clone(), cluster.clone());
        let addr = srv.start((addr.ip().to_string(), addr.port())).await?;
        services.push(srv);
        info!("HTTP API server listening on {}", addr);
    }

    // RPC API service.
    {
        let addr = conf.flight_api_address.parse::<std::net::SocketAddr>()?;
        let srv = RpcService::create(session_manager.clone());
        let addr = srv.start((addr.ip().to_string(), addr.port())).await?;
        services.push(srv);
        info!("RPC API server listening on {}", addr);
    }

    // Ctrl + C 100 times in five seconds
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    ctrlc::set_handler(move || {
        if let Err(error) = tx.blocking_send(()) {
            log::error!("Could not send signal on channel {}", error);
            std::process::exit(1);
        }
    })
    .expect("Error setting Ctrl-C handler");

    let cloned_services = services.clone();
    tokio::spawn(async move {
        let cloned_services = cloned_services;
        rx.recv().await;
        log::info!(
            "FuseQuery is shutting down. \
            Try to wait 5 seconds for the currently executing query. \
            You can press Ctrl + C again to force shutdown."
        );

        if let Err(error) = abort_services(&cloned_services, false) {
            log::info!("Cannot abort FuseQuery: {:?}", error);
            std::process::exit(error.code() as i32);
        }

        match futures::future::select(
            Box::pin(rx.recv()),
            Box::pin(wait_services_terminal(
                &cloned_services,
                Some(Duration::from_secs(5)),
            )),
        )
        .await
        {
            futures::future::Either::Left(_) | futures::future::Either::Right((Err(_), _)) => {
                // Two consecutive Ctrl + C or 5 seconds has not been closed.
                log::info!("Force Shutting down FuseQuery.");
                if let Err(error) = abort_services(&cloned_services, true) {
                    log::info!("Cannot force abort FuseQuery: {:?}", error);
                    std::process::exit(error.code() as i32);
                }

                if let Err(error) = wait_services_terminal(&cloned_services, None).await {
                    log::info!("Cannot force abort FuseQuery: {:?}", error);
                    std::process::exit(error.code() as i32);
                }
            }
            _ => { /* do nothing */ }
        };
    });

    wait_services_terminal(&services, None).await.expect("");
    log::info!("Shutdown server.");
    Ok(())
}

fn abort_services(services: &[AbortableServer], force: bool) -> common_exception::Result<()> {
    for service in services {
        service.abort(force)?;
    }

    Ok(())
}

async fn wait_services_terminal(
    services: &[AbortableServer],
    duration: Option<Duration>,
) -> common_exception::Result<()> {
    match duration {
        None => {
            for service in services {
                service.wait_terminal(None).await?;
            }
        }
        Some(duration) => {
            let mut duration = duration;
            for service in services {
                if duration.is_zero() {
                    return Err(ErrorCode::Timeout(format!(
                        "Service did not shutdown in {:?}",
                        duration
                    )));
                }

                let elapsed = service.wait_terminal(Some(duration)).await?;
                duration = duration.sub(std::cmp::min(elapsed, duration));
            }
        }
    };
    Ok(())
}
