// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;

use common_management::cluster::ClusterManager;
use common_runtime::tokio;
use common_tracing::init_tracing_with_file;
<<<<<<< HEAD:query/src/bin/datafuse-query.rs
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
=======
use fuse_query::api::HttpService;
use fuse_query::api::RpcService;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::servers::ClickHouseHandler;
use fuse_query::servers::MySQLHandler;
use fuse_query::servers::ShutdownHandle;
use fuse_query::sessions::SessionManager;
>>>>>>> cluster_manager:fusequery/query/src/bin/fuse-query.rs
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

    // Prefer to use env variable in cloud native deployment
    // Override configs based on env variables
    conf = Config::load_from_env(&conf)?;

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log.log_level.to_lowercase().as_str()),
    )
    .init();
    let _guards = init_tracing_with_file(
        "datafuse-query",
        conf.log.log_dir.as_str(),
        conf.log.log_level.as_str(),
    );

    info!("{:?}", conf);
    info!(
        "DatafuseQuery v-{}",
        *datafuse_query::configs::config::FUSE_COMMIT_VERSION,
    );

    let session_manager = SessionManager::from_conf(conf.clone())?;
    let mut shutdown_handle = ShutdownHandle::create(session_manager.clone());

    // MySQL handler.
    {
<<<<<<< HEAD:query/src/bin/datafuse-query.rs
        let listening = format!(
            "{}:{}",
            conf.query.mysql_handler_host.clone(),
            conf.query.mysql_handler_port
        );
=======
        let hostname = conf.mysql_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.mysql_handler_port);
>>>>>>> cluster_manager:fusequery/query/src/bin/fuse-query.rs
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
        let hostname = conf.query.clickhouse_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.clickhouse_handler_port);
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
        let listening = conf
            .query
            .metric_api_address
            .parse::<std::net::SocketAddr>()?;
        let mut srv = MetricService::create();
        let listening = srv.start(listening).await?;
        shutdown_handle.add_service(srv);
        info!("Metric API server listening on {}", listening);
    }

    // HTTP API service.
    {
<<<<<<< HEAD:query/src/bin/datafuse-query.rs
        let listening = conf
            .query
            .http_api_address
            .parse::<std::net::SocketAddr>()?;
        let mut srv = HttpService::create(conf.clone(), cluster.clone());
=======
        let listening = conf.http_api_address.parse::<std::net::SocketAddr>()?;
        let mut srv = HttpService::create(session_manager.clone());
>>>>>>> cluster_manager:fusequery/query/src/bin/fuse-query.rs
        let listening = srv.start(listening).await?;
        shutdown_handle.add_service(srv);
        info!("HTTP API server listening on {}", listening);
    }

    // RPC API service.
    {
        let addr = conf
            .query
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;
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
