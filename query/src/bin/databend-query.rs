// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_base::RuntimeTracker;
use common_macros::databend_main;
use common_meta_embedded::MetaEmbedded;
use common_metrics::init_default_metrics_recorder;
use common_tracing::init_global_tracing;
use common_tracing::set_panic_hook;
use common_tracing::tracing;
use databend_query::api::HttpService;
use databend_query::api::RpcService;
use databend_query::configs::Config;
use databend_query::metrics::MetricService;
use databend_query::servers::ClickHouseHandler;
use databend_query::servers::HttpHandler;
use databend_query::servers::MySQLHandler;
use databend_query::servers::Server;
use databend_query::servers::ShutdownHandle;
use databend_query::sessions::SessionManager;

#[databend_main]
async fn main(_global_tracker: Arc<RuntimeTracker>) -> common_exception::Result<()> {
    // First load configs from args.
    let mut conf = Config::load_from_args();

    // If config file is not empty: -c xx.toml
    // Reload configs from the file.
    if !conf.config_file.is_empty() {
        tracing::info!("Config reload from {:?}", conf.config_file);
        conf = Config::load_from_toml(conf.config_file.as_str())?;
    }

    // Prefer to use env variable in cloud native deployment
    // Override configs based on env variables
    conf = Config::load_from_env(&conf)?;

    if conf.meta.meta_address.is_empty() {
        MetaEmbedded::init_global_meta_store(conf.meta.meta_embedded_dir.clone()).await?;
    }

    let app_name = format!(
        "databend-query-{}@{}:{}",
        conf.query.cluster_id, conf.query.mysql_handler_host, conf.query.mysql_handler_port
    );
    //let _guards = init_tracing_with_file(
    let _guards = init_global_tracing(
        app_name.as_str(),
        conf.log.log_dir.as_str(),
        conf.log.log_level.as_str(),
    );

    init_default_metrics_recorder();

    set_panic_hook();
    tracing::info!("{:?}", conf);
    tracing::info!(
        "DatabendQuery v-{}",
        *databend_query::configs::DATABEND_COMMIT_VERSION,
    );

    let session_manager = SessionManager::from_conf(conf.clone()).await?;
    let mut shutdown_handle = ShutdownHandle::create(session_manager.clone());

    // MySQL handler.
    {
        let hostname = conf.query.mysql_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.mysql_handler_port);
        let mut handler = MySQLHandler::create(session_manager.clone());
        let listening = handler.start(listening.parse()?).await?;
        shutdown_handle.add_service(handler);

        tracing::info!(
            "MySQL handler listening on {}, Usage: mysql -uroot -h{} -P{}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // ClickHouse handler.
    {
        let hostname = conf.query.clickhouse_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.clickhouse_handler_port);

        let mut srv = ClickHouseHandler::create(session_manager.clone());
        let listening = srv.start(listening.parse()?).await?;
        shutdown_handle.add_service(srv);

        tracing::info!(
            "ClickHouse handler listening on {}, Usage: clickhouse-client --host {} --port {}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }
    // HTTP handler.
    {
        let hostname = conf.query.http_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.http_handler_port);

        let mut srv = HttpHandler::create(session_manager.clone());
        let listening = srv.start(listening.parse()?).await?;
        shutdown_handle.add_service(srv);

        let http_handler_usage = HttpHandler::usage(listening);
        tracing::info!(
            "Http handler listening on {} {}",
            listening,
            http_handler_usage
        );
    }

    // Metric API service.
    {
        let address = conf.query.metric_api_address.clone();
        let mut srv = MetricService::create(session_manager.clone());
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        tracing::info!("Metric API server listening on {}/metrics", listening);
    }

    // HTTP API service.
    {
        let address = conf.query.http_api_address.clone();
        let mut srv = HttpService::create(session_manager.clone());
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        tracing::info!("HTTP API server listening on {}", listening);
    }

    // RPC API service.
    {
        let address = conf.query.flight_api_address.clone();
        let mut srv = RpcService::create(session_manager.clone());
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        tracing::info!("RPC API server listening on {}", listening);
    }

    // Cluster register.
    {
        let cluster_discovery = session_manager.get_cluster_discovery();
        let register_to_metastore = cluster_discovery.register_to_metastore(&conf);
        register_to_metastore.await?;
        tracing::info!(
            "Databend query has been registered:{:?} to metasrv:[{:?}].",
            conf.query.cluster_id,
            conf.meta.meta_address
        );
    }

    tracing::info!("Ready for connections.");
    shutdown_handle.wait_for_termination_request().await;
    tracing::info!("Shutdown server.");
    Ok(())
}
