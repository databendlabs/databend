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

use std::env;
use std::ops::Deref;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::RuntimeTracker;
use common_exception::ErrorCode;
use common_macros::databend_main;
use common_meta_embedded::MetaEmbedded;
use common_meta_grpc::MIN_METASRV_SEMVER;
use common_metrics::init_default_metrics_recorder;
use common_tracing::set_panic_hook;
use common_tracing::QueryLogger;
use databend_query::api::DataExchangeManager;
use databend_query::api::HttpService;
use databend_query::api::RpcService;
use databend_query::catalogs::{CatalogManager, CatalogManagerHelper};
use databend_query::clusters::ClusterDiscovery;
use databend_query::interpreters::AsyncInsertManager;
use databend_query::metrics::MetricService;
use databend_query::servers::http::v1::HttpQueryManager;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::servers::MySQLHandler;
use databend_query::servers::Server;
use databend_query::servers::ShutdownHandle;
use databend_query::sessions::SessionManager;
use databend_query::Config;
use databend_query::QUERY_SEMVER;
use tracing::info;
use common_storage::StorageOperator;
use common_users::{RoleCacheManager, UserApiProvider};
use databend_query::storages::cache::CacheManager;

#[databend_main]
async fn main(_global_tracker: Arc<RuntimeTracker>) -> common_exception::Result<()> {
    let conf: Config = Config::load()?;

    if run_cmd(&conf) {
        return Ok(());
    }

    if conf.meta.address.is_empty() && conf.meta.endpoints.is_empty() {
        MetaEmbedded::init_global_meta_store(conf.meta.embedded_dir.clone()).await?;
    }
    let tenant = conf.query.tenant_id.clone();
    let cluster_id = conf.query.cluster_id.clone();
    let flight_addr = conf.query.flight_api_address.clone();

    let mut _sentry_guard = None;
    let bend_sentry_env = env::var("DATABEND_SENTRY_DSN").unwrap_or_else(|_| "".to_string());
    if !bend_sentry_env.is_empty() {
        // NOTE: `traces_sample_rate` is 0.0 by default, which disable sentry tracing.
        let traces_sample_rate = env::var("SENTRY_TRACES_SAMPLE_RATE")
            .ok()
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| panic!("`{}` was defined but could not be parsed", s))
            })
            .unwrap_or(0.0);

        _sentry_guard = Some(sentry::init((bend_sentry_env, sentry::ClientOptions {
            release: common_tracing::databend_semver!(),
            traces_sample_rate,
            ..Default::default()
        })));
        sentry::configure_scope(|scope| scope.set_tag("tenant", tenant));
        sentry::configure_scope(|scope| scope.set_tag("cluster_id", cluster_id));
        sentry::configure_scope(|scope| scope.set_tag("address", flight_addr));
    }

    init_default_metrics_recorder();
    set_panic_hook();

    global_init(&conf).await?;
    let mut shutdown_handle = ShutdownHandle::create()?;

    info!("Databend Query start with config: {:?}", conf);

    // MySQL handler.
    {
        let hostname = conf.query.mysql_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.mysql_handler_port);
        let mut handler = MySQLHandler::create()?;
        let listening = handler.start(listening.parse()?).await?;
        shutdown_handle.add_service(handler);

        info!(
            "Listening for MySQL compatibility protocol: {}, Usage: mysql -uroot -h{} -P{}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // ClickHouse HTTP handler.
    {
        let hostname = conf.query.clickhouse_http_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.clickhouse_http_handler_port);

        let mut srv = HttpHandler::create(HttpHandlerKind::Clickhouse, conf.clone())?;
        let listening = srv.start(listening.parse()?).await?;
        shutdown_handle.add_service(srv);

        let http_handler_usage = HttpHandlerKind::Clickhouse.usage(listening);
        info!(
            "Listening for ClickHouse compatibility http protocol: {}, Usage: {}",
            listening, http_handler_usage
        );
    }

    // Databend HTTP handler.
    {
        let hostname = conf.query.http_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.http_handler_port);

        let mut srv = HttpHandler::create(HttpHandlerKind::Query, conf.clone())?;
        let listening = srv.start(listening.parse()?).await?;
        shutdown_handle.add_service(srv);

        let http_handler_usage = HttpHandlerKind::Query.usage(listening);
        info!(
            "Listening for Databend HTTP API:  {}, Usage: {}",
            listening, http_handler_usage
        );
    }

    // Metric API service.
    {
        let address = conf.query.metric_api_address.clone();
        let mut srv = MetricService::create()?;
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        info!("Listening for Metric API: {}/metrics", listening);
    }

    // Admin HTTP API service.
    {
        let address = conf.query.admin_api_address.clone();
        let mut srv = HttpService::create(&conf)?;
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        info!("Listening for Admin HTTP API: {}", listening);
    }

    // RPC API service.
    {
        let address = conf.query.flight_api_address.clone();
        let mut srv = RpcService::create(conf.clone())?;
        let listening = srv.start(address.parse()?).await?;
        shutdown_handle.add_service(srv);
        info!("Listening for RPC API (interserver): {}", listening);
    }

    // Cluster register.
    {
        let cluster_discovery = ClusterDiscovery::instance();
        let register_to_metastore = cluster_discovery.register_to_metastore(&conf);
        register_to_metastore.await?;
        info!(
            "Databend query has been registered:{:?} to metasrv:[{:?}].",
            conf.query.cluster_id, conf.meta.address
        );
    }

    // Async Insert Queue
    {
        let async_insert_queue = AsyncInsertManager::instance();
        async_insert_queue.start().await;
        info!("Databend async insert has been enabled.")
    }

    // Print information to users.
    println!("Databend Query");
    println!();
    println!("Version: {}", *databend_query::DATABEND_COMMIT_VERSION);
    println!("Log:");
    println!("    File: {}", conf.log.file);
    println!("    Stderr: {}", conf.log.stderr);
    println!(
        "Meta: {}",
        if conf.meta.address.is_empty() && conf.meta.endpoints.is_empty() {
            format!("embedded at {}", conf.meta.embedded_dir)
        } else if !conf.meta.endpoints.is_empty() {
            format!("connected to endpoints {:#?}", conf.meta.endpoints)
        } else {
            format!("connected to address {}", conf.meta.address)
        }
    );
    println!("Storage: {}", conf.storage.params);
    println!();
    println!("Admin");
    println!("    listened at {}", conf.query.admin_api_address);
    println!("MySQL");
    println!(
        "    listened at {}:{}",
        conf.query.mysql_handler_host, conf.query.mysql_handler_port
    );
    println!(
        "    connect via: mysql -uroot -h{} -P{}",
        conf.query.mysql_handler_host, conf.query.mysql_handler_port
    );
    println!("Clickhouse(http)");
    println!(
        "    listened at {}:{}",
        conf.query.clickhouse_http_handler_host, conf.query.clickhouse_http_handler_port
    );
    println!(
        "    usage: {}",
        HttpHandlerKind::Clickhouse.usage(
            format!(
                "{}:{}",
                conf.query.clickhouse_http_handler_host, conf.query.clickhouse_http_handler_port
            )
                .parse()?
        )
    );
    println!("Databend HTTP");
    println!(
        "    listened at {}:{}",
        conf.query.http_handler_host, conf.query.http_handler_port
    );
    println!(
        "    usage: {}",
        HttpHandlerKind::Query.usage(
            format!(
                "{}:{}",
                conf.query.http_handler_host, conf.query.http_handler_port
            )
                .parse()?
        )
    );

    info!("Ready for connections.");
    shutdown_handle.wait_for_termination_request().await;
    info!("Shutdown server.");
    Ok(())
}

async fn global_init(conf: &Config) -> Result<(), ErrorCode> {
    // The order of initialization is very important
    let app_name_shuffle = format!("{}-{}", conf.query.tenant_id, conf.query.cluster_id);

    QueryLogger::init(app_name_shuffle, &conf.log)?;
    GlobalIORuntime::init(conf.query.num_cpus as usize)?;

    // Cluster discovery.
    ClusterDiscovery::init(conf.clone()).await?;

    StorageOperator::init(&conf.storage).await?;
    AsyncInsertManager::init(conf)?;
    CacheManager::init(&conf.query)?;
    CatalogManager::init(conf).await?;
    HttpQueryManager::init(conf).await?;
    DataExchangeManager::init(conf.clone())?;
    SessionManager::init(conf.clone())?;
    UserApiProvider::init(conf.meta.to_meta_grpc_client_conf()).await?;
    RoleCacheManager::init()
}

fn run_cmd(conf: &Config) -> bool {
    if conf.cmd.is_empty() {
        return false;
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", QUERY_SEMVER.deref());
            println!("min-compatible-metasrv-version: {}", MIN_METASRV_SEMVER);
        }
        _ => {
            eprintln!("Invalid cmd: {}", conf.cmd);
            eprintln!("Available cmds:");
            eprintln!("  --cmd ver");
            eprintln!("    Print version and the min compatible databend-meta version");
        }
    }

    true
}
