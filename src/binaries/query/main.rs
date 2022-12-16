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

use common_base::runtime::Runtime;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_base::set_alloc_error_hook;
use common_config::Config;
use common_config::DATABEND_COMMIT_VERSION;
use common_config::QUERY_SEMVER;
use common_exception::Result;
use common_meta_client::MIN_METASRV_SEMVER;
use common_meta_embedded::MetaEmbedded;
use common_metrics::init_default_metrics_recorder;
use common_tracing::set_panic_hook;
use databend_query::api::HttpService;
use databend_query::api::RpcService;
use databend_query::clusters::ClusterDiscovery;
use databend_query::metrics::MetricService;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::servers::MySQLHandler;
use databend_query::servers::Server;
use databend_query::servers::ShutdownHandle;
use databend_query::GlobalServices;
use tracing::info;

fn main() {
    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("Databend Query start failure, cause: {:?}", cause);
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(main_entrypoint()) {
                eprintln!("Databend Query start failure, cause: {:?}", cause);
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

async fn main_entrypoint() -> Result<()> {
    let conf: Config = Config::load()?;

    if run_cmd(&conf) {
        return Ok(());
    }

    init_default_metrics_recorder();
    set_panic_hook();
    set_alloc_error_hook();

    if conf.meta.is_embedded_meta()? {
        MetaEmbedded::init_global_meta_store(conf.meta.embedded_dir.clone()).await?;
    }
    // Make sure global services have been inited.
    GlobalServices::init(conf.clone()).await?;

    if conf.query.max_memory_limit_enabled {
        let size = conf.query.max_server_memory_usage as i64;
        info!("Set memory limit: {}", size);
        GLOBAL_MEM_STAT.set_limit(size);
    }

    let tenant = conf.query.tenant_id.clone();
    let cluster_id = conf.query.cluster_id.clone();
    let flight_addr = conf.query.flight_api_address.clone();

    let mut _sentry_guard = None;
    let bend_sentry_env = env::var("DATABEND_SENTRY_DSN").unwrap_or_else(|_| "".to_string());
    if !bend_sentry_env.is_empty() {
        // NOTE: `traces_sample_rate` is 0.0 by default, which disable sentry tracing.
        let traces_sample_rate = env::var("SENTRY_TRACES_SAMPLE_RATE").ok().map_or(0.0, |s| {
            s.parse()
                .unwrap_or_else(|_| panic!("`{}` was defined but could not be parsed", s))
        });

        _sentry_guard = Some(sentry::init((bend_sentry_env, sentry::ClientOptions {
            release: common_tracing::databend_semver!(),
            traces_sample_rate,
            ..Default::default()
        })));
        sentry::configure_scope(|scope| scope.set_tag("tenant", tenant));
        sentry::configure_scope(|scope| scope.set_tag("cluster_id", cluster_id));
        sentry::configure_scope(|scope| scope.set_tag("address", flight_addr));
    }

    #[cfg(not(target_os = "macos"))]
    check_max_open_files();

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

        let mut srv = HttpHandler::create(HttpHandlerKind::Clickhouse)?;
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

        let mut srv = HttpHandler::create(HttpHandlerKind::Query)?;
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
        ClusterDiscovery::instance()
            .register_to_metastore(&conf)
            .await?;
        info!(
            "Databend query has been registered:{:?} to metasrv:{:?}.",
            conf.query.cluster_id, conf.meta.endpoints
        );
    }

    // Print information to users.
    println!("Databend Query");
    println!();
    println!("Version: {}", *DATABEND_COMMIT_VERSION);
    println!();
    println!("Logging:");
    println!("    file: {}", conf.log.file);
    println!("    stderr: {}", conf.log.stderr);
    println!(
        "Meta: {}",
        if conf.meta.is_embedded_meta()? {
            format!("embedded at {}", conf.meta.embedded_dir)
        } else {
            format!("connected to endpoints {:#?}", conf.meta.endpoints)
        }
    );
    println!(
        "Memory: {}",
        if conf.query.max_memory_limit_enabled {
            format!(
                "Memory: server memory limit to {} (bytes)",
                conf.query.max_server_memory_usage
            )
        } else {
            "unlimited".to_string()
        }
    );
    println!("Cluster: {}", {
        let cluster = ClusterDiscovery::instance().discover(&conf).await?;
        let nodes = cluster.nodes.len();
        if nodes > 1 {
            format!("[{}] nodes", nodes)
        } else {
            "standalone".to_string()
        }
    });
    println!("Storage: {}", conf.storage.params);
    println!("Cache: {}", conf.storage.cache.params);
    println!(
        "Builtin users: {}",
        conf.query
            .idm
            .users
            .keys()
            .map(|name| name.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
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

fn run_cmd(conf: &Config) -> bool {
    if conf.cmd.is_empty() {
        return false;
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", *QUERY_SEMVER);
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

#[cfg(not(target_os = "macos"))]
fn check_max_open_files() {
    let limits = match limits_rs::get_own_limits() {
        Ok(limits) => limits,
        Err(err) => {
            tracing::warn!("get system limit of databend-query failed: {:?}", err);
            return;
        }
    };
    let max_open_files_limit = limits.max_open_files.soft;
    if let Some(max_open_files) = max_open_files_limit {
        if max_open_files < 65535 {
            tracing::warn!(
                "The open file limit is too low for the databend-query. Please consider increase it by running `ulimit -n 65535`"
            );
        }
    }
}
