// Copyright 2021 Datafuse Labs
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

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_base::shutdown::ShutdownGroup;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_storage::init_operator;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::GlobalLogger;
use databend_common_tracing::set_panic_hook;
use databend_common_version::DATABEND_COMMIT_VERSION;
use databend_common_version::DATABEND_GIT_SEMVER;
use databend_common_version::DATABEND_SEMVER;
use databend_common_version::METASRV_COMMIT_VERSION;
use databend_common_version::VERGEN_GIT_SHA;
use databend_meta::api::GrpcServer;
use databend_meta::configs::MetaServiceConfig;
use databend_meta::meta_node::meta_handle::MetaHandle;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::meta_service::MetaNode;
use databend_meta::metrics::server_metrics;
use databend_meta::version::raft_client_requires;
use databend_meta::version::raft_server_provides;
use databend_meta_admin::HttpService;
use databend_meta_admin::HttpServiceConfig;
use databend_meta_cli_config::MetaConfig;
use databend_meta_raft_store::config::RaftConfig;
use databend_meta_raft_store::ondisk::DATA_VERSION;
use databend_meta_raft_store::ondisk::OnDisk;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_sled_store::openraft::MessageSummary;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::MetaAPIError;
use databend_meta_types::node::Node;
use databend_meta_ver::MIN_QUERY_VER_FOR_METASRV;
use log::info;
use log::warn;
use tokio::time::Instant;
use tokio::time::sleep;

pub async fn entry<RT: RuntimeApi>(conf: MetaConfig) -> anyhow::Result<()> {
    if run_cmd(&conf) {
        return Ok(());
    }
    let binary_version = DATABEND_COMMIT_VERSION.clone();

    set_panic_hook(binary_version);

    init_logging_system(&conf.service.raft_config, &conf.log).await?;

    info!("Databend Meta version: {}", METASRV_COMMIT_VERSION.as_str());
    info!(
        "Databend Meta start with config: {:?}",
        serde_json::to_string_pretty(&conf).unwrap()
    );

    conf.service.raft_config.check()?;

    // Leave cluster and quit if `--leave-via` and `--leave-id` is specified.
    // Leaving does not access the local store thus it can be done before the store is initialized.
    let has_left = MetaNode::<RT>::leave_cluster(&conf.service.raft_config).await?;
    if has_left {
        info!(
            "node {:?} has left cluster",
            conf.service.raft_config.leave_id
        );
        return Ok(());
    }

    let single_or_join = if conf.service.raft_config.single {
        "single".to_string()
    } else {
        format!("join {:?}", conf.service.raft_config.join)
    };

    let grpc_advertise = if let Some(a) = conf.service.grpc.advertise_address() {
        a
    } else {
        "-".to_string()
    };

    let raft_listen = conf.service.raft_config.raft_api_listen_host_string();
    let raft_advertise = conf.service.raft_config.raft_api_advertise_host_string();

    // Print information to users.
    println!("Databend Metasrv");
    println!();
    println!("Version: {}", METASRV_COMMIT_VERSION.as_str());
    println!("Working DataVersion: {:?}", DATA_VERSION);
    println!();

    println!("Raft Feature set:");
    println!("    Server Provide: {{ {} }}", raft_server_provides());
    println!("    Client Require: {{ {} }}", raft_client_requires());
    println!();

    info!(
        "Initialize on-disk data at {}",
        conf.service.raft_config.raft_dir
    );

    let mut on_disk = OnDisk::open(&conf.service.raft_config).await?;
    on_disk.log_stderr(true);

    let h = &on_disk.header;

    #[rustfmt::skip]
    {
        println!("Disk  Data: {:?}; Upgrading: {:?}", h.version, h.upgrading);
        println!("      Dir: {}", conf.service.raft_config.raft_dir);
        println!();
        println!("Log   File:   {}", conf.log.file);
        println!("      Stderr: {}", conf.log.stderr);
        if conf.log.otlp.on {
            println!("    OpenTelemetry: {}", conf.log.otlp);
        }
        if conf.log.tracing.on {
            println!("    Tracing: {}", conf.log.tracing);
        }
        if conf.log.history.on {
            println!("    Storage: {}", conf.log.history.on);
        }
        let r = &conf.service.raft_config;
        println!("Raft  Id: {}; Cluster: {}", r.id, r.cluster_name);
        println!("      Dir: {}", r.raft_dir);
        println!("      Status: {}", single_or_join);
        println!();
        let grpc_listen = conf.service.grpc.api_address().unwrap_or_else(|| "-".to_string());
        let max_message = r.raft_grpc_max_message_size();
        println!("HTTP API listen at: {}", conf.admin.api_address);
        println!("gRPC API listen at: {} advertise: {}", grpc_listen, grpc_advertise);
        println!("Raft API listen at: {} advertise: {}", raft_listen, raft_advertise);
        println!("Raft API max message: {}MB", max_message / (1024 * 1024));
        println!();
    }

    on_disk.upgrade::<RT>().await?;

    info!(
        "Starting MetaNode, is-single: {} with config: {:?}",
        conf.service.raft_config.single, conf
    );

    let runtime = RT::new(Some(32), Some("meta-io-rt".to_string())).map_err(|e| {
        databend_meta_types::MetaStartupError::MetaServiceError(format!(
            "Cannot create meta IO runtime: {}",
            e
        ))
    })?;
    let meta_handle =
        MetaWorker::create_meta_worker(conf.service.clone(), Arc::new(runtime)).await?;
    let meta_handle = Arc::new(meta_handle);

    let mut stop_handler = ShutdownGroup::<AnyError>::new();
    let stop_tx = ShutdownGroup::<AnyError>::install_termination_handle();

    // HTTP API service.
    {
        server_metrics::set_version(DATABEND_GIT_SEMVER.to_string(), VERGEN_GIT_SHA.to_string());
        let http_cfg = HttpServiceConfig {
            admin: conf.admin.clone(),
            config_display: format!("{:?}", conf),
        };
        let mut srv =
            HttpService::create(http_cfg, DATABEND_SEMVER.to_string(), meta_handle.clone());
        info!("HTTP API server listening on {}", conf.admin.api_address);
        srv.do_start().await.expect("Failed to start http server");
        stop_handler.push(srv);
    }

    // gRPC API service.
    {
        let mut srv = GrpcServer::<RT>::create(&conf.service, meta_handle.clone());
        info!(
            "Databend meta server listening on {:?}",
            conf.service.grpc.api_address()
        );
        srv.do_start().await.expect("Databend meta service error");
        stop_handler.push(Box::new(srv));
    }

    // Join a raft cluster only after all service started.
    let service_config = conf.service.clone();
    let join_res = meta_handle
        .request(move |mn| {
            let fu = async move { mn.join_cluster(&service_config).await };
            Box::pin(fu)
        })
        .await??;

    info!("Join result: {:?}", join_res);

    register_node::<RT>(&meta_handle, &conf).await?;

    println!("Databend Metasrv started");

    stop_handler.wait_to_terminate(stop_tx).await;
    info!("Databend-meta is done shutting down");

    println!("Databend Metasrv shutdown");

    Ok(())
}

async fn do_register<RT: RuntimeApi>(
    meta_handle: &Arc<MetaHandle<RT>>,
    config: &MetaServiceConfig,
) -> Result<(), MetaAPIError> {
    let node_id = meta_handle.id;
    let raft_endpoint = config.raft_config.raft_api_advertise_host_endpoint();
    let node = Node::new(node_id, raft_endpoint)
        .with_grpc_advertise_address(config.grpc.advertise_address());

    println!("Register this node: {{{}}}", node);
    println!();

    let ent = LogEntry::new(Cmd::AddNode {
        node_id,
        node,
        overriding: true,
    });
    info!("Raft log entry for updating node: {:?}", ent);

    meta_handle.handle_write(ent).await.unwrap()?;
    info!("Done register");
    Ok(())
}

/// The meta service GRPC API address can be changed by administrator in the config file.
///
/// Thus every time a meta server starts up, re-register the node info to broadcast its latest grpc address
#[fastrace::trace]
async fn register_node<RT: RuntimeApi>(
    meta_handle: &Arc<MetaHandle<RT>>,
    conf: &MetaConfig,
) -> Result<(), anyhow::Error> {
    info!(
        "Register node to update raft_api_advertise_host_endpoint and grpc_api_advertise_address"
    );

    let mut last_err = None;
    let mut sleep_time = Duration::from_millis(500);

    let timeout = Duration::from_millis(conf.service.raft_config.wait_leader_timeout);
    let timeout_at = Instant::now() + timeout;

    info!(
        "Wait {:?} for active leader to register node, raft election timeouts: {:?}",
        timeout,
        conf.service.raft_config.election_timeout()
    );
    println!("Wait for {:?} for active leader...", timeout);

    loop {
        if Instant::now() > timeout_at {
            break;
        }

        let wait = meta_handle
            .handle_raft_metrics_wait(Some(timeout_at - Instant::now()))
            .await?;

        let metrics = wait
            .metrics(|x| x.current_leader.is_some(), "receive an active leader")
            .await?;

        info!("Current raft node metrics: {:?}", metrics);

        let leader_id = metrics.current_leader.unwrap();

        println!("Leader Id: {}", leader_id);
        println!(
            "    Metrics: id={}, {:?}, term={}, last_log={:?}, last_applied={}, membership={{log_id:{}, {}}}",
            metrics.id,
            metrics.state,
            metrics.current_term,
            metrics.last_log_index,
            metrics.last_applied.summary(),
            metrics.membership_config.log_id().summary(),
            metrics.membership_config.membership().summary(),
        );
        println!();

        if meta_handle.handle_get_node(leader_id).await?.is_none() {
            warn!("Leader node is not replicated to local store, wait and try again");
            sleep(Duration::from_millis(500)).await;
            continue;
        }

        info!("Registering node with grpc-advertise-addr...");

        let res = do_register::<RT>(meta_handle, &conf.service).await;
        info!("Register-node result: {:?}", res);

        match res {
            Ok(_) => {
                info!("Register-node Ok");
                println!("    Register-node: Ok");
                println!();

                return Ok(());
            }
            Err(e) => {
                warn!(
                    "Error while registering node: {}, sleep {:?} and retry",
                    e, sleep_time
                );
                println!("    Error: {}", e);
                println!("    Sleep {:?} and retry", sleep_time);
                println!();

                last_err = Some(e);
                sleep(sleep_time).await;
                sleep_time = std::cmp::min(sleep_time * 2, Duration::from_secs(5));
            }
        }
    }

    if let Some(e) = last_err {
        return Err(e.into());
    }

    Err(anyhow::anyhow!("timeout; no error received"))
}

fn run_cmd(conf: &MetaConfig) -> bool {
    if conf.cmd.is_empty() {
        return false;
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", DATABEND_SEMVER.deref());
            println!(
                "min-compatible-client-version: {}",
                *MIN_QUERY_VER_FOR_METASRV
            );
            println!("data-version: {:?}", DATA_VERSION);
        }
        "show-config" => {
            println!(
                "config:\n{}",
                pretty(&conf).unwrap_or_else(|e| format!("error format config: {}", e))
            );
        }
        _ => {
            eprintln!("Invalid cmd: {}", conf.cmd);
            eprintln!("Available cmds:");
            eprintln!("  --cmd ver");
            eprintln!("    Print version and min compatible meta-client version");
            eprintln!("  --cmd show-config");
            eprintln!("    Print effective config");
        }
    }

    true
}

async fn init_logging_system(
    raft_config: &RaftConfig,
    log_config: &LogConfig,
) -> anyhow::Result<()> {
    let app_name = format!(
        "databend-meta-{}@{}",
        raft_config.id, raft_config.cluster_name
    );

    let log_labels = [
        ("cluster_name", raft_config.cluster_name.as_str()),
        ("node_id", &raft_config.id.to_string()),
        ("cluster_id", raft_config.cluster_name.as_str()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    GlobalInstance::init_production();
    GlobalLogger::init(&app_name, log_config, log_labels);

    if log_config.history.on {
        GlobalIORuntime::init(num_cpus::get())?;

        let params = log_config.history.storage_params.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Log history is enabled but storage_params is not set")
        })?;

        let remote_log_op = init_operator(params).map_err(|e| anyhow::anyhow!(e))?;
        GlobalLogger::instance().set_operator(remote_log_op).await;
    }

    Ok(())
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: serde::Serialize {
    serde_json::to_string_pretty(v)
}
