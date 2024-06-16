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

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::StopHandle;
use databend_common_base::base::Stoppable;
use databend_common_grpc::RpcClientConf;
use databend_common_meta_raft_store::ondisk::OnDisk;
use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_sled_store::get_sled_db;
use databend_common_meta_sled_store::init_sled_db;
use databend_common_meta_sled_store::openraft::MessageSummary;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::Node;
use databend_common_tracing::init_logging;
use databend_common_tracing::set_panic_hook;
use databend_meta::api::GrpcServer;
use databend_meta::api::HttpService;
use databend_meta::configs::Config;
use databend_meta::meta_service::MetaNode;
use databend_meta::metrics::server_metrics;
use databend_meta::version::raft_client_requires;
use databend_meta::version::raft_server_provides;
use databend_meta::version::METASRV_COMMIT_VERSION;
use databend_meta::version::METASRV_GIT_SEMVER;
use databend_meta::version::METASRV_GIT_SHA;
use databend_meta::version::METASRV_SEMVER;
use databend_meta::version::MIN_METACLI_SEMVER;
use log::info;
use log::warn;
use tokio::time::sleep;
use tokio::time::Instant;

use crate::kvapi::KvApiCommand;

const CMD_KVAPI_PREFIX: &str = "kvapi::";

pub async fn entry(conf: Config) -> anyhow::Result<()> {
    if run_cmd(&conf).await {
        return Ok(());
    }

    set_panic_hook();

    // app name format: node_id@cluster_id
    let app_name_shuffle = format!(
        "databend-meta-{}@{}",
        conf.raft_config.id, conf.raft_config.cluster_name
    );
    let mut log_labels = BTreeMap::new();
    log_labels.insert(
        "cluster_name".to_string(),
        conf.raft_config.cluster_name.clone(),
    );
    let _guards = init_logging(&app_name_shuffle, &conf.log, log_labels);

    info!("Databend Meta version: {}", METASRV_COMMIT_VERSION.as_str());
    info!(
        "Databend Meta start with config: {:?}",
        serde_json::to_string_pretty(&conf).unwrap()
    );

    conf.raft_config.check()?;

    // Leave cluster and quit if `--leave-via` and `--leave-id` is specified.
    // Leaving does not access the local store thus it can be done before the store is initialized.
    let has_left = MetaNode::leave_cluster(&conf.raft_config).await?;
    if has_left {
        info!("node {:?} has left cluster", conf.raft_config.leave_id);
        return Ok(());
    }

    init_sled_db(
        conf.raft_config.raft_dir.clone(),
        conf.raft_config.sled_cache_size(),
    );

    let single_or_join = if conf.raft_config.single {
        "single".to_string()
    } else {
        format!("join {:#?}", conf.raft_config.join)
    };

    let grpc_advertise = if let Some(a) = conf.grpc_api_advertise_address() {
        a
    } else {
        "-".to_string()
    };

    let raft_listen = conf.raft_config.raft_api_listen_host_string();
    let raft_advertise = conf.raft_config.raft_api_advertise_host_string();

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

    info!("Initialize on-disk data at {}", conf.raft_config.raft_dir);

    let db = get_sled_db();
    let mut on_disk = OnDisk::open(&db, &conf.raft_config).await?;
    on_disk.log_stderr(true);

    println!("On Disk Data:");
    println!("    Dir: {}", conf.raft_config.raft_dir);
    println!("    DataVersion: {:?}", on_disk.header.version);
    println!("    In-Upgrading: {:?}", on_disk.header.upgrading);
    println!();
    println!("Log:");
    println!("    File: {}", conf.log.file);
    println!("    Stderr: {}", conf.log.stderr);
    if conf.log.otlp.on {
        println!("    OpenTelemetry: {}", conf.log.otlp);
    }
    if conf.log.tracing.on {
        println!("    Tracing: {}", conf.log.tracing);
    }
    println!("Id: {}", conf.raft_config.id);
    println!("Raft Cluster Name: {}", conf.raft_config.cluster_name);
    println!("Raft Dir: {}", conf.raft_config.raft_dir);
    println!("Raft Status: {}", single_or_join);
    println!();
    println!("HTTP API");
    println!("   listening at {}", conf.admin_api_address);
    println!("gRPC API");
    println!("   listening at {}", conf.grpc_api_address);
    println!("   advertise:  {}", grpc_advertise);
    println!("Raft API");
    println!("   listening at {}", raft_listen,);
    println!("   advertise:  {}", raft_advertise,);
    println!();

    on_disk.upgrade().await?;

    info!(
        "Starting MetaNode, is-single: {} with config: {:?}",
        conf.raft_config.single, conf
    );

    let meta_node = MetaNode::start(&conf).await?;

    let mut stop_handler = StopHandle::<AnyError>::create();
    let stop_tx = StopHandle::<AnyError>::install_termination_handle();

    // HTTP API service.
    {
        server_metrics::set_version(METASRV_GIT_SEMVER.to_string(), METASRV_GIT_SHA.to_string());
        let mut srv = HttpService::create(conf.clone(), meta_node.clone());
        info!("HTTP API server listening on {}", conf.admin_api_address);
        srv.start().await.expect("Failed to start http server");
        stop_handler.push(srv);
    }

    // gRPC API service.
    {
        let mut srv = GrpcServer::create(conf.clone(), meta_node.clone());
        info!(
            "Databend meta server listening on {}",
            conf.grpc_api_address.clone()
        );
        srv.start().await.expect("Databend meta service error");
        stop_handler.push(Box::new(srv));
    }

    // Join a raft cluster only after all service started.
    let join_res = meta_node
        .join_cluster(&conf.raft_config, conf.grpc_api_advertise_address())
        .await?;

    info!("Join result: {:?}", join_res);

    register_node(&meta_node, &conf).await?;

    println!("Databend Metasrv started");

    stop_handler.wait_to_terminate(stop_tx).await;
    info!("Databend-meta is done shutting down");

    println!("Databend Metasrv shutdown");

    Ok(())
}

async fn do_register(meta_node: &Arc<MetaNode>, conf: &Config) -> Result<(), MetaAPIError> {
    let node_id = meta_node.sto.id;
    let raft_endpoint = conf.raft_config.raft_api_advertise_host_endpoint();
    let node = Node::new(node_id, raft_endpoint)
        .with_grpc_advertise_address(conf.grpc_api_advertise_address());

    println!("Register this node: {{{}}}", node);
    println!();

    let ent = LogEntry {
        txid: None,
        time_ms: None,
        cmd: Cmd::AddNode {
            node_id,
            node,
            overriding: true,
        },
    };
    info!("Raft log entry for updating node: {:?}", ent);

    meta_node.write(ent).await?;
    info!("Done register");
    Ok(())
}

async fn run_kvapi_command(conf: &Config, op: &str) {
    match KvApiCommand::from_config(conf, op) {
        Ok(kv_cmd) => {
            let rpc_conf = RpcClientConf {
                endpoints: vec![conf.grpc_api_address.clone()],
                username: conf.username.clone(),
                password: conf.password.clone(),
                ..Default::default()
            };
            let client = match MetaStoreProvider::new(rpc_conf).create_meta_store().await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };

            match kv_cmd.execute(client).await {
                Ok(res) => {
                    println!("{}", res);
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("{}", e);
        }
    }
}

/// The meta service GRPC API address can be changed by administrator in the config file.
///
/// Thus every time a meta server starts up, re-register the node info to broadcast its latest grpc address
#[minitrace::trace]
async fn register_node(meta_node: &Arc<MetaNode>, conf: &Config) -> Result<(), anyhow::Error> {
    info!(
        "Register node to update raft_api_advertise_host_endpoint and grpc_api_advertise_address"
    );

    let mut last_err = None;
    let mut sleep_time = Duration::from_millis(500);

    let timeout = Duration::from_millis(conf.raft_config.wait_leader_timeout);
    let timeout_at = Instant::now() + timeout;

    info!(
        "Wait {:?} for active leader to register node, raft election timeouts: {:?}",
        timeout,
        conf.raft_config.election_timeout()
    );
    println!("Wait for {:?} for active leader...", timeout);

    loop {
        if Instant::now() > timeout_at {
            break;
        }

        let wait = meta_node.raft.wait(Some(timeout_at - Instant::now()));
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

        if meta_node.get_node(&leader_id).await.is_none() {
            warn!("Leader node is not replicated to local store, wait and try again");
            sleep(Duration::from_millis(500)).await
        }

        info!("Registering node with grpc-advertise-addr...");

        let res = do_register(meta_node, conf).await;
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

async fn run_cmd(conf: &Config) -> bool {
    if conf.cmd.is_empty() {
        return false;
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", METASRV_SEMVER.deref());
            println!("min-compatible-client-version: {}", MIN_METACLI_SEMVER);
            println!("data-version: {:?}", DATA_VERSION);
        }
        "show-config" => {
            println!(
                "config:\n{}",
                pretty(&conf).unwrap_or_else(|e| format!("error format config: {}", e))
            );
        }
        cmd => {
            if cmd.starts_with(CMD_KVAPI_PREFIX) {
                if let Some(op) = cmd.strip_prefix(CMD_KVAPI_PREFIX) {
                    run_kvapi_command(conf, op).await;
                    return true;
                }
            }
            eprintln!("Invalid cmd: {}", conf.cmd);
            eprintln!("Available cmds:");
            eprintln!("  --cmd ver");
            eprintln!("    Print version and min compatible meta-client version");
            eprintln!("  --cmd show-config");
            eprintln!("    Print effective config");
            eprintln!("  --cmd kvapi::<cmd>");
            eprintln!("    Run kvapi command (upsert, get, mget, list)");
        }
    }

    true
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: serde::Serialize {
    serde_json::to_string_pretty(v)
}
