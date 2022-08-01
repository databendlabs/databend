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

use common_base::base::RuntimeTracker;
use common_base::base::StopHandle;
use common_base::base::Stoppable;
use common_grpc::RpcClientConf;
use common_macros::databend_main;
use common_meta_sled_store::init_sled_db;
use common_meta_store::MetaStoreProvider;
use common_tracing::init_logging;
use databend_meta::api::GrpcServer;
use databend_meta::api::HttpService;
use databend_meta::cmd;
use databend_meta::configs::Config;
use databend_meta::meta_service::MetaNode;
use databend_meta::metrics::init_meta_metrics_recorder;
use databend_meta::version::METASRV_COMMIT_VERSION;
use databend_meta::version::METASRV_SEMVER;
use databend_meta::version::MIN_METACLI_SEMVER;
use tracing::info;

const CMD_KVAPI_PREFIX: &str = "kvapi::";

#[databend_main]
async fn main(_global_tracker: Arc<RuntimeTracker>) -> common_exception::Result<()> {
    let conf = Config::load()?;

    if run_cmd(&conf).await {
        return Ok(());
    }

    let mut _sentry_guard = None;
    let bend_sentry_env = env::var("DATABEND_SENTRY_DSN").unwrap_or_else(|_| "".to_string());
    if !bend_sentry_env.is_empty() {
        // NOTE: `traces_sample_rate` is 0.0 by default, which disable sentry tracing
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
    }

    let _guards = init_logging("databend-meta", &conf.log);

    info!("Databend Meta version: {}", METASRV_COMMIT_VERSION.as_str());
    info!(
        "Databend Meta start with config: {:?}",
        serde_json::to_string_pretty(&conf).unwrap()
    );

    conf.raft_config.check()?;

    // Leave cluster and quit if `--leave-via` and `--leave-id` is specified.
    let has_left = MetaNode::leave_cluster(&conf.raft_config).await?;
    if has_left {
        info!("node {:?} has left cluster", conf.raft_config.leave_id);
        return Ok(());
    }

    init_sled_db(conf.raft_config.raft_dir.clone());
    init_meta_metrics_recorder();

    info!(
        "Starting MetaNode single: {} with config: {:?}",
        conf.raft_config.single, conf
    );

    let meta_node = MetaNode::start(&conf).await?;

    let mut stop_handler = StopHandle::create();
    let stop_tx = StopHandle::install_termination_handle();

    // HTTP API service.
    {
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
    meta_node
        .join_cluster(&conf.raft_config, conf.grpc_api_address.clone())
        .await?;

    // Print information to users.
    println!("Databend Metasrv");
    println!();
    println!("Version: {}", METASRV_COMMIT_VERSION.as_str());
    println!("Log:");
    println!("    File: {}", conf.log.file);
    println!("    Stderr: {}", conf.log.stderr);
    println!("Id: {}", conf.raft_config.config_id);
    println!("Raft Cluster Name: {}", conf.raft_config.cluster_name);
    println!("Raft Dir: {}", conf.raft_config.raft_dir);
    println!(
        "Raft Status: {}",
        if conf.raft_config.single {
            "single".to_string()
        } else {
            format!("join {:#?}", conf.raft_config.join)
        }
    );
    println!();
    println!("HTTP API");
    println!("   listened at {}", conf.admin_api_address);
    println!("gRPC API");
    println!("   listened at {}", conf.grpc_api_address);

    stop_handler.wait_to_terminate(stop_tx).await;
    info!("Databend-meta is done shutting down");

    Ok(())
}

async fn run_kvapi_command(conf: &Config, op: &str) {
    match cmd::KvApiCommand::from_config(conf, op) {
        Ok(kv_cmd) => {
            let rpc_conf = RpcClientConf {
                address: conf.grpc_api_address.clone(),
                username: conf.username.clone(),
                password: conf.password.clone(),
                ..Default::default()
            };
            let client = match MetaStoreProvider::new(rpc_conf).try_get_meta_store().await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    eprintln!("{}", e.message());
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

async fn run_cmd(conf: &Config) -> bool {
    if conf.cmd.is_empty() {
        return false;
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", METASRV_SEMVER.deref());
            println!("min-compatible-client-version: {}", MIN_METACLI_SEMVER);
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
