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

#![allow(clippy::uninlined_format_args)]

mod grpc;
use common_tracing::QueryLogConfig;
use common_tracing::TracingConfig;
use grpc::export_meta;

mod snapshot;

use std::time::Duration;

use clap::Parser;
use common_base::base::tokio;
use common_meta_client::MetaGrpcClient;
use common_meta_kvapi::kvapi::KVApi;
use common_meta_raft_store::config::RaftConfig;
use common_tracing::init_logging;
use common_tracing::Config as LogConfig;
use common_tracing::FileConfig;
use common_tracing::StderrConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;
use serde::Serialize;

// TODO(xuanwo)
//
// We should make metactl config keeps backward compatibility too.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
pub struct Config {
    /// Run a command
    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long)]
    pub status: bool,

    #[clap(long)]
    pub import: bool,

    #[clap(long)]
    pub export: bool,

    #[clap(
        long,
        env = "METASRV_GRPC_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub grpc_api_address: String,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// When export raft data, this is the name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    /// When import raft data, this is the name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// initial_cluster format: node_id=endpoint,grpc_api_addr
    #[clap(long)]
    pub initial_cluster: Vec<String>,

    /// The node id. Used in these cases:
    /// 1. when this server is not initialized, e.g. --boot or --single for the first time.
    /// 2. --initial_cluster with new cluster node id.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,
}

impl From<Config> for RaftConfig {
    #[allow(clippy::field_reassign_with_default)]
    fn from(value: Config) -> Self {
        let mut c = Self::default();

        c.raft_dir = value.raft_dir.unwrap_or_default();
        c.id = value.id;
        c
    }
}

/// Usage:
/// - To dump a sled db: `$0 --raft-dir ./_your_meta_dir/`:
///   ```
///   ["header",{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}]
///   ["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":1}}}]
///   ["raft_state",{"RaftStateKV":{"key":"HardState","value":{"HardState":{"leader_id":{"term":1,"node_id":1},"committed":false}}}}]
///   ["raft_log",{"Logs":{"key":0,"value":{"log_id":{"leader_id":{"term":0,"node_id":0},"index":0},"payload":{"Membership":{"configs":[[1]],"nodes":{"1":{}}}}}}}]
///   ["raft_log",{"Logs":{"key":1,"value":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":1},"payload":"Blank"}}}]
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    let log_config = LogConfig {
        file: FileConfig {
            on: true,
            level: config.log_level.clone(),
            dir: ".databend/logs".to_string(),
            format: "text".to_string(),
        },
        stderr: StderrConfig::default(),
        query: QueryLogConfig::default(),
        tracing: TracingConfig::from_env(),
    };

    let _guards = init_logging("metactl", &log_config);

    if config.status {
        return show_status(&config).await;
    }

    eprintln!();
    eprintln!("╔╦╗╔═╗╔╦╗╔═╗   ╔═╗╔╦╗╦  ");
    eprintln!("║║║║╣  ║ ╠═╣───║   ║ ║  ");
    eprintln!("╩ ╩╚═╝ ╩ ╩ ╩   ╚═╝ ╩ ╩═╝ Databend");
    eprintln!();

    // eprintln!("███╗   ███╗███████╗████████╗ █████╗        ██████╗████████╗██╗     ");
    // eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗      ██╔════╝╚══██╔══╝██║     ");
    // eprintln!("██╔████╔██║█████╗     ██║   ███████║█████╗██║        ██║   ██║     ");
    // eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║╚════╝██║        ██║   ██║     ");
    // eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║      ╚██████╗   ██║   ███████╗");
    // eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝       ╚═════╝   ╚═╝   ╚══════╝");

    // ██████╗  █████╗ ████████╗ █████╗ ██████╗ ███████╗███╗   ██╗██████╗
    // ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██╔════╝████╗  ██║██╔══██╗
    // ██║  ██║███████║   ██║   ███████║██████╔╝█████╗  ██╔██╗ ██║██║  ██║
    // ██║  ██║██╔══██║   ██║   ██╔══██║██╔══██╗██╔══╝  ██║╚██╗██║██║  ██║
    // ██████╔╝██║  ██║   ██║   ██║  ██║██████╔╝███████╗██║ ╚████║██████╔╝
    // ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═══╝╚═════╝
    // ╔╦╗╔═╗╔╦╗╔═╗   ╔═╗╔╦╗╦
    // ║║║║╣  ║ ╠═╣───║   ║ ║
    // ╩ ╩╚═╝ ╩ ╩ ╩   ╚═╝ ╩ ╩═╝

    eprintln!("Version: {}", METASRV_COMMIT_VERSION.as_str());
    eprintln!();
    eprintln!("Config: {}", pretty(&config)?);

    if !config.cmd.is_empty() {
        return match config.cmd.as_str() {
            "bench-client-conn-num" => {
                bench_client_num_conn(&config).await?;
                Ok(())
            }

            _ => {
                eprintln!("valid commands are");
                eprintln!("  --cmd bench-client-conn-num");
                eprintln!("    Keep create new connections to metasrv.");
                eprintln!("    Requires --grpc-api-address.");

                Err(anyhow::anyhow!("unknown cmd: {}", config.cmd))
            }
        };
    }

    if config.export {
        eprintln!();
        eprintln!("Export:");
        return snapshot::export_data(&config).await;
    }

    if config.import {
        eprintln!();
        eprintln!("Import:");
        return snapshot::import_data(&config).await;
    }

    Err(anyhow::anyhow!("Nothing to do"))
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}

async fn bench_client_num_conn(conf: &Config) -> anyhow::Result<()> {
    let addr = &conf.grpc_api_address;

    println!(
        "loop: connect to metasrv {}, get_kv('foo'), do not drop the connection",
        addr
    );

    let mut clients = vec![];
    let mut i = 0;

    loop {
        i += 1;
        let client = MetaGrpcClient::try_create(
            vec![addr.to_string()],
            "root",
            "xxx",
            None,
            None,
            Duration::from_secs(10),
            None,
        )?;

        let res = client.get_kv("foo").await;
        println!("{}-th: get_kv(foo): {:?}", i, res);

        clients.push(client);
    }
}

async fn show_status(conf: &Config) -> anyhow::Result<()> {
    let addr = &conf.grpc_api_address;

    let client = MetaGrpcClient::try_create(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        None,
        Duration::from_secs(10),
        None,
    )?;

    let res = client.get_cluster_status().await?;
    println!("BinaryVersion: {}", res.binary_version);
    println!("DataVersion: {}", res.data_version);
    println!("DBSize: {}", res.db_size);
    println!("Node: id={} raft={}", res.id, res.endpoint);
    println!("State: {}", res.state);
    if let Some(leader) = res.leader {
        println!("Leader: {}", leader);
    }
    println!("CurrentTerm: {}", res.current_term);
    println!("LastSeq: {:?}", res.last_seq);
    println!("LastLogIndex: {}", res.last_log_index);
    println!("LastApplied: {}", res.last_applied);
    if let Some(last_log_id) = res.snapshot_last_log_id {
        println!("SnapshotLastLogID: {}", last_log_id);
    }
    if let Some(purged) = res.purged {
        println!("Purged: {}", purged);
    }
    if !res.replication.is_empty() {
        println!("Replication:");
        for (k, v) in res.replication {
            if v != res.last_applied {
                println!("  - [{}] {} *", k, v);
            } else {
                println!("  - [{}] {}", k, v);
            }
        }
    }
    if !res.voters.is_empty() {
        println!("Voters:");
        for v in res.voters {
            println!("  - {}", v);
        }
    }
    if !res.non_voters.is_empty() {
        println!("NonVoters:");
        for v in res.non_voters {
            println!("  - {}", v);
        }
    }
    Ok(())
}
