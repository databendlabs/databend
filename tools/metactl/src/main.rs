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

mod grpc;
use grpc::export_meta;

mod snapshot;

use clap::Parser;
use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_raft_store::config::get_default_raft_advertise_host;
use common_tracing::init_global_tracing;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;
use serde::Serialize;

/// TODO(xuanwo)
///
/// We should make metactl config keeps backward compatibility too.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
pub struct Config {
    /// Run a command
    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long)]
    pub import: bool,

    #[clap(long)]
    pub export: bool,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,

    /// When export raft data, this is the name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    /// When import raft data, this is the name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    #[clap(long, default_value = "")]
    pub initial_cluster: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

/// TODO: This is a temp copy of RaftConfig, we will migrate them in the future.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct RaftConfig {
    /// Identify a config.
    /// This is only meant to make debugging easier with more than one Config involved.
    #[clap(long, default_value = "")]
    pub config_id: String,

    /// The local listening host for metadata communication.
    /// This config does not need to be stored in raft-store,
    /// only used when metasrv startup and listen to.
    #[clap(long, default_value = "127.0.0.1")]
    #[serde(alias = "kvsrv_listen_host")]
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    #[clap(long, default_value = "localhost")]
    #[serde(alias = "kvsrv_advertise_host")]
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    #[clap(long, default_value = "28004")]
    #[serde(alias = "kvsrv_api_port")]
    pub raft_api_port: u32,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./_meta")]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: String,

    /// Whether to fsync meta to disk for every meta write(raft log, state machine etc).
    /// No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING.
    #[clap(long)]
    #[serde(alias = "kvsrv_no_sync")]
    pub no_sync: bool,

    /// The number of logs since the last snapshot to trigger next snapshot.
    #[clap(long, default_value = "1024")]
    #[serde(alias = "kvsrv_snapshot_logs_since_last")]
    pub snapshot_logs_since_last: u64,

    /// The interval in milli seconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    #[clap(long, default_value = "1000")]
    #[serde(alias = "kvsrv_heartbeat_intervalt")]
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, default_value = "4000")]
    #[serde(alias = "kvsrv_install_snapshot_timeout")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, default_value = "1000")]
    #[serde(alias = "raft_max_applied_log_to_keep")]
    pub max_applied_log_to_keep: u64,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    #[clap(long)]
    #[serde(alias = "kvsrv_single")]
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long, multiple_occurrences = true, multiple_values = true)]
    #[serde(alias = "metasrv_join")]
    pub join: Vec<String>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            config_id: "".to_string(),
            raft_listen_host: "127.0.0.1".to_string(),
            raft_advertise_host: get_default_raft_advertise_host(),
            raft_api_port: 28004,
            raft_dir: "./_meta".to_string(),
            no_sync: false,
            snapshot_logs_since_last: 1024,
            heartbeat_interval: 1000,
            install_snapshot_timeout: 4000,
            max_applied_log_to_keep: 1000,
            single: false,
            join: vec![],
            id: 0,
            sled_tree_prefix: "".to_string(),
        }
    }
}

/// Usage:
/// - To dump a sled db: `$0 --raft-dir ./_your_meta_dir/`:
///   ```
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","generic-kv",13]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table-lookup",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table_id",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","tables",1]
///   ["global-local-kvstate_machine/0",8,"sledks::Databases",1,{"seq":1,"meta":null,"data":{"engine":"","engine_options":{},"options":{},"created_on":"2022-02-16T03:20:26.007286Z"}}]
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    let _guards = init_global_tracing("metactl", "./_metactl_log", &config.log_level);

    eprintln!();
    eprintln!("███╗   ███╗███████╗████████╗ █████╗        ██████╗████████╗██╗     ");
    eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗      ██╔════╝╚══██╔══╝██║     ");
    eprintln!("██╔████╔██║█████╗     ██║   ███████║█████╗██║        ██║   ██║     ");
    eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║╚════╝██║        ██║   ██║     ");
    eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║      ╚██████╗   ██║   ███████╗");
    eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝       ╚═════╝   ╚═╝   ╚══════╝");
    eprintln!();

    eprintln!("config: {}", pretty(&config)?);

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
        return snapshot::export_data(&config).await;
    }

    if config.import {
        return snapshot::import_data(&config);
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
        let client = MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None)?;

        let res = client.get_kv("foo").await;
        println!("{}-th: get_kv(foo): {:?}", i, res);

        clients.push(client);
    }
}
