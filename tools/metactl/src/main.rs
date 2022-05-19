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

#![feature(stdin_forwarders)]

mod grpc;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

use anyhow::anyhow;
use clap::Parser;
use common_base::base::tokio;
use common_meta_raft_store::config::get_default_raft_advertise_host;
use common_meta_raft_store::sled_key_spaces::KeySpaceKV;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_tracing::init_global_tracing;
use databend_meta::export::deserialize_to_kv_variant;
use databend_meta::export::serialize_kv_variant;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpSocket;

/// TODO(xuanwo)
///
/// We should make metactl config keeps backward compatibility too.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
struct Config {
    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long)]
    pub import: bool,

    #[clap(long)]
    pub export: bool,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,

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
    let raft_config = &config.raft_config;

    let _guards = init_global_tracing("metactl", "./_metactl_log", &config.log_level);

    eprintln!();
    eprintln!("███╗   ███╗███████╗████████╗ █████╗        ██████╗████████╗██╗     ");
    eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗      ██╔════╝╚══██╔══╝██║     ");
    eprintln!("██╔████╔██║█████╗     ██║   ███████║█████╗██║        ██║   ██║     ");
    eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║╚════╝██║        ██║   ██║     ");
    eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║      ╚██████╗   ██║   ███████╗");
    eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝       ╚═════╝   ╚═╝   ╚══════╝");
    eprintln!();

    eprintln!("raft_config: {}", pretty(raft_config)?);

    if config.export {
        // export from grpc api if metasrv is running
        if config.grpc_api_address.is_empty() {
            eprintln!("export meta dir from: {}", raft_config.raft_dir);

            init_sled_db(raft_config.raft_dir.clone());
            export_from_dir()?;
        } else {
            export_from_running_node(&config).await?;
        }

        return Ok(());
    }

    if config.import {
        eprintln!("import meta dir into: {}", raft_config.raft_dir);

        init_sled_db(raft_config.raft_dir.clone());

        clear()?;
        import_from_stdin()?;

        return Ok(());
    }

    Err(anyhow::anyhow!("Nothing to do"))
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}

fn clear() -> anyhow::Result<()> {
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;
        let tree = db.open_tree(&name)?;
        tree.clear()?;
        eprintln!("Clear sled tree {} Done", name);
    }

    Ok(())
}

/// Read every line from stdin, deserialize it into tree_name, key and value. Insert them into sled db and flush.
fn import_from_stdin() -> anyhow::Result<()> {
    let db = get_sled_db();

    let mut trees = BTreeMap::new();

    let lines = io::stdin().lines();
    let mut n = 0;
    for line in lines {
        let l = line?;
        let (tree_name, kv_variant): (String, KeySpaceKV) = serde_json::from_str(&l)?;
        // eprintln!("line: {}", l);

        if !trees.contains_key(&tree_name) {
            let tree = db.open_tree(&tree_name)?;
            trees.insert(tree_name.clone(), tree);
        }

        let tree = trees.get(&tree_name).unwrap();

        let (k, v) = serialize_kv_variant(&kv_variant)?;

        tree.insert(k, v)?;
        n += 1;
    }

    for tree in trees.values() {
        tree.flush()?;
    }

    eprintln!("Imported {} records", n);

    Ok(())
}

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, {key_space: {key, value}}]`
/// E.g.:
/// `["test-29000-state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}`
fn export_from_dir() -> anyhow::Result<()> {
    let db = get_sled_db();

    let mut tree_names = db.tree_names();
    tree_names.sort();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;

        let tree = db.open_tree(&name)?;
        for x in tree.iter() {
            let kv = x?;
            let kv = vec![kv.0.to_vec(), kv.1.to_vec()];

            let kv_variant = deserialize_to_kv_variant(&kv)?;
            let tree_kv = (name.clone(), kv_variant);

            let line = serde_json::to_string(&tree_kv)?;

            println!("{}", line);
        }
    }

    Ok(())
}

/// Dump metasrv data, raft-log, state machine etc in json to stdout.
async fn export_from_running_node(config: &Config) -> Result<(), anyhow::Error> {
    eprintln!("export meta dir from remote: {}", config.grpc_api_address);

    let grpc_api_addr: SocketAddr = match config.grpc_api_address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!(
                "ERROR: grpc api address is invalid: {}",
                &config.grpc_api_address
            );
            return Err(anyhow!(e));
        }
    };

    if service_is_running(grpc_api_addr).await? {
        grpc::export_meta(&config.grpc_api_address).await?;
        return Ok(());
    }
    Ok(())
}

// if port is open, service is running
async fn service_is_running(addr: SocketAddr) -> Result<bool, io::Error> {
    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await;

    Ok(stream.is_ok())
}
