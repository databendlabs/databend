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

use clap::Args;
use databend_common_tracing::CONFIG_DEFAULT_LOG_LEVEL;
use databend_meta_raft_store::config::RaftConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Args)]
pub struct GlobalArgs {
    #[clap(long, default_value = CONFIG_DEFAULT_LOG_LEVEL)]
    pub log_level: String,

    /// DEPRECATE: use subcommand instead.
    #[clap(
        long,
        env = "METASRV_GRPC_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub grpc_api_address: String,

    /// DEPRECATE: use subcommand instead.
    #[clap(long)]
    pub import: bool,

    /// DEPRECATE: use subcommand instead.
    #[clap(long)]
    pub export: bool,

    /// DEPRECATE: use subcommand instead.
    ///
    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// DEPRECATE: use subcommand instead.
    ///
    /// The N.O. json strings in a export stream item.
    ///
    /// Set this to a smaller value if you get gRPC message body too large error.
    /// This requires meta-service >= 1.2.315; For older version, this argument is ignored.
    ///
    /// By default it is 32.
    #[clap(long)]
    pub export_chunk_size: Option<u64>,

    /// DEPRECATE: use subcommand instead.
    ///
    /// When export raft data, this is the name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    /// When import raft data, this is the name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// DEPRECATE: use subcommand instead.
    ///
    /// initial_cluster format: node_id=endpoint,grpc_api_addr
    #[clap(long)]
    pub initial_cluster: Vec<String>,

    /// DEPRECATE: use subcommand instead.
    ///
    /// The node id. Used in these cases:
    ///
    /// 1. when this server is not initialized, e.g. --boot or --single for the first time.
    /// 2. --initial_cluster with new cluster node id.
    ///
    /// Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct StatusArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct ExportArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// The N.O. json strings in a export stream item.
    ///
    /// Set this to a smaller value if you get gRPC message body too large error.
    /// This requires meta-service >= 1.2.315; For older version, this argument is ignored.
    ///
    /// By default it is 32.
    #[clap(long)]
    pub chunk_size: Option<u64>,

    /// The name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// The node id. Used in these cases:
    ///
    /// 1. when this server is not initialized, e.g. --boot or --single for the first time.
    /// 2. --initial_cluster with new cluster node id.
    ///
    /// Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,
}

impl From<ExportArgs> for RaftConfig {
    #[allow(clippy::field_reassign_with_default)]
    fn from(value: ExportArgs) -> Self {
        let mut c = Self::default();

        c.raft_dir = value.raft_dir.unwrap_or_default();
        c.id = value.id;
        c
    }
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct ImportArgs {
    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// The name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// initial_cluster format: <node_id>=<raft_api_host>:<raft_api_port>
    ///
    /// For example, the following command restores the node(id=1) of a cluster of two:
    /// ```text
    /// $0 \
    /// --raft-dir ./meta_dir \
    /// --db meta.db \
    /// --id=1 \
    /// --initial-cluster 1=localhost:29103 \
    /// --initial-cluster 1=localhost:29103
    /// ```
    ///
    /// If it is empty, the cluster information in the imported data is kept.
    /// For example:
    /// ```text
    /// ["state_machine/0",{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":null,"membership":{"configs":[[4]],"nodes":{"4":{}}}}}}}]
    /// ["state_machine/0",{"Nodes":{"key":4,"value":{"name":"4","endpoint":{"addr":"127.0.0.1","port":28004},"grpc_api_advertise_address":null}}}]
    /// ```
    #[clap(long)]
    pub initial_cluster: Vec<String>,

    /// The node id. Used in these cases:
    ///
    /// 1. when this server is not initialized, e.g. --boot or --single for the first time.
    /// 2. --initial_cluster with new cluster node id.
    ///
    /// Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,
}

impl From<ImportArgs> for RaftConfig {
    #[allow(clippy::field_reassign_with_default)]
    fn from(value: ImportArgs) -> Self {
        let mut c = Self::default();

        c.raft_dir = value.raft_dir.unwrap_or_default();
        c.id = value.id;
        c
    }
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct TransferLeaderArgs {
    #[clap(long)]
    pub to: Option<u64>,

    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct SetFeature {
    #[clap(long)]
    pub feature: String,

    #[clap(long, action = clap::ArgAction::Set)]
    pub enable: bool,

    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct ListFeatures {
    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct BenchArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// Maximum number of connections to create (default: 10000)
    #[clap(long, default_value = "10000")]
    pub num: u64,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct WatchArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// The prefix of a key space to watch the changes.
    #[clap(long)]
    pub prefix: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct UpsertArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// The key to set
    #[clap(long)]
    pub key: String,

    // The value to set
    #[clap(long)]
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct GetArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// The key to get
    #[clap(long)]
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct TriggerSnapshotArgs {
    #[clap(long, default_value = "127.0.0.1:28101")]
    pub admin_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct LuaArgs {
    /// Path to the Lua script file. If not provided, script is read from stdin
    #[clap(long)]
    pub file: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct MetricsArgs {
    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct MemberListArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct KeysLayoutArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// Limit the depth of directory hierarchy to return.
    /// depth=1 returns only top-level prefixes (no slashes),
    /// depth=2 returns prefixes with 0-1 slashes, etc.
    /// If not specified, returns all levels.
    #[clap(long)]
    pub depth: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Args)]
pub struct DumpRaftLogWalArgs {
    /// The dir to store persisted meta state, e.g., `.databend/meta1`
    #[clap(long)]
    pub raft_dir: String,

    /// Decode protobuf-encoded values in UpsertKV and Transaction operations
    #[clap(short = 'V', long, default_value_t = false)]
    pub decode_values: bool,

    /// Show raw protobuf bytes for values in UpsertKV and Transaction operations
    #[clap(short = 'R', long, default_value_t = false)]
    pub raw: bool,
}
