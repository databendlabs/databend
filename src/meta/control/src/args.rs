// Copyright 2025 Datafuse Labs.
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
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_tracing::CONFIG_DEFAULT_LOG_LEVEL;
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

    /// initial_cluster format: node_id=endpoint,grpc_api_addr
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
pub struct BenchArgs {
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,
}
