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

mod export_from_grpc;

pub mod admin;
pub mod export_from_disk;
pub mod import;
pub(crate) mod reading;
pub mod upgrade;

use std::collections::BTreeMap;

use admin::MetaAdminClient;
use clap::Args;
use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use databend_common_base::base::tokio;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_sled_store::init_sled_db;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::FileConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Args)]
pub struct GlobalArgs {
    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(
        long,
        env = "METASRV_GRPC_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub grpc_api_address: String,

    #[clap(
        long,
        env = "METASRV_ADMIN_API_ADDRESS",
        default_value = "127.0.0.1:28002"
    )]
    pub admin_api_address: String,

    /// DEPRECATED
    #[clap(long)]
    pub import: bool,

    /// DEPRECATED
    #[clap(long)]
    pub export: bool,

    /// DEPRECATED
    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// DEPRECATED
    /// The N.O. json strings in a export stream item.
    ///
    /// Set this to a smaller value if you get gRPC message body too large error.
    /// This requires meta-service >= 1.2.315; For older version, this argument is ignored.
    ///
    /// By default it is 32.
    #[clap(long)]
    pub export_chunk_size: Option<u64>,

    /// DEPRECATED
    /// When export raft data, this is the name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    /// When import raft data, this is the name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// DEPRECATED
    /// initial_cluster format: node_id=endpoint,grpc_api_addr
    #[clap(long)]
    pub initial_cluster: Vec<String>,

    /// DEPRECATED
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
pub struct ExportArgs {
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
}

#[derive(Debug, Deserialize, Parser)]
#[clap(name = "databend-metactl", about, version = &**METASRV_COMMIT_VERSION, author)]
struct App {
    #[clap(subcommand)]
    command: Option<CtlCommand>,

    #[clap(flatten)]
    globals: GlobalArgs,
}

impl App {
    fn print_help(&self) -> anyhow::Result<()> {
        let mut cmd = Self::command();
        cmd.print_help()?;
        Ok(())
    }

    async fn show_status(&self) -> anyhow::Result<()> {
        let addr = self.globals.grpc_api_address.clone();
        let client = MetaGrpcClient::try_create(vec![addr], "root", "xxx", None, None, None)?;

        let res = client.get_cluster_status().await?;
        println!("BinaryVersion: {}", res.binary_version);
        println!("DataVersion: {}", res.data_version);
        println!("DBSize: {}", res.db_size);
        println!("KeyNumber: {}", res.key_num);
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

    async fn bench_client_num_conn(&self) -> anyhow::Result<()> {
        let addr = self.globals.grpc_api_address.clone();
        println!(
            "loop: connect to metasrv {}, get_kv('foo'), do not drop the connection",
            addr
        );
        let mut clients = vec![];
        let mut i = 0;
        loop {
            i += 1;
            let client =
                MetaGrpcClient::try_create(vec![addr.clone()], "root", "xxx", None, None, None)?;
            let res = client.get_kv("foo").await;
            println!("{}-th: get_kv(foo): {:?}", i, res);
            clients.push(client);
        }
    }

    async fn transfer_leader(&self, args: &TransferLeaderArgs) -> anyhow::Result<()> {
        let client = MetaAdminClient::new(self.globals.admin_api_address.as_str());
        client.transfer_leader(args.to).await?;
        Ok(())
    }

    async fn export(&self, args: &ExportArgs) -> anyhow::Result<()> {
        match args.raft_dir {
            None => {
                export_from_grpc::export_from_running_node(
                    self.globals.grpc_api_address.as_str(),
                    args,
                )
                .await?;
            }
            Some(ref dir) => {
                init_sled_db(dir.clone(), 64 * 1024 * 1024 * 1024);
                export_from_disk::export_from_dir(args).await?;
            }
        }
        Ok(())
    }

    async fn import(&self, args: &ImportArgs) -> anyhow::Result<()> {
        import::import_data(args).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Subcommand)]
enum CtlCommand {
    Status,
    Export(ExportArgs),
    Import(ImportArgs),
    TransferLeader(TransferLeaderArgs),
    BenchClientNumConn,
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
    let app = App::parse();

    let log_config = LogConfig {
        file: FileConfig {
            on: true,
            level: app.globals.log_level.clone(),
            dir: ".databend/logs".to_string(),
            format: "text".to_string(),
            limit: 48,
            prefix_filter: "databend_".to_string(),
        },
        ..Default::default()
    };
    let _guards = init_logging("metactl", &log_config, BTreeMap::new());

    match app.command {
        Some(ref cmd) => match cmd {
            CtlCommand::Status => {
                app.show_status().await?;
            }
            CtlCommand::BenchClientNumConn => {
                app.bench_client_num_conn().await?;
            }
            CtlCommand::TransferLeader(args) => {
                app.transfer_leader(&args).await?;
            }
            CtlCommand::Export(args) => {
                app.export(&args).await?;
            }
            CtlCommand::Import(args) => {
                app.import(&args).await?;
            }
        },
        // for backward compatibility
        None => {
            if app.globals.export {
                let args = ExportArgs {
                    raft_dir: app.globals.raft_dir.clone(),
                    db: app.globals.db.clone(),
                    id: app.globals.id,
                    chunk_size: app.globals.export_chunk_size,
                };
                app.export(&args).await?;
            }

            if app.globals.import {
                let args = ImportArgs {
                    raft_dir: app.globals.raft_dir.clone(),
                    db: app.globals.db.clone(),
                    id: app.globals.id,
                    initial_cluster: app.globals.initial_cluster.clone(),
                };
                app.import(&args).await?;
            }

            app.print_help()?;
        }
    }

    Ok(())
}
