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

use std::collections::BTreeMap;

use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use databend_common_base::base::tokio;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_control::admin::MetaAdminClient;
use databend_common_meta_control::args::BenchArgs;
use databend_common_meta_control::args::ExportArgs;
use databend_common_meta_control::args::GlobalArgs;
use databend_common_meta_control::args::ImportArgs;
use databend_common_meta_control::args::StatusArgs;
use databend_common_meta_control::args::TransferLeaderArgs;
use databend_common_meta_control::export_from_disk;
use databend_common_meta_control::export_from_grpc;
use databend_common_meta_control::import;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::FileConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;

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

    async fn show_status(&self, args: &StatusArgs) -> anyhow::Result<()> {
        let addr = args.grpc_api_address.clone();
        let client = MetaGrpcClient::try_create(vec![addr], "root", "xxx", None, None, None)?;

        let res = client.get_cluster_status().await?;
        println!("BinaryVersion: {}", res.binary_version);
        println!("DataVersion: {}", res.data_version);
        println!("RaftLogSize: {}", res.raft_log_size);
        if let Some(s) = res.raft_log_status {
            println!("RaftLog:");
            println!("  - CacheItems: {}", s.cache_items);
            println!("  - CacheUsedSize: {}", s.cache_used_size);
            println!("  - WALTotalSize: {}", s.wal_total_size);
            println!("  - WALOpenChunkSize: {}", s.wal_open_chunk_size);
            println!("  - WALOffset: {}", s.wal_offset);
            println!("  - WALClosedChunkCount: {}", s.wal_closed_chunk_count);
            println!(
                "  - WALClosedChunkTotalSize: {}",
                s.wal_closed_chunk_total_size
            );
            println!("  - WALClosedChunkSizes:");
            for (k, v) in s.wal_closed_chunk_sizes {
                println!("    - {}: {}", k, v);
            }
        }
        println!("SnapshotKeyCount: {}", res.snapshot_key_count);
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

    async fn bench_client_num_conn(&self, args: &BenchArgs) -> anyhow::Result<()> {
        let addr = args.grpc_api_address.clone();
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
        let client = MetaAdminClient::new(args.admin_api_address.as_str());
        let result = client.transfer_leader(args.to).await?;
        println!(
            "triggered leader transfer from {} to {}.",
            result.from, result.to
        );
        println!("voter ids: {:?}", result.voter_ids);
        Ok(())
    }

    async fn export(&self, args: &ExportArgs) -> anyhow::Result<()> {
        match args.raft_dir {
            None => {
                export_from_grpc::export_from_running_node(args).await?;
            }
            Some(ref _dir) => {
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
    Status(StatusArgs),
    Export(ExportArgs),
    Import(ImportArgs),
    TransferLeader(TransferLeaderArgs),
    BenchClientNumConn(BenchArgs),
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
        },
        ..Default::default()
    };
    let guards = init_logging("metactl", &log_config, BTreeMap::new());
    Box::new(guards).leak();

    match app.command {
        Some(ref cmd) => match cmd {
            CtlCommand::Status(args) => {
                app.show_status(args).await?;
            }
            CtlCommand::BenchClientNumConn(args) => {
                app.bench_client_num_conn(args).await?;
            }
            CtlCommand::TransferLeader(args) => {
                app.transfer_leader(args).await?;
            }
            CtlCommand::Export(args) => {
                app.export(args).await?;
            }
            CtlCommand::Import(args) => {
                app.import(args).await?;
            }
        },
        // for backward compatibility
        None => {
            if app.globals.export {
                let args = ExportArgs {
                    grpc_api_address: app.globals.grpc_api_address.clone(),
                    raft_dir: app.globals.raft_dir.clone(),
                    db: app.globals.db.clone(),
                    id: app.globals.id,
                    chunk_size: app.globals.export_chunk_size,
                };
                app.export(&args).await?;
            } else if app.globals.import {
                let args = ImportArgs {
                    raft_dir: app.globals.raft_dir.clone(),
                    db: app.globals.db.clone(),
                    id: app.globals.id,
                    initial_cluster: app.globals.initial_cluster.clone(),
                };
                app.import(&args).await?;
            } else {
                app.print_help()?;
            }
        }
    }

    Ok(())
}
