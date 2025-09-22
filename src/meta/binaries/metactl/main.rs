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
use std::io::Read;
use std::io::{self};
use std::sync::Arc;

use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use databend_common_base::base::tokio;
use databend_common_meta_client::errors::CreationError;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_control::admin::MetaAdminClient;
use databend_common_meta_control::args::BenchArgs;
use databend_common_meta_control::args::ExportArgs;
use databend_common_meta_control::args::GetArgs;
use databend_common_meta_control::args::GlobalArgs;
use databend_common_meta_control::args::ImportArgs;
use databend_common_meta_control::args::ListFeatures;
use databend_common_meta_control::args::LuaArgs;
use databend_common_meta_control::args::MemberListArgs;
use databend_common_meta_control::args::MetricsArgs;
use databend_common_meta_control::args::SetFeature;
use databend_common_meta_control::args::StatusArgs;
use databend_common_meta_control::args::TransferLeaderArgs;
use databend_common_meta_control::args::TriggerSnapshotArgs;
use databend_common_meta_control::args::UpsertArgs;
use databend_common_meta_control::args::WatchArgs;
use databend_common_meta_control::export_from_disk;
use databend_common_meta_control::export_from_grpc;
use databend_common_meta_control::import;
use databend_common_meta_control::lua_support;
use databend_common_meta_kvapi::kvapi::KvApiExt;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::FileConfig;
use databend_common_tracing::LogFormat;
use databend_common_version::BUILD_INFO;
use databend_common_version::METASRV_COMMIT_VERSION;
use display_more::DisplayOptionExt;
use futures::stream::TryStreamExt;
use mlua::Lua;
use serde::Deserialize;

#[derive(Debug, Deserialize, Parser)]
#[clap(name = "databend-metactl", about, version = METASRV_COMMIT_VERSION.as_str(), author)]
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
        let client =
            MetaGrpcClient::try_create(vec![addr], &BUILD_INFO, "root", "xxx", None, None, None)?;

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
            let client = MetaGrpcClient::try_create(
                vec![addr.clone()],
                &BUILD_INFO,
                "root",
                "xxx",
                None,
                None,
                None,
            )?;
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

    async fn trigger_snapshot(&self, args: &TriggerSnapshotArgs) -> anyhow::Result<()> {
        let client = MetaAdminClient::new(args.admin_api_address.as_str());
        client.trigger_snapshot().await?;
        println!("triggered snapshot successfully.");
        Ok(())
    }

    async fn export(&self, args: &ExportArgs) -> anyhow::Result<()> {
        match args.raft_dir {
            None => {
                export_from_grpc::export_from_running_node(args, &BUILD_INFO).await?;
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

    async fn watch(&self, args: &WatchArgs) -> anyhow::Result<()> {
        let addresses = vec![args.grpc_api_address.clone()];
        let client = self.new_grpc_client(addresses)?;

        let watch = WatchRequest::new_dir(&args.prefix).with_initial_flush(true);

        let mut strm = client.request(watch).await?;
        while let Some(watch_response) = strm.try_next().await? {
            println!("received-watch-event: {}", watch_response);
        }
        Ok(())
    }

    async fn upsert(&self, args: &UpsertArgs) -> anyhow::Result<()> {
        let addresses = vec![args.grpc_api_address.clone()];
        let client = self.new_grpc_client(addresses)?;

        let upsert = UpsertKV::update(args.key.clone(), args.value.as_bytes());

        let res = client.request(upsert).await?;
        println!(
            "upsert-result: {}: {:?} -> {:?}",
            res.ident.display(),
            res.prev,
            res.result
        );
        Ok(())
    }

    async fn get(&self, args: &GetArgs) -> anyhow::Result<()> {
        let addresses = vec![args.grpc_api_address.clone()];
        let client = self.new_grpc_client(addresses)?;

        let res = client.get_kv(&args.key).await?;
        println!("{}", serde_json::to_string(&res)?);
        Ok(())
    }

    async fn run_lua(&self, args: &LuaArgs) -> anyhow::Result<()> {
        let lua = Lua::new();

        // Setup Lua environment with gRPC client support
        lua_support::setup_lua_environment(&lua, &BUILD_INFO)?;

        let script = match &args.file {
            Some(path) => std::fs::read_to_string(path)?,
            None => {
                let mut buffer = String::new();
                io::stdin().read_to_string(&mut buffer)?;
                buffer
            }
        };

        #[allow(clippy::disallowed_types)]
        let local = tokio::task::LocalSet::new();
        let res = local.run_until(lua.load(&script).exec_async()).await;

        if let Err(e) = res {
            return Err(anyhow::anyhow!("Lua execution error: {}", e));
        }
        Ok(())
    }

    async fn get_metrics(&self, args: &MetricsArgs) -> anyhow::Result<()> {
        let lua_script = format!(
            r#"
local admin_client = metactl.new_admin_client("{}")
local metrics, err = admin_client:metrics()
if err then
    return nil, err
end
print(metrics)
return metrics, nil
"#,
            args.admin_api_address
        );

        match lua_support::run_lua_script_with_result(&lua_script, &BUILD_INFO).await? {
            Ok(_result) => Ok(()),
            Err(error_msg) => Err(anyhow::anyhow!("Failed to get metrics: {}", error_msg)),
        }
    }

    async fn member_list(&self, args: &MemberListArgs) -> anyhow::Result<()> {
        let addresses = vec![args.grpc_api_address.clone()];
        let client = self.new_grpc_client(addresses)?;

        let res = client.get_member_list().await?;
        for member in res.data {
            println!("{}", member);
        }
        Ok(())
    }

    fn new_grpc_client(&self, addresses: Vec<String>) -> Result<Arc<ClientHandle>, CreationError> {
        lua_support::new_grpc_client(addresses, &BUILD_INFO)
    }
}

#[derive(Debug, Clone, Deserialize, Subcommand)]
enum CtlCommand {
    Status(StatusArgs),
    Export(ExportArgs),
    Import(ImportArgs),
    TransferLeader(TransferLeaderArgs),
    TriggerSnapshot(TriggerSnapshotArgs),
    SetFeature(SetFeature),
    ListFeatures(ListFeatures),
    BenchClientNumConn(BenchArgs),
    Watch(WatchArgs),
    Upsert(UpsertArgs),
    Get(GetArgs),
    Lua(LuaArgs),
    MemberList(MemberListArgs),
    Metrics(MetricsArgs),
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
            format: LogFormat::Text,
            limit: 48,
            max_size: 4294967296,
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
            CtlCommand::TriggerSnapshot(args) => {
                app.trigger_snapshot(args).await?;
            }
            CtlCommand::SetFeature(args) => {
                let client = MetaAdminClient::new(args.admin_api_address.as_str());
                let res = client.set_feature(&args.feature, args.enable).await?;
                println!(
                    "set-feature: Done; features:\n{}",
                    serde_json::to_string_pretty(&res)?
                );
            }
            CtlCommand::ListFeatures(args) => {
                let client = MetaAdminClient::new(args.admin_api_address.as_str());
                let res = client.list_features().await?;
                println!("{}", serde_json::to_string_pretty(&res)?);
            }
            CtlCommand::Export(args) => {
                app.export(args).await?;
            }
            CtlCommand::Import(args) => {
                app.import(args).await?;
            }
            CtlCommand::Watch(args) => {
                app.watch(args).await?;
            }
            CtlCommand::Upsert(args) => {
                app.upsert(args).await?;
            }
            CtlCommand::Get(args) => {
                app.get(args).await?;
            }
            CtlCommand::Lua(args) => {
                app.run_lua(args).await?;
            }
            CtlCommand::MemberList(args) => {
                app.member_list(args).await?;
            }
            CtlCommand::Metrics(args) => {
                app.get_metrics(args).await?;
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
