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
use databend_common_meta_control::admin::MetaAdminClient;
use databend_common_meta_control::args::BenchArgs;
use databend_common_meta_control::args::DumpRaftLogWalArgs;
use databend_common_meta_control::args::ExportArgs;
use databend_common_meta_control::args::GetArgs;
use databend_common_meta_control::args::GlobalArgs;
use databend_common_meta_control::args::ImportArgs;
use databend_common_meta_control::args::KeysLayoutArgs;
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
use databend_common_meta_control::keys_layout_from_grpc;
use databend_common_meta_control::lua_support;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::FileConfig;
use databend_common_tracing::LogFormat;
use databend_common_tracing::init_logging;
use databend_common_version::METASRV_COMMIT_VERSION;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::UpsertKV;
use databend_meta_types::protobuf::WatchRequest;
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
        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr],
            "root",
            "xxx",
            None,
            None,
            None,
            DEFAULT_GRPC_MESSAGE_SIZE,
        )?;

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
            "loop: connect to metasrv {}, get_kv('foo'), do not drop the connection, num={}",
            addr, args.num
        );
        let mut clients = vec![];
        for i in 1..=args.num {
            let client = MetaGrpcClient::<DatabendRuntime>::try_create(
                vec![addr.clone()],
                "root",
                "xxx",
                None,
                None,
                None,
                DEFAULT_GRPC_MESSAGE_SIZE,
            )?;
            let res = client.get_kv("foo").await;
            println!("{}-th: get_kv(foo): {:?}", i, res);
            clients.push(client);
        }
        println!("bench completed: {} connections created", args.num);
        Ok(())
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
                export_from_grpc::export_from_running_node(args).await?;
            }
            Some(ref _dir) => {
                export_from_disk::export_from_dir::<DatabendRuntime>(args).await?;
            }
        }
        Ok(())
    }

    async fn import(&self, args: &ImportArgs) -> anyhow::Result<()> {
        import::import_data::<DatabendRuntime>(args).await?;
        Ok(())
    }

    async fn keys_layout(&self, args: &KeysLayoutArgs) -> anyhow::Result<()> {
        keys_layout_from_grpc::keys_layout_from_running_node(args).await?;
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

        let res = client.upsert_kv(upsert).await?;
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
        lua_support::setup_lua_environment(&lua)?;

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

        match lua_support::run_lua_script_with_result(&lua_script).await? {
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

    fn new_grpc_client(
        &self,
        addresses: Vec<String>,
    ) -> Result<Arc<ClientHandle<DatabendRuntime>>, CreationError> {
        lua_support::new_grpc_client(addresses)
    }
}

#[derive(Debug, Clone, Deserialize, Subcommand)]
enum CtlCommand {
    #[command(verbatim_doc_comment)]
    /// Show cluster status: node info, replication, voters, etc.
    ///
    /// Example:
    ///   metactl status --grpc-api-address 127.0.0.1:9191
    ///
    /// Sample output:
    ///   BinaryVersion: v1.2.345-nightly-abc1234(rust-1.75-nightly-2024-01-01T12:00:00)
    ///   DataVersion: V002
    ///   RaftLogSize: 1234
    ///   SnapshotKeyCount: 5678
    ///   Node: id=0 raft=127.0.0.1:28004
    ///   State: Leader
    ///   Leader: Node { name: "0", endpoint: 127.0.0.1:28004 }
    ///   CurrentTerm: 1
    ///   LastSeq: 100
    ///   LastLogIndex: 200
    ///   LastApplied: T1-N0.200
    ///   Voters:
    ///     - Node { name: "0", endpoint: 127.0.0.1:28004 }
    Status(StatusArgs),

    #[command(verbatim_doc_comment)]
    /// Export metadata from a running node or a local raft-dir.
    ///
    /// Example (from running node to stdout):
    ///   metactl export --grpc-api-address 127.0.0.1:9191
    ///
    /// Example (from running node to file):
    ///   metactl export --grpc-api-address 127.0.0.1:9191 --db meta.db
    ///
    /// Example (from local raft-dir):
    ///   metactl export --raft-dir .databend/meta
    ///
    /// Sample output (JSON lines):
    ///   ["header",{"DataHeader":{"key":"header","value":{"version":"V002",...}}}]
    ///   ["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]
    ///   ["log",{"Logs":{"key":0,"value":{"log_id":...,"payload":"Membership"...}}}]
    Export(ExportArgs),

    #[command(verbatim_doc_comment)]
    /// Import metadata into a new raft-dir from stdin or a file.
    ///
    /// Example (from stdin):
    ///   cat meta.db | metactl import --raft-dir .databend/meta --id 0
    ///
    /// Example (from file):
    ///   metactl import --raft-dir .databend/meta --db meta.db --id 0
    ///
    /// Example (with cluster config):
    ///   metactl import --raft-dir .databend/meta --db meta.db --id 1 \
    ///     --initial-cluster 1=localhost:28103 \
    ///     --initial-cluster 2=localhost:28203
    Import(ImportArgs),

    #[command(verbatim_doc_comment)]
    /// Dump state machine keys grouped by prefix.
    ///
    /// Useful for understanding the key distribution in the state machine.
    /// Groups keys by directory prefix and returns counts at each level.
    ///
    /// Example:
    ///   metactl keys-layout --grpc-api-address 127.0.0.1:9191
    ///   metactl keys-layout --grpc-api-address 127.0.0.1:9191 --depth 1
    ///
    /// Sample input keys:
    ///   databases/1/tables/users/columns/id
    ///   databases/1/tables/users/columns/name
    ///   databases/1/tables/orders/columns/id
    ///   databases/2/tables/products/columns/id
    ///   system/config/max_connections
    ///   system/stats/query_count
    ///
    /// Sample output (no depth limit):
    ///   databases/1/tables/orders/columns 1
    ///   databases/1/tables/orders 1
    ///   databases/1/tables/users/columns 2
    ///   databases/1/tables/users 2
    ///   databases/1/tables 3
    ///   databases/1 3
    ///   databases/2/tables/products/columns 1
    ///   databases/2/tables/products 1
    ///   databases/2/tables 1
    ///   databases/2 1
    ///   databases 4
    ///   system/config 1
    ///   system/stats 1
    ///   system 2
    ///    6
    ///
    /// Sample output (--depth 1):
    ///   databases 4
    ///   system 2
    ///    6
    KeysLayout(KeysLayoutArgs),

    #[command(verbatim_doc_comment)]
    /// Trigger a leader transfer to another node.
    ///
    /// Example (transfer to a specific node):
    ///   metactl transfer-leader --to 2 --admin-api-address 127.0.0.1:28002
    ///
    /// Example (transfer to any other voter):
    ///   metactl transfer-leader --admin-api-address 127.0.0.1:28002
    ///
    /// Sample output:
    ///   triggered leader transfer from 0 to 2.
    ///   voter ids: [0, 1, 2]
    TransferLeader(TransferLeaderArgs),

    #[command(verbatim_doc_comment)]
    /// Trigger a snapshot creation on the target node.
    ///
    /// Example:
    ///   metactl trigger-snapshot --admin-api-address 127.0.0.1:28002
    ///
    /// Sample output:
    ///   triggered snapshot successfully.
    TriggerSnapshot(TriggerSnapshotArgs),

    #[command(verbatim_doc_comment)]
    /// Enable or disable a feature flag.
    ///
    /// Example:
    ///   metactl set-feature --feature pb-messages --enable true
    ///   metactl set-feature --feature pb-messages --enable false
    ///
    /// Sample output:
    ///   set-feature: Done; features:
    ///   {
    ///     "pb-messages": true
    ///   }
    SetFeature(SetFeature),

    #[command(verbatim_doc_comment)]
    /// List all feature flags and their status.
    ///
    /// Example:
    ///   metactl list-features --admin-api-address 127.0.0.1:28002
    ///
    /// Sample output:
    ///   {
    ///     "pb-messages": true
    ///   }
    ListFeatures(ListFeatures),

    #[command(verbatim_doc_comment)]
    /// Benchmark client connection handling (for debugging).
    ///
    /// Creates connections in a loop without closing them.
    /// Useful for testing connection limits and resource usage.
    ///
    /// Example:
    ///   metactl bench-client-num-conn --grpc-api-address 127.0.0.1:9191
    ///
    /// Sample output:
    ///   loop: connect to metasrv 127.0.0.1:9191, get_kv('foo'), do not drop the connection
    ///   1-th: get_kv(foo): Ok(None)
    ///   2-th: get_kv(foo): Ok(None)
    ///   ...
    BenchClientNumConn(BenchArgs),

    #[command(verbatim_doc_comment)]
    /// Watch key changes under a prefix in real-time.
    ///
    /// Example:
    ///   metactl watch --grpc-api-address 127.0.0.1:9191 --prefix __fd_clusters/
    ///
    /// Sample output:
    ///   received-watch-event: WatchResponse { ... }
    Watch(WatchArgs),

    #[command(verbatim_doc_comment)]
    /// Update or insert a key-value pair.
    ///
    /// Example:
    ///   metactl upsert --grpc-api-address 127.0.0.1:9191 --key mykey --value myvalue
    ///
    /// Sample output:
    ///   upsert-result: mykey: None -> Some(SeqV { seq: 123, ... })
    Upsert(UpsertArgs),

    #[command(verbatim_doc_comment)]
    /// Get the value of a key.
    ///
    /// Example:
    ///   metactl get --grpc-api-address 127.0.0.1:9191 --key mykey
    ///
    /// Sample output:
    ///   {"seq":123,"meta":null,"data":[109,121,118,97,108,117,101]}
    Get(GetArgs),

    #[command(verbatim_doc_comment)]
    /// Execute a Lua script with metasrv client bindings.
    ///
    /// Provides Lua bindings for interacting with metasrv:
    /// - metactl.new_grpc_client(address) - create gRPC client
    /// - metactl.new_admin_client(address) - create admin client
    ///
    /// Example (from file):
    ///   metactl lua --file script.lua
    ///
    /// Example (from stdin):
    ///   echo 'print("hello")' | metactl lua
    ///
    /// Sample Lua script:
    ///   local client = metactl.new_grpc_client("127.0.0.1:9191")
    ///   local result, err = client:get_kv("mykey")
    ///   if err then print("error:", err) else print(result) end
    Lua(LuaArgs),

    #[command(verbatim_doc_comment)]
    /// List all members in the cluster.
    ///
    /// Example:
    ///   metactl member-list --grpc-api-address 127.0.0.1:9191
    ///
    /// Sample output:
    ///   Node { name: "0", endpoint: 127.0.0.1:28004, grpc_api_addr: 127.0.0.1:9191 }
    ///   Node { name: "1", endpoint: 127.0.0.1:28104, grpc_api_addr: 127.0.0.1:9192 }
    MemberList(MemberListArgs),

    #[command(verbatim_doc_comment)]
    /// Fetch Prometheus metrics from the admin API.
    ///
    /// Example:
    ///   metactl metrics --admin-api-address 127.0.0.1:28002
    ///
    /// Sample output:
    ///   # HELP metasrv_server_last_log_index The last log index.
    ///   # TYPE metasrv_server_last_log_index gauge
    ///   metasrv_server_last_log_index 1234
    ///   # HELP metasrv_server_current_term The current term.
    ///   ...
    Metrics(MetricsArgs),

    #[command(verbatim_doc_comment)]
    /// Dump the Raft log WAL (Write-Ahead Log) contents.
    ///
    /// Shows each record in the WAL with offset, size, and decoded payload.
    /// Useful for debugging Raft log corruption or understanding log structure.
    ///
    /// Sample output:
    ///
    ///   RaftLog:
    ///   ChunkId(00_000_000_000_000_000_000)
    ///     R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(...)
    ///     R-00001: [000_000_018, 000_000_046) Size(28): RaftLogState(...)
    ///     R-00002: [000_000_046, 000_000_125) Size(79): Append(log_id: T0-N0.0, ...)
    ///     R-00003: [000_000_125, 000_000_175) Size(50): SaveVote(<T1-N0:->)
    ///     R-00004: [000_000_175, 000_000_225) Size(50): SaveVote(<T1-N0:Q>)
    ///     R-00005: [000_000_225, 000_000_277) Size(52): Append(log_id: T1-N0.1, payload: blank)
    ///     R-00006: [000_000_277, 000_000_461) Size(184): Append(log_id: T1-N0.2, ...)
    ///     R-00007: [000_000_461, 000_000_507) Size(46): Commit(T1-N0.1)
    DumpRaftLogWal(DumpRaftLogWalArgs),
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
            CtlCommand::KeysLayout(args) => {
                app.keys_layout(args).await?;
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
            CtlCommand::DumpRaftLogWal(args) => {
                databend_common_meta_control::dump_raft_log_wal::dump_raft_log_wal(args)?;
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
