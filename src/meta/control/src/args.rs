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

use clap::ArgMatches;
use clap::Args;
use clap::Command;
use clap::Error;
use clap::FromArgMatches;
use databend_common_tracing::CONFIG_DEFAULT_LOG_LEVEL;
use databend_meta::raft_store::config::RaftConfig;
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
pub struct FilterTenantArgs {
    /// Tenant name to keep in the exported dump.
    #[clap(long)]
    pub tenant: String,

    /// Input ndjson export file. If empty, read from stdin.
    #[clap(long, default_value = "")]
    pub input: String,

    /// Output ndjson export file. If empty, write to stdout.
    #[clap(long, default_value = "")]
    pub output: String,
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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum LuaScript {
    File(String),
    Inline(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct LuaArgs {
    #[serde(default)]
    pub scripts: Vec<LuaScript>,
}

impl FromArgMatches for LuaArgs {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, Error> {
        Ok(Self {
            scripts: ordered_lua_scripts(matches),
        })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for LuaArgs {
    fn augment_args(command: Command) -> Command {
        add_lua_args(command)
    }

    fn augment_args_for_update(command: Command) -> Command {
        add_lua_args(command)
    }
}

fn add_lua_args(command: Command) -> Command {
    command
        .arg(
            clap::Arg::new("file")
                .long("file")
                .value_name("FILE")
                .help("Path to a Lua script file. May be specified multiple times.")
                .action(clap::ArgAction::Append),
        )
        .arg(
            clap::Arg::new("script")
                .long("script")
                .value_name("SCRIPT")
                .help("Lua code to execute. May be specified multiple times.")
                .action(clap::ArgAction::Append),
        )
}

fn ordered_lua_scripts(matches: &ArgMatches) -> Vec<LuaScript> {
    let mut scripts = matches
        .indices_of("file")
        .into_iter()
        .flatten()
        .zip(matches.get_many::<String>("file").into_iter().flatten())
        .map(|(index, path)| (index, LuaScript::File(path.clone())))
        .chain(
            matches
                .indices_of("script")
                .into_iter()
                .flatten()
                .zip(matches.get_many::<String>("script").into_iter().flatten())
                .map(|(index, script)| (index, LuaScript::Inline(script.clone()))),
        )
        .collect::<Vec<_>>();

    scripts.sort_unstable_by_key(|(index, _)| *index);
    scripts.into_iter().map(|(_, script)| script).collect()
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

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap::Subcommand;

    use super::LuaArgs;
    use super::LuaScript;

    #[derive(Parser)]
    struct TestApp {
        #[command(subcommand)]
        command: TestCommand,
    }

    #[derive(Subcommand)]
    enum TestCommand {
        Lua(LuaArgs),
    }

    #[test]
    fn test_lua_scripts_preserve_command_line_order() {
        let app = TestApp::try_parse_from([
            "metactl",
            "lua",
            "--script",
            "first()",
            "--file",
            "first.lua",
            "--script",
            "second()",
            "--file",
            "second.lua",
        ])
        .unwrap();

        let TestCommand::Lua(args) = app.command;
        assert_eq!(args.scripts, vec![
            LuaScript::Inline("first()".to_string()),
            LuaScript::File("first.lua".to_string()),
            LuaScript::Inline("second()".to_string()),
            LuaScript::File("second.lua".to_string()),
        ]);
    }
}
