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

use std::env;

use clap::ArgAction;
use clap::Args;
use clap::Parser;
use databend_common_meta_raft_store::config::get_default_raft_advertise_host;
use databend_common_meta_raft_store::config::RaftConfig as InnerRaftConfig;
use databend_common_meta_types::MetaStartupError;
use databend_common_tracing::Config as InnerLogConfig;
use databend_common_tracing::FileConfig as InnerFileLogConfig;
use databend_common_tracing::OTLPConfig;
use databend_common_tracing::ProfileLogConfig;
use databend_common_tracing::QueryLogConfig;
use databend_common_tracing::StderrConfig as InnerStderrLogConfig;
use databend_common_tracing::StructLogConfig;
use databend_common_tracing::TracingConfig;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

use super::inner::Config as InnerConfig;
use crate::version::METASRV_COMMIT_VERSION;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
#[serde(default)]
pub struct Config {
    /// Run a command
    ///
    /// Supported commands:
    ///
    /// - `ver`: print version and quit.
    ///
    /// - `show-config`: print effective config and quit.
    ///
    /// - `kvapi::<cmd>`: run kvapi command. The command can be `upsert`, `delete`, `get`, `mget` and `list`:
    ///
    /// -    `--cmd kvapi::upsert --key    foo --value bar`
    ///
    /// -    `--cmd kvapi::delete --key    foo`
    ///
    /// -    `--cmd kvapi::get    --key    foo`
    ///
    /// -    `--cmd kvapi::mget   --key    foo bar`
    ///
    /// -    `--cmd kvapi::list   --prefix foo/`
    #[clap(long, default_value = "")]
    pub cmd: String,

    /// The key sent to databend-meta server and is only used when running with `--cmd kvapi::*`
    #[clap(long, default_value = "")]
    pub key: Vec<String>,

    /// The value sent to databend-meta server and is only used when running with `--cmd kvapi::upsert`
    #[clap(long, default_value = "")]
    pub value: String,

    /// The seconds after which the value should expire. Only used when running with `--cmd kvapi::upsert`
    #[clap(long)]
    pub expire_after: Option<u64>,

    /// The prefix sent to databend-meta server and is only used when running with `--cmd kvapi::list`
    #[clap(long, default_value = "")]
    pub prefix: String,

    #[clap(long, default_value = "root")]
    pub username: String,

    #[clap(long, default_value = "")]
    pub password: String,

    #[clap(long, short = 'c', default_value = "")]
    pub config_file: String,

    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-level", default_value = "INFO")]
    pub log_level: String,

    /// Log file dir
    #[clap(long = "log-dir", default_value = "./.databend/logs")]
    pub log_dir: String,

    #[clap(flatten)]
    pub log: LogConfig,

    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_cert: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_key: String,

    /// Listening address for public APIs
    ///
    /// This address is only used by meta service to build a listening endpoint.
    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// Connecting address for public APIs
    ///
    /// This address is published to meta service client to connect to.
    #[clap(long)]
    pub grpc_api_advertise_host: Option<String>,

    /// Certificate for server to identify itself
    #[clap(long, default_value = "")]
    pub grpc_tls_server_cert: String,

    #[clap(long, default_value = "")]
    pub grpc_tls_server_key: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        InnerConfig::default().into()
    }
}

impl From<Config> for InnerConfig {
    fn from(outer: Config) -> Self {
        let mut log: InnerLogConfig = outer.log.into();
        if outer.log_level != "INFO" {
            log.file.level = outer.log_level.to_string();
        }
        if outer.log_dir != "./.databend/logs" {
            log.file.dir = outer.log_dir.to_string();
        }

        InnerConfig {
            cmd: outer.cmd,
            key: outer.key,
            value: outer.value,
            expire_after: outer.expire_after,
            prefix: outer.prefix,
            username: outer.username,
            password: outer.password,
            config_file: outer.config_file,
            log,
            admin_api_address: outer.admin_api_address,
            admin_tls_server_cert: outer.admin_tls_server_cert,
            admin_tls_server_key: outer.admin_tls_server_key,
            grpc_api_address: outer.grpc_api_address,
            grpc_api_advertise_host: outer.grpc_api_advertise_host,
            grpc_tls_server_cert: outer.grpc_tls_server_cert,
            grpc_tls_server_key: outer.grpc_tls_server_key,
            raft_config: outer.raft_config.into(),
        }
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self {
            cmd: inner.cmd,
            key: inner.key,
            value: inner.value,
            expire_after: inner.expire_after,
            prefix: inner.prefix,
            username: inner.username,
            password: inner.password,
            config_file: inner.config_file,
            log_level: inner.log.file.level.clone(),
            log_dir: inner.log.file.dir.clone(),
            log: inner.log.into(),
            admin_api_address: inner.admin_api_address,
            admin_tls_server_cert: inner.admin_tls_server_cert,
            admin_tls_server_key: inner.admin_tls_server_key,
            grpc_api_address: inner.grpc_api_address,
            grpc_api_advertise_host: inner.grpc_api_advertise_host,
            grpc_tls_server_cert: inner.grpc_tls_server_cert,
            grpc_tls_server_key: inner.grpc_tls_server_key,
            raft_config: inner.raft_config.into(),
        }
    }
}

impl Config {
    /// Load will load config from file, env and args.
    ///
    /// - Load from file as default.
    /// - Load from env, will override config from file.
    /// - Load from args as finally override
    ///
    /// # Notes
    ///
    /// with_args is to control whether we need to load from args or not.
    /// We should set this to false during tests because we don't want
    /// our test binary to parse cargo's args.
    pub fn load(with_args: bool) -> Result<Self, MetaStartupError> {
        let mut arg_conf = Self::default();

        if with_args {
            arg_conf = Self::parse();
        }

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let config_file = if !arg_conf.config_file.is_empty() {
                // TODO: remove this `allow(clippy::redundant_clone)`
                // as soon as this issue is fixed:
                // https://github.com/rust-lang/rust-clippy/issues/10940
                #[allow(clippy::redundant_clone)]
                arg_conf.config_file.clone()
            } else if let Ok(path) = env::var("METASRV_CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            if !config_file.is_empty() {
                builder = builder.collect(from_file(Toml, &config_file));
            }
        }

        // Then, load from env.
        let cfg_via_env: ConfigViaEnv = serfig::Builder::default()
            .collect(from_env())
            .build()
            .map_err(|e| MetaStartupError::InvalidConfig(e.to_string()))?;
        builder = builder.collect(from_self(cfg_via_env.into()));

        // Finally, load from args.
        if with_args {
            builder = builder.collect(from_self(arg_conf));
        }

        builder
            .build()
            .map_err(|e| MetaStartupError::InvalidConfig(e.to_string()))
    }
}

/// #[serde(flatten)] doesn't work correctly for env.
/// We should work around it by flatten them manually.
/// We are seeking for better solutions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConfigViaEnv {
    pub metasrv_config_file: String,
    pub metasrv_log_level: String,
    pub metasrv_log_dir: String,
    pub metasrv_log_file_on: bool,
    pub metasrv_log_file_level: String,
    pub metasrv_log_file_dir: String,
    pub metasrv_log_file_format: String,
    pub metasrv_log_file_limit: usize,
    pub metasrv_log_stderr_on: bool,
    pub metasrv_log_stderr_level: String,
    pub metasrv_log_stderr_format: String,
    pub admin_api_address: String,
    pub admin_tls_server_cert: String,
    pub admin_tls_server_key: String,
    pub metasrv_grpc_api_address: String,
    pub metasrv_grpc_api_advertise_host: Option<String>,
    pub grpc_tls_server_cert: String,
    pub grpc_tls_server_key: String,

    pub config_id: String,
    pub kvsrv_listen_host: String,
    pub kvsrv_advertise_host: String,
    pub kvsrv_api_port: u16,
    pub kvsrv_raft_dir: String,
    pub kvsrv_no_sync: bool,
    pub kvsrv_snapshot_logs_since_last: u64,
    pub kvsrv_heartbeat_interval: u64,
    pub kvsrv_install_snapshot_timeout: u64,
    pub kvsrv_wait_leader_timeout: u64,
    pub raft_max_applied_log_to_keep: u64,
    pub raft_snapshot_chunk_size: u64,
    pub kvsrv_single: bool,
    pub metasrv_join: Vec<String>,
    pub kvsrv_id: u64,
    pub sled_tree_prefix: String,
    pub sled_max_cache_size_mb: u64,
    pub cluster_name: String,
}

impl Default for ConfigViaEnv {
    fn default() -> Self {
        Config::default().into()
    }
}

impl From<Config> for ConfigViaEnv {
    fn from(cfg: Config) -> ConfigViaEnv {
        Self {
            metasrv_config_file: cfg.config_file,
            metasrv_log_level: cfg.log.file.file_level.clone(),
            metasrv_log_dir: cfg.log.file.file_dir.clone(),
            metasrv_log_file_on: cfg.log.file.file_on,
            metasrv_log_file_level: cfg.log.file.file_level,
            metasrv_log_file_dir: cfg.log.file.file_dir,
            metasrv_log_file_format: cfg.log.file.file_format,
            metasrv_log_file_limit: cfg.log.file.file_limit,
            metasrv_log_stderr_on: cfg.log.stderr.stderr_on,
            metasrv_log_stderr_level: cfg.log.stderr.stderr_level,
            metasrv_log_stderr_format: cfg.log.stderr.stderr_format,
            admin_api_address: cfg.admin_api_address,
            admin_tls_server_cert: cfg.admin_tls_server_cert,
            admin_tls_server_key: cfg.admin_tls_server_key,
            metasrv_grpc_api_address: cfg.grpc_api_address,
            metasrv_grpc_api_advertise_host: cfg.grpc_api_advertise_host,
            grpc_tls_server_cert: cfg.grpc_tls_server_cert,
            grpc_tls_server_key: cfg.grpc_tls_server_key,
            config_id: cfg.raft_config.config_id,
            kvsrv_listen_host: cfg.raft_config.raft_listen_host,
            kvsrv_advertise_host: cfg.raft_config.raft_advertise_host,
            kvsrv_api_port: cfg.raft_config.raft_api_port,
            kvsrv_raft_dir: cfg.raft_config.raft_dir,
            kvsrv_no_sync: cfg.raft_config.no_sync,
            kvsrv_snapshot_logs_since_last: cfg.raft_config.snapshot_logs_since_last,
            kvsrv_heartbeat_interval: cfg.raft_config.heartbeat_interval,
            kvsrv_install_snapshot_timeout: cfg.raft_config.install_snapshot_timeout,
            kvsrv_wait_leader_timeout: cfg.raft_config.wait_leader_timeout,
            raft_max_applied_log_to_keep: cfg.raft_config.max_applied_log_to_keep,
            raft_snapshot_chunk_size: cfg.raft_config.snapshot_chunk_size,
            kvsrv_single: cfg.raft_config.single,
            metasrv_join: cfg.raft_config.join,
            kvsrv_id: cfg.raft_config.id,
            sled_tree_prefix: cfg.raft_config.sled_tree_prefix,
            sled_max_cache_size_mb: cfg.raft_config.sled_max_cache_size_mb,
            cluster_name: cfg.raft_config.cluster_name,
        }
    }
}

// Implement Into target on ConfigViaEnv to make the transform logic more clear.
#[allow(clippy::from_over_into)]
impl Into<Config> for ConfigViaEnv {
    fn into(self) -> Config {
        let raft_config = RaftConfig {
            config_id: self.config_id,
            raft_listen_host: self.kvsrv_listen_host,
            raft_advertise_host: self.kvsrv_advertise_host,
            raft_api_port: self.kvsrv_api_port,
            raft_dir: self.kvsrv_raft_dir,
            no_sync: self.kvsrv_no_sync,
            snapshot_logs_since_last: self.kvsrv_snapshot_logs_since_last,
            heartbeat_interval: self.kvsrv_heartbeat_interval,
            install_snapshot_timeout: self.kvsrv_install_snapshot_timeout,
            wait_leader_timeout: self.kvsrv_wait_leader_timeout,
            max_applied_log_to_keep: self.raft_max_applied_log_to_keep,
            snapshot_chunk_size: self.raft_snapshot_chunk_size,
            single: self.kvsrv_single,
            join: self.metasrv_join,
            // Do not allow to leave via environment variable
            leave_via: vec![],
            // Do not allow to leave via environment variable
            leave_id: None,
            id: self.kvsrv_id,
            sled_tree_prefix: self.sled_tree_prefix,
            sled_max_cache_size_mb: self.sled_max_cache_size_mb,
            cluster_name: self.cluster_name,
        };
        let log_config = LogConfig {
            file: FileLogConfig {
                file_on: self.metasrv_log_file_on,
                file_level: self.metasrv_log_file_level,
                file_dir: self.metasrv_log_file_dir,
                file_format: self.metasrv_log_file_format,
                file_limit: self.metasrv_log_file_limit,
                file_prefix_filter: "databend_,openraft".to_string(),
            },
            stderr: StderrLogConfig {
                stderr_on: self.metasrv_log_stderr_on,
                stderr_level: self.metasrv_log_stderr_level,
                stderr_format: self.metasrv_log_stderr_format,
            },
        };

        Config {
            // cmd, key, value and prefix should only be passed in from CLI
            cmd: "".to_string(),
            key: vec![],
            value: "".to_string(),
            expire_after: None,
            prefix: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            config_file: self.metasrv_config_file,
            log_level: self.metasrv_log_level,
            log_dir: self.metasrv_log_dir,
            log: log_config,
            admin_api_address: self.admin_api_address,
            admin_tls_server_cert: self.admin_tls_server_cert,
            admin_tls_server_key: self.admin_tls_server_key,
            grpc_api_address: self.metasrv_grpc_api_address,
            grpc_api_advertise_host: self.metasrv_grpc_api_advertise_host,
            grpc_tls_server_cert: self.grpc_tls_server_cert,
            grpc_tls_server_key: self.grpc_tls_server_key,
            raft_config,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
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
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    #[clap(long, default_value_t = get_default_raft_advertise_host())]
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    #[clap(long, default_value = "28004")]
    pub raft_api_port: u16,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./.databend/meta")]
    pub raft_dir: String,

    /// Whether to fsync meta to disk for every meta write(raft log, state machine etc).
    /// No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING.
    #[clap(long)]
    pub no_sync: bool,

    /// The number of logs since the last snapshot to trigger next snapshot.
    #[clap(long, default_value = "1024")]
    pub snapshot_logs_since_last: u64,

    /// The interval in milli seconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    /// This value `t` also affect the election timeout:
    /// Election timeout is a random between `[t*2, t*3)`,
    /// i.e., a node start to elect in `[t*2, t*3)` without RequestVote from Candidate.
    /// And a follower starts to elect after `[t*5, t*6)` without heartbeat from Leader.
    #[clap(long, default_value = "500")]
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, default_value = "4000")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, default_value = "1000")]
    pub max_applied_log_to_keep: u64,

    /// The size of chunk for transmitting snapshot. The default is 4MB
    #[clap(long, default_value = "4194304")]
    pub snapshot_chunk_size: u64,

    /// Start databend-meta in single node mode.
    /// It initialize a single node cluster, if meta data is not initialized.
    /// If on-disk data is already initialized, this argument has no effect.
    #[clap(long)]
    pub single: bool,

    /// Bring up a databend-meta node and join a cluster.
    ///
    /// It will take effect only when the meta data is not initialized.
    /// If on-disk data is already initialized, this argument has no effect.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long)]
    pub join: Vec<String>,

    /// Do not run databend-meta, but just remove a node from its cluster via the provided endpoints.
    ///
    /// This node will be removed by `id`.
    #[clap(long)]
    pub leave_via: Vec<String>,

    /// The node id to leave from a cluster.
    #[clap(long)]
    pub leave_id: Option<u64>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    pub id: u64,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,

    /// The maximum memory in MB that sled can use for caching. Default is 10GB
    #[clap(long, default_value = "10240")]
    pub sled_max_cache_size_mb: u64,

    /// The node name. If the user specifies a name, the user-supplied name is used,
    /// if not, the default name is used
    #[clap(long, default_value = "foo_cluster")]
    pub cluster_name: String,

    /// Max timeout(in milli seconds) when waiting a cluster leader.
    #[clap(long, default_value = "180000")]
    pub wait_leader_timeout: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        InnerRaftConfig::default().into()
    }
}

impl From<RaftConfig> for InnerRaftConfig {
    fn from(x: RaftConfig) -> InnerRaftConfig {
        InnerRaftConfig {
            config_id: x.config_id,
            raft_listen_host: x.raft_listen_host,
            raft_advertise_host: x.raft_advertise_host,
            raft_api_port: x.raft_api_port,
            raft_dir: x.raft_dir,
            no_sync: x.no_sync,
            snapshot_logs_since_last: x.snapshot_logs_since_last,
            heartbeat_interval: x.heartbeat_interval,
            install_snapshot_timeout: x.install_snapshot_timeout,
            max_applied_log_to_keep: x.max_applied_log_to_keep,
            snapshot_chunk_size: x.snapshot_chunk_size,
            single: x.single,
            join: x.join,
            leave_via: x.leave_via,
            leave_id: x.leave_id,
            id: x.id,
            sled_tree_prefix: x.sled_tree_prefix,
            sled_max_cache_size_mb: x.sled_max_cache_size_mb,
            cluster_name: x.cluster_name,
            wait_leader_timeout: x.wait_leader_timeout,
        }
    }
}

impl From<InnerRaftConfig> for RaftConfig {
    fn from(inner: InnerRaftConfig) -> Self {
        Self {
            config_id: inner.config_id,
            raft_listen_host: inner.raft_listen_host,
            raft_advertise_host: inner.raft_advertise_host,
            raft_api_port: inner.raft_api_port,
            raft_dir: inner.raft_dir,
            no_sync: inner.no_sync,
            snapshot_logs_since_last: inner.snapshot_logs_since_last,
            heartbeat_interval: inner.heartbeat_interval,
            install_snapshot_timeout: inner.install_snapshot_timeout,
            max_applied_log_to_keep: inner.max_applied_log_to_keep,
            snapshot_chunk_size: inner.snapshot_chunk_size,
            single: inner.single,
            join: inner.join,
            leave_via: inner.leave_via,
            leave_id: inner.leave_id,
            id: inner.id,
            sled_tree_prefix: inner.sled_tree_prefix,
            sled_max_cache_size_mb: inner.sled_max_cache_size_mb,
            cluster_name: inner.cluster_name,
            wait_leader_timeout: inner.wait_leader_timeout,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LogConfig {
    #[clap(flatten)]
    pub file: FileLogConfig,

    #[clap(flatten)]
    pub stderr: StderrLogConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        InnerLogConfig::default().into()
    }
}

#[allow(clippy::from_over_into)]
impl Into<InnerLogConfig> for LogConfig {
    fn into(self) -> InnerLogConfig {
        InnerLogConfig {
            file: self.file.into(),
            stderr: self.stderr.into(),
            otlp: OTLPConfig::default(),
            query: QueryLogConfig::default(),
            profile: ProfileLogConfig::default(),
            structlog: StructLogConfig::default(),
            tracing: TracingConfig::default(),
        }
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            file: inner.file.into(),
            stderr: inner.stderr.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FileLogConfig {
    #[clap(long = "log-file-on", default_value = "true", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    #[serde(rename = "on")]
    pub file_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-file-level", default_value = "INFO")]
    #[serde(rename = "level")]
    pub file_level: String,

    /// Log file dir
    #[clap(long = "log-file-dir", default_value = "./.databend/logs")]
    #[serde(rename = "dir")]
    pub file_dir: String,

    /// Log file format
    #[clap(long = "log-file-format", default_value = "json")]
    #[serde(rename = "format")]
    pub file_format: String,

    /// Log file max
    #[clap(long = "log-file-limit", default_value = "48")]
    #[serde(rename = "limit")]
    pub file_limit: usize,

    /// Log prefix filter, separated by comma.
    /// For example, `"databend_,openraft"` enables logging for `databend_*` crates and `openraft` crate.
    /// This filter does not affect `WARNING` and `ERROR` log.
    #[clap(long = "log-file-prefix-filter", default_value = "databend_,openraft")]
    #[serde(rename = "prefix_filter")]
    pub file_prefix_filter: String,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        InnerFileLogConfig::default().into()
    }
}

#[allow(clippy::from_over_into)]
impl Into<InnerFileLogConfig> for FileLogConfig {
    fn into(self) -> InnerFileLogConfig {
        InnerFileLogConfig {
            on: self.file_on,
            level: self.file_level,
            dir: self.file_dir,
            format: self.file_format,
            limit: self.file_limit,
            prefix_filter: self.file_prefix_filter,
        }
    }
}

impl From<InnerFileLogConfig> for FileLogConfig {
    fn from(inner: InnerFileLogConfig) -> Self {
        Self {
            file_on: inner.on,
            file_level: inner.level,
            file_dir: inner.dir,
            file_format: inner.format,
            file_limit: inner.limit,
            file_prefix_filter: inner.prefix_filter,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StderrLogConfig {
    #[clap(long = "log-stderr-on", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    #[serde(rename = "on")]
    pub stderr_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-stderr-level", default_value = "INFO")]
    #[serde(rename = "level")]
    pub stderr_level: String,

    #[clap(long = "log-stderr-format", default_value = "text")]
    #[serde(rename = "format")]
    pub stderr_format: String,
}

impl Default for StderrLogConfig {
    fn default() -> Self {
        InnerStderrLogConfig::default().into()
    }
}

#[allow(clippy::from_over_into)]
impl Into<InnerStderrLogConfig> for StderrLogConfig {
    fn into(self) -> InnerStderrLogConfig {
        InnerStderrLogConfig {
            on: std::env::var("LOG_STDERR_ON").as_deref() == Ok("true")
                || std::env::var("RUST_LOG").is_ok()
                || self.stderr_on,
            level: std::env::var("RUST_LOG").unwrap_or(self.stderr_level),
            format: self.stderr_format,
        }
    }
}

impl From<InnerStderrLogConfig> for StderrLogConfig {
    fn from(inner: InnerStderrLogConfig) -> Self {
        Self {
            stderr_on: inner.on,
            stderr_level: inner.level,
            stderr_format: inner.format,
        }
    }
}
