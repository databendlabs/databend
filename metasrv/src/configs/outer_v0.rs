// Copyright 2022 Datafuse Labs.
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

use clap::Args;
use clap::Parser;
use common_meta_raft_store::config::get_default_raft_advertise_host;
use common_meta_raft_store::config::RaftConfig as InnerRaftConfig;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_tracing::Config as InnerLogConfig;
use common_tracing::FileConfig as InnerFileLogConfig;
use common_tracing::StderrConfig as InnerStderrLogConfig;
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
    /// - `ver`: print version and quit.
    /// - `show-config`: print effective config and quit.
    /// - `kvapi::<cmd>`: run kvapi command. The command can be `upsert`, `get`, `mget` and `list`
    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(long, default_value = "", multiple = true)]
    pub key: Vec<String>,

    #[clap(long, default_value = "")]
    pub value: String,

    #[clap(long, default_value = "")]
    pub prefix: String,

    #[clap(long, default_value = "root")]
    pub username: String,

    #[clap(long, default_value = "")]
    pub password: String,

    #[clap(long, short = 'c', default_value = "")]
    pub config_file: String,

    #[clap(flatten)]
    pub log: LogConfig,

    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_cert: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_key: String,

    #[clap(long, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

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
    fn from(x: Config) -> Self {
        InnerConfig {
            cmd: x.cmd,
            key: x.key,
            value: x.value,
            prefix: x.prefix,
            username: x.username,
            password: x.password,
            config_file: x.config_file,
            log: x.log.into(),
            admin_api_address: x.admin_api_address,
            admin_tls_server_cert: x.admin_tls_server_cert,
            admin_tls_server_key: x.admin_tls_server_key,
            grpc_api_address: x.grpc_api_address,
            grpc_tls_server_cert: x.grpc_tls_server_cert,
            grpc_tls_server_key: x.grpc_tls_server_key,
            raft_config: x.raft_config.into(),
        }
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self {
            cmd: inner.cmd,
            key: inner.key,
            value: inner.value,
            prefix: inner.prefix,
            username: inner.username,
            password: inner.password,
            config_file: inner.config_file,
            log: inner.log.into(),
            admin_api_address: inner.admin_api_address,
            admin_tls_server_cert: inner.admin_tls_server_cert,
            admin_tls_server_key: inner.admin_tls_server_key,
            grpc_api_address: inner.grpc_api_address,
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
    pub fn load() -> MetaResult<Self> {
        let arg_conf = Self::parse();

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let config_file = if !arg_conf.config_file.is_empty() {
                arg_conf.config_file.clone()
            } else if let Ok(path) = env::var("METASRV_CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            builder = builder.collect(from_file(Toml, &config_file));
        }

        // Then, load from env.
        let cfg_via_env: ConfigViaEnv = serfig::Builder::default()
            .collect(from_env())
            .build()
            .map_err(|e| MetaError::InvalidConfig(e.to_string()))?;
        builder = builder.collect(from_self(cfg_via_env.into()));

        // Finally, load from args.
        builder = builder.collect(from_self(arg_conf));

        builder
            .build()
            .map_err(|e| MetaError::InvalidConfig(e.to_string()))
    }
}

/// #[serde(flatten)] doesn't work correctly for env.
/// We should work around it by flatten them manully.
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
    pub metasrv_log_stderr_on: bool,
    pub metasrv_log_stderr_level: String,
    pub admin_api_address: String,
    pub admin_tls_server_cert: String,
    pub admin_tls_server_key: String,
    pub metasrv_grpc_api_address: String,
    pub grpc_tls_server_cert: String,
    pub grpc_tls_server_key: String,

    pub config_id: String,
    pub kvsrv_listen_host: String,
    pub kvsrv_advertise_host: String,
    pub kvsrv_api_port: u32,
    pub kvsrv_raft_dir: String,
    pub kvsrv_no_sync: bool,
    pub kvsrv_snapshot_logs_since_last: u64,
    pub kvsrv_heartbeat_interval: u64,
    pub kvsrv_install_snapshot_timeout: u64,
    pub raft_max_applied_log_to_keep: u64,
    pub kvsrv_single: bool,
    pub metasrv_join: Vec<String>,
    pub kvsrv_id: u64,
    pub sled_tree_prefix: String,
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
            metasrv_log_level: "INFO".to_string(),
            metasrv_log_dir: "./.databend/logs".to_string(),
            metasrv_log_file_on: cfg.log.file.on,
            metasrv_log_file_level: cfg.log.file.level,
            metasrv_log_file_dir: cfg.log.file.dir,
            metasrv_log_stderr_on: cfg.log.stderr.on,
            metasrv_log_stderr_level: cfg.log.stderr.level,
            admin_api_address: cfg.admin_api_address,
            admin_tls_server_cert: cfg.admin_tls_server_cert,
            admin_tls_server_key: cfg.admin_tls_server_key,
            metasrv_grpc_api_address: cfg.grpc_api_address,
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
            raft_max_applied_log_to_keep: cfg.raft_config.max_applied_log_to_keep,
            kvsrv_single: cfg.raft_config.single,
            metasrv_join: cfg.raft_config.join,
            kvsrv_id: cfg.raft_config.id,
            sled_tree_prefix: cfg.raft_config.sled_tree_prefix,
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
            max_applied_log_to_keep: self.raft_max_applied_log_to_keep,
            single: self.kvsrv_single,
            join: self.metasrv_join,
            // Do not allow to leave via environment variable
            leave_via: vec![],
            // Do not allow to leave via environment variable
            leave_id: None,
            id: self.kvsrv_id,
            sled_tree_prefix: self.sled_tree_prefix,
            cluster_name: self.cluster_name,
        };
        let log_config = LogConfig {
            level: self.metasrv_log_level,
            dir: self.metasrv_log_dir,
            file: FileLogConfig {
                on: self.metasrv_log_file_on,
                level: self.metasrv_log_file_level,
                dir: self.metasrv_log_file_dir,
            },
            stderr: StderrLogConfig {
                on: self.metasrv_log_stderr_on,
                level: self.metasrv_log_stderr_level,
            },
        };

        Config {
            // cmd, key, value and prefix should only be passed in from CLI
            cmd: "".to_string(),
            key: vec![],
            value: "".to_string(),
            prefix: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            config_file: self.metasrv_config_file,
            log: log_config,
            admin_api_address: self.admin_api_address,
            admin_tls_server_cert: self.admin_tls_server_cert,
            admin_tls_server_key: self.admin_tls_server_key,
            grpc_api_address: self.metasrv_grpc_api_address,
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
    pub raft_api_port: u32,

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
    #[clap(long, default_value = "1000")]
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, default_value = "4000")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, default_value = "1000")]
    pub max_applied_log_to_keep: u64,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    #[clap(long)]
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long, multiple_occurrences = true, multiple_values = true)]
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
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    pub id: u64,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,

    /// Tne node name. If the user specifies a name, the user-supplied name is used,
    /// if not, the default name is used
    #[clap(long, default_value = "foo_cluster")]
    pub cluster_name: String,
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
            single: x.single,
            join: x.join,
            leave_via: x.leave_via,
            leave_id: x.leave_id,
            id: x.id,
            sled_tree_prefix: x.sled_tree_prefix,
            cluster_name: x.cluster_name,
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
            single: inner.single,
            join: inner.join,
            leave_via: inner.leave_via,
            leave_id: inner.leave_id,
            id: inner.id,
            sled_tree_prefix: inner.sled_tree_prefix,
            cluster_name: inner.cluster_name,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-level", default_value = "INFO")]
    #[serde(alias = "log_level")]
    pub level: String,

    /// Log file dir
    #[clap(long = "log-dir", default_value = "./.databend/logs")]
    #[serde(alias = "log_dir")]
    pub dir: String,

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

impl Into<InnerLogConfig> for LogConfig {
    fn into(self) -> InnerLogConfig {
        let mut file: InnerFileLogConfig = self.file.into();
        if self.level != "INFO" {
            file.level = self.level.to_string();
        }
        if self.dir != "./.databend/logs" {
            file.dir = self.dir.to_string();
        }

        InnerLogConfig {
            file,
            stderr: self.stderr.into(),
        }
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            level: "INFO".to_string(),
            dir: "./.databend/logs".to_string(),
            file: inner.file.into(),
            stderr: inner.stderr.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FileLogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-file-on")]
    pub on: bool,

    #[clap(long = "log-file-level")]
    pub level: String,

    /// Log file dir
    #[clap(long = "log-file-dir", default_value = "./.databend/logs")]
    pub dir: String,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        InnerFileLogConfig::default().into()
    }
}

impl Into<InnerFileLogConfig> for FileLogConfig {
    fn into(self) -> InnerFileLogConfig {
        InnerFileLogConfig {
            on: self.on,
            level: self.level,
            dir: self.dir,
        }
    }
}

impl From<InnerFileLogConfig> for FileLogConfig {
    fn from(inner: InnerFileLogConfig) -> Self {
        Self {
            on: inner.on,
            level: inner.level,
            dir: inner.dir,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StderrLogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-stderr-on")]
    pub on: bool,

    #[clap(long = "log-stderr-level")]
    pub level: String,
}

impl Default for StderrLogConfig {
    fn default() -> Self {
        InnerStderrLogConfig::default().into()
    }
}

impl Into<InnerStderrLogConfig> for StderrLogConfig {
    fn into(self) -> InnerStderrLogConfig {
        InnerStderrLogConfig {
            on: self.on,
            level: self.level,
        }
    }
}

impl From<InnerStderrLogConfig> for StderrLogConfig {
    fn from(inner: InnerStderrLogConfig) -> Self {
        Self {
            on: inner.on,
            level: inner.level,
        }
    }
}
