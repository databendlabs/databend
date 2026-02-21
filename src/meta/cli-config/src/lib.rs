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

//! CLI configuration parsing layer for databend-meta.
//!
//! This crate provides the "outer" configuration struct that maps to CLI arguments
//! and config files, then converts to the inner [`databend_meta::configs::MetaServiceConfig`].
//!
//! The separation allows the service library (`databend-meta`) to remain free of
//! CLI-specific dependencies like `clap` and `serfig`.

use std::env;
use std::net::SocketAddr;
use std::sync::LazyLock;

use clap::ArgAction;
use clap::Args;
use clap::Parser;
use databend_common_config::StorageConfig;
use databend_common_storage::StorageConfig as InnerStorageConfig;
use databend_common_tracing::CONFIG_DEFAULT_LOG_LEVEL;
use databend_common_tracing::Config as InnerLogConfig;
use databend_common_tracing::FileConfig as InnerFileLogConfig;
use databend_common_tracing::HistoryConfig as InnerLogHistoryConfig;
use databend_common_tracing::LogFormat;
use databend_common_tracing::OTLPConfig;
use databend_common_tracing::ProfileLogConfig;
use databend_common_tracing::QueryLogConfig;
use databend_common_tracing::StderrConfig as InnerStderrLogConfig;
use databend_common_tracing::StructLogConfig;
use databend_common_tracing::TracingConfig;
use databend_common_version::DATABEND_GIT_SEMVER;
use databend_common_version::VERGEN_BUILD_TIMESTAMP;
use databend_common_version::VERGEN_GIT_SHA;
use databend_common_version::VERGEN_RUSTC_SEMVER;
use databend_meta::configs::AdminConfig;
use databend_meta::configs::GrpcConfig;
use databend_meta::configs::MetaServiceConfig;
use databend_meta::configs::TlsConfig;
use databend_meta_raft_store::config::RaftConfig as InnerRaftConfig;
use databend_meta_raft_store::config::get_default_raft_advertise_host;
use databend_meta_raft_store::ondisk::DATA_VERSION;
use databend_meta_types::MetaStartupError;
use databend_meta_ver::MIN_QUERY_VER_FOR_METASRV;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

/// Full version string for databend-meta including build info, min client version, and data version
static FULL_VERSION: LazyLock<String> = LazyLock::new(|| {
    let rustc_semver = VERGEN_RUSTC_SEMVER;
    let timestamp = VERGEN_BUILD_TIMESTAMP;

    let first_line = match (rustc_semver, timestamp) {
        (Some(rustc_semver), Some(timestamp)) => {
            format!("{DATABEND_GIT_SEMVER}-{VERGEN_GIT_SHA}(rust-{rustc_semver}-{timestamp})")
        }
        _ => format!("{DATABEND_GIT_SEMVER}-{VERGEN_GIT_SHA}"),
    };

    format!(
        "{}\nmin-compatible-client-version: {}\ndata-version: {:?}",
        first_line, *MIN_QUERY_VER_FOR_METASRV, DATA_VERSION
    )
});

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, author, version = &**FULL_VERSION)]
#[serde(default)]
pub struct Config {
    /// Run a command
    ///
    /// Supported commands:
    ///
    /// - `ver`: print version and quit.
    ///
    /// - `show-config`: print effective config and quit.
    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(long, short = 'c', default_value = "")]
    pub config_file: String,

    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-level", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
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

    /// Maximum message size for MetaService gRPC communication (in bytes).
    /// Default: 32MB (33554432).
    #[clap(long)]
    pub grpc_api_max_message_size: Option<usize>,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        MetaConfig::default().into()
    }
}

/// Full startup configuration for databend-meta.
///
/// This struct combines CLI-specific fields (cmd, config_file, log, admin)
/// with the core service configuration. It's used by the binary entry point,
/// while the inner `MetaServiceConfig` is used by the service library.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MetaConfig {
    /// Command to run (e.g., "ver", "show-config")
    pub cmd: String,
    /// Path to the config file
    pub config_file: String,
    /// Logging configuration
    pub log: InnerLogConfig,
    /// Admin HTTP API configuration
    pub admin: AdminConfig,
    /// Core service configuration (grpc + raft)
    pub service: MetaServiceConfig,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            cmd: "".to_string(),
            config_file: "".to_string(),
            log: InnerLogConfig::default(),
            admin: AdminConfig {
                api_address: "127.0.0.1:28002".to_string(),
                tls: TlsConfig::default(),
            },
            service: MetaServiceConfig::default(),
        }
    }
}

fn parse_host_port(addr: &str) -> (String, Option<u16>) {
    match addr.parse::<SocketAddr>() {
        Ok(sa) => (sa.ip().to_string(), Some(sa.port())),
        Err(_) => (addr.to_string(), None),
    }
}

impl TryFrom<Config> for MetaConfig {
    type Error = String;

    fn try_from(outer: Config) -> Result<Self, Self::Error> {
        let mut log: InnerLogConfig = outer.log.try_into()?;
        if outer.log_level != CONFIG_DEFAULT_LOG_LEVEL {
            log.file.level = outer.log_level.to_string();
        }
        if outer.log_dir != "./.databend/logs" {
            log.file.dir = outer.log_dir.to_string();
        }

        let (listen_host, listen_port) = parse_host_port(&outer.grpc_api_address);

        Ok(MetaConfig {
            cmd: outer.cmd,
            config_file: outer.config_file,
            log,
            admin: AdminConfig {
                api_address: outer.admin_api_address,
                tls: TlsConfig {
                    cert: outer.admin_tls_server_cert,
                    key: outer.admin_tls_server_key,
                },
            },
            service: MetaServiceConfig {
                grpc: GrpcConfig {
                    listen_host,
                    listen_port,
                    advertise_host: outer.grpc_api_advertise_host,
                    tls: TlsConfig {
                        cert: outer.grpc_tls_server_cert,
                        key: outer.grpc_tls_server_key,
                    },
                    max_message_size: outer.grpc_api_max_message_size,
                },
                raft_config: outer.raft_config.into(),
            },
        })
    }
}

impl From<MetaConfig> for Config {
    fn from(inner: MetaConfig) -> Self {
        let grpc_api_address = inner
            .service
            .grpc
            .api_address()
            .unwrap_or_else(|| inner.service.grpc.listen_host.clone());

        Self {
            cmd: inner.cmd,
            config_file: inner.config_file,
            log_level: inner.log.file.level.clone(),
            log_dir: inner.log.file.dir.clone(),
            log: inner.log.into(),
            admin_api_address: inner.admin.api_address,
            admin_tls_server_cert: inner.admin.tls.cert,
            admin_tls_server_key: inner.admin.tls.key,
            grpc_api_address,
            grpc_api_advertise_host: inner.service.grpc.advertise_host,
            grpc_tls_server_cert: inner.service.grpc.tls.cert,
            grpc_tls_server_key: inner.service.grpc.tls.key,
            grpc_api_max_message_size: inner.service.grpc.max_message_size,
            raft_config: inner.service.raft_config.into(),
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

        // Finally, load from args.
        if with_args {
            builder = builder.collect(from_self(arg_conf));
        }

        builder
            .build()
            .map_err(|e| MetaStartupError::InvalidConfig(e.to_string()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct RaftConfig {
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

    /// The listening port for raft communication.
    #[clap(long, default_value = "28004")]
    pub raft_api_port: u16,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./.databend/meta")]
    pub raft_dir: String,

    /// The maximum number of log entries for log entries cache. Default value is 1_000_000.
    #[clap(long, default_value = "1000000")]
    pub log_cache_max_items: u64,

    /// The maximum memory in bytes for the log entries cache. Default value is 1G.
    #[clap(long, default_value = "1073741824")]
    pub log_cache_capacity: u64,

    /// Maximum number of records in a chunk of raft-log WAL. Default value is 100_000.
    #[clap(long, default_value = "100000")]
    pub log_wal_chunk_max_records: u64,

    /// Maximum size in bytes for a chunk of raft-log WAL. Default value si 256M
    #[clap(long, default_value = "268435456")]
    pub log_wal_chunk_max_size: u64,

    /// The number of logs since the last snapshot to trigger next snapshot.
    #[clap(long, default_value = "1024")]
    pub snapshot_logs_since_last: u64,

    /// The interval in milliseconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    /// This value `t` also affect the election timeout:
    /// Election timeout is a random between `[t*2, t*3)`,
    /// i.e., a node start to elect in `[t*2, t*3)` without RequestVote from Candidate.
    /// And a follower starts to elect after `[t*5, t*6)` without heartbeat from Leader.
    #[clap(long, default_value = "500")]
    pub heartbeat_interval: u64,

    /// The max time in milliseconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, default_value = "4000")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, default_value = "1000")]
    pub max_applied_log_to_keep: u64,

    /// The size of chunk for transmitting snapshot. The default is 4MB
    #[clap(long, default_value = "4194304")]
    pub snapshot_chunk_size: u64,

    /// Whether to check keys fed to snapshot are sorted.
    #[clap(long, default_value = "true")]
    pub snapshot_db_debug_check: bool,

    /// The maximum number of keys allowed in a block within a snapshot db.
    ///
    /// A block serves as the caching unit in a snapshot database.
    /// Smaller blocks enable more granular cache control but may increase the index size.
    #[clap(long, default_value = "8000")]
    pub snapshot_db_block_keys: u64,

    /// The total block to cache.
    #[clap(long, default_value = "1024")]
    pub snapshot_db_block_cache_item: u64,

    /// The total cache size for snapshot blocks.
    ///
    /// By default, it is 1GB.
    #[clap(long, default_value = "1073741824")]
    pub snapshot_db_block_cache_size: u64,

    /// Interval in milliseconds to compact the in memory immutable levels.
    ///
    /// Set to 0 to disable automatic compaction. Default is 1000 ms
    ///
    /// This reduces the writable level and improves read performance.
    #[clap(long)]
    pub compact_immutables_ms: Option<u64>,

    /// Start databend-meta in single node mode.
    /// It initializes a single node cluster, if meta data is not initialized.
    /// If on-disk data is already initialized, this argument has no effect.
    #[clap(long)]
    pub single: bool,

    /// Bring up a databend-meta node and join a cluster.
    ///
    /// It will take effect only when the metadata is not initialized.
    /// If on-disk data is already initialized, this argument has no effect.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long)]
    pub join: Vec<String>,

    /// Whether a node is joined the cluster as learner.
    ///
    /// It is only used when `join` is provided.
    ///
    /// A learner does not vote in elections and does not count towards quorum.
    #[clap(long)]
    pub learner: bool,

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
    ///  Otherwise, this argument is ignored.
    #[clap(long, default_value = "0")]
    pub id: u64,

    /// The node name. If the user specifies a name, the user-supplied name is used,
    /// if not, the default name is used
    #[clap(long, default_value = "foo_cluster")]
    pub cluster_name: String,

    /// Max timeout(in milliseconds) when waiting a cluster leader.
    #[clap(long, default_value = "180000")]
    pub wait_leader_timeout: u64,

    /// Maximum message size for Raft gRPC communication (in bytes).
    /// Default: 32MB (33554432).
    #[clap(long)]
    pub raft_grpc_max_message_size: Option<usize>,
}

// TODO(rotbl): should not be used.
impl Default for RaftConfig {
    fn default() -> Self {
        InnerRaftConfig::default().into()
    }
}

impl From<RaftConfig> for InnerRaftConfig {
    fn from(x: RaftConfig) -> InnerRaftConfig {
        InnerRaftConfig {
            config_id: "".to_string(),
            raft_listen_host: x.raft_listen_host,
            raft_advertise_host: x.raft_advertise_host,
            raft_api_port: x.raft_api_port,
            raft_dir: x.raft_dir,

            log_cache_max_items: x.log_cache_max_items,
            log_cache_capacity: x.log_cache_capacity,
            log_wal_chunk_max_records: x.log_wal_chunk_max_records,
            log_wal_chunk_max_size: x.log_wal_chunk_max_size,

            snapshot_logs_since_last: x.snapshot_logs_since_last,
            heartbeat_interval: x.heartbeat_interval,
            install_snapshot_timeout: x.install_snapshot_timeout,
            max_applied_log_to_keep: x.max_applied_log_to_keep,
            snapshot_chunk_size: x.snapshot_chunk_size,

            snapshot_db_debug_check: x.snapshot_db_debug_check,
            snapshot_db_block_keys: x.snapshot_db_block_keys,
            snapshot_db_block_cache_item: x.snapshot_db_block_cache_item,
            snapshot_db_block_cache_size: x.snapshot_db_block_cache_size,

            compact_immutables_ms: x.compact_immutables_ms,
            single: x.single,
            join: x.join,
            learner: x.learner,
            leave_via: x.leave_via,
            leave_id: x.leave_id,
            id: x.id,
            cluster_name: x.cluster_name,
            wait_leader_timeout: x.wait_leader_timeout,
            raft_grpc_max_message_size: x.raft_grpc_max_message_size,
        }
    }
}

impl From<InnerRaftConfig> for RaftConfig {
    fn from(inner: InnerRaftConfig) -> Self {
        Self {
            raft_listen_host: inner.raft_listen_host,
            raft_advertise_host: inner.raft_advertise_host,
            raft_api_port: inner.raft_api_port,
            raft_dir: inner.raft_dir,

            log_cache_max_items: inner.log_cache_max_items,
            log_cache_capacity: inner.log_cache_capacity,
            log_wal_chunk_max_records: inner.log_wal_chunk_max_records,
            log_wal_chunk_max_size: inner.log_wal_chunk_max_size,

            snapshot_logs_since_last: inner.snapshot_logs_since_last,
            heartbeat_interval: inner.heartbeat_interval,
            install_snapshot_timeout: inner.install_snapshot_timeout,
            max_applied_log_to_keep: inner.max_applied_log_to_keep,
            snapshot_chunk_size: inner.snapshot_chunk_size,

            snapshot_db_debug_check: inner.snapshot_db_debug_check,
            snapshot_db_block_keys: inner.snapshot_db_block_keys,
            snapshot_db_block_cache_item: inner.snapshot_db_block_cache_item,
            snapshot_db_block_cache_size: inner.snapshot_db_block_cache_size,

            compact_immutables_ms: inner.compact_immutables_ms,
            single: inner.single,
            join: inner.join,
            learner: inner.learner,
            leave_via: inner.leave_via,
            leave_id: inner.leave_id,
            id: inner.id,
            cluster_name: inner.cluster_name,
            wait_leader_timeout: inner.wait_leader_timeout,
            raft_grpc_max_message_size: inner.raft_grpc_max_message_size,
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

    #[clap(flatten)]
    pub storage: StorageLogConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        InnerLogConfig::default().into()
    }
}

impl TryInto<InnerLogConfig> for LogConfig {
    type Error = String;

    fn try_into(self) -> Result<InnerLogConfig, Self::Error> {
        Ok(InnerLogConfig {
            file: self.file.try_into()?,
            stderr: self.stderr.try_into()?,
            otlp: OTLPConfig::default(),
            query: QueryLogConfig::default(),
            profile: ProfileLogConfig::default(),
            structlog: StructLogConfig::default(),
            tracing: TracingConfig::default(),
            history: self.storage.into(),
        })
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            file: inner.file.into(),
            stderr: inner.stderr.into(),
            storage: inner.history.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FileLogConfig {
    #[clap(
        long = "log-file-on", default_value = "true", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub file_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-file-level", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
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

    /// Log file max size, default is 4GB
    #[clap(long = "log-file-max-size", default_value = "4294967296")]
    #[serde(rename = "max_size")]
    pub file_max_size: usize,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    #[serde(rename = "prefix_filter")]
    pub file_prefix_filter: Option<String>,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        InnerFileLogConfig::default().into()
    }
}

impl TryInto<InnerFileLogConfig> for FileLogConfig {
    type Error = String;

    fn try_into(self) -> Result<InnerFileLogConfig, Self::Error> {
        let format = self
            .file_format
            .parse::<LogFormat>()
            .map_err(|e| format!("Invalid file log format: {}", e))?;

        Ok(InnerFileLogConfig {
            on: self.file_on,
            level: self.file_level,
            dir: self.file_dir,
            format,
            limit: self.file_limit,
            max_size: self.file_max_size,
        })
    }
}

impl From<InnerFileLogConfig> for FileLogConfig {
    fn from(inner: InnerFileLogConfig) -> Self {
        Self {
            file_on: inner.on,
            file_level: inner.level,
            file_dir: inner.dir,
            file_format: inner.format.to_string(),
            file_limit: inner.limit,
            file_max_size: inner.max_size,

            // Deprecated Fields
            file_prefix_filter: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StderrLogConfig {
    #[clap(
        long = "log-stderr-on", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub stderr_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-stderr-level", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
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

impl TryInto<InnerStderrLogConfig> for StderrLogConfig {
    type Error = String;

    fn try_into(self) -> Result<InnerStderrLogConfig, Self::Error> {
        let format = self
            .stderr_format
            .parse::<LogFormat>()
            .map_err(|e| format!("Invalid stderr log format: {}", e))?;

        Ok(InnerStderrLogConfig {
            on: self.stderr_on,
            level: self.stderr_level,
            format,
        })
    }
}

impl From<InnerStderrLogConfig> for StderrLogConfig {
    fn from(inner: InnerStderrLogConfig) -> Self {
        Self {
            stderr_on: inner.on,
            stderr_level: inner.level,
            stderr_format: inner.format.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageLogConfig {
    #[clap(
        long = "log-storage-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub log_storage_on: bool,

    /// Specifies the interval in seconds for how often the log is flushed
    #[clap(
        long = "log-storage-interval",
        value_name = "VALUE",
        default_value = "2"
    )]
    #[serde(rename = "interval")]
    pub log_storage_interval: usize,

    /// Specifies the name of the staging area that temporarily holds log data before it is finally copied into the table
    ///
    /// Note:
    /// The default value uses an uuid to avoid conflicts with existing stages
    #[clap(
        long = "log-storage-stage-name",
        value_name = "VALUE",
        default_value = "log_1f93b76af0bd4b1d8e018667865fbc65"
    )]
    #[serde(rename = "stage_name")]
    pub log_storage_stage_name: String,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(
        long = "log-storage-level",
        value_name = "VALUE",
        default_value = "INFO"
    )]
    #[serde(rename = "level")]
    pub log_storage_level: String,

    /// Specify store the log into where
    #[clap(skip)]
    #[serde(rename = "params")]
    pub log_storage_params: StorageConfig,
}

impl Default for StorageLogConfig {
    fn default() -> Self {
        StorageLogConfig {
            log_storage_on: false,
            log_storage_interval: 2,
            log_storage_stage_name: "log_1f93b76af0bd4b1d8e018667865fbc65".to_string(),
            log_storage_level: "INFO".to_string(),
            log_storage_params: Default::default(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<InnerLogHistoryConfig> for StorageLogConfig {
    fn into(self) -> InnerLogHistoryConfig {
        // Falling back to default storage params when conversion fails is intentional:
        // log-history storage is optional and a misconfigured value should not prevent
        // the node from starting.
        let storage_params: Option<InnerStorageConfig> =
            Some(self.log_storage_params.try_into().unwrap_or_default());
        InnerLogHistoryConfig {
            on: self.log_storage_on,
            interval: self.log_storage_interval,
            stage_name: self.log_storage_stage_name,
            level: self.log_storage_level,
            storage_params: storage_params.map(|cfg| cfg.params),
            ..Default::default()
        }
    }
}

impl From<InnerLogHistoryConfig> for StorageLogConfig {
    fn from(value: InnerLogHistoryConfig) -> Self {
        let inner_storage_config: Option<InnerStorageConfig> =
            value.storage_params.map(|params| InnerStorageConfig {
                params,
                ..Default::default()
            });
        Self {
            log_storage_on: value.on,
            log_storage_interval: value.interval,
            log_storage_stage_name: value.stage_name,
            log_storage_level: value.level,
            log_storage_params: inner_storage_config.map(Into::into).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use tempfile::tempdir;

    use crate::Config;
    use crate::MetaConfig;

    #[test]
    fn test_load_config() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let d = tempdir()?;
        let file_path = d.path().join("foo.toml");
        let mut file = File::create(&file_path)?;
        write!(
            file,
            r#"
log_level = "ERROR"
log_dir = "foo/logs"
metric_api_address = "127.0.0.1:8000"
admin_api_address = "127.0.0.1:9000"
admin_tls_server_cert = "admin tls cert"
admin_tls_server_key = "admin tls key"
grpc_api_address = "127.0.0.1:10000"
grpc_tls_server_cert = "grpc server cert"
grpc_tls_server_key = "grpc server key"

[raft_config]
config_id = "raft config id"
raft_api_host = "127.0.0.1"
raft_listen_host = "127.0.0.1"
raft_api_port = 11000
raft_dir = "raft dir"
no_sync = true
snapshot_logs_since_last = 1000
heartbeat_interval = 2000
install_snapshot_timeout = 3000
wait_leader_timeout = 3000
single = false
join = ["j1", "j2"]
id = 20
sled_tree_prefix = "sled_foo"
cluster_name = "foo_cluster"
             "#
        )?;

        temp_env::with_var("METASRV_CONFIG_FILE", Some(file_path.clone()), || {
            let cfg: MetaConfig = Config::load(false)
                .expect("load must success")
                .try_into()
                .expect("conversion must success");
            assert_eq!(cfg.log.file.level, "ERROR");
            assert_eq!(cfg.log.file.dir, "foo/logs");
            assert_eq!(cfg.admin.api_address, "127.0.0.1:9000");
            assert_eq!(cfg.admin.tls.cert, "admin tls cert");
            assert_eq!(cfg.admin.tls.key, "admin tls key");
            assert_eq!(cfg.service.grpc.listen_host, "127.0.0.1");
            assert_eq!(cfg.service.grpc.listen_port, Some(10000));
            assert_eq!(cfg.service.grpc.tls.cert, "grpc server cert");
            assert_eq!(cfg.service.grpc.tls.key, "grpc server key");
            assert_eq!(cfg.service.raft_config.raft_listen_host, "127.0.0.1");
            assert_eq!(cfg.service.raft_config.raft_api_port, 11000);
            assert_eq!(cfg.service.raft_config.raft_dir, "raft dir");
            assert_eq!(cfg.service.raft_config.snapshot_logs_since_last, 1000);
            assert_eq!(cfg.service.raft_config.heartbeat_interval, 2000);
            assert_eq!(cfg.service.raft_config.install_snapshot_timeout, 3000);
            assert_eq!(cfg.service.raft_config.wait_leader_timeout, 3000);
            assert!(!cfg.service.raft_config.single);
            assert_eq!(cfg.service.raft_config.join, vec!["j1", "j2"]);
            assert_eq!(cfg.service.raft_config.id, 20);
            assert_eq!(cfg.service.raft_config.cluster_name, "foo_cluster");
        });

        Ok(())
    }
}
