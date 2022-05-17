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

use clap::Parser;
use common_meta_raft_store::config::RaftConfig as InnerRaftConfig;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

use super::inner::Config as InnerConfig;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', default_value = "")]
    #[serde(alias = "metasrv_config_file")]
    pub config_file: String,

    #[clap(long, default_value = "INFO")]
    #[serde(alias = "metasrc_log_level")]
    pub log_level: String,

    #[clap(long, default_value = "./_logs")]
    #[serde(alias = "metasrc_log_dir")]
    pub log_dir: String,

    #[clap(long, default_value = "127.0.0.1:28001")]
    #[serde(alias = "metasrv_metric_api_address")]
    pub metric_api_address: String,

    #[clap(long, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_cert: String,

    #[clap(long, default_value = "")]
    pub admin_tls_server_key: String,

    #[clap(long, default_value = "127.0.0.1:9191")]
    #[serde(alias = "metasrv_grpc_api_address")]
    pub grpc_api_address: String,

    /// Certificate for server to identify itself
    #[clap(long, default_value = "")]
    pub grpc_tls_server_cert: String,

    #[clap(long, default_value = "")]
    pub grpc_tls_server_key: String,

    // TODO(xuanwo)
    //
    // This is a dirty tricks to make both env and config file works.
    // We should deprecated them.
    #[clap(flatten)]
    #[serde(flatten)]
    pub inner_raft_config: RaftConfig,
    #[clap(skip)]
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        InnerConfig::default().into()
    }
}

impl TryInto<InnerConfig> for Config {
    type Error = MetaError;

    fn try_into(self) -> MetaResult<InnerConfig> {
        Ok(InnerConfig {
            config_file: self.config_file,
            log_level: self.log_level,
            log_dir: self.log_dir,
            metric_api_address: self.metric_api_address,
            admin_api_address: self.admin_api_address,
            admin_tls_server_cert: self.admin_tls_server_cert,
            admin_tls_server_key: self.admin_tls_server_key,
            grpc_api_address: self.grpc_api_address,
            grpc_tls_server_cert: self.grpc_tls_server_cert,
            grpc_tls_server_key: self.grpc_tls_server_key,
            raft_config: self.raft_config.try_into()?,
        })
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self {
            config_file: inner.config_file,
            log_level: inner.log_level,
            log_dir: inner.log_dir,
            metric_api_address: inner.metric_api_address,
            admin_api_address: inner.admin_api_address,
            admin_tls_server_cert: inner.admin_tls_server_cert,
            admin_tls_server_key: inner.admin_tls_server_key,
            grpc_api_address: inner.grpc_api_address,
            grpc_tls_server_cert: inner.grpc_tls_server_cert,
            grpc_tls_server_key: inner.grpc_tls_server_key,
            inner_raft_config: inner.raft_config.clone().into(),
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
        builder = builder.collect(from_env());

        // Finally, load from args.
        builder = builder.collect(from_self(arg_conf));

        builder
            .build()
            .map_err(|e| MetaError::InvalidConfig(e.to_string()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
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
    #[serde(alias = "kvsrv_listen_host")]
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    #[clap(long, default_value = "localhost")]
    #[serde(alias = "kvsrv_advertise_host")]
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    #[clap(long, default_value = "28004")]
    #[serde(alias = "kvsrv_api_port")]
    pub raft_api_port: u32,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./_meta")]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: String,

    /// Whether to fsync meta to disk for every meta write(raft log, state machine etc).
    /// No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING.
    #[clap(long)]
    #[serde(alias = "kvsrv_no_sync")]
    pub no_sync: bool,

    /// The number of logs since the last snapshot to trigger next snapshot.
    #[clap(long, default_value = "1024")]
    #[serde(alias = "kvsrv_snapshot_logs_since_last")]
    pub snapshot_logs_since_last: u64,

    /// The interval in milli seconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    #[clap(long, default_value = "1000")]
    #[serde(alias = "kvsrv_heartbeat_intervalt")]
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, default_value = "4000")]
    #[serde(alias = "kvsrv_install_snapshot_timeout")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, default_value = "1000")]
    #[serde(alias = "raft_max_applied_log_to_keep")]
    pub max_applied_log_to_keep: u64,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    #[clap(long)]
    #[serde(alias = "kvsrv_single")]
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long, multiple_occurrences = true, multiple_values = true)]
    #[serde(alias = "metasrv_join")]
    pub join: Vec<String>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        InnerRaftConfig::default().into()
    }
}

impl TryInto<InnerRaftConfig> for RaftConfig {
    type Error = MetaError;

    fn try_into(self) -> MetaResult<InnerRaftConfig> {
        let irc = InnerRaftConfig {
            config_id: self.config_id,
            raft_listen_host: self.raft_listen_host,
            raft_advertise_host: self.raft_advertise_host,
            raft_api_port: self.raft_api_port,
            raft_dir: self.raft_dir,
            no_sync: self.no_sync,
            snapshot_logs_since_last: self.snapshot_logs_since_last,
            heartbeat_interval: self.heartbeat_interval,
            install_snapshot_timeout: self.install_snapshot_timeout,
            max_applied_log_to_keep: self.max_applied_log_to_keep,
            single: self.single,
            join: self.join,
            id: self.id,
            sled_tree_prefix: self.sled_tree_prefix,
        };

        irc.check()?;

        Ok(irc)
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
            id: inner.id,
            sled_tree_prefix: inner.sled_tree_prefix,
        }
    }
}
