// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::raft::types::NodeId;

lazy_static! {
    pub static ref DATABEND_COMMIT_VERSION: String = {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

        let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
            #[cfg(not(feature = "simd"))]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
            #[cfg(feature = "simd")]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => {
                format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
            }
            _ => String::new(),
        };
        ver
    };
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct Config {
    #[structopt(long, env = "METASRV_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = "METASRV_LOG_DIR", default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(
        long,
        env = "METASRV_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:28001"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = "ADMIN_API_ADDRESS", default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[structopt(long, env = "ADMIN_TLS_SERVER_CERT", default_value = "")]
    pub admin_tls_server_cert: String,

    #[structopt(long, env = "ADMIN_TLS_SERVER_KEY", default_value = "")]
    pub admin_tls_server_key: String,

    #[structopt(
        long,
        env = "METASRV_FLIGHT_API_ADDRESS",
        default_value = "127.0.0.1:28003"
    )]
    pub flight_api_address: String,

    #[structopt(
        long,
        env = "FLIGHT_TLS_SERVER_CERT",
        default_value = "",
        help = "Certificate for server to identify itself"
    )]
    pub flight_tls_server_cert: String,

    #[structopt(long, env = "FLIGHT_TLS_SERVER_KEY", default_value = "")]
    pub flight_tls_server_key: String,

    #[structopt(flatten)]
    pub meta_config: MetaConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct MetaConfig {
    /// Identify a config. This is only meant to make debugging easier with more than one Config involved.
    #[structopt(long, default_value = "")]
    pub config_id: String,

    #[structopt(
        long,
        env = "METASRV_API_HOST",
        default_value = "127.0.0.1",
        help = "The listening host for metadata communication"
    )]
    pub raft_api_host: String,

    #[structopt(
        long,
        env = "METASRV_API_PORT",
        default_value = "28004",
        help = "The listening port for metadata communication"
    )]
    pub raft_api_port: u32,

    #[structopt(
        long,
        env = "METASRV_RAFT_DIR",
        default_value = "./_meta",
        help = "The dir to store persisted meta state, including raft logs, state machine etc."
    )]
    pub raft_dir: String,

    #[structopt(
    long,
    env = "METASRV_NO_SYNC",
    help = concat!("Whether to fsync meta to disk for every meta write(raft log, state machine etc).",
    " No-sync brings risks of data loss during a crash.",
    " You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING."
    ),
    )]
    pub no_sync: bool,

    // raft config
    #[structopt(
        long,
        env = "METASRV_SNAPSHOT_LOGS_SINCE_LAST",
        default_value = "1024",
        help = "The number of logs since the last snapshot to trigger next snapshot."
    )]
    pub snapshot_logs_since_last: u64,

    #[structopt(
    long,
    env = "METASRV_HEARTBEAT_INTERVAL",
    default_value = "1000",
    help = concat!("The interval in milli seconds at which a leader send heartbeat message to followers.",
    " Different value of this setting on leader and followers may cause unexpected behavior.")
    )]
    pub heartbeat_interval: u64,

    #[structopt(
    long,
    env = "METASRV_INSTALL_SNAPSHOT_TIMEOUT",
    default_value = "4000",
    help = concat!("The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.")
    )]
    pub install_snapshot_timeout: u64,

    #[structopt(
        long,
        env = "METASRV_BOOT",
        help = "Whether to boot up a new cluster. If already booted, it is ignored"
    )]
    pub boot: bool,

    #[structopt(
    long,
    env = "METASRV_SINGLE",
    help = concat!("Single node metasrv. It creates a single node cluster if meta data is not initialized.",
    " Otherwise it opens the previous one.",
    " This is mainly for testing purpose.")
    )]
    pub single: bool,

    #[structopt(
    long,
    env = "METASRV_ID",
    default_value = "0",
    help = concat!("The node id. Only used when this server is not initialized,",
    " e.g. --boot or --single for the first time.",
    " Otherwise this argument is ignored.")
    )]
    pub id: NodeId,

    #[structopt(
        long,
        default_value = "",
        help = "For test only: specifies the tree name prefix"
    )]
    pub sled_tree_prefix: String,
}

impl Config {
    /// StructOptToml provides a default Default impl that loads config from cli args,
    /// which conflicts with unit test if case-filter arguments passed, e.g.:
    /// `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.flight_tls_server_key.is_empty() && !self.flight_tls_server_cert.is_empty()
    }
}

impl MetaConfig {
    pub fn raft_api_addr(&self) -> String {
        format!("{}:{}", self.raft_api_host, self.raft_api_port)
    }

    /// Returns true to fsync after a write operation to meta.
    pub fn is_sync(&self) -> bool {
        !self.no_sync
    }

    pub fn check(&self) -> common_exception::Result<()> {
        if self.boot && self.single {
            return Err(ErrorCode::InvalidConfig(
                "--boot and --single can not be both set",
            ));
        }

        Ok(())
    }

    /// Create a unique sled::Tree name by prepending a unique prefix.
    /// So that multiple instance that depends on a sled::Tree can be used in one process.
    /// sled does not allow to open multiple `sled::Db` in one process.
    pub fn tree_name(&self, name: impl std::fmt::Display) -> String {
        format!("{}{}", self.sled_tree_prefix, name)
    }
}
