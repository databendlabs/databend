// Copyright 2021 Datafuse Labs.
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

use std::net::Ipv4Addr;

use clap::Parser;
use common_exception::Result;
use common_grpc::DNSResolver;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_meta_types::NodeId;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde::Serialize;

pub static DATABEND_COMMIT_VERSION: Lazy<String> = Lazy::new(|| {
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
});

pub const KVSRV_LISTEN_HOST: &str = "KVSRV_LISTEN_HOST";
pub const KVSRV_ADVERTISE_HOST: &str = "KVSRV_ADVERTISE_HOST";
pub const KVSRV_API_PORT: &str = "KVSRV_API_PORT";
pub const KVSRV_RAFT_DIR: &str = "KVSRV_RAFT_DIR";
pub const KVSRV_NO_SYNC: &str = "KVSRV_NO_SYNC";
pub const KVSRV_SNAPSHOT_LOGS_SINCE_LAST: &str = "KVSRV_SNAPSHOT_LOGS_SINCE_LAST";
pub const KVSRV_HEARTBEAT_INTERVAL: &str = "KVSRV_HEARTBEAT_INTERVAL";
pub const KVSRV_INSTALL_SNAPSHOT_TIMEOUT: &str = "KVSRV_INSTALL_SNAPSHOT_TIMEOUT";
pub const KVSRV_BOOT: &str = "KVSRV_BOOT";
pub const KVSRV_SINGLE: &str = "KVSRV_SINGLE";
pub const KVSRV_ID: &str = "KVSRV_ID";

pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Parser)]
#[serde(default)]
pub struct RaftConfig {
    /// Identify a config.
    /// This is only meant to make debugging easier with more than one Config involved.
    #[clap(long, default_value = "")]
    pub config_id: String,

    /// The local listening host for metadata communication.
    /// This config does not need to be stored in raft-store,
    /// only used when metasrv startup and listen to.
    #[clap(long, env = KVSRV_LISTEN_HOST, default_value = DEFAULT_LISTEN_HOST)]
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    #[clap(long, env = KVSRV_ADVERTISE_HOST, default_value = "localhost")]
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    #[clap(long, env = KVSRV_API_PORT, default_value = "28004")]
    pub raft_api_port: u32,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, env = KVSRV_RAFT_DIR, default_value = "./_meta")]
    pub raft_dir: String,

    /// Whether to fsync meta to disk for every meta write(raft log, state machine etc).
    /// No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING.
    #[clap(long, env = KVSRV_NO_SYNC)]
    pub no_sync: bool,

    /// The number of logs since the last snapshot to trigger next snapshot.
    #[clap(long, env = KVSRV_SNAPSHOT_LOGS_SINCE_LAST, default_value = "1024")]
    pub snapshot_logs_since_last: u64,

    /// The interval in milli seconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    #[clap(long, env = KVSRV_HEARTBEAT_INTERVAL, default_value = "1000")]
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    #[clap(long, env = KVSRV_INSTALL_SNAPSHOT_TIMEOUT, default_value = "4000")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, env = "RAFT_MAX_APPLIED_LOG_TO_KEEP", default_value = "1000")]
    pub max_applied_log_to_keep: u64,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    #[clap(long, env = KVSRV_SINGLE)]
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    #[clap(long, env = "METASRV_JOIN")]
    pub join: Vec<String>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, env = KVSRV_ID, default_value = "0")]
    pub id: NodeId,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,
}

pub fn get_default_raft_advertise_host() -> String {
    match hostname::get() {
        Ok(h) => match h.into_string() {
            Ok(h) => h,
            _ => "UnknownHost".to_string(),
        },
        _ => "UnknownHost".to_string(),
    }
}

pub fn get_default_raft_listen_host() -> String {
    DEFAULT_LISTEN_HOST.to_string()
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            config_id: "".to_string(),
            raft_listen_host: get_default_raft_listen_host(),
            raft_advertise_host: get_default_raft_advertise_host(),
            raft_api_port: 28004,
            raft_dir: "./_meta".to_string(),
            no_sync: false,
            snapshot_logs_since_last: 1024,
            heartbeat_interval: 1000,
            install_snapshot_timeout: 4000,
            max_applied_log_to_keep: 1000,
            single: false,
            join: vec![],
            id: 0,
            sled_tree_prefix: "".to_string(),
        }
    }
}

impl RaftConfig {
    /// StructOptToml provides a default Default impl that loads config from cli args,
    /// which conflicts with unit test if case-filter arguments passed, e.g.:
    /// `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as Parser>::parse_from(&Vec::<&'static str>::new())
    }

    fn raft_api_listen_host_string(&self) -> String {
        format!("{}:{}", self.raft_listen_host, self.raft_api_port)
    }

    pub fn raft_api_advertise_host_string(&self) -> String {
        format!("{}:{}", self.raft_advertise_host, self.raft_api_port)
    }

    /// Support ip address and hostname
    pub async fn raft_api_addr(&self) -> Result<String> {
        let ipv4_addr = self.raft_advertise_host.as_str().parse::<Ipv4Addr>();
        match ipv4_addr {
            Ok(addr) => Ok(format!("{}:{}", addr, self.raft_api_port)),
            Err(_) => {
                let _ip_addrs = DNSResolver::instance()?
                    .resolve(self.raft_advertise_host.clone())
                    .await?;
                Ok(format!("{}:{}", _ip_addrs[0], self.raft_api_port))
            }
        }
    }

    /// Returns true to fsync after a write operation to meta.
    pub fn is_sync(&self) -> bool {
        !self.no_sync
    }

    pub fn check(&self) -> MetaResult<()> {
        if !self.join.is_empty() && self.single {
            return Err(MetaError::InvalidConfig(String::from(
                "--join and --single can not be both set",
            )));
        }
        let self_addr = self.raft_api_listen_host_string();
        if self.join.contains(&self_addr) {
            return Err(MetaError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )));
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
