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

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::LazyLock;

use databend_common_exception::Result;
use databend_common_grpc::DNSResolver;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::MetaStartupError;

use crate::ondisk::DATA_VERSION;
use crate::raft_log_v004;

pub static DATABEND_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    match (
        databend_common_version::VERGEN_BUILD_SEMVER,
        databend_common_version::VERGEN_GIT_SHA,
        databend_common_version::VERGEN_RUSTC_SEMVER,
        databend_common_version::VERGEN_BUILD_TIMESTAMP,
    ) {
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
        _ => String::new(),
    }
});

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftConfig {
    /// Identify a config.
    /// This is only meant to make debugging easier with more than one Config involved.
    pub config_id: String,

    /// The local listening host for metadata communication.
    /// This config does not need to be stored in raft-store,
    /// only used when metasrv startup and listen to.
    pub raft_listen_host: String,

    /// The hostname that other nodes will use to connect this node.
    /// This host should be stored in raft store and be replicated to the raft cluster,
    /// i.e., when calling add_node().
    /// Use `localhost` by default.
    pub raft_advertise_host: String,

    /// The listening port for metadata communication.
    pub raft_api_port: u16,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    pub raft_dir: String,

    /// Whether to fsync meta to disk for every meta write(raft log, state machine etc).
    /// No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING.
    pub no_sync: bool,

    /// The maximum number of log entries for log entries cache.
    pub log_cache_max_items: u64,

    /// The maximum memory in bytes for the log entries cache.
    pub log_cache_capacity: u64,

    /// Maximum number of records in a chunk of raft-log WAL.
    pub log_wal_chunk_max_records: u64,

    /// Maximum size in bytes for a chunk of raft-log WAL.
    pub log_wal_chunk_max_size: u64,

    /// The number of logs since the last snapshot to trigger next snapshot.
    pub snapshot_logs_since_last: u64,

    /// The interval in milli seconds at which a leader send heartbeat message to followers.
    /// Different value of this setting on leader and followers may cause unexpected behavior.
    pub heartbeat_interval: u64,

    /// The max time in milli seconds that a leader wait for install-snapshot ack from a follower or non-voter.
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    pub max_applied_log_to_keep: u64,

    /// The size of chunk for transmitting snapshot. The default is 64MB
    pub snapshot_chunk_size: u64,

    /// Whether to check keys fed to snapshot are sorted.
    pub snapshot_db_debug_check: bool,

    /// The maximum number of keys allowed in a block within a snapshot db.
    ///
    /// A block serves as the caching unit in a snapshot database.
    /// Smaller blocks enable more granular cache control but may increase the index size.
    pub snapshot_db_block_keys: u64,

    /// The total block to cache.
    pub snapshot_db_block_cache_item: u64,

    /// The total cache size for snapshot blocks.
    ///
    /// By default it is 1GB.
    pub snapshot_db_block_cache_size: u64,

    /// Single node metasrv. It creates a single node cluster if meta data is not initialized.
    /// Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    pub single: bool,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    pub join: Vec<String>,

    /// Do not run databend-meta, but just remove a node from its cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `leave` request.
    pub leave_via: Vec<String>,

    /// The node id to leave from the cluster.
    ///
    /// It will be ignored if `--leave-via` is absent.
    pub leave_id: Option<NodeId>,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    pub id: NodeId,

    ///  The node name. If the user specifies a name,
    /// the user-supplied name is used, if not, the default name is used.
    pub cluster_name: String,

    /// Max timeout(in milli seconds) when waiting a cluster leader.
    pub wait_leader_timeout: u64,
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

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            config_id: "".to_string(),
            raft_listen_host: "127.0.0.1".to_string(),
            raft_advertise_host: get_default_raft_advertise_host(),
            raft_api_port: 28004,
            raft_dir: "./.databend/meta".to_string(),
            no_sync: false,

            log_cache_max_items: 1_000_000,
            log_cache_capacity: 1024 * 1024 * 1024,
            log_wal_chunk_max_records: 100_000,
            log_wal_chunk_max_size: 256 * 1024 * 1024,

            snapshot_logs_since_last: 1024,
            heartbeat_interval: 1000,
            install_snapshot_timeout: 4000,
            max_applied_log_to_keep: 1000,
            snapshot_chunk_size: 4194304, // 4MB

            snapshot_db_debug_check: true,
            snapshot_db_block_keys: 8000,
            snapshot_db_block_cache_item: 1024,
            snapshot_db_block_cache_size: 1073741824,

            single: false,
            join: vec![],
            leave_via: vec![],
            leave_id: None,
            id: 0,
            cluster_name: "foo_cluster".to_string(),
            wait_leader_timeout: 70000,
        }
    }
}

impl RaftConfig {
    pub fn to_rotbl_config(&self) -> rotbl::v001::Config {
        rotbl::v001::Config::default()
            .with_debug_check(self.snapshot_db_debug_check)
            .with_block_config(
                rotbl::v001::BlockConfig::default()
                    .with_max_items(self.snapshot_db_block_keys as usize),
            )
            .with_block_cache_config(
                rotbl::v001::BlockCacheConfig::default()
                    .with_max_items(self.snapshot_db_block_cache_item as usize)
                    .with_capacity(self.snapshot_db_block_cache_size as usize),
            )
    }

    /// Build [`RaftLogV004`](crate::raft_log_v004::RaftLogV004) config from [`RaftConfig`].
    pub fn to_raft_log_config(&self) -> raft_log_v004::RaftLogConfig {
        let p = Path::new(&self.raft_dir)
            .join("df_meta")
            .join(format!("{}", DATA_VERSION))
            .join("log");

        let dir = p.to_str().unwrap().to_string();

        raft_log_v004::RaftLogConfig {
            dir,
            log_cache_max_items: Some(self.log_cache_max_items as usize),
            log_cache_capacity: Some(self.log_cache_capacity as usize),
            chunk_max_records: Some(self.log_wal_chunk_max_records as usize),
            chunk_max_size: Some(self.log_wal_chunk_max_size as usize),
            read_buffer_size: None,
            truncate_incomplete_record: None,
        }
    }

    pub fn raft_api_listen_host_string(&self) -> String {
        format!("{}:{}", self.raft_listen_host, self.raft_api_port)
    }

    pub fn raft_api_advertise_host_string(&self) -> String {
        format!("{}:{}", self.raft_advertise_host, self.raft_api_port)
    }

    pub fn raft_api_listen_host_endpoint(&self) -> Endpoint {
        Endpoint::new(&self.raft_listen_host, self.raft_api_port)
    }

    pub fn raft_api_advertise_host_endpoint(&self) -> Endpoint {
        Endpoint::new(&self.raft_advertise_host, self.raft_api_port)
    }

    /// Support ip address and hostname
    pub async fn raft_api_addr(&self) -> Result<Endpoint> {
        let ipv4_addr = self.raft_advertise_host.as_str().parse::<Ipv4Addr>();
        match ipv4_addr {
            Ok(addr) => Ok(Endpoint::new(addr, self.raft_api_port)),
            Err(_) => {
                let _ip_addrs = DNSResolver::instance()?
                    .resolve(self.raft_advertise_host.clone())
                    .await?;
                Ok(Endpoint::new(_ip_addrs[0], self.raft_api_port))
            }
        }
    }

    /// Returns true to fsync after a write operation to meta.
    pub fn is_sync(&self) -> bool {
        !self.no_sync
    }

    /// Returns the min and max election timeout, in milli seconds.
    ///
    /// Raft will choose a random timeout in this range for next election.
    pub fn election_timeout(&self) -> (u64, u64) {
        (self.heartbeat_interval * 2, self.heartbeat_interval * 3)
    }

    pub fn check(&self) -> std::result::Result<(), MetaStartupError> {
        // If just leaving, does not need to check other config
        if !self.leave_via.is_empty() {
            return Ok(());
        }

        // There two cases:
        // - both join and single is set
        // - neither join nor single is set
        if self.join.is_empty() != self.single {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "at least one of `single` and `join` needs to be enabled",
            )));
        }

        let self_addr = self.raft_api_listen_host_string();
        if self.join.contains(&self_addr) {
            return Err(MetaStartupError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )));
        }
        Ok(())
    }
}
