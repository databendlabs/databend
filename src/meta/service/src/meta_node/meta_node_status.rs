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

use std::collections::BTreeMap;

use databend_common_meta_raft_store::ondisk::DataVersion;
use databend_common_meta_raft_store::raft_log_v004::RaftLogStat;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MetaNodeStatus {
    pub id: NodeId,

    /// The build version of meta-service binary.
    pub binary_version: String,

    /// The version of the data this meta-service is serving.
    pub data_version: DataVersion,

    /// The raft service endpoint for internal communication
    pub endpoint: Option<String>,

    /// The status about local raft-log
    pub raft_log: RaftLogStatus,

    /// Total number of keys in current snapshot
    pub snapshot_key_count: u64,

    /// The count of keys in each key space in the snapshot data.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub snapshot_key_space_stat: BTreeMap<String, u64>,

    /// Server state, one of "Follower", "Learner", "Candidate", "Leader".
    pub state: String,

    /// Is this node a leader.
    pub is_leader: bool,

    /// Current term.
    pub current_term: u64,

    /// Last received log index
    pub last_log_index: u64,

    /// Last log id that has been committed and applied to state machine.
    pub last_applied: LogId,

    /// The last log id contained in the last built snapshot.
    pub snapshot_last_log_id: Option<LogId>,

    /// The last log id that has been purged, inclusive.
    pub purged: Option<LogId>,

    /// The last known leader node.
    pub leader: Option<Node>,

    /// The replication state of all nodes.
    ///
    /// Only leader node has non-None data for this field, i.e., `is_leader` is true.
    pub replication: Option<BTreeMap<NodeId, Option<LogId>>>,

    /// Nodes that can vote in election can grant replication.
    pub voters: Vec<Node>,

    /// Also known as `learner`s.
    pub non_voters: Vec<Node>,

    /// The last `seq` used by GenericKV sub tree.
    ///
    /// `seq` is a monotonically incremental integer for every value that is inserted or updated.
    pub last_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RaftLogStatus {
    pub cache_items: u64,
    pub cache_used_size: u64,
    pub wal_total_size: u64,
    pub wal_open_chunk_size: u64,
    pub wal_offset: u64,
    pub wal_closed_chunk_count: u64,
    pub wal_closed_chunk_total_size: u64,
    pub wal_closed_chunk_sizes: BTreeMap<String, u64>,
}

impl From<RaftLogStat> for RaftLogStatus {
    fn from(s: RaftLogStat) -> Self {
        let closed_sizes = s
            .closed_chunks
            .iter()
            .map(|c| (c.chunk_id.to_string(), c.size))
            .collect();

        let closed_total_size = s.closed_chunks.iter().map(|c| c.size).sum::<u64>();
        let wal_total_size = closed_total_size + s.open_chunk.size;

        Self {
            cache_items: s.payload_cache_item_count,
            cache_used_size: s.payload_cache_size,
            wal_total_size,
            wal_open_chunk_size: s.open_chunk.size,
            wal_offset: s.open_chunk.global_end,
            wal_closed_chunk_count: s.closed_chunks.len() as u64,
            wal_closed_chunk_total_size: closed_total_size,
            wal_closed_chunk_sizes: closed_sizes,
        }
    }
}
