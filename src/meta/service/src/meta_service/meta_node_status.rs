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
use databend_common_meta_types::LogId;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;

#[derive(serde::Serialize)]
pub struct MetaNodeStatus {
    pub id: NodeId,

    /// The build version of meta-service binary.
    pub binary_version: String,

    /// The version of the data this meta-service is serving.
    pub data_version: DataVersion,

    /// The raft service endpoint for internal communication
    pub endpoint: String,

    /// The size in bytes of the on disk data.
    pub db_size: u64,

    /// key number of current snapshot
    pub key_num: u64,

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
