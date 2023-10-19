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
use std::fmt;
use std::net::AddrParseError;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::Endpoint;
use crate::LogId;
use crate::NodeId;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Node {
    /// Node name for display.
    pub name: String,

    /// Raft service endpoint to connect to.
    pub endpoint: Endpoint,

    /// For backward compatibility, it can not be removed.
    /// 2023-02-09
    #[serde(skip)]
    #[deprecated(note = "it is listening addr, not advertise addr")]
    pub grpc_api_addr: Option<String>,

    /// The address `ip:port` for a meta-client to connect to.
    pub grpc_api_advertise_address: Option<String>,
}

impl Node {
    pub fn new(name: impl ToString, endpoint: Endpoint) -> Self {
        Self {
            name: name.to_string(),
            endpoint,
            ..Default::default()
        }
    }

    pub fn with_grpc_advertise_address(mut self, g: Option<impl ToString>) -> Self {
        self.grpc_api_advertise_address = g.map(|x| x.to_string());
        self
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let grpc_addr_display = if let Some(grpc_addr) = &self.grpc_api_advertise_address {
            grpc_addr.to_string()
        } else {
            "".to_string()
        };
        write!(
            f,
            "id={} raft={} grpc={}",
            self.name, self.endpoint, grpc_addr_display
        )
    }
}

/// Query node
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
#[serde(default)]
pub struct NodeInfo {
    pub id: String,
    pub cpu_nums: u64,
    pub version: u32,
    pub flight_address: String,
    pub binary_version: String,
}

impl NodeInfo {
    pub fn create(
        id: String,
        cpu_nums: u64,
        flight_address: String,
        binary_version: String,
    ) -> NodeInfo {
        NodeInfo {
            id,
            cpu_nums,
            version: 0,
            flight_address,
            binary_version,
        }
    }

    pub fn ip_port(&self) -> Result<(String, u16), AddrParseError> {
        let addr = SocketAddr::from_str(&self.flight_address)?;

        Ok((addr.ip().to_string(), addr.port()))
    }
}

#[derive(Debug, serde::Serialize)]
pub struct NodeStatus {
    pub id: NodeId,

    /// The build version of meta-service binary.
    pub binary_version: String,

    /// The version of the data this meta-service is serving.
    // NOTE: change from DataVersion to String for import.
    pub data_version: String,

    /// The raft service endpoint for internal communication
    pub endpoint: String,

    /// The size in bytes of the on disk data.
    pub db_size: u64,

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

#[cfg(test)]
mod tests {
    use crate::Node;

    #[test]
    fn test_serde_node_compatible() -> anyhow::Result<()> {
        // Without `grpc_api_addr`
        let s1 = r#"{"name":"a","endpoint":{"addr":"b","port":3},"grpc_api_advertise_address":"grpc_advertise"}"#;
        let _n1: Node = serde_json::from_str(s1)?;
        // println!("{n:?}");

        // With `grpc_api_addr`
        let s2 = r#"{"name":"a","endpoint":{"addr":"b","port":3},"grpc_api_addr":"grpc_addr","grpc_api_advertise_address":"grpc_advertise"}"#;
        let _n2: Node = serde_json::from_str(s2)?;
        // println!("{n:?}");
        Ok(())
    }
}
