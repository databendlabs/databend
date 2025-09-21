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
use std::collections::BTreeSet;

use log::debug;

use crate::node::Node;
use crate::raft_types::LogId;
use crate::raft_types::NodeId;
use crate::raft_types::StoredMembership;

/// Snapshot System data(non-user data).
///
/// System data is **NOT** leveled. At each level, there is a complete copy of the system data.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SysData {
    /// The last applied log id.
    last_applied: Option<LogId>,

    /// The last applied membership log.
    last_membership: StoredMembership,

    /// All the nodes in this cluster, including the voters and learners.
    nodes: BTreeMap<NodeId, Node>,

    /// The sequence number for every [`SeqV`] value that is stored in this state machine.
    ///
    /// A seq is globally unique and monotonically increasing.
    sequence: u64,

    /// The seq number for each new data level created.
    #[serde(skip_serializing_if = "Option::is_none")]
    data_seq: Option<u64>,

    /// The number of keys in each sub key space.
    ///
    /// If it is absent in the serialized data, serde should skip it when deserializing.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    key_counts: BTreeMap<String, u64>,

    /// State machine features enabled in the cluster.
    ///
    /// Features must be consistently enabled across all cluster nodes to maintain
    /// state consistency. These are controlled via raft log entries and affect
    /// subsequent log processing.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    sm_features: BTreeSet<String>,
}

impl SysData {
    pub fn curr_seq(&self) -> u64 {
        self.sequence
    }

    pub fn update_seq(&mut self, seq: u64) {
        self.sequence = seq;
    }

    /// Increase the sequence number by 1 and return the updated value
    pub fn next_seq(&mut self) -> u64 {
        self.sequence += 1;
        debug!("next_seq: {}", self.sequence);
        // dbg!("next_seq", self.sequence);

        self.sequence
    }

    pub fn incr_data_seq(&mut self) -> u64 {
        let v = self.data_seq.unwrap_or(0);

        self.data_seq = Some(v + 1);
        debug!("incr_data_seq: {:?}", self.data_seq);
        self.data_seq.unwrap()
    }

    pub fn last_applied(&self) -> &Option<LogId> {
        &self.last_applied
    }

    pub fn last_applied_mut(&mut self) -> &mut Option<LogId> {
        &mut self.last_applied
    }

    pub fn last_membership_mut(&mut self) -> &mut StoredMembership {
        &mut self.last_membership
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }

    pub fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        &mut self.nodes
    }

    pub fn key_counts(&self) -> &BTreeMap<String, u64> {
        &self.key_counts
    }

    pub fn key_counts_mut(&mut self) -> &mut BTreeMap<String, u64> {
        &mut self.key_counts
    }

    pub fn feature_enabled(&self, feature: &str) -> bool {
        self.sm_features.contains(feature)
    }

    pub fn features(&self) -> &BTreeSet<String> {
        &self.sm_features
    }

    pub fn features_mut(&mut self) -> &mut BTreeSet<String> {
        &mut self.sm_features
    }

    pub fn last_applied_ref(&self) -> &Option<LogId> {
        &self.last_applied
    }

    pub fn last_membership_ref(&self) -> &StoredMembership {
        &self.last_membership
    }

    pub fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::raft_types::new_log_id;
    use crate::raft_types::Membership;
    use crate::Endpoint;

    /// Fields with default value should not be serialized.
    #[test]
    fn test_sys_data_serialize_default_fields() {
        let sys_data = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: None,
            key_counts: BTreeMap::from([]),
            sm_features: BTreeSet::from([]),
        };

        let want = r#"{
  "last_applied": {
    "leader_id": {
      "term": 4,
      "node_id": 5
    },
    "index": 6
  },
  "last_membership": {
    "log_id": {
      "leader_id": {
        "term": 1,
        "node_id": 2
      },
      "index": 3
    },
    "membership": {
      "configs": [
        [
          7,
          8
        ]
      ],
      "nodes": {
        "7": {},
        "8": {},
        "9": {}
      }
    }
  },
  "nodes": {
    "2": {
      "name": "node2",
      "endpoint": {
        "addr": "127.0.0.1",
        "port": 16
      },
      "grpc_api_advertise_address": null
    }
  },
  "sequence": 5
}"#;

        let serialized = serde_json::to_string_pretty(&sys_data).unwrap();
        assert_eq!(want, serialized);
    }

    /// Test serialze will all fields set.
    #[test]
    fn test_sys_data_serialize() {
        let sys_data = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: Some(10),
            key_counts: BTreeMap::from([("foo".to_string(), 5), ("bar".to_string(), 6)]),
            sm_features: BTreeSet::from(["f1".to_string(), "f2".to_string()]),
        };

        let want = r#"{
  "last_applied": {
    "leader_id": {
      "term": 4,
      "node_id": 5
    },
    "index": 6
  },
  "last_membership": {
    "log_id": {
      "leader_id": {
        "term": 1,
        "node_id": 2
      },
      "index": 3
    },
    "membership": {
      "configs": [
        [
          7,
          8
        ]
      ],
      "nodes": {
        "7": {},
        "8": {},
        "9": {}
      }
    }
  },
  "nodes": {
    "2": {
      "name": "node2",
      "endpoint": {
        "addr": "127.0.0.1",
        "port": 16
      },
      "grpc_api_advertise_address": null
    }
  },
  "sequence": 5,
  "data_seq": 10,
  "key_counts": {
    "bar": 6,
    "foo": 5
  },
  "sm_features": [
    "f1",
    "f2"
  ]
}"#;

        let serialized = serde_json::to_string_pretty(&sys_data).unwrap();
        assert_eq!(want, serialized);
    }

    /// Test newer program can deserialize upto 2025-06-16 version(inclusive)
    #[test]
    fn test_sys_data_deserialize_2025_06_16() {
        // The string is serialized with old version SysData, never change it.
        let serialized = r#"{
          "last_applied": { "leader_id": { "term": 4, "node_id": 5 }, "index": 6 },
          "last_membership": {
            "log_id": { "leader_id": { "term": 1, "node_id": 2 }, "index": 3 },
            "membership": {
              "configs": [ [ 7, 8 ] ],
              "nodes": { "7": {}, "8": {}, "9": {} }
            }
          },
          "nodes": {
            "2": {
              "name": "node2",
              "endpoint": { "addr": "127.0.0.1", "port": 16 },
              "grpc_api_advertise_address": null
            }
          },
          "sequence": 5
        }
        "#;

        let want = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: None,
            key_counts: BTreeMap::new(),
            sm_features: BTreeSet::new(),
        };

        println!("{}", serde_json::to_string_pretty(&want).unwrap());

        let deserialized: SysData = serde_json::from_str(serialized).unwrap();
        assert_eq!(want, deserialized);
    }

    /// Test newer program can deserialize 2025-06-17 version: add field `key_counts`
    #[test]
    fn test_sys_data_deserialize_2025_06_17() {
        // The string is serialized with old version SysData, never change it.
        let serialized = r#"{
          "last_applied": { "leader_id": { "term": 4, "node_id": 5 }, "index": 6 },
          "last_membership": {
            "log_id": { "leader_id": { "term": 1, "node_id": 2 }, "index": 3 },
            "membership": {
              "configs": [ [ 7, 8 ] ],
              "nodes": { "7": {}, "8": {}, "9": {} }
            }
          },
          "nodes": {
            "2": {
              "name": "node2",
              "endpoint": { "addr": "127.0.0.1", "port": 16 },
              "grpc_api_advertise_address": null
            }
          },
          "sequence": 5,
          "key_counts": { "bar": 6, "foo": 5 }
         }
        "#;

        let want = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: None,
            key_counts: BTreeMap::from([("foo".to_string(), 5), ("bar".to_string(), 6)]),
            sm_features: Default::default(),
        };

        println!("{}", serde_json::to_string_pretty(&want).unwrap());

        let deserialized: SysData = serde_json::from_str(serialized).unwrap();
        assert_eq!(want, deserialized);
    }

    /// Test newer program can deserialize 2025-06-23 version: add field `sm_features`
    #[test]
    fn test_sys_data_deserialize_2025_06_23() {
        // The string is serialized with old version SysData, never change it.
        let serialized = r#"{
          "last_applied": { "leader_id": { "term": 4, "node_id": 5 }, "index": 6 },
          "last_membership": {
            "log_id": { "leader_id": { "term": 1, "node_id": 2 }, "index": 3 },
            "membership": {
              "configs": [ [ 7, 8 ] ],
              "nodes": { "7": {}, "8": {}, "9": {} }
            }
          },
          "nodes": {
            "2": {
              "name": "node2",
              "endpoint": { "addr": "127.0.0.1", "port": 16 },
              "grpc_api_advertise_address": null
            }
          },
          "sequence": 5,
          "key_counts": { "bar": 6, "foo": 5 },
          "sm_features": [ "f1", "f2" ]
         }
        "#;

        let want = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: None,
            key_counts: BTreeMap::from([("foo".to_string(), 5), ("bar".to_string(), 6)]),
            sm_features: BTreeSet::from(["f1".to_string(), "f2".to_string()]),
        };

        println!("{}", serde_json::to_string_pretty(&want).unwrap());

        let deserialized: SysData = serde_json::from_str(serialized).unwrap();
        assert_eq!(want, deserialized);
    }

    /// Test newer program can deserialize 2025-08-31 version: add field `data_seq`
    #[test]
    fn test_sys_data_deserialize_2025_08_31() {
        // The string is serialized with old version SysData, never change it.
        let serialized = r#"{
          "last_applied": { "leader_id": { "term": 4, "node_id": 5 }, "index": 6 },
          "last_membership": {
            "log_id": { "leader_id": { "term": 1, "node_id": 2 }, "index": 3 },
            "membership": {
              "configs": [ [ 7, 8 ] ],
              "nodes": { "7": {}, "8": {}, "9": {} }
            }
          },
          "nodes": {
            "2": {
              "name": "node2",
              "endpoint": { "addr": "127.0.0.1", "port": 16 },
              "grpc_api_advertise_address": null
            }
          },
          "sequence": 5,
          "data_seq": 10,
          "key_counts": { "bar": 6, "foo": 5 },
          "sm_features": [ "f1", "f2" ]
         }
        "#;

        let want = SysData {
            last_applied: Some(new_log_id(4, 5, 6)),
            last_membership: StoredMembership::new(
                Some(new_log_id(1, 2, 3)),
                Membership::new(vec![BTreeSet::from([7, 8])], BTreeSet::from([7u64, 8, 9]))
                    .unwrap(),
            ),
            nodes: BTreeMap::from([(2, Node::new("node2", Endpoint::new("127.0.0.1", 16)))]),
            sequence: 5,
            data_seq: Some(10),
            key_counts: BTreeMap::from([("foo".to_string(), 5), ("bar".to_string(), 6)]),
            sm_features: BTreeSet::from(["f1".to_string(), "f2".to_string()]),
        };

        println!("{}", serde_json::to_string_pretty(&want).unwrap());

        let deserialized: SysData = serde_json::from_str(serialized).unwrap();
        assert_eq!(want, deserialized);
    }
}
