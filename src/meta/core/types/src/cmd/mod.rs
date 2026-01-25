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

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::TxnRequest;
use crate::node::Node;
use crate::raft_types::NodeId;

mod cmd_context;
mod io_timing;
mod meta_spec;
mod upsert_kv;

pub use cmd_context::CmdContext;
pub use io_timing::IoTimer;
pub use io_timing::IoTiming;
pub use meta_spec::MetaSpec;
pub use upsert_kv::UpsertKV;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Cmd {
    /// Add node if absent
    AddNode {
        node_id: NodeId,
        node: Node,
        /// Whether to override existing record.
        #[serde(default)]
        overriding: bool,
    },

    /// Remove node
    RemoveNode { node_id: NodeId },

    /// Toggle a state machine feature.
    ///
    /// A feature that is implemented by meta-service must be enabled consistently across all nodes.
    /// e.g, every node enable it or every node disable it.Cause the future affect the behavior of the state machine, if one server enables a feature and another no doesn't, it will result in inconsistent state across the cluster.
    SetFeature { feature: String, enable: bool },

    /// Update or insert a general purpose kv store
    UpsertKV(UpsertKV),

    /// Update one or more kv with a transaction.
    Transaction(TxnRequest),
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Cmd::AddNode {
                node_id,
                node,
                overriding,
            } => {
                if *overriding {
                    write!(f, "add_node(override):{}={}", node_id, node)
                } else {
                    write!(f, "add_node(no-override):{}={}", node_id, node)
                }
            }
            Cmd::RemoveNode { node_id } => {
                write!(f, "remove_node:{}", node_id)
            }
            Cmd::SetFeature { feature, enable } => {
                write!(f, "set_feature:{} to {}", feature, enable)
            }
            Cmd::UpsertKV(upsert_kv) => {
                write!(f, "upsert_kv:{}", upsert_kv)
            }
            Cmd::Transaction(txn) => {
                write!(f, "txn:{}", txn)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::Endpoint;
    use crate::TxnCondition;
    use crate::TxnOp;
    use crate::TxnRequest;
    use crate::UpsertKV;

    #[test]
    fn test_serde() -> anyhow::Result<()> {
        // AddNode, override = true
        let cmd = super::Cmd::AddNode {
            node_id: 1,
            node: super::Node::new("n1", Endpoint::new("e1", 12)),
            overriding: true,
        };

        let want = r#"{"AddNode":{"node_id":1,"node":{"name":"n1","endpoint":{"addr":"e1","port":12},"grpc_api_advertise_address":null},"overriding":true}}"#;

        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // AddNode, override = false
        let cmd = super::Cmd::AddNode {
            node_id: 1,
            node: super::Node::new("n1", Endpoint::new("e1", 12)),
            overriding: false,
        };

        let want = r#"{"AddNode":{"node_id":1,"node":{"name":"n1","endpoint":{"addr":"e1","port":12},"grpc_api_advertise_address":null},"overriding":false}}"#;
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // Decode from absent override field
        let want = r#"{"AddNode":{"node_id":1,"node":{"name":"n1","endpoint":{"addr":"e1","port":12},"grpc_api_advertise_address":null}}}"#;
        assert_eq!(cmd, serde_json::from_str(want)?);

        // RemoveNode
        let cmd = super::Cmd::RemoveNode { node_id: 1 };
        let want = r#"{"RemoveNode":{"node_id":1}}"#;
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // SetFeature, enable = true
        let cmd = super::Cmd::SetFeature {
            feature: "test_feature".to_string(),
            enable: true,
        };
        let want = r#"{"SetFeature":{"feature":"test_feature","enable":true}}"#;
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // SetFeature, enable = false
        let cmd = super::Cmd::SetFeature {
            feature: "test_feature".to_string(),
            enable: false,
        };
        let want = r#"{"SetFeature":{"feature":"test_feature","enable":false}}"#;
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // UpsertKV
        let cmd = super::Cmd::UpsertKV(UpsertKV::insert("k", b"v"));
        let want = r#"{"UpsertKV":{"key":"k","seq":{"Exact":0},"value":{"Update":[118]},"value_meta":null}}"#;
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        // Transaction
        let cmd = super::Cmd::Transaction(TxnRequest::new(
            vec![TxnCondition::eq_value("k", b("v"))],
            vec![TxnOp::put_with_ttl(
                "k",
                b("v"),
                Some(Duration::from_millis(100)),
            )],
        ));
        let want = concat!(
            r#"{"Transaction":{"#,
            r#""condition":[{"key":"k","expected":0,"target":{"Value":[118]}}],"#,
            r#""if_then":[{"request":{"Put":{"key":"k","value":[118],"prev_value":true,"expire_at":null,"ttl_ms":100}}}],"#,
            r#""else_then":[]"#,
            r#"}}"#
        );
        assert_eq!(want, serde_json::to_string(&cmd)?);
        assert_eq!(cmd, serde_json::from_str(want)?);

        Ok(())
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().into_bytes()
    }
}
