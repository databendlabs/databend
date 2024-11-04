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

use crate::Node;
use crate::NodeId;
use crate::TxnRequest;

mod cmd_context;
mod meta_spec;
mod upsert_kv;

pub use cmd_context::CmdContext;
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
            Cmd::UpsertKV(upsert_kv) => {
                write!(f, "upsert_kv:{}", upsert_kv)
            }
            Cmd::Transaction(txn) => {
                write!(f, "txn:{}", txn)
            }
        }
    }
}
