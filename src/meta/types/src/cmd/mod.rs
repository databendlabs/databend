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
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::with::With;
use crate::MatchSeq;
use crate::Node;
use crate::NodeId;
use crate::Operation;
use crate::TxnRequest;

mod cmd_context;
mod meta_spec;

pub use cmd_context::CmdContext;
pub use meta_spec::MetaSpec;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

/// Update or insert a general purpose kv store
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpsertKV {
    pub key: String,

    /// Since a sequence number is always positive, using Exact(0) to perform an add-if-absent operation.
    /// - GE(1) to perform an update-any operation.
    /// - Exact(n) to perform an update on some specified version.
    /// - Any to perform an update or insert that always takes effect.
    pub seq: MatchSeq,

    /// The value to set. A `None` indicates to delete it.
    pub value: Operation<Vec<u8>>,

    /// Meta data of a value.
    pub value_meta: Option<MetaSpec>,
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

impl fmt::Display for UpsertKV {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({:?}) = {:?} ({:?})",
            self.key, self.seq, self.value, self.value_meta
        )
    }
}

impl UpsertKV {
    pub fn new(
        key: impl ToString,
        seq: MatchSeq,
        value: Operation<Vec<u8>>,
        value_meta: Option<MetaSpec>,
    ) -> Self {
        Self {
            key: key.to_string(),
            seq,
            value,
            value_meta,
        }
    }

    pub fn insert(key: impl ToString, value: &[u8]) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::Exact(0),
            value: Operation::Update(value.to_vec()),
            value_meta: None,
        }
    }

    pub fn update(key: impl ToString, value: &[u8]) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::GE(0),
            value: Operation::Update(value.to_vec()),
            value_meta: None,
        }
    }

    pub fn delete(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::GE(0),
            value: Operation::Delete,
            value_meta: None,
        }
    }

    pub fn with_expire_sec(self, expire_at_sec: u64) -> Self {
        self.with(MetaSpec::new_expire(expire_at_sec))
    }

    /// Set the time to last for the value.
    /// When the ttl is passed, the value is deleted.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        self.with(MetaSpec::new_ttl(ttl))
    }
}

impl With<MatchSeq> for UpsertKV {
    fn with(mut self, seq: MatchSeq) -> Self {
        self.seq = seq;
        self
    }
}

impl With<MetaSpec> for UpsertKV {
    fn with(mut self, meta: MetaSpec) -> Self {
        self.value_meta = Some(meta);
        self
    }
}
