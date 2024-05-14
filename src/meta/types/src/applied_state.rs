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
use std::fmt::Formatter;

use crate::protobuf::RaftReply;
use crate::Change;
use crate::Node;
use crate::TxnReply;

/// The state of an applied raft log.
/// Normally it includes two fields: the state before applying and the state after applying the log.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::From,
    derive_more::TryInto,
)]
pub enum AppliedState {
    Node {
        prev: Option<Node>,
        result: Option<Node>,
    },

    KV(Change<Vec<u8>>),

    TxnReply(TxnReply),

    #[try_into(ignore)]
    None,
}

impl fmt::Display for AppliedState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "AppliedState: ")?;
        match self {
            AppliedState::Node { prev, result } => {
                write!(f, "Node: prev: {:?}, result: {:?}", prev, result)
            }
            AppliedState::KV(change) => {
                write!(f, "KV: {}", change)
            }
            AppliedState::TxnReply(txnreply) => {
                write!(f, "Txn: {}", txnreply)
            }
            AppliedState::None => {
                write!(f, "None")
            }
        }
    }
}

impl AppliedState {
    /// Whether the state changed
    pub fn changed(&self) -> bool {
        match self {
            AppliedState::Node {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::KV(ref ch) => ch.is_changed(),
            AppliedState::None => false,
            AppliedState::TxnReply(txn) => txn.success,
        }
    }

    pub fn prev_is_some(&self) -> bool {
        !self.prev_is_none()
    }

    pub fn result_is_some(&self) -> bool {
        !self.result_is_none()
    }

    pub fn is_some(&self) -> (bool, bool) {
        (self.prev_is_some(), self.result_is_some())
    }

    pub fn is_none(&self) -> (bool, bool) {
        (self.prev_is_none(), self.result_is_none())
    }

    pub fn prev_is_none(&self) -> bool {
        match self {
            AppliedState::Node { ref prev, .. } => prev.is_none(),
            AppliedState::KV(Change { ref prev, .. }) => prev.is_none(),
            AppliedState::None => true,
            AppliedState::TxnReply(_txn) => true,
        }
    }

    pub fn result_is_none(&self) -> bool {
        match self {
            AppliedState::Node { ref result, .. } => result.is_none(),
            AppliedState::KV(Change { ref result, .. }) => result.is_none(),
            AppliedState::None => true,
            AppliedState::TxnReply(txn) => !txn.success,
        }
    }
}

impl From<AppliedState> for RaftReply {
    fn from(msg: AppliedState) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftReply {
            data,
            error: "".to_string(),
        }
    }
}
