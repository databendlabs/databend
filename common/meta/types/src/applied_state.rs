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

use std::fmt::Debug;

use openraft::AppDataResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::AddResult;
use crate::Change;
use crate::DeleteByPrefixReply;
use crate::MetaError;
use crate::Node;
use crate::TxnReply;

/// The state of an applied raft log.
/// Normally it includes two fields: the state before applying and the state after applying the log.
#[allow(clippy::large_enum_variant)]
#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::From, derive_more::TryInto,
)]
pub enum AppliedState {
    Seq {
        seq: u64,
    },

    Node {
        prev: Option<Node>,
        result: Option<Node>,
    },

    MetaSrvAddr {
        prev: Option<String>,
        result: Option<String>,
    },

    KV(Change<Vec<u8>>),

    DeleteByPrefixReply(DeleteByPrefixReply),

    TxnReply(TxnReply),

    #[try_into(ignore)]
    None,
}

impl AppDataResponse for AppliedState {}

impl<T, ID> TryInto<AddResult<T, ID>> for AppliedState
where
    ID: Clone + PartialEq + Debug,
    T: Clone + PartialEq + Debug,
    Change<T, ID>: TryFrom<AppliedState>,
    <Change<T, ID> as TryFrom<AppliedState>>::Error: Debug,
{
    type Error = MetaError;

    fn try_into(self) -> Result<AddResult<T, ID>, Self::Error> {
        let typ = std::any::type_name::<T>();

        let ch = TryInto::<Change<T, ID>>::try_into(self).expect(typ);
        let add_res = ch.into_add_result()?;
        Ok(add_res)
    }
}

pub enum PrevOrResult<'a> {
    Prev(&'a AppliedState),
    Result(&'a AppliedState),
}

impl<'a> PrevOrResult<'a> {
    pub fn is_some(&self) -> bool {
        match self {
            PrevOrResult::Prev(state) => state.prev_is_some(),
            PrevOrResult::Result(state) => state.result_is_some(),
        }
    }
    pub fn is_none(&self) -> bool {
        !self.is_some()
    }
}

impl AppliedState {
    pub fn prev(&self) -> PrevOrResult {
        PrevOrResult::Prev(self)
    }

    pub fn result(&self) -> PrevOrResult {
        PrevOrResult::Result(self)
    }

    /// Whether the state changed
    pub fn changed(&self) -> bool {
        match self {
            AppliedState::Seq { .. } => true,
            AppliedState::Node {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::MetaSrvAddr {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::KV(ref ch) => ch.changed(),
            AppliedState::None => false,
            AppliedState::TxnReply(txn) => txn.success,
            AppliedState::DeleteByPrefixReply(reply) => reply.count > 0,
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
            AppliedState::Seq { .. } => false,
            AppliedState::Node { ref prev, .. } => prev.is_none(),
            AppliedState::MetaSrvAddr { ref prev, .. } => prev.is_none(),
            AppliedState::KV(Change { ref prev, .. }) => prev.is_none(),
            AppliedState::None => true,
            AppliedState::TxnReply(_txn) => true,
            AppliedState::DeleteByPrefixReply(_r) => true,
        }
    }

    pub fn result_is_none(&self) -> bool {
        match self {
            AppliedState::Seq { .. } => false,
            AppliedState::Node { ref result, .. } => result.is_none(),
            AppliedState::MetaSrvAddr { ref result, .. } => result.is_none(),
            AppliedState::KV(Change { ref result, .. }) => result.is_none(),
            AppliedState::None => true,
            AppliedState::TxnReply(txn) => !txn.success,
            AppliedState::DeleteByPrefixReply(r) => r.count == 0,
        }
    }
}
