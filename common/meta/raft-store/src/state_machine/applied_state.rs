// Copyright 2020 Datafuse Labs.
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

use async_raft::AppDataResponse;
use common_meta_types::DatabaseInfo;
use common_meta_types::Node;
use common_meta_types::SeqV;
use common_meta_types::TableInfo;
use serde::Deserialize;
use serde::Serialize;

/// The state of an applied raft log.
/// Normally it includes two fields: the state before applying and the state after applying the log.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AppliedState {
    String {
        // The value before applying a RaftRequest.
        prev: Option<String>,
        // The value after applying a RaftRequest.
        result: Option<String>,
    },

    Seq {
        seq: u64,
    },

    Node {
        prev: Option<Node>,
        result: Option<Node>,
    },

    DataBase {
        prev: Option<SeqDBInfo>,
        result: Option<SeqDBInfo>,
    },

    Table {
        prev: Option<SeqV<TableInfo>>,
        result: Option<SeqV<TableInfo>>,
    },

    KV {
        prev: Option<SeqV<Vec<u8>>>,
        result: Option<SeqV<Vec<u8>>>,
    },

    DataPartsCount {
        prev: Option<usize>,
        result: Option<usize>,
    },

    None,
}

impl AppDataResponse for AppliedState {}

// === raw applied result to AppliedState

impl From<(Option<String>, Option<String>)> for AppliedState {
    fn from(v: (Option<String>, Option<String>)) -> Self {
        AppliedState::String {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<u64> for AppliedState {
    fn from(seq: u64) -> Self {
        AppliedState::Seq { seq }
    }
}

impl From<(Option<usize>, Option<usize>)> for AppliedState {
    fn from(v: (Option<usize>, Option<usize>)) -> Self {
        AppliedState::DataPartsCount {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<Node>, Option<Node>)> for AppliedState {
    fn from(v: (Option<Node>, Option<Node>)) -> Self {
        AppliedState::Node {
            prev: v.0,
            result: v.1,
        }
    }
}

type SeqDBInfo = SeqV<DatabaseInfo>;

impl From<(Option<SeqDBInfo>, Option<SeqDBInfo>)> for AppliedState {
    fn from(v: (Option<SeqDBInfo>, Option<SeqDBInfo>)) -> Self {
        AppliedState::DataBase {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<SeqV<TableInfo>>, Option<SeqV<TableInfo>>)> for AppliedState {
    fn from(v: (Option<SeqV<TableInfo>>, Option<SeqV<TableInfo>>)) -> Self {
        AppliedState::Table {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<SeqV>, Option<SeqV>)> for AppliedState {
    fn from(v: (Option<SeqV>, Option<SeqV>)) -> Self {
        AppliedState::KV {
            prev: v.0,
            result: v.1,
        }
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
    pub fn changed(self) -> bool {
        match self {
            AppliedState::String {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::Seq { .. } => true,
            AppliedState::Node {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::DataBase {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::Table {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::KV {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::DataPartsCount {
                ref prev,
                ref result,
            } => prev != result,
            AppliedState::None => false,
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
            AppliedState::String { ref prev, .. } => prev.is_none(),
            AppliedState::Seq { .. } => false,
            AppliedState::Node { ref prev, .. } => prev.is_none(),
            AppliedState::DataBase { ref prev, .. } => prev.is_none(),
            AppliedState::Table { ref prev, .. } => prev.is_none(),
            AppliedState::KV { ref prev, .. } => prev.is_none(),
            AppliedState::DataPartsCount { ref prev, .. } => prev.is_none(),
            AppliedState::None => true,
        }
    }

    pub fn result_is_none(&self) -> bool {
        match self {
            AppliedState::String { ref result, .. } => result.is_none(),
            AppliedState::Seq { .. } => false,
            AppliedState::Node { ref result, .. } => result.is_none(),
            AppliedState::DataBase { ref result, .. } => result.is_none(),
            AppliedState::Table { ref result, .. } => result.is_none(),
            AppliedState::KV { ref result, .. } => result.is_none(),
            AppliedState::DataPartsCount { ref result, .. } => result.is_none(),
            AppliedState::None => true,
        }
    }
}
