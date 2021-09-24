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
use common_metatypes::Database;
use common_metatypes::KVValue;
use common_metatypes::Node;
use common_metatypes::SeqValue;
use common_metatypes::Table;
use common_store_api_sdk::storage_api_impl::DataPartInfo;
use serde::Deserialize;
use serde::Serialize;

/// The state of an applied raft log.
/// Normally it includes two fields: the state before applying and the state after applying the log.
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
        prev: Option<Database>,
        result: Option<Database>,
    },

    Table {
        prev: Option<Table>,
        result: Option<Table>,
    },

    DataParts {
        prev: Option<Vec<DataPartInfo>>,
        result: Option<Vec<DataPartInfo>>,
    },

    KV {
        prev: Option<SeqValue<KVValue>>,
        result: Option<SeqValue<KVValue>>,
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

impl From<(Option<Database>, Option<Database>)> for AppliedState {
    fn from(v: (Option<Database>, Option<Database>)) -> Self {
        AppliedState::DataBase {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<Table>, Option<Table>)> for AppliedState {
    fn from(v: (Option<Table>, Option<Table>)) -> Self {
        AppliedState::Table {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<Vec<DataPartInfo>>, Option<Vec<DataPartInfo>>)> for AppliedState {
    fn from(v: (Option<Vec<DataPartInfo>>, Option<Vec<DataPartInfo>>)) -> Self {
        AppliedState::DataParts {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<(Option<SeqValue<KVValue>>, Option<SeqValue<KVValue>>)> for AppliedState {
    fn from(v: (Option<SeqValue<KVValue>>, Option<SeqValue<KVValue>>)) -> Self {
        AppliedState::KV {
            prev: v.0,
            result: v.1,
        }
    }
}
