// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::AppDataResponse;
use common_flights::storage_api_impl::DataPartInfo;
use common_metatypes::Database;
use common_metatypes::SeqValue;
use common_metatypes::Table;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::Node;
use crate::meta_service::RaftMes;
use crate::meta_service::RetryableError;

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
        prev: Option<SeqValue>,
        result: Option<SeqValue>,
    },
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

impl From<(Option<SeqValue>, Option<SeqValue>)> for AppliedState {
    fn from(v: (Option<SeqValue>, Option<SeqValue>)) -> Self {
        AppliedState::KV {
            prev: v.0,
            result: v.1,
        }
    }
}

// === from and to transport message

impl From<AppliedState> for RaftMes {
    fn from(msg: AppliedState) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftMes {
            data,
            error: "".to_string(),
        }
    }
}
impl From<Result<AppliedState, RetryableError>> for RaftMes {
    fn from(rst: Result<AppliedState, RetryableError>) -> Self {
        match rst {
            Ok(resp) => resp.into(),
            Err(err) => err.into(),
        }
    }
}

impl From<RaftMes> for Result<AppliedState, RetryableError> {
    fn from(msg: RaftMes) -> Self {
        if !msg.data.is_empty() {
            let resp: AppliedState = serde_json::from_str(&msg.data).expect("fail to deserialize");
            Ok(resp)
        } else {
            let err: RetryableError =
                serde_json::from_str(&msg.error).expect("fail to deserialize");
            Err(err)
        }
    }
}
