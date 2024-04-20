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

use crate::protobuf as pb;
use crate::SeqV;
use crate::TxnRequest;

impl TxnRequest {
    /// Creates a transaction request that performs the specified operations
    /// unconditionally.
    pub fn unconditional(ops: Vec<pb::TxnOp>) -> Self {
        Self {
            condition: vec![],
            if_then: ops,
            else_then: vec![],
        }
    }
}

impl pb::TxnCondition {
    /// Create a txn condition that checks if the `seq` matches.
    pub fn eq_seq(key: impl ToString, seq: u64) -> Self {
        Self::match_seq(key, pb::txn_condition::ConditionResult::Eq, seq)
    }

    /// Create a txn condition that checks if the `seq` match.
    pub fn match_seq(key: impl ToString, op: pb::txn_condition::ConditionResult, seq: u64) -> Self {
        Self {
            key: key.to_string(),
            expected: op as i32,
            target: Some(pb::txn_condition::Target::Seq(seq)),
        }
    }
}

impl pb::TxnOp {
    /// Create a txn operation that puts a record.
    pub fn put(key: impl ToString, value: Vec<u8>) -> pb::TxnOp {
        Self::put_with_expire(key, value, None)
    }

    /// Create a txn operation that puts a record with expiration time.
    pub fn put_with_expire(
        key: impl ToString,
        value: Vec<u8>,
        expire_at: Option<u64>,
    ) -> pb::TxnOp {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                key: key.to_string(),
                value,
                prev_value: true,
                expire_at,
                ttl_ms: None,
            })),
        }
    }

    /// Create a txn operation that puts a record with ttl.
    ///
    /// `ttl` is relative expire time while `expire_at` is absolute expire time.
    pub fn put_with_ttl(key: impl ToString, value: Vec<u8>, ttl_ms: Option<u64>) -> pb::TxnOp {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                key: key.to_string(),
                value,
                prev_value: true,
                expire_at: None,
                ttl_ms,
            })),
        }
    }

    /// Create a new `TxnOp` with a `Delete` operation.
    pub fn delete(key: impl ToString) -> Self {
        Self::delete_exact(key, None)
    }

    /// Create a new `TxnOp` with a `Delete` operation that will be executed only when the `seq` matches.
    pub fn delete_exact(key: impl ToString, seq: Option<u64>) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest {
                key: key.to_string(),
                prev_value: true,
                match_seq: seq,
            })),
        }
    }

    /// Create a new `TxnOp` with a `Get` operation.
    pub fn get(key: impl ToString) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Get(pb::TxnGetRequest {
                key: key.to_string(),
            })),
        }
    }
}

impl pb::TxnOpResponse {
    /// Create a new `TxnOpResponse` of a `Delete` operation.
    pub fn delete(key: impl ToString, success: bool, prev_value: Option<pb::SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Delete(
                pb::TxnDeleteResponse {
                    key: key.to_string(),
                    success,
                    prev_value,
                },
            )),
        }
    }

    /// Create a new `TxnOpResponse` of a `Put` operation.
    pub fn put(key: impl ToString, prev_value: Option<pb::SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Put(pb::TxnPutResponse {
                key: key.to_string(),
                prev_value,
            })),
        }
    }

    pub fn get(key: impl ToString, value: Option<SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Get(pb::TxnGetResponse {
                key: key.to_string(),
                value: value.map(pb::SeqV::from),
            })),
        }
    }
}

impl pb::TxnGetResponse {
    pub fn new(key: impl ToString, value: Option<pb::SeqV>) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }
}
