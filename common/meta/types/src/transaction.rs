//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::fmt::Display;
use std::fmt::Formatter;
use std::vec::Vec;

use crate::protobuf::txn_condition;
use crate::protobuf::txn_op;
use crate::protobuf::txn_op_response;
use crate::protobuf::SeqV as pb_seqv;
use crate::protobuf::TxnCondition;
use crate::protobuf::TxnDeleteRequest as pb_del_request;
use crate::protobuf::TxnDeleteResponse as pb_del_response;
use crate::protobuf::TxnGetRequest as pb_get_request;
use crate::protobuf::TxnGetResponse as pb_get_response;
use crate::protobuf::TxnOp;
use crate::protobuf::TxnOpResponse;
use crate::protobuf::TxnPutRequest as pb_put_request;
use crate::protobuf::TxnPutResponse as pb_put_response;
use crate::protobuf::TxnReply;
use crate::protobuf::TxnRequest;
use crate::SeqV;

//const TXN_KEY_DELIMITER: char = '/';

fn convert_seqv_to_pb(seqv: &Option<SeqV>) -> Option<pb_seqv> {
    seqv.as_ref().map(|seqv| pb_seqv {
        seq: seqv.seq,
        data: seqv.data.clone(),
    })
}

fn convert_to_pb_to_seqv(seqv: &Option<pb_seqv>) -> Option<SeqV> {
    seqv.as_ref().map(|seqv| SeqV {
        seq: seqv.seq,
        data: seqv.data.clone(),
        meta: None,
    })
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[repr(i32)]
pub enum TransactionConditionTarget {
    Key = txn_condition::ConditionTarget::Key as i32,

    Value = txn_condition::ConditionTarget::Value as i32,
}

impl From<i32> for TransactionConditionTarget {
    fn from(target: i32) -> Self {
        if target == txn_condition::ConditionTarget::Key as i32 {
            TransactionConditionTarget::Key
        } else {
            TransactionConditionTarget::Value
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[repr(i32)]
pub enum TransactionConditionResult {
    KeyExist = txn_condition::ConditionResult::KeyExist as i32,
    KeyNotExist = txn_condition::ConditionResult::KeyNotExist as i32,

    ValueEqual = txn_condition::ConditionResult::ValueEqual as i32,
    ValueGreater = txn_condition::ConditionResult::ValueGreater as i32,
    ValueLess = txn_condition::ConditionResult::ValueLess as i32,
    ValueNotEqual = txn_condition::ConditionResult::ValueNotEqual as i32,
}

impl From<i32> for TransactionConditionResult {
    fn from(target: i32) -> Self {
        if target == txn_condition::ConditionResult::KeyExist as i32 {
            TransactionConditionResult::KeyExist
        } else if target == txn_condition::ConditionResult::KeyNotExist as i32 {
            TransactionConditionResult::KeyNotExist
        } else if target == txn_condition::ConditionResult::ValueEqual as i32 {
            TransactionConditionResult::ValueEqual
        } else if target == txn_condition::ConditionResult::ValueGreater as i32 {
            TransactionConditionResult::ValueGreater
        } else if target == txn_condition::ConditionResult::ValueLess as i32 {
            TransactionConditionResult::ValueLess
        } else {
            TransactionConditionResult::ValueNotEqual
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionCondition {
    pub key: String,
    pub target: TransactionConditionTarget,
    pub expected: TransactionConditionResult,
    pub value: Option<SeqV>,
}

impl From<&TxnCondition> for TransactionCondition {
    fn from(cond: &TxnCondition) -> Self {
        TransactionCondition {
            key: cond.key.clone(),
            target: cond.target.into(),
            expected: cond.expected.into(),
            value: convert_to_pb_to_seqv(&cond.value),
        }
    }
}

impl TransactionCondition {
    pub fn is_key_condition(&self) -> bool {
        self.target == (txn_condition::ConditionTarget::Key as i32).into()
    }

    pub fn return_key_condition_result(&self, key_exist: bool) -> bool {
        if self.expected == (txn_condition::ConditionResult::KeyExist as i32).into() {
            return key_exist;
        }
        if self.expected == (txn_condition::ConditionResult::KeyNotExist as i32).into() {
            return !key_exist;
        }

        false
    }

    pub fn return_value_condition_result(&self, val: SeqV) -> bool {
        if let Some(value) = &self.value {
            let v = value.clone();
            if self.expected == (txn_condition::ConditionResult::ValueEqual as i32).into() {
                return v == val;
            }
            if self.expected == (txn_condition::ConditionResult::ValueGreater as i32).into() {
                return v > val;
            }
            if self.expected == (txn_condition::ConditionResult::ValueLess as i32).into() {
                return v < val;
            }
            if self.expected == (txn_condition::ConditionResult::ValueNotEqual as i32).into() {
                return v != val;
            }
        }

        false
    }

    pub fn to_pb(&self) -> TxnCondition {
        TxnCondition {
            key: self.key.clone(),
            target: self.target.clone() as i32,
            value: convert_seqv_to_pb(&self.value),
            expected: self.expected.clone() as i32,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionGetRequest {
    pub key: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionPutRequest {
    pub key: String,
    pub value: Vec<u8>,
    pub prev_kv: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionDeleteRequest {
    pub key: String,
    pub prev_kv: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum TransactionOperation {
    TransactionGetRequest(TransactionGetRequest),
    TransactionPutRequest(TransactionPutRequest),
    TransactionDeleteRequest(TransactionDeleteRequest),
}

impl TransactionOperation {
    pub fn to_pb(&self) -> TxnOp {
        match &*self {
            TransactionOperation::TransactionGetRequest(get) => TxnOp {
                request: Some(txn_op::Request::Get(pb_get_request {
                    key: get.key.clone(),
                })),
            },
            TransactionOperation::TransactionPutRequest(put) => TxnOp {
                request: Some(txn_op::Request::Put(pb_put_request {
                    key: put.key.clone(),
                    value: put.value.clone(),
                    prev_kv: put.prev_kv,
                })),
            },
            TransactionOperation::TransactionDeleteRequest(delete) => TxnOp {
                request: Some(txn_op::Request::Delete(pb_del_request {
                    key: delete.key.clone(),
                    prev_kv: delete.prev_kv,
                })),
            },
        }
    }
}

impl From<&TxnOp> for TransactionOperation {
    fn from(op: &TxnOp) -> Self {
        return {
            let req = op.request.as_ref().unwrap();
            match req {
                txn_op::Request::Get(pb_get_request { key }) => {
                    TransactionOperation::TransactionGetRequest(TransactionGetRequest {
                        key: key.clone(),
                    })
                }
                txn_op::Request::Put(pb_put_request {
                    key,
                    value,
                    prev_kv,
                }) => TransactionOperation::TransactionPutRequest(TransactionPutRequest {
                    key: key.clone(),
                    value: value.clone(),
                    prev_kv: *prev_kv,
                }),
                txn_op::Request::Delete(pb_del_request { key, prev_kv }) => {
                    TransactionOperation::TransactionDeleteRequest(TransactionDeleteRequest {
                        key: key.clone(),
                        prev_kv: *prev_kv,
                    })
                }
            }
        };
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From, PartialEq)]
pub struct TransactionReq {
    pub condition: Vec<TransactionCondition>,
    pub if_then: Vec<TransactionOperation>,
    pub else_then: Vec<TransactionOperation>,
}

impl TransactionReq {
    pub fn new(req: TxnRequest) -> Self {
        return Self {
            condition: req.condition.iter().map(|op| (op.into())).collect(),
            if_then: req.if_then.iter().map(|op| (op.into())).collect(),
            else_then: req.else_then.iter().map(|op| (op.into())).collect(),
        };
    }

    pub fn to_pb(&self) -> TxnRequest {
        TxnRequest {
            condition: self.condition.iter().map(|op| (op.to_pb())).collect(),
            if_then: self.if_then.iter().map(|op| (op.to_pb())).collect(),
            else_then: self.else_then.iter().map(|op| (op.to_pb())).collect(),
        }
    }
}

impl Display for TransactionReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "txn:if {:?} then {:?} else {:?}",
            self.condition, self.if_then, self.else_then
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionGetResponse {
    pub key: String,
    pub value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionPutResponse {
    pub key: String,
    pub prev_value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionDeleteResponse {
    pub key: String,
    pub success: bool,
    pub prev_value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum TransactionOperationResponse {
    TransactionGetResponse(TransactionGetResponse),
    TransactionPutResponse(TransactionPutResponse),
    TransactionDeleteResponse(TransactionDeleteResponse),
}

impl From<&TxnOpResponse> for TransactionOperationResponse {
    fn from(op: &TxnOpResponse) -> Self {
        return {
            let req = op.response.as_ref().unwrap();
            match req {
                txn_op_response::Response::Get(pb_get_response { key, value }) => {
                    TransactionOperationResponse::TransactionGetResponse(TransactionGetResponse {
                        key: key.clone(),
                        value: convert_to_pb_to_seqv(value),
                    })
                }
                txn_op_response::Response::Put(pb_put_response { key, prev_value }) => {
                    TransactionOperationResponse::TransactionPutResponse(TransactionPutResponse {
                        key: key.clone(),
                        prev_value: convert_to_pb_to_seqv(prev_value),
                    })
                }
                txn_op_response::Response::Delete(pb_del_response {
                    key,
                    success,
                    prev_value,
                }) => TransactionOperationResponse::TransactionDeleteResponse(
                    TransactionDeleteResponse {
                        key: key.clone(),
                        success: *success,
                        prev_value: convert_to_pb_to_seqv(prev_value),
                    },
                ),
            }
        };
    }
}

impl TransactionOperationResponse {
    pub fn to_pb(&self) -> TxnOpResponse {
        match &*self {
            TransactionOperationResponse::TransactionGetResponse(get) => {
                let get_resp = pb_get_response {
                    key: get.key.clone(),
                    value: convert_seqv_to_pb(&get.value),
                };
                let response = txn_op_response::Response::Get(get_resp);
                TxnOpResponse {
                    response: Some(response),
                }
            }
            TransactionOperationResponse::TransactionPutResponse(put) => {
                let put_resp = pb_put_response {
                    key: put.key.clone(),
                    prev_value: convert_seqv_to_pb(&put.prev_value),
                };
                let response = txn_op_response::Response::Put(put_resp);
                TxnOpResponse {
                    response: Some(response),
                }
            }
            TransactionOperationResponse::TransactionDeleteResponse(delete) => {
                let put_resp = pb_del_response {
                    key: delete.key.clone(),
                    success: delete.success,
                    prev_value: convert_seqv_to_pb(&delete.prev_value),
                };
                let response = txn_op_response::Response::Delete(put_resp);
                TxnOpResponse {
                    response: Some(response),
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From, PartialEq)]
pub struct TransactionReply {
    pub success: bool,
    pub responses: Vec<TransactionOperationResponse>,
}

impl TransactionReply {
    pub fn new(reply: TxnReply) -> Self {
        TransactionReply {
            success: reply.success,
            responses: vec![],
        }
    }

    pub fn to_pb(&self) -> TxnReply {
        TxnReply {
            success: self.success,
            responses: self.responses.iter().map(|op| (op.to_pb())).collect(),
        }
    }
}

impl From<TxnReply> for TransactionReply {
    fn from(reply: TxnReply) -> Self {
        TransactionReply {
            success: reply.success,
            responses: reply.responses.iter().map(|op| (op.into())).collect(),
        }
    }
}
