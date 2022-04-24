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
use crate::protobuf::DeleteRequest as pb_del_request;
use crate::protobuf::DeleteResponse as pb_del_response;
use crate::protobuf::GetRequest as pb_get_request;
use crate::protobuf::GetResponse as pb_get_response;
use crate::protobuf::PutRequest as pb_put_request;
use crate::protobuf::PutResponse as pb_put_response;
use crate::protobuf::SeqV as pb_seqv;
use crate::protobuf::TxnCondition;
use crate::protobuf::TxnKey;
use crate::protobuf::TxnOp;
use crate::protobuf::TxnOpResponse;
use crate::protobuf::TxnReply;
use crate::protobuf::TxnRequest;
use crate::SeqV;

const TXN_KEY_DELIMITER: char = '/';

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
pub struct TransactionKey {
    pub key: String,
    pub tenant: String,
    pub tree: String,
}

impl TransactionKey {
    pub fn generate_key(&self) -> String {
        format!("{}{}{}", self.tenant, TXN_KEY_DELIMITER, self.key)
    }

    pub fn to_pb(&self) -> TxnKey {
        TxnKey {
            key: self.key.clone(),
            tenant: self.tenant.clone(),
            tree: self.tree.clone(),
        }
    }
}

impl From<&TxnKey> for TransactionKey {
    fn from(key: &TxnKey) -> Self {
        TransactionKey {
            key: key.key.clone(),
            tenant: key.tenant.clone(),
            tree: key.tenant.clone(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionCondition {
    pub key: TransactionKey,
    pub target: i32,
    pub expected: i32,
    pub value: Option<SeqV>,
}

impl From<TxnCondition> for TransactionCondition {
    fn from(cond: TxnCondition) -> Self {
        TransactionCondition {
            key: (&(cond.key.unwrap())).into(),
            target: cond.target,
            expected: cond.expected,
            value: convert_to_pb_to_seqv(&cond.value),
        }
    }
}

impl TransactionCondition {
    pub fn is_key_condition(&self) -> bool {
        self.target == txn_condition::ConditionTarget::Key as i32
    }

    pub fn return_key_condition_result(&self, key_exist: bool) -> bool {
        if self.expected == txn_condition::ConditionResult::Exist as i32 {
            return key_exist;
        }
        if self.expected == txn_condition::ConditionResult::NotExist as i32 {
            return !key_exist;
        }

        false
    }

    pub fn return_value_condition_result(&self, val: SeqV) -> bool {
        if let Some(value) = &self.value {
            let v = value.clone();
            if self.expected == txn_condition::ConditionResult::Equal as i32 {
                return v == val;
            }
            if self.expected == txn_condition::ConditionResult::Greater as i32 {
                return v > val;
            }
            if self.expected == txn_condition::ConditionResult::Less as i32 {
                return v < val;
            }
            if self.expected == txn_condition::ConditionResult::NotEqual as i32 {
                return v != val;
            }
        }

        false
    }

    pub fn to_pb(&self) -> TxnCondition {
        TxnCondition {
            key: Some(self.key.clone().to_pb()),
            target: self.target,
            value: convert_seqv_to_pb(&self.value),
            expected: self.expected,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionGetRequest {
    pub key: TransactionKey,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionPutRequest {
    pub key: TransactionKey,
    pub value: Vec<u8>,
    pub prev_kv: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionDeleteRequest {
    pub key: TransactionKey,
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
                    key: Some(get.key.to_pb()),
                })),
            },
            TransactionOperation::TransactionPutRequest(put) => TxnOp {
                request: Some(txn_op::Request::Put(pb_put_request {
                    key: Some(put.key.to_pb()),
                    value: put.value.clone(),
                    prev_kv: put.prev_kv,
                })),
            },
            TransactionOperation::TransactionDeleteRequest(delete) => TxnOp {
                request: Some(txn_op::Request::Delete(pb_del_request {
                    key: Some(delete.key.to_pb()),
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
                        key: key.as_ref().unwrap().into(),
                    })
                }
                txn_op::Request::Put(pb_put_request {
                    key,
                    value,
                    prev_kv,
                }) => TransactionOperation::TransactionPutRequest(TransactionPutRequest {
                    key: key.as_ref().unwrap().into(),
                    value: value.clone(),
                    prev_kv: *prev_kv,
                }),
                txn_op::Request::Delete(pb_del_request { key, prev_kv }) => {
                    TransactionOperation::TransactionDeleteRequest(TransactionDeleteRequest {
                        key: key.as_ref().unwrap().into(),
                        prev_kv: *prev_kv,
                    })
                }
            }
        };
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From, PartialEq)]
pub struct TransactionReq {
    pub cond: TransactionCondition,
    pub if_then: Vec<TransactionOperation>,
    pub else_then: Vec<TransactionOperation>,
}

impl TransactionReq {
    pub fn new(req: TxnRequest) -> Self {
        return Self {
            cond: req.cond.unwrap().into(),
            if_then: req.if_then.iter().map(|op| (op.into())).collect(),
            else_then: req.else_then.iter().map(|op| (op.into())).collect(),
        };
    }

    pub fn to_pb(&self) -> TxnRequest {
        TxnRequest {
            cond: Some(self.cond.to_pb()),
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
            self.cond, self.if_then, self.else_then
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionGetResponse {
    pub key: TransactionKey,
    pub value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionPutResponse {
    pub key: TransactionKey,
    pub prev_value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionDeleteResponse {
    pub key: TransactionKey,
    pub success: bool,
    pub prev_value: Option<SeqV>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum TransactionOperationResponse {
    TransactionGetResponse(TransactionGetResponse),
    TransactionPutResponse(TransactionPutResponse),
    TransactionDeleteResponse(TransactionDeleteResponse),
}

impl TransactionOperationResponse {
    pub fn to_pb(&self) -> TxnOpResponse {
        match &*self {
            TransactionOperationResponse::TransactionGetResponse(get) => {
                let get_resp = pb_get_response {
                    key: Some(get.key.to_pb()),
                    value: convert_seqv_to_pb(&get.value),
                };
                let response = txn_op_response::Response::Get(get_resp);
                TxnOpResponse {
                    response: Some(response),
                }
            }
            TransactionOperationResponse::TransactionPutResponse(put) => {
                let put_resp = pb_put_response {
                    key: Some(put.key.to_pb()),
                    prev_value: convert_seqv_to_pb(&put.prev_value),
                };
                let response = txn_op_response::Response::Put(put_resp);
                TxnOpResponse {
                    response: Some(response),
                }
            }
            TransactionOperationResponse::TransactionDeleteResponse(delete) => {
                let put_resp = pb_del_response {
                    key: Some(delete.key.to_pb()),
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
pub struct TransactionResponse {
    pub success: bool,
    pub responses: Vec<TransactionOperationResponse>,
}

impl TransactionResponse {
    pub fn new(reply: TxnReply) -> Self {
        TransactionResponse {
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
