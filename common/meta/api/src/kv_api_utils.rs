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

use anyerror::AnyError;
use common_meta_types::txn_condition::Target;
use common_meta_types::txn_op::Request;
use common_meta_types::ConditionResult;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReq;
use common_proto_conv::FromToProto;

use crate::KVApi;
use crate::KVApiKey;

pub const TXN_MAX_RETRY_TIMES: u32 = 10;

/// Get value that its type is `u64`.
///
/// It expects the kv-value's type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`, or
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`.
///
/// It returns (seq, `u64` value).
/// If not found, (0,0) is returned.
pub async fn get_u64_value<T: KVApiKey>(
    kv_api: &(impl KVApi + ?Sized),
    key: &T,
) -> Result<(u64, u64), MetaError> {
    let res = kv_api.get_kv(&key.to_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, deserialize_u64(&seq_v.data)?))
    } else {
        Ok((0, 0))
    }
}

/// Get a struct value.
///
/// It returns seq number and the data.
pub async fn get_struct_value<K, PB, T>(
    kv_api: &(impl KVApi + ?Sized),
    k: &K,
) -> Result<(u64, Option<T>), MetaError>
where
    K: KVApiKey,
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
{
    let res = kv_api.get_kv(&k.to_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, Some(deserialize_struct(&seq_v.data)?)))
    } else {
        Ok((0, None))
    }
}

pub fn deserialize_u64(v: &[u8]) -> Result<u64, MetaError> {
    let id = serde_json::from_slice(v).map_err(meta_encode_err)?;
    Ok(id)
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
pub async fn fetch_id<T: KVApiKey>(kv_api: &impl KVApi, generator: T) -> Result<u64, MetaError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: generator.to_key(),
            seq: MatchSeq::Any,
            value: Operation::Update(b"".to_vec()),
            value_meta: None,
        })
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

pub fn serialize_u64(value: u64) -> Result<Vec<u8>, MetaError> {
    let v = serde_json::to_vec(&value).map_err(meta_encode_err)?;
    Ok(v)
}

pub fn serialize_struct<PB: common_protos::prost::Message>(
    value: &impl FromToProto<PB>,
) -> Result<Vec<u8>, MetaError> {
    let p = value.to_pb().map_err(meta_encode_err)?;
    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf).map_err(meta_encode_err)?;
    Ok(buf)
}

pub fn deserialize_struct<PB, T>(buf: &[u8]) -> Result<T, MetaError>
where
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
{
    let p: PB = common_protos::prost::Message::decode(buf).map_err(meta_encode_err)?;
    let v: T = FromToProto::from_pb(p).map_err(meta_encode_err)?;

    Ok(v)
}

pub fn meta_encode_err<E: std::error::Error + 'static>(e: E) -> MetaError {
    MetaError::EncodeError(AnyError::new(&e))
}

pub async fn send_txn(
    kv_api: &impl KVApi,
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
    let tx_reply = kv_api.transaction(txn_req).await?;
    let res: Result<_, MetaError> = tx_reply.into();
    let (succ, responses) = res?;
    Ok((succ, responses))
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_seq(key: &impl KVApiKey, op: ConditionResult, seq: u64) -> TxnCondition {
    TxnCondition {
        key: key.to_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    }
}

/// Build a txn operation that puts a record.
pub fn txn_op_put(key: &impl KVApiKey, value: Vec<u8>) -> TxnOp {
    TxnOp {
        request: Some(Request::Put(TxnPutRequest {
            key: key.to_key(),
            value,
            prev_value: true,
        })),
    }
}

/// Build a txn operation that deletes a record.
pub fn txn_op_del(key: &impl KVApiKey) -> TxnOp {
    TxnOp {
        request: Some(Request::Delete(TxnDeleteRequest {
            key: key.to_key(),
            prev_value: true,
        })),
    }
}
