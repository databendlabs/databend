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

use std::fmt::Display;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_types::InvalidReply;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use seq_marked::SeqValue;

use crate::serde::Quota;

pub fn serialize_struct<T, ErrFn, CtxFn, D>(
    value: &T,
    err_code_fn: ErrFn,
    context_fn: CtxFn,
) -> Result<Vec<u8>>
where
    T: FromToProto + 'static,
    ErrFn: FnOnce(String) -> ErrorCode + Copy,
    D: Display,
    CtxFn: FnOnce() -> D + Copy,
{
    let p = value.to_pb().map_err_to_code(err_code_fn, context_fn)?;
    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err_to_code(err_code_fn, context_fn)?;
    Ok(buf)
}

pub fn deserialize_struct<T, ErrFn, CtxFn, D>(
    buf: &[u8],
    err_code_fn: ErrFn,
    context_fn: CtxFn,
) -> Result<T>
where
    T: FromToProto,
    ErrFn: FnOnce(String) -> ErrorCode + Copy,
    D: Display,
    CtxFn: FnOnce() -> D + Copy,
{
    let p: T::PB = prost::Message::decode(buf).map_err_to_code(err_code_fn, context_fn)?;
    let v: T = FromToProto::from_pb(p).map_err_to_code(err_code_fn, context_fn)?;

    Ok(v)
}

/// Check if the data is in protobuf format, if not, upgrade it to protobuf format.
///
/// Because upgrading relies on RPC, it takes a long time if it upgrades too many records.
/// Thus if the upgrading quota is used up,
/// it will just convert the data but do not write to backend storage.
pub async fn check_and_upgrade_to_pb<T>(
    quota: &mut Quota,
    key: &String,
    seq_value: &SeqV,
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
) -> std::result::Result<SeqV<T>, MetaError>
where
    T: FromToProto + serde::de::DeserializeOwned + 'static,
{
    let deserialize_result = databend_common_meta_api::deserialize_struct(&seq_value.data);

    let err = match deserialize_result {
        Ok(data) => return Ok(SeqV::new(seq_value.seq, data)),
        Err(err) => err,
    };

    log::debug!("deserialize as pb err: {}, rollback to use serde json", err);

    // If there's an error, try to deserialize as JSON.
    let data = serde_json::from_slice::<T>(&seq_value.data).map_err(|e| {
        let inv_reply = InvalidReply::new("non json non pb data", &e);
        MetaNetworkError::from(inv_reply)
    })?;

    if quota.is_used_up() {
        return Ok(SeqV::new(seq_value.seq, data));
    }
    quota.decrement();

    // If we reached here, it means JSON deserialization was successful but we need to serialize to protobuf format.
    let value = databend_common_meta_api::serialize_struct(&data)?;

    let res = kv_api
        .upsert_kv(UpsertKV::new(
            key,
            MatchSeq::Exact(seq_value.seq),
            Operation::Update(value),
            None,
        ))
        .await?;

    let seq = if res.is_changed() {
        res.result.seq()
    } else {
        seq_value.seq
    };

    Ok(SeqV::new(seq, data))
}
