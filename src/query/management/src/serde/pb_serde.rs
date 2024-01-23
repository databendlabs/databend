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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_proto_conv::FromToProto;

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

pub async fn check_and_upgrade_to_pb<'a, T, ErrFn, CtxFn, D>(
    key: String,
    seq_value: &'a SeqV,
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
    err_code_fn: ErrFn,
    context_fn: CtxFn,
) -> std::result::Result<SeqV<T>, ErrorCode>
where
    T: FromToProto + serde::de::DeserializeOwned + 'a + 'static,
    ErrFn: FnOnce(String) -> ErrorCode + Copy,
    D: Display,
    CtxFn: FnOnce() -> D + Copy,
{
    let deserialize_result =
        deserialize_struct(&seq_value.data, ErrorCode::IllegalUserInfoFormat, || "");

    let err = match deserialize_result {
        Ok(data) => return Ok(SeqV::new(seq_value.seq, data)),
        Err(err) => err,
    };

    log::debug!("deserialize as pb err: {}, rollback to use serde json", err);

    // If there's an error, try to deserialize as JSON.
    let data = serde_json::from_slice::<T>(&seq_value.data)?;

    // If we reached here, it means JSON deserialization was successful but we need to serialize to protobuf format.
    let value = serialize_struct(&data, ErrorCode::IllegalUserInfoFormat, || "")?;
    match kv_api
        .upsert_kv(UpsertKVReq::new(
            &key,
            MatchSeq::Exact(seq_value.seq),
            Operation::Update(value),
            None,
        ))
        .await
        .map_err_to_code(err_code_fn, context_fn)
    {
        Ok(res) => {
            if let Some(seq) = res.result {
                Ok(SeqV::new(seq.seq, data))
            } else {
                Ok(SeqV::new(seq_value.seq, data))
            }
        }
        Err(err) => {
            log::error!("json to pb upsert_kv failed, error: {}", err);
            Err(err)
        }
    }
}
