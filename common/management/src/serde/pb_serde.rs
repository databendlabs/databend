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

use std::fmt::Display;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_proto_conv::FromToProto;

pub fn serialize_struct<PB: common_protos::prost::Message, ErrFn, CtxFn, D>(
    value: &impl FromToProto<PB>,
    err_code_fn: ErrFn,
    context_fn: CtxFn,
) -> Result<Vec<u8>>
where
    ErrFn: FnOnce(String) -> ErrorCode + std::marker::Copy,
    D: Display,
    CtxFn: FnOnce() -> D + std::marker::Copy,
{
    let p = value.to_pb().map_err_to_code(err_code_fn, context_fn)?;
    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf).map_err_to_code(err_code_fn, context_fn)?;
    Ok(buf)
}

pub fn deserialize_struct<PB, T, ErrFn, CtxFn, D>(
    buf: &[u8],
    err_code_fn: ErrFn,
    context_fn: CtxFn,
) -> Result<T>
where
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
    ErrFn: FnOnce(String) -> ErrorCode + std::marker::Copy,
    D: Display,
    CtxFn: FnOnce() -> D + std::marker::Copy,
{
    let p: PB =
        common_protos::prost::Message::decode(buf).map_err_to_code(err_code_fn, context_fn)?;
    let v: T = FromToProto::from_pb(p).map_err_to_code(err_code_fn, context_fn)?;

    Ok(v)
}
