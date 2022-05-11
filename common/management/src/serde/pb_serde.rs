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
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaError;
use common_proto_conv::FromToProto;

pub fn encode_err<E: std::error::Error + 'static>(e: E) -> ErrorCode {
    MetaError::EncodeError(AnyError::new(&e)).into()
}

pub fn serialize_struct<PB: common_protos::prost::Message>(
    value: &impl FromToProto<PB>,
) -> Result<Vec<u8>> {
    let p = value.to_pb().map_err(encode_err)?;
    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf).map_err(encode_err)?;
    Ok(buf)
}

pub fn deserialize_struct<PB, T>(buf: &[u8]) -> Result<T>
where
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
{
    let p: PB = common_protos::prost::Message::decode(buf).map_err(encode_err)?;
    let v: T = FromToProto::from_pb(p).map_err(encode_err)?;

    Ok(v)
}
