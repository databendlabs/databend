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

use databend_common_meta_app::primitive::Id;
use databend_common_proto_conv::FromToProto;
use databend_meta_types::InvalidArgument;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaNetworkError;

use crate::kv_pb_api::decode_pb;
use crate::kv_pb_api::encode_pb;

pub fn serialize_u64(value: impl Into<Id>) -> Result<Vec<u8>, MetaNetworkError> {
    let v = serde_json::to_vec(&*value.into()).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(v)
}

pub fn deserialize_u64(v: &[u8]) -> Result<Id, MetaNetworkError> {
    let id = serde_json::from_slice(v).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    Ok(Id::new(id))
}

pub fn serialize_struct<T>(value: &T) -> Result<Vec<u8>, MetaNetworkError>
where T: FromToProto + 'static {
    encode_pb(value).map_err(|e| MetaNetworkError::InvalidArgument(InvalidArgument::new(e, "")))
}

pub fn deserialize_struct<T>(buf: &[u8]) -> Result<T, MetaNetworkError>
where T: FromToProto {
    decode_pb(buf).map_err(|e| MetaNetworkError::InvalidReply(InvalidReply::new("", &e)))
}
