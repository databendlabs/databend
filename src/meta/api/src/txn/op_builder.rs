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

use std::time::Duration;

use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_types::InvalidArgument;
use databend_meta_types::TxnOp;

pub fn txn_put_pb<K>(key: &K, value: &K::ValueType) -> Result<TxnOp, InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    let p = value.to_pb().map_err(|e| InvalidArgument::new(e, ""))?;

    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| InvalidArgument::new(e, ""))?;

    Ok(TxnOp::put(key.to_string_key(), buf))
}

/// Deprecate this. Replace it with `txn_put_pb().with_ttl()`
pub fn txn_op_put_pb<K>(
    key: &K,
    value: &K::ValueType,
    ttl: Option<Duration>,
) -> Result<TxnOp, InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    let p = value.to_pb().map_err(|e| InvalidArgument::new(e, ""))?;

    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| InvalidArgument::new(e, ""))?;

    Ok(TxnOp::put_with_ttl(key.to_string_key(), buf, ttl))
}

/// Build a txn operation that puts a record.
pub fn txn_op_put(key: &impl kvapi::Key, value: Vec<u8>) -> TxnOp {
    TxnOp::put(key.to_string_key(), value)
}

/// Build a txn operation that gets value by key.
pub fn txn_op_get(key: &impl kvapi::Key) -> TxnOp {
    TxnOp::get(key.to_string_key())
}

/// Build a txn operation that deletes a record.
pub fn txn_op_del(key: &impl kvapi::Key) -> TxnOp {
    TxnOp::delete(key.to_string_key())
}
