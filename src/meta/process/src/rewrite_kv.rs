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

use databend_common_meta_api::kv_pb_api::decode_pb;
use databend_common_meta_api::kv_pb_api::encode_pb;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_meta_kvapi::kvapi::Key;

/// Convert old version TableMeta protobuf message to new version.
///
/// The input is generic kv key and pb encoded message.
pub fn upgrade_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    // Only convert the key space `table-by-id`
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let v1: TableMeta = decode_pb(v.as_slice())?;
    let buf = encode_pb(&v1)?;
    Ok(buf)
}

/// It does not update any data but just print TableMeta in protobuf message format
/// that are found in log or state machine.
pub fn print_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let val: TableMeta = decode_pb(v.as_slice())?;
    println!("{:?}", val);

    Ok(v)
}
