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

use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_proto_conv::FromToProto;
use databend_common_protos::pb;
use databend_common_protos::prost::Message;

/// Convert old version TableMeta protobuf message to new version.
///
/// The input is generic kv key and pb encoded message.
pub fn upgrade_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    // Only convert the key space `table-by-id`
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let p: pb::TableMeta = Message::decode(v.as_slice())?;
    let v1: TableMeta = FromToProto::from_pb(p)?;

    let p_latest = FromToProto::to_pb(&v1)?;

    let mut buf = vec![];
    Message::encode(&p_latest, &mut buf)?;
    Ok(buf)
}

/// It does not update any data but just print TableMeta in protobuf message format
/// that are found in log or state machine.
pub fn print_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let p: pb::TableMeta = Message::decode(v.as_slice())?;

    println!("{:?}", p);

    Ok(v)
}
