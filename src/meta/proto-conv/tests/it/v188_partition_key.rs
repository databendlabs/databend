// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema::TableMeta;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;
use maplit::btreemap;

use crate::common;

// Persisted v188 bytes. Like other version fixtures, these must not be rewritten
// when future metadata versions are introduced.
#[test]
fn test_decode_v188_partition_key() -> anyhow::Result<()> {
    let bytes = vec![
        10, 7, 160, 6, 188, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 188,
        1, 168, 6, 24, 194, 2, 28, 101, 118, 101, 110, 116, 95, 116, 105, 109, 101, 32, 43, 32, 73,
        78, 84, 69, 82, 86, 65, 76, 32, 51, 48, 32, 68, 65, 89, 160, 6, 188, 1, 168, 6, 188, 1, 42,
        23, 10, 12, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 98, 121, 18, 7, 40, 112, 32,
        37, 32, 50, 41, 200, 2, 7, 208, 2, 6,
    ];
    let want = TableMeta {
        options: btreemap! {"partition_by".to_owned() => "(p % 2)".to_owned()},
        partition_key_seq: 7,
        partition_key_id: Some(6),
        ttl: Some("event_time + INTERVAL 30 DAY".to_owned()),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        engine: String::new(),
        ..Default::default()
    };
    assert_eq!(want.to_pb().min_reader_ver, 188);
    common::test_pb_from_to(func_name!(), want.clone())?;
    common::test_load_old(func_name!(), &bytes, 188, want)?;
    Ok(())
}

#[test]
fn test_partition_key_drop_preserves_sequence_roundtrip() -> anyhow::Result<()> {
    let dropped = TableMeta {
        partition_key_seq: 7,
        partition_key_id: None,
        ..Default::default()
    };
    let encoded = dropped.to_pb();
    assert_eq!(encoded.min_reader_ver, 188);
    let decoded = TableMeta::from_pb(encoded)?;
    assert_eq!(decoded, dropped);
    assert_eq!(decoded.partition_key_seq, 7);
    assert_eq!(decoded.partition_key_id, None);
    Ok(())
}
