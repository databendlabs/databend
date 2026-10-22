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
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::TableMeta;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::MIN_READER_VER;
use databend_common_protos::pb;
use prost::Message;

use crate::common;

fn table() -> TableMeta {
    TableMeta {
        engine: String::new(),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        column_mask_policy_columns_ids: [(0, SecurityPolicyColumnMap::new(7, vec![0]))].into(),
        masking_where_use_unmasked_value: [0].into(),
        ..Default::default()
    }
}

#[test]
fn test_masking_where_original_roundtrip_and_reader_version() -> anyhow::Result<()> {
    let mut meta = table();
    let pb = meta.to_pb();
    assert_eq!(pb.min_reader_ver, 190);
    assert_eq!(pb.masking_where_use_unmasked_value, vec![0]);
    common::test_pb_from_to("v190_masking_where_original", meta.clone())?;
    assert_eq!(TableMeta::from_pb(pb.clone())?, meta);

    // The previous reader rejects the *outer* message before it can ignore field 43.
    assert!(189 < pb.min_reader_ver);
    let mut future = pb;
    future.min_reader_ver = databend_common_proto_conv::VER + 1;
    assert!(TableMeta::from_pb(future).is_err());

    // Old tables and unrelated writes retain the old compatibility floor.
    meta.masking_where_use_unmasked_value.clear();
    assert_eq!(meta.to_pb().min_reader_ver, MIN_READER_VER);
    meta.partition_key_seq = 7;
    assert_eq!(meta.to_pb().min_reader_ver, 188);
    meta.masking_where_use_unmasked_value.insert(0);
    assert_eq!(meta.to_pb().min_reader_ver, 190);
    meta.comment = "unrelated metadata update".into();
    assert_eq!(TableMeta::from_pb(meta.to_pb())?, meta);
    Ok(())
}

#[test]
fn test_decode_v190_masking_where_original() -> anyhow::Result<()> {
    // This independent fixture exercises field 43 even if the encoder later changes.
    // Embedded schema/statistics retain their v188 encoding.
    let bytes = vec![
        10, 7, 160, 6, 188, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 188,
        1, 168, 6, 24, 160, 6, 190, 1, 168, 6, 190, 1, 162, 2, 16, 8, 0, 18, 12, 160, 6, 190, 1,
        168, 6, 24, 8, 7, 18, 1, 0, 218, 2, 1, 0,
    ];
    let pb = pb::TableMeta::decode(bytes.as_slice())?;
    assert_eq!(pb.ver, 190);
    assert_eq!(pb.min_reader_ver, 190);
    assert_eq!(TableMeta::from_pb(pb)?, table());
    Ok(())
}
