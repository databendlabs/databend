// Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::LockType;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
#[test]
fn test_decode_v62_table_lock_meta() -> anyhow::Result<()> {
    let bytes = vec![
        10, 4, 114, 111, 111, 116, 18, 4, 110, 111, 100, 101, 26, 5, 113, 117, 101, 114, 121, 34,
        23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84,
        67, 42, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 53, 32,
        85, 84, 67, 58, 10, 10, 3, 107, 101, 121, 18, 3, 118, 97, 108, 160, 6, 62, 168, 6, 24,
    ];

    let want = || LockMeta {
        user: "root".to_string(),
        node: "node".to_string(),
        query_id: "query".to_string(),
        created_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        acquired_on: Some(Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 15).unwrap()),
        lock_type: LockType::TABLE,
        extra_info: BTreeMap::from([("key".to_string(), "val".to_string())]),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 62, want())?;
    Ok(())
}
