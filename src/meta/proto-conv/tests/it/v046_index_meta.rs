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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexType;
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
// The message bytes are built from the output of `proto_conv::test_build_pb_buf()`
#[test]
fn test_decode_v46_index() -> anyhow::Result<()> {
    let index_v046 = vec![
        8, 7, 16, 1, 26, 23, 50, 48, 49, 53, 45, 48, 51, 45, 48, 57, 32, 50, 48, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 42, 55, 83, 69, 76, 69, 67, 84, 32, 97, 44, 32, 115, 117, 109, 40,
        98, 41, 32, 70, 82, 79, 77, 32, 100, 101, 102, 97, 117, 108, 116, 46, 116, 49, 32, 87, 72,
        69, 82, 69, 32, 97, 32, 62, 32, 51, 32, 71, 82, 79, 85, 80, 32, 66, 89, 32, 98, 160, 6, 46,
        168, 6, 24,
    ];

    let want = || {
        let table_id = 7;
        let index_type = IndexType::AGGREGATING;
        let created_on = Utc.with_ymd_and_hms(2015, 3, 9, 20, 0, 9).unwrap();
        let original_query = "".to_string();
        let query = "SELECT a, sum(b) FROM default.t1 WHERE a > 3 GROUP BY b".to_string();

        IndexMeta {
            table_id,
            index_type,
            created_on,
            dropped_on: None,
            original_query,
            query,
            updated_on: None,
            sync_creation: false,
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), index_v046.as_slice(), 46, want())?;

    Ok(())
}
