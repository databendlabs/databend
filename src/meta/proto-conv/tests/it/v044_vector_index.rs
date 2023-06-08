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

use chrono::TimeZone;
use chrono::Utc;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexType;
use common_meta_app::schema::IvfFlatIndex;
use common_meta_app::schema::VectorIndex;

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
// The message bytes are built from the output of `test_build_pb_buf()`
#[test]
fn test_decode_v44_table_meta() -> anyhow::Result<()> {
    let bytes = vec![
        8, 7, 16, 3, 26, 23, 50, 48, 49, 53, 45, 48, 51, 45, 48, 57, 32, 50, 48, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 50, 14, 73, 118, 102, 70, 108, 97, 116, 95, 49, 48, 48, 95, 55, 48,
        160, 6, 44, 168, 6, 24,
    ];

    let want = || {
        let table_id = 7;
        let index_type = IndexType::VECTOR;
        let created_on = Utc.with_ymd_and_hms(2015, 3, 9, 20, 0, 9).unwrap();
        let query = "".to_string();
        let vector_index = VectorIndex::IvfFlat(IvfFlatIndex {
            nlist: 100,
            nprobe: 70,
        });

        IndexMeta {
            table_id,
            index_type,
            created_on,
            drop_on: None,
            query,
            vector_index: Some(vector_index),
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 44, want())
}
