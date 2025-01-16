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
use databend_common_meta_app::schema::MarkedDeletedIndexMeta;
use databend_common_meta_app::schema::MarkedDeletedIndexType;
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
fn test_decode_v116_marked_deleted_index() -> anyhow::Result<()> {
    let marked_deleted_index_meta_v116 = vec![
        10, 23, 50, 48, 49, 53, 45, 48, 51, 45, 48, 57, 32, 50, 48, 58, 48, 48, 58, 48, 57, 32, 85,
        84, 67, 16, 1, 160, 6, 116, 168, 6, 24,
    ];

    let want = || {
        let created_on = Utc.with_ymd_and_hms(2015, 3, 9, 20, 0, 9).unwrap();
        MarkedDeletedIndexMeta {
            index_type: MarkedDeletedIndexType::AGGREGATING,
            dropped_on: created_on,
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        marked_deleted_index_meta_v116.as_slice(),
        116,
        want(),
    )?;

    Ok(())
}
