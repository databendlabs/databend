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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
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
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v173_table_branch() -> anyhow::Result<()> {
    let table_branch_v173: Vec<u8> = vec![
        10, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85,
        84, 67, 16, 1, 160, 6, 173, 1, 168, 6, 24,
    ];

    let want = || mt::TableBranch {
        expire_at: DateTime::<Utc>::from_timestamp(1702603569, 0),
        branch_id: 1,
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_branch_v173.as_slice(), 173, want())
}
