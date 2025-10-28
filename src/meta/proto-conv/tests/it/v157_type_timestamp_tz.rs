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

use databend_common_expression::TableDataType;
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
fn test_decode_v157_data_type_timestamp_tz() -> anyhow::Result<()> {
    let table_data_type_v157 = vec![170, 3, 0, 160, 6, 157, 1, 168, 6, 24];

    let want = || TableDataType::TimestampTz;
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_data_type_v157.as_slice(), 157, want())?;

    Ok(())
}
