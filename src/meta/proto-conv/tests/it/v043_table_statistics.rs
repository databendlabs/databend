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
fn test_decode_v43_table_statistics() -> anyhow::Result<()> {
    let bytes_table_statistics_v43 = [
        8, 100, 16, 200, 1, 24, 15, 32, 20, 40, 1, 48, 2, 160, 6, 43, 168, 6, 24,
    ];
    let want = || databend_common_meta_app::schema::TableStatistics {
        number_of_rows: 100,
        data_bytes: 200,
        compressed_data_bytes: 15,
        index_data_bytes: 20,
        number_of_segments: Some(1),
        number_of_blocks: Some(2),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        bytes_table_statistics_v43.as_slice(),
        43,
        want(),
    )
}
