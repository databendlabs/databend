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

use databend_common_meta_app::schema::TableStatistics;
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
fn test_decode_v138_table_statistics() -> anyhow::Result<()> {
    let bytes = vec![
        8, 100, 16, 128, 8, 24, 128, 6, 32, 128, 8, 40, 1, 48, 2, 56, 128, 2, 64, 128, 1, 72, 128,
        1, 80, 128, 4, 160, 6, 138, 1, 168, 6, 24,
    ];
    let want = || TableStatistics {
        number_of_rows: 100,
        data_bytes: 1024,
        compressed_data_bytes: 768,
        index_data_bytes: 1024,
        bloom_index_size: Some(256),
        ngram_index_size: Some(128),
        inverted_index_size: Some(128),
        vector_index_size: Some(512),
        virtual_column_size: None,
        number_of_segments: Some(1),
        number_of_blocks: Some(2),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 138, want())
}
