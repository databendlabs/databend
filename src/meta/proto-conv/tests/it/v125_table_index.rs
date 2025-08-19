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

// These bytes are built when a new version in introduced,

use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use fastrace::func_name;
use maplit::btreemap;

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
fn test_decode_v125_table_index() -> anyhow::Result<()> {
    let udf_server_headers_v124 = vec![
        10, 4, 105, 100, 120, 49, 18, 2, 1, 2, 24, 1, 34, 32, 102, 49, 48, 98, 50, 51, 48, 49, 53,
        51, 101, 49, 52, 102, 50, 99, 56, 52, 54, 48, 51, 57, 53, 56, 100, 55, 102, 56, 54, 52,
        102, 56, 42, 20, 10, 9, 116, 111, 107, 101, 110, 105, 122, 101, 114, 18, 7, 99, 104, 105,
        110, 101, 115, 101, 48, 1, 160, 6, 125, 168, 6, 24,
    ];
    let want = || TableIndex {
        index_type: TableIndexType::Ngram,
        name: "idx1".to_string(),
        column_ids: vec![1, 2],
        sync_creation: true,
        version: "f10b230153e14f2c84603958d7f864f8".to_string(),
        options: btreemap! {s("tokenizer") => s("chinese")},
    };

    common::test_load_old(
        func_name!(),
        udf_server_headers_v124.as_slice(),
        125,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
