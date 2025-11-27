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
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::principal::UDTF;
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
fn test_decode_v143_udtf() -> anyhow::Result<()> {
    let bytes = vec![
        10, 9, 116, 101, 115, 116, 95, 117, 100, 116, 102, 18, 21, 84, 104, 105, 115, 32, 105, 115,
        32, 97, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 66, 79, 10, 16, 10, 2,
        99, 49, 18, 10, 146, 2, 0, 160, 6, 143, 1, 168, 6, 24, 10, 16, 10, 2, 99, 50, 18, 10, 138,
        2, 0, 160, 6, 143, 1, 168, 6, 24, 18, 16, 10, 2, 99, 51, 18, 10, 170, 2, 0, 160, 6, 143, 1,
        168, 6, 24, 26, 16, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 116,
        49, 160, 6, 143, 1, 168, 6, 24, 42, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49,
        58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 160, 6, 143, 1, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "test_udtf".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDTF(UDTF {
            arg_types: vec![(s("c1"), DataType::String), (s("c2"), DataType::Boolean)],
            return_types: vec![(s("c3"), DataType::Date)],
            sql: "select * from t1".to_string(),
        }),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 143, want())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
