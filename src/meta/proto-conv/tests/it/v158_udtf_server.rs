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

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDTFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
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
fn test_decode_v158_server_udtf() -> anyhow::Result<()> {
    let bytes = vec![
        10, 15, 116, 101, 115, 116, 95, 115, 99, 97, 108, 97, 114, 95, 117, 100, 102, 18, 21, 84,
        104, 105, 115, 32, 105, 115, 32, 97, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111,
        110, 82, 144, 1, 10, 21, 104, 116, 116, 112, 58, 47, 47, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 58, 56, 56, 56, 56, 18, 11, 112, 108, 117, 115, 95, 105, 110, 116, 95, 112, 121,
        26, 6, 112, 121, 116, 104, 111, 110, 34, 10, 146, 2, 0, 160, 6, 158, 1, 168, 6, 24, 34, 10,
        138, 2, 0, 160, 6, 158, 1, 168, 6, 24, 42, 16, 10, 2, 99, 49, 18, 10, 146, 2, 0, 160, 6,
        158, 1, 168, 6, 24, 42, 25, 10, 2, 99, 50, 18, 19, 154, 2, 9, 42, 0, 160, 6, 158, 1, 168,
        6, 24, 160, 6, 158, 1, 168, 6, 24, 50, 14, 10, 4, 107, 101, 121, 49, 18, 6, 118, 97, 108,
        117, 101, 49, 66, 2, 99, 49, 66, 2, 99, 50, 160, 6, 158, 1, 168, 6, 24, 42, 23, 50, 48, 50,
        51, 45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 160, 6,
        158, 1, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "test_scalar_udf".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDTFServer(UDTFServer {
            address: "http://localhost:8888".to_string(),
            handler: "plus_int_py".to_string(),
            headers: vec![("key1".to_string(), "value1".to_string())]
                .into_iter()
                .collect(),
            language: "python".to_string(),
            arg_names: vec![s("c1"), s("c2")],
            arg_types: vec![DataType::String, DataType::Boolean],
            return_types: vec![
                (s("c1"), DataType::String),
                (s("c2"), DataType::Number(NumberDataType::Int8)),
            ],
            immutable: None,
        }),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 158, want())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
