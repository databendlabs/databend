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
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
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
fn test_decode_v135_udf_server() -> anyhow::Result<()> {
    let bytes = vec![
        10, 5, 109, 121, 95, 102, 110, 18, 21, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 100,
        101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 34, 113, 10, 21, 104, 116, 116, 112, 58,
        47, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 56, 56, 56, 56, 18, 7, 105, 115, 101, 109,
        112, 116, 121, 26, 6, 112, 121, 116, 104, 111, 110, 34, 10, 146, 2, 0, 160, 6, 135, 1, 168,
        6, 24, 42, 10, 138, 2, 0, 160, 6, 135, 1, 168, 6, 24, 50, 19, 10, 13, 88, 45, 65, 112, 105,
        45, 86, 101, 114, 115, 105, 111, 110, 18, 2, 49, 49, 50, 17, 10, 7, 88, 45, 84, 111, 107,
        101, 110, 18, 6, 97, 98, 99, 49, 50, 51, 56, 1, 160, 6, 135, 1, 168, 6, 24, 42, 23, 49, 57,
        55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6,
        135, 1, 168, 6, 24,
    ];
    let want = || UserDefinedFunction {
        name: "my_fn".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDFServer(UDFServer {
            address: "http://127.0.0.1:8888".to_string(),
            handler: "isempty".to_string(),
            headers: BTreeMap::from([
                ("X-Token".to_string(), "abc123".to_string()),
                ("X-Api-Version".to_string(), "11".to_string()),
            ]),
            language: "python".to_string(),
            arg_names: vec![],
            arg_types: vec![DataType::String],
            return_type: DataType::Boolean,
            immutable: Some(true),
        }),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 135, want())
}

#[test]
fn test_decode_v135_udf_script() -> anyhow::Result<()> {
    let bytes = vec![
        10, 5, 109, 121, 95, 102, 110, 18, 21, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 100,
        101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 50, 121, 10, 9, 115, 111, 109, 101, 32,
        99, 111, 100, 101, 18, 5, 109, 121, 95, 102, 110, 26, 6, 112, 121, 116, 104, 111, 110, 34,
        19, 154, 2, 9, 58, 0, 160, 6, 134, 1, 168, 6, 24, 160, 6, 134, 1, 168, 6, 24, 42, 19, 154,
        2, 9, 74, 0, 160, 6, 134, 1, 168, 6, 24, 160, 6, 134, 1, 168, 6, 24, 50, 6, 51, 46, 49, 50,
        46, 50, 58, 9, 64, 115, 49, 47, 97, 46, 122, 105, 112, 58, 8, 64, 115, 50, 47, 98, 46, 112,
        121, 66, 5, 110, 117, 109, 112, 121, 66, 6, 112, 97, 110, 100, 97, 115, 72, 1, 160, 6, 134,
        1, 168, 6, 24, 42, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
        48, 48, 32, 85, 84, 67, 160, 6, 135, 1, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "my_fn".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDFScript(UDFScript {
            code: "some code".to_string(),
            handler: "my_fn".to_string(),
            language: "python".to_string(),
            arg_types: vec![DataType::Number(NumberDataType::Int32)],
            return_type: DataType::Number(NumberDataType::Float32),
            imports: vec!["@s1/a.zip".to_string(), "@s2/b.py".to_string()],
            packages: vec!["numpy".to_string(), "pandas".to_string()],
            runtime_version: "3.12.2".to_string(),
            immutable: Some(true),
        }),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 135, want())
}
