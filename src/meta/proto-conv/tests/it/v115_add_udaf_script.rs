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

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::DataField;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
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
// The message bytes are built from the output of `proto_conv::test_build_pb_buf()`

#[test]
fn test_decode_v115_add_udaf_script() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 5, 109, 121, 95, 102, 110, 18, 21, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 100,
        101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 58, 99, 10, 9, 115, 111, 109, 101, 32, 99,
        111, 100, 101, 18, 10, 106, 97, 118, 97, 115, 99, 114, 105, 112, 116, 34, 17, 154, 2, 8,
        74, 0, 160, 6, 115, 168, 6, 24, 160, 6, 115, 168, 6, 24, 42, 17, 154, 2, 8, 58, 0, 160, 6,
        115, 168, 6, 24, 160, 6, 115, 168, 6, 24, 50, 30, 10, 3, 115, 117, 109, 26, 17, 154, 2, 8,
        66, 0, 160, 6, 115, 168, 6, 24, 160, 6, 115, 168, 6, 24, 160, 6, 115, 168, 6, 24, 160, 6,
        115, 168, 6, 24, 42, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48,
        58, 48, 48, 32, 85, 84, 67, 160, 6, 115, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "my_fn".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDAFScript(UDAFScript {
            code: "some code".to_string(),
            language: "javascript".to_string(),
            arg_types: vec![DataType::Number(NumberDataType::Int32)],
            state_fields: vec![DataField::new(
                "sum",
                DataType::Number(NumberDataType::Int64),
            )],
            return_type: DataType::Number(NumberDataType::Float32),
            runtime_version: "".to_string(),
            imports: vec![],
            packages: vec![],
        }),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 115, want())
}
