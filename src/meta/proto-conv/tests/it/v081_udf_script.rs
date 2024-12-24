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
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::UDFDefinition;
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
fn test_decode_v81_udf_python() -> anyhow::Result<()> {
    let bytes = vec![
        10, 8, 112, 108, 117, 115, 95, 105, 110, 116, 18, 21, 84, 104, 105, 115, 32, 105, 115, 32,
        97, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 34, 107, 10, 21, 104, 116,
        116, 112, 58, 47, 47, 108, 111, 99, 97, 108, 104, 111, 115, 116, 58, 56, 56, 56, 56, 18,
        11, 112, 108, 117, 115, 95, 105, 110, 116, 95, 112, 121, 26, 6, 112, 121, 116, 104, 111,
        110, 34, 17, 154, 2, 8, 58, 0, 160, 6, 81, 168, 6, 24, 160, 6, 81, 168, 6, 24, 34, 17, 154,
        2, 8, 58, 0, 160, 6, 81, 168, 6, 24, 160, 6, 81, 168, 6, 24, 42, 17, 154, 2, 8, 66, 0, 160,
        6, 81, 168, 6, 24, 160, 6, 81, 168, 6, 24, 160, 6, 81, 168, 6, 24, 42, 23, 50, 48, 50, 51,
        45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 160, 6, 81,
        168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "plus_int".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDFServer(UDFServer {
            address: "http://localhost:8888".to_string(),
            handler: "plus_int_py".to_string(),
            language: "python".to_string(),
            arg_types: vec![
                DataType::Number(NumberDataType::Int32),
                DataType::Number(NumberDataType::Int32),
            ],
            return_type: DataType::Number(NumberDataType::Int64),
        }),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 81, want())
}

#[test]
fn test_decode_v81_udf_sql() -> anyhow::Result<()> {
    let bytes = vec![
        10, 10, 105, 115, 110, 111, 116, 101, 109, 112, 116, 121, 18, 21, 84, 104, 105, 115, 32,
        105, 115, 32, 97, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 26, 34, 10, 1,
        112, 18, 23, 40, 112, 41, 32, 45, 62, 32, 40, 78, 79, 84, 32, 105, 115, 95, 110, 117, 108,
        108, 40, 112, 41, 41, 160, 6, 81, 168, 6, 24, 42, 23, 49, 57, 55, 53, 45, 48, 53, 45, 50,
        53, 32, 49, 54, 58, 51, 57, 58, 52, 52, 32, 85, 84, 67, 160, 6, 81, 168, 6, 24,
    ];
    let want = || UserDefinedFunction {
        name: "isnotempty".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::LambdaUDF(LambdaUDF {
            parameters: vec!["p".to_string()],
            definition: "(p) -> (NOT is_null(p))".to_string(),
        }),
        created_on: DateTime::<Utc>::from_timestamp(170267984, 0).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 81, want())
}
