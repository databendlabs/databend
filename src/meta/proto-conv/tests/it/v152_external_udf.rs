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
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v152_external_udf() -> anyhow::Result<()> {
    let user_defined_function_v152 = vec![
        10, 8, 116, 101, 115, 116, 95, 117, 100, 102, 18, 17, 105, 115, 32, 97, 32, 101, 120, 116,
        101, 114, 110, 97, 108, 32, 117, 100, 102, 34, 123, 10, 21, 104, 116, 116, 112, 58, 47, 47,
        49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 56, 56, 56, 56, 18, 7, 105, 115, 101, 109, 112,
        116, 121, 26, 6, 112, 121, 116, 104, 111, 110, 34, 10, 162, 3, 0, 160, 6, 152, 1, 168, 6,
        24, 34, 10, 146, 2, 0, 160, 6, 152, 1, 168, 6, 24, 42, 10, 138, 2, 0, 160, 6, 152, 1, 168,
        6, 24, 50, 19, 10, 13, 88, 45, 65, 112, 105, 45, 86, 101, 114, 115, 105, 111, 110, 18, 2,
        49, 49, 50, 17, 10, 7, 88, 45, 84, 111, 107, 101, 110, 18, 6, 97, 98, 99, 49, 50, 51, 160,
        6, 152, 1, 168, 6, 24, 42, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48,
        48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 152, 1, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "test_udf".to_string(),
        description: "is a external udf".to_string(),
        definition: UDFDefinition::UDFServer(UDFServer {
            address: "http://127.0.0.1:8888".to_string(),
            handler: "isempty".to_string(),
            headers: BTreeMap::from([
                ("X-Token".to_string(), "abc123".to_string()),
                ("X-Api-Version".to_string(), "11".to_string()),
            ]),
            language: "python".to_string(),
            arg_names: vec![],
            arg_types: vec![DataType::StageLocation, DataType::String],
            return_type: DataType::Boolean,
            immutable: None,
        }),
        created_on: DateTime::<Utc>::default(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        user_defined_function_v152.as_slice(),
        152,
        want(),
    )?;

    Ok(())
}
