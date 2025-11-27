// Copyright 2025 Datafuse Labs.
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
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UserDefinedFunction;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v160_udf_update_on() -> anyhow::Result<()> {
    let user_defined_function_v160 = vec![
        10, 15, 117, 100, 102, 95, 119, 105, 116, 104, 95, 117, 112, 100, 97, 116, 101, 18, 17,
        117, 100, 102, 32, 119, 105, 116, 104, 32, 116, 114, 97, 99, 107, 105, 110, 103, 26, 22,
        10, 1, 112, 18, 10, 40, 112, 41, 32, 45, 62, 32, 40, 112, 41, 160, 6, 160, 1, 168, 6, 24,
        42, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85,
        84, 67, 90, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 52, 50, 58, 53, 48,
        32, 85, 84, 67, 160, 6, 160, 1, 168, 6, 24,
    ];

    let want = || UserDefinedFunction {
        name: "udf_with_update".to_string(),
        description: "udf with tracking".to_string(),
        definition: UDFDefinition::LambdaUDF(LambdaUDF {
            parameters: vec!["p".to_string()],
            definition: "(p) -> (p)".to_string(),
        }),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(1702604570, 0).unwrap(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        user_defined_function_v160.as_slice(),
        160,
        want(),
    )?;
    Ok(())
}
