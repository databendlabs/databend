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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::UDFCloudScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UserDefinedFunction;
use fastrace::func_name;

use crate::common;

#[test]
fn test_pb_from_to_v165_udf_cloud_script() -> anyhow::Result<()> {
    let want = UserDefinedFunction {
        name: "my_fn".to_string(),
        description: "This is a description".to_string(),
        definition: UDFDefinition::UDFCloudScript(UDFCloudScript {
            code: "some code".to_string(),
            handler: "my_fn".to_string(),
            language: "python".to_string(),
            arg_types: vec![DataType::Number(NumberDataType::Int32)],
            return_type: DataType::Number(NumberDataType::Float32),
            imports: vec!["@s1/a.zip".to_string()],
            packages: vec!["numpy".to_string()],
            dockerfile: "FROM python:3.12-slim\n".to_string(),
            immutable: Some(true),
        }),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}
