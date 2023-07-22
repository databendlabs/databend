// Copyright 2021 Datafuse Labs.
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

use common_exception::exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_meta_app::principal::UserDefinedFunction;

#[test]
fn test_udf() -> Result<()> {
    // lambda udf
    let udf = UserDefinedFunction::create_lambda_udf(
        "is_not_null",
        vec!["p".to_string()],
        "not(is_null(p))",
        "this is a description",
    );
    let ser = serde_json::to_string(&udf)?;

    let de = UserDefinedFunction::try_from(ser.into_bytes())?;
    assert_eq!(udf, de);

    // udf server
    let udf = UserDefinedFunction::create_udf_server(
        "strlen",
        "http://localhost:8888",
        vec![DataType::String],
        DataType::Number(NumberDataType::Int64),
        "this is a description",
    );
    let ser = serde_json::to_string(&udf)?;

    let de = UserDefinedFunction::try_from(ser.into_bytes())?;
    assert_eq!(udf, de);

    Ok(())
}
