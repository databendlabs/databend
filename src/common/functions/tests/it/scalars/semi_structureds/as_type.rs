// Copyright 2022 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;
use serde_json::json;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_as_boolean_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_boolean",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![Some(true), Some(false), None, None, None, None, None]),
            error: "",
        },
    ];
    test_scalar_functions("as_boolean", &tests)
}

fn test_as_integer_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_integer",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![None, None, Some(123), None, None, None, None]),
            error: "",
        },
    ];
    test_scalar_functions("as_integer", &tests)
}

fn test_as_float_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_float",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![None, None, Some(123.0), Some(12.34), None, None, None]),
            error: "",
        },
    ];
    test_scalar_functions("as_float", &tests)
}

fn test_as_string_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_string",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![None, None, None, None, Some("abc"), None, None]),
            error: "",
        },
    ];
    test_scalar_functions("as_string", &tests)
}

fn test_as_array_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_array",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![None, None, None, None, None, Some(VariantValue::from(json!([1,2,3]))), None]),
            error: "",
        },
    ];
    test_scalar_functions("as_array", &tests)
}

fn test_as_object_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "test_as_object_function",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(true)),
                    VariantValue::from(json!(false)),
                    VariantValue::from(json!(123)),
                    VariantValue::from(json!(12.34)),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!([1,2,3])),
                    VariantValue::from(json!({"a":"b"})),
                ]),
            ],
            expect: Series::from_data(vec![None, None, None, None, None, None, Some(VariantValue::from(json!({"a":"b"})))]),
            error: "",
        },
    ];
    test_scalar_functions("as_object", &tests)
}
