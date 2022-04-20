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
fn test_get_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "get_by_field_name",
            columns: vec![
                Series::from_data(vec![
                    json!({"a":1_i32,"b":2_i32}),
                    json!({"A":3_i32,"B":4_i32}),
                    json!([1_i32, 2, 3]),
                ]),
                Series::from_data(vec!["a"]),
            ],
            expect: Series::from_data(vec![Some(json!(1_i32)), None, None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_by_index",
            columns: vec![
                Series::from_data(vec![
                    json!([0_i32, 1, 2]),
                    json!(["\"a\"", "\"b\"", "\"c\""]),
                    json!({"key":"val"}),
                ]),
                Series::from_data(vec![0_u32, 1_u32]),
            ],
            expect: Series::from_data(vec![
                Some(json!(0_i32)),
                Some(json!("\"a\"")),
                None,
                Some(json!(1_i32)),
                Some(json!("\"b\"")),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_by_field_name_error_type",
            columns: vec![
                Series::from_data(vec!["abc", "123"]),
                Series::from_data(vec![0_i32]),
            ],
            expect: Series::from_data(vec![None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'GET': (String, Int32)",
        },
    ];

    test_scalar_functions("get", &tests)
}

#[test]
fn test_get_ignore_case_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "get_by_field_name",
            columns: vec![
                Series::from_data(vec![
                    json!({"aa":1_i32, "aA":2, "Aa":3}),
                    json!([1_i32, 2, 3]),
                ]),
                Series::from_data(vec!["aA", "AA"]),
            ],
            expect: Series::from_data(vec![Some(json!(2_i32)), None, Some(json!(1_i32)), None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_by_field_name_error_type",
            columns: vec![
                Series::from_data(vec!["abc", "123"]),
                Series::from_data(vec![0_i32]),
            ],
            expect: Series::from_data(vec![None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'GET_IGNORE_CASE': (String, Int32)",
        },
    ];

    test_scalar_functions("get_ignore_case", &tests)
}

#[test]
fn test_get_path_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "get_by_path",
            columns: vec![
                Series::from_data(vec![json!({"a":[[1_i32],[2_i32]],"o":{"p":{"q":"r"}}})]),
                Series::from_data(vec!["a[0][0]", "a.b", "o.p:q", "o['p']['q']", "o[0]"]),
            ],
            expect: Series::from_data(vec![
                Some(json!(1_i32)),
                None,
                Some(json!("r")),
                Some(json!("r")),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_by_path_error_type",
            columns: vec![
                Series::from_data(vec!["abc", "123"]),
                Series::from_data(vec![0_i32]),
            ],
            expect: Series::from_data(vec![None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'GET_PATH': (String, Int32)",
        },
    ];

    test_scalar_functions("get_path", &tests)
}
