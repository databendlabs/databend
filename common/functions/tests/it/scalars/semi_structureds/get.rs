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
                    VariantValue::from(json!({"a":1_i32,"b":2_i32})),
                    VariantValue::from(json!({"A":3_i32,"B":4_i32})),
                    VariantValue::from(json!([1_i32, 2, 3])),
                ]),
                Series::from_data(vec!["a"]),
            ],
            expect: Series::from_data(vec![Some(VariantValue::from(json!(1_i32))), None, None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_by_index",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!([0_i32, 1, 2])),
                    VariantValue::from(json!(["\"a\"", "\"b\"", "\"c\""])),
                    VariantValue::from(json!({"key":"val"})),
                ]),
                Series::from_data(vec![0_u32, 1_u32]),
            ],
            expect: Series::from_data(vec![
                Some(VariantValue::from(json!(0_i32))),
                Some(VariantValue::from(json!("\"a\""))),
                None,
                Some(VariantValue::from(json!(1_i32))),
                Some(VariantValue::from(json!("\"b\""))),
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
            name: "get_ignore_case",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!({"aa":1_i32, "aA":2, "Aa":3})),
                    VariantValue::from(json!([1_i32, 2, 3])),
                ]),
                Series::from_data(vec!["aA", "AA"]),
            ],
            expect: Series::from_data(vec![
                Some(VariantValue::from(json!(2_i32))),
                None,
                Some(VariantValue::from(json!(1_i32))),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_ignore_case_error_type",
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
            name: "get_path",
            columns: vec![
                Series::from_data(vec![VariantValue::from(
                    json!({"a":[[1_i32],[2_i32]],"o":{"p":{"q":"r"}}}),
                )]),
                Series::from_data(vec!["a[0][0]", "a.b", "o.p:q", "o['p']['q']", "o[0]"]),
            ],
            expect: Series::from_data(vec![
                Some(VariantValue::from(json!(1_i32))),
                None,
                Some(VariantValue::from(json!("r"))),
                Some(VariantValue::from(json!("r"))),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "get_path_error_type",
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

#[test]
fn test_array_get_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "array_get_int64",
            columns: vec![
                Series::from_data(vec![
                    ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
                    ArrayValue::new(vec![4_i64.into(), 5_i64.into(), 6_i64.into()]),
                    ArrayValue::new(vec![7_i64.into(), 8_i64.into(), 9_i64.into()]),
                ]),
                Series::from_data(vec![0_u32, 0, 0]),
            ],
            expect: Series::from_data(vec![Some(1_i64), Some(4_i64), Some(7_i64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "array_get_string",
            columns: vec![
                Series::from_data(vec![
                    ArrayValue::new(vec![
                        "a1".as_bytes().into(),
                        "a2".as_bytes().into(),
                        "a3".as_bytes().into(),
                    ]),
                    ArrayValue::new(vec![
                        "b1".as_bytes().into(),
                        "b2".as_bytes().into(),
                        "b3".as_bytes().into(),
                    ]),
                    ArrayValue::new(vec![
                        "c1".as_bytes().into(),
                        "c2".as_bytes().into(),
                        "c3".as_bytes().into(),
                    ]),
                ]),
                Series::from_data(vec![0_u32, 0, 0]),
            ],
            expect: Series::from_data(vec![Some("a1"), Some("b1"), Some("c1")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "array_get_out_of_bounds",
            columns: vec![
                Series::from_data(vec![
                    ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
                    ArrayValue::new(vec![4_i64.into(), 5_i64.into(), 6_i64.into()]),
                    ArrayValue::new(vec![7_i64.into(), 8_i64.into(), 9_i64.into()]),
                ]),
                Series::from_data(vec![3_u32, 3, 3]),
            ],
            expect: Series::from_data(vec![None::<&str>]),
            error: "Index out of array column bounds: the len is 3 but the index is 3",
        },
        ScalarFunctionTest {
            name: "array_get_error_type",
            columns: vec![
                Series::from_data(vec![
                    ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
                    ArrayValue::new(vec![4_i64.into(), 5_i64.into(), 6_i64.into()]),
                    ArrayValue::new(vec![7_i64.into(), 8_i64.into(), 9_i64.into()]),
                ]),
                Series::from_data(vec!["a", "a", "a"]),
            ],
            expect: Series::from_data(vec![None::<&str>]),
            error: "Invalid argument types for function 'GET': (Array, String)",
        },
    ];

    test_scalar_functions("get", &tests)
}
