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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use serde_json::json;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_check_json_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "check_json_bool",
            columns: vec![Series::from_data(vec![true, false])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_int",
            columns: vec![Series::from_data(vec![1_i16, -1, 100])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_float",
            columns: vec![Series::from_data(vec![12.34_f64, 56.79, 0.12345679])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_string",
            columns: vec![Series::from_data(vec!["\"abcd\"", "true", "123"])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_array",
            columns: vec![Series::from_data(vec![
                "[\"an\", \"array\"]",
                "[\"str\", true]",
                "[1, 2, 3]",
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_object",
            columns: vec![Series::from_data(vec![
                "{\"an\": \"object\"}",
                "{\"key\": true}",
                "{\"k\": 1}",
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_invalid_string",
            columns: vec![Series::from_data(vec!["\"abcd\"", "[1,2", "{\"k"])],
            expect: Series::from_data(vec![
                None::<&str>,
                Some("EOF while parsing a list at line 1 column 4"),
                Some("EOF while parsing a string at line 1 column 3"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_bool",
            columns: vec![Series::from_data(vec![None, Some(true), Some(false)])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_int",
            columns: vec![Series::from_data(vec![Some(1_i16), Some(-1), Some(100)])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_float",
            columns: vec![Series::from_data(vec![
                Some(12.34_f64),
                Some(56.79),
                Some(0.12345679),
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_string",
            columns: vec![Series::from_data(vec![
                None,
                Some("\"abcd\""),
                Some("true"),
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_array",
            columns: vec![Series::from_data(vec![
                Some("[\"an\", \"array\"]"),
                Some("[\"str\", true]"),
                Some("[1, 2, 3]"),
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_object",
            columns: vec![Series::from_data(vec![
                Some("{\"an\": \"object\"}"),
                Some("{\"k\": true}"),
                Some("{\"k\": 1}"),
            ])],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_nullable_invalid_string",
            columns: vec![Series::from_data(vec![
                Some("\"abcd\""),
                Some("[1,2"),
                Some("{\"k"),
            ])],
            expect: Series::from_data(vec![
                None::<&str>,
                Some("EOF while parsing a list at line 1 column 4"),
                Some("EOF while parsing a string at line 1 column 3"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_array",
            columns: vec![Arc::new(ArrayColumn::from_data(
                Arc::new(ArrayType::create(StringType::arc())),
                vec![0, 1, 3, 6].into(),
                Series::from_data(vec!["test", "data", "bend", "hello", "world", "NULL"]),
            ))],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'CHECK_JSON': Array",
        },
        ScalarFunctionTest {
            name: "check_json_struct",
            columns: vec![Arc::new(StructColumn::from_data(
                vec![
                    Series::from_data(vec![18869i32, 18948i32, 1]),
                    Series::from_data(vec![1i8, 2i8, 3]),
                ],
                Arc::new(StructType::create(
                    vec!["date".to_owned(), "integer".to_owned()],
                    vec![Date32Type::arc(), Int8Type::arc()],
                )),
            ))],
            expect: Series::from_data(vec![None::<&str>, None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'CHECK_JSON': Struct",
        },
        ScalarFunctionTest {
            name: "check_json_variant",
            columns: vec![Arc::new(VariantColumn::new_from_vec(vec![
                VariantValue::from(json!(null)),
                VariantValue::from(json!(true)),
                VariantValue::from(json!(false)),
                VariantValue::from(json!(123)),
                VariantValue::from(json!(12.34)),
                VariantValue::from(json!("{\"k\": 1}")),
                VariantValue::from(json!("\"abcd\"")),
                VariantValue::from(json!("[1,2")),
            ]))],
            expect: Series::from_data(vec![
                None::<&str>,
                None::<&str>,
                None::<&str>,
                None::<&str>,
                None::<&str>,
                None::<&str>,
                None::<&str>,
                Some("EOF while parsing a list at line 1 column 4"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "check_json_null",
            columns: vec![Arc::new(NullColumn::new(1))],
            expect: Arc::new(NullColumn::new(1)),
            error: "",
        },
    ];

    test_scalar_functions("check_json", &tests)
}
