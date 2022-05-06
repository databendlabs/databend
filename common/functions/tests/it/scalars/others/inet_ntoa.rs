// Copyright 2022 Datafuse Labs
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

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_try_inet_ntoa_function() -> Result<()> {
    let tests = vec![
        // integer input test cases
        ScalarFunctionTest {
            name: "integer_input_i32_positive",
            columns: vec![Series::from_data(vec![2130706433_i32])],
            expect: Series::from_data(vec![Some("127.0.0.1")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_i32_negative",
            columns: vec![Series::from_data(vec![-1])],
            expect: Series::from_data(vec![Some("255.255.255.255")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u8",
            columns: vec![Series::from_data(vec![Some(0_u8)])],
            expect: Series::from_data(vec![Some("0.0.0.0")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u32",
            columns: vec![Series::from_data(vec![Some(3232235777_u32)])],
            expect: Series::from_data(vec![Some("192.168.1.1")]),
            error: "",
        },
        // float input test cases
        ScalarFunctionTest {
            name: "float_input_f64",
            columns: vec![Series::from_data(vec![2130706433.3917_f64])],
            expect: Series::from_data(vec![Some("127.0.0.1")]),
            error: "",
        },
        // string input test cases
        ScalarFunctionTest {
            name: "string_input_u32",
            columns: vec![Series::from_data(vec!["3232235777"])],
            expect: Series::from_data(vec![Some("192.168.1.1")]),
            error: "Expected a numeric type, but got String",
        },
    ];

    test_scalar_functions("try_inet_ntoa", &tests)
}

#[test]
fn test_inet_ntoa_function() -> Result<()> {
    let tests = vec![
        // integer input test cases
        ScalarFunctionTest {
            name: "integer_input_i32_positive",
            columns: vec![Series::from_data([2130706433_i32])],
            expect: Series::from_data(["127.0.0.1"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_i32_negative",
            columns: vec![Series::from_data([-1])],
            expect: Series::from_data(["255.255.255.255"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u8",
            columns: vec![Series::from_data([Some(0_u8)])],
            expect: Series::from_data([Some("0.0.0.0")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u32",
            columns: vec![Series::from_data([Some(3232235777_u32)])],
            expect: Series::from_data([Some("192.168.1.1")]),
            error: "",
        },
        // float input test cases
        ScalarFunctionTest {
            name: "float_input_f64",
            columns: vec![Series::from_data([2130706433.3917_f64])],
            expect: Series::from_data(["127.0.0.1"]),
            error: "",
        },
        // string input test cases
        ScalarFunctionTest {
            name: "string_input_empty",
            columns: vec![Series::from_data([""])],
            expect: Series::from_data([""]),
            error: "Expected a numeric type, but got String",
        },
    ];

    test_scalar_functions("inet_ntoa", &tests)
}
