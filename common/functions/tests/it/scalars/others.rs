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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::InetAtonFunction;
use common_functions::scalars::InetNtoaFunction;
use common_functions::scalars::RunningDifferenceFunction;

use super::scalar_function2_test::test_scalar_functions2;
use super::scalar_function2_test::ScalarFunction2Test;
use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionTest;
use crate::scalars::scalar_function_test::ScalarFunctionTestWithType;

#[test]
fn test_running_difference_first_null() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "i8_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_i8), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i16)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u8_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_u8), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i16)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i16_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_i16), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i32)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u16_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_u16), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i32)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i32_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_i32), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i64)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u32_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_u32), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i64)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i64_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_i64), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i64)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u64_first_null",
            nullable: true,
            columns: vec![Series::new([None, Some(1_u64), None, Some(3), Some(7)]).into()],
            expect: Series::new([None, None, None, None, Some(4_i64)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i8_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_i8), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i16), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u8_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_u8), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i16), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i16_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_i16), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i32), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u16_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_u16), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i32), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i32_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_i32), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i64), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u32_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_u32), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i64), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i64_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_i64), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i64), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u64_first_not_null",
            nullable: true,
            columns: vec![Series::new([Some(2_u64), Some(3), None, Some(4), Some(10)]).into()],
            expect: Series::new([Some(0_i64), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
    ];

    test_scalar_functions(RunningDifferenceFunction::try_create("a")?, &tests)
}

#[test]
fn test_running_difference_datetime32_first_null() -> Result<()> {
    let tests = vec![
        ScalarFunctionTestWithType {
            name: "datetime32_first_null",
            nullable: true,
            columns: vec![DataColumnWithField::new(
                Series::new([None, Some(3_u32), None, Some(4), Some(10)]).into(),
                DataField::new("dummy_1", DataType::DateTime32(None), true),
            )],
            expect: Series::new([None, None, None, None, Some(6_i64)]).into(),
            error: "",
        },
        ScalarFunctionTestWithType {
            name: "datetime32_first_not_null",
            nullable: true,
            columns: vec![DataColumnWithField::new(
                Series::new([Some(2_u32), Some(3), None, Some(4), Some(10)]).into(),
                DataField::new("dummy_1", DataType::DateTime32(None), true),
            )],
            expect: Series::new([Some(0_i64), Some(1), None, None, Some(6)]).into(),
            error: "",
        },
    ];

    test_scalar_functions_with_type(RunningDifferenceFunction::try_create("a")?, &tests)
}

#[test]
fn test_inet_aton_function() -> Result<()> {
    use common_datavalues2::prelude::*;

    let tests = vec![
        ScalarFunction2Test {
            name: "valid input",
            columns: vec![Series::from_data(vec!["127.0.0.1"])],
            expect: Series::from_data(vec![Option::<u32>::Some(2130706433_u32)]),
            error: "",
        },
        ScalarFunction2Test {
            name: "invalid input",
            columns: vec![Series::from_data(vec![Some("invalid")])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "",
        },
        ScalarFunction2Test {
            name: "null input",
            columns: vec![Series::from_data(vec![Option::<Vec<u8>>::None])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "",
        },
    ];

    let test_func = InetAtonFunction::try_create("inet_aton")?;
    test_scalar_functions2(test_func, &tests)
}

#[test]
fn test_inet_ntoa_function() -> Result<()> {
    use common_datavalues2::prelude::*;

    let tests = vec![
        // integer input test cases
        ScalarFunction2Test {
            name: "integer_input_i32_positive",
            columns: vec![Series::from_data(vec![2130706433_i32])],
            expect: Series::from_data(vec![Some("127.0.0.1")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "integer_input_i32_negative",
            columns: vec![Series::from_data(vec!["-1"])],
            expect: Series::from_data(vec![Option::<Vec<u8>>::None]),
            error: "",
        },
        ScalarFunction2Test {
            name: "integer_input_u8",
            columns: vec![Series::from_data(vec![Some(0_u8)])],
            expect: Series::from_data(vec![Some("0.0.0.0")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "integer_input_u32",
            columns: vec![Series::from_data(vec![Some(3232235777_u32)])],
            expect: Series::from_data(vec![Some("192.168.1.1")]),
            error: "",
        },
        // float input test cases
        ScalarFunction2Test {
            name: "float_input_f64",
            columns: vec![Series::from_data(vec![2130706433.3917_f64])],
            expect: Series::from_data(vec![Some("127.0.0.1")]),
            error: "",
        },
        // string input test cases
        ScalarFunction2Test {
            name: "string_input_empty",
            columns: vec![Series::from_data(vec![""])],
            expect: Series::from_data(vec![Option::<Vec<u8>>::None]),
            error: "",
        },
        ScalarFunction2Test {
            name: "string_input_u32",
            columns: vec![Series::from_data(vec!["3232235777"])],
            expect: Series::from_data(vec![Some("192.168.1.1")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "string_input_f64",
            columns: vec![Series::from_data(vec!["3232235777.72319"])],
            expect: Series::from_data(vec![Some("192.168.1.1")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "string_input_starts_with_integer",
            columns: vec![Series::from_data(vec!["323a"])],
            expect: Series::from_data(vec![Some("0.0.1.67")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "string_input_char_inside_integer",
            columns: vec![Series::from_data(vec!["323a111"])],
            expect: Series::from_data(vec![Some("0.0.1.67")]),
            error: "",
        },
        ScalarFunction2Test {
            name: "string_input_invalid_string",
            columns: vec![Series::from_data(["-sad"])],
            expect: Series::from_data(vec![Option::<Vec<u8>>::None]),
            error: "",
        },
    ];

    let test_func = InetNtoaFunction::try_create("inet_ntoa")?;
    test_scalar_functions2(test_func, &tests)
}
