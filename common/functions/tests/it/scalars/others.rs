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
    let tests = vec![
        // integer input test cases
        ScalarFunctionTest {
            name: "integer_input_i32_positive",
            nullable: true,
            columns: vec![Series::new([2130706433_i32]).into()],
            expect: Series::new(["127.0.0.1"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_i32_negative",
            nullable: true,
            columns: vec![Series::new(["-1"]).into()],
            expect: DataColumn::Constant(DataValue::String(None), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u8",
            nullable: true,
            columns: vec![Series::new([0_u8]).into()],
            expect: Series::new(["0.0.0.0"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "integer_input_u32",
            nullable: true,
            columns: vec![Series::new([3232235777_u32]).into()],
            expect: Series::new(["192.168.1.1"]).into(),
            error: "",
        },
        // float input test cases
        ScalarFunctionTest {
            name: "float_input_f64",
            nullable: true,
            columns: vec![Series::new([2130706433.3917_f64]).into()],
            expect: Series::new(["127.0.0.1"]).into(),
            error: "",
        },
        // string input test cases
        ScalarFunctionTest {
            name: "string_input_empty",
            nullable: true,
            columns: vec![Series::new([""]).into()],
            expect: Series::new(["0.0.0.0"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "string_input_u32",
            nullable: true,
            columns: vec![Series::new(["3232235777"]).into()],
            expect: Series::new(["192.168.1.1"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "string_input_f64",
            nullable: true,
            columns: vec![Series::new(["3232235777.72319"]).into()],
            expect: Series::new(["192.168.1.1"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "string_input_starts_with_integer",
            nullable: true,
            columns: vec![Series::new(["323a"]).into()],
            expect: Series::new(["0.0.1.67"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "string_input_char_inside_integer",
            nullable: true,
            columns: vec![Series::new(["323a111"]).into()],
            expect: Series::new(["0.0.1.67"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "string_input_invalid_string",
            nullable: true,
            columns: vec![Series::new(["-sad"]).into()],
            expect: Series::new(["0.0.0.0"]).into(),
            error: "",
        },
    ];

    let test_func = InetNtoaFunction::try_create("inet_ntoa")?;
    test_scalar_functions(test_func, &tests)
}
