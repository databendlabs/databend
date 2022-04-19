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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::ColumnWithField;
use common_exception::Result;

use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionWithFieldTest;

#[test]
fn test_toyyyymm_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32, 1, 2, 3]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![197001u32, 197001u32, 197001u32, 197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_constant_date16",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_constant_date32",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymm_constant_datetime",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toYYYYMM", &tests)
}

#[test]
fn test_to_yyyymmdd_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_constant_date16",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_constant_date32",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmdd_constant_datetime",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1630833797i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toYYYYMMDD", &tests)
}

#[test]
fn test_toyyyymmddhhmmss_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![20210905092317u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_toyyyymmddhhmmss_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1630833797i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![20210905092317u64]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toYYYYMMDDhhmmss", &tests)
}

#[test]
fn test_tomonth_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633081817i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonth_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1633081817i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toMonth", &tests)
}

#[test]
fn test_todayofyear_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![275u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofyear_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1633173324i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![275u16]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toDayOfYear", &tests)
}

#[test]
fn test_todatofweek_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![6u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofweek_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1633173324i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![6u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toDayOfWeek", &tests)
}

#[test]
fn test_todayofmonth_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![2u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_todayofmonth_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1633173324i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![2u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toDayOfMonth", &tests)
}

#[test]
fn test_tohour_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_tohour_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tohour_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tohour_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tohour_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tohour_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tohour_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1634551542i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toHour", &tests)
}

#[test]
fn test_tominute_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_tominute_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tominute_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tominute_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![5u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tominute_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tominute_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tominute_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1634551542i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![5u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toMinute", &tests)
}

#[test]
fn test_tosecond_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![42u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tosecond_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![1634551542i64]), 1)),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![42u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toSecond", &tests)
}

#[test]
fn test_tomonday_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_tomonday_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![18919i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonday_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![18919i32]),
                DataField::new("dummy_1", DateType::arc()),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_tomonday_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634614318i64]),
                DataField::new("dummy_1", DateTimeType::arc(0, None)),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("toMonday", &tests)
}
