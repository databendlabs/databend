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
fn test_to_yyyymm_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32, 1, 2, 3]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![197001u32, 197001u32, 197001u32, 197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_constant_date16",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_constant_date32",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymm_constant_datetime",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i64]), 1)),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![197001u32]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_yyyymm", &tests)
}

#[test]
fn test_to_yyyymmdd_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_constant_date16",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_constant_date32",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101u32]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmdd_constant_datetime",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1630833797000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![20210905u32]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_yyyymmdd", &tests)
}

#[test]
fn test_to_yyyymmddhhmmss_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630833797000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![20210905092317u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![19700101000000u64]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_yyyymmddhhmmss_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1630833797000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![20210905092317u64]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_yyyymmddhhmmss", &tests)
}

#[test]
fn test_to_month_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_month_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_month_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_month_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633081817000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_month_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_month_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_month_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1633081817000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_month", &tests)
}

#[test]
fn test_to_day_of_year_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![275u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_year_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1633173324000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![275u16]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_day_of_year", &tests)
}

#[test]
fn test_todatefweek_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![6u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![4u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_week_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1633173324000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![6u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_day_of_week", &tests)
}

#[test]
fn test_to_day_of_month_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1633173324000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![2u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![1u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_day_of_month_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1633173324000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![2u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_day_of_month", &tests)
}

#[test]
fn test_to_hour_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_hour_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1634551542000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![10u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_hour", &tests)
}

#[test]
fn test_to_minute_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![5u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_minute_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1634551542000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![5u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_minute", &tests)
}

#[test]
fn test_to_second_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_second_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_second_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![0i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_second_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634551542000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![42u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_second_date16_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_second_date32_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(Series::from_data(vec![0i32]), 1)),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![0u8]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_second_datetime_constant",
            columns: vec![ColumnWithField::new(
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![1634551542000000i64]),
                    1,
                )),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![42u8]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_second", &tests)
}

#[test]
fn test_to_monday_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionWithFieldTest {
            name: "test_to_monday_date16",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![18919i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_monday_date32",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![18919i32]),
                DataField::new("dummy_1", DateType::new_impl()),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
        ScalarFunctionWithFieldTest {
            name: "test_to_monday_datetime",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1634614318000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![18918u16]),
            error: "",
        },
    ];

    test_scalar_functions_with_type("to_monday", &tests)
}
