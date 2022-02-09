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

use common_datavalues2::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use super::scalar_function2_test::test_scalar_functions2;
use super::scalar_function2_test::ScalarFunction2Test;

#[test]
fn test_arithmetic_function() -> Result<()> {
    let tests = vec![
        (
            ArithmeticPlusFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "add-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2, 1]),
                    Series::from_data(vec![1i64, 2, 3, 4]),
                ],
                expect: Series::from_data(vec![5i64, 5, 5, 5]),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[&Int16Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "add-diff-passed",
                columns: vec![
                    Series::from_data(vec![1i16, 2, 3, 4]),
                    Series::from_data(vec![1i64, 2, 3, 4]),
                ],
                expect: Series::from_data(vec![2i64, 4, 6, 8]),
                error: "",
            },
        ),
        (
            ArithmeticMinusFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "sub-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2]),
                    Series::from_data(vec![1i64, 2, 3]),
                ],
                expect: Series::from_data(vec![3i64, 1, -1]),
                error: "",
            },
        ),
        (
            ArithmeticMulFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "mul-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2]),
                    Series::from_data(vec![1i64, 2, 3]),
                ],
                expect: Series::from_data(vec![4i64, 6, 6]),
                error: "",
            },
        ),
        (
            ArithmeticDivFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "div-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2]),
                    Series::from_data(vec![1i64, 2, 3]),
                ],
                expect: Series::from_data(vec![4.0, 1.5, 0.6666666666666666]),
                error: "",
            },
        ),
        (
            ArithmeticIntDivFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "intdiv-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2]),
                    Series::from_data(vec![1i64, 2, 3]),
                ],
                expect: Series::from_data(vec![4i64, 1, 0]),
                error: "",
            },
        ),
        (
            ArithmeticModuloFunction::try_create_func("", &[&Int64Type::arc(), &Int64Type::arc()])?,
            ScalarFunction2Test {
                name: "mod-int64-passed",
                columns: vec![
                    Series::from_data(vec![4i64, 3, 2]),
                    Series::from_data(vec![1i64, 2, 3]),
                ],
                expect: Series::from_data(vec![0i64, 1, 2]),
                error: "",
            },
        ),
    ];

    for (test_function, test) in tests {
        test_scalar_functions2(test_function, &[test])?
    }

    Ok(())
}

/*
use common_datavalues::chrono;
use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;
use common_functions::scalars::*;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionTestWithType;

#[test]
fn test_arithmetic_date_interval() -> Result<()> {
    let to_seconds = |y: i32, m: u32, d: u32, h: u32, min: u32, s: u32| -> u32 {
        let date_time = chrono::NaiveDate::from_ymd(y, m, d).and_hms(h, min, s);
        date_time.timestamp() as u32
    };

    let to_days = |y: i32, m: u32, d: u32| -> i32 {
        let date_time = chrono::NaiveDate::from_ymd(y, m, d).and_hms(0, 0, 1);
        (date_time.timestamp() / (24 * 3600)) as i32
    };

    let daytime_to_ms = |days: i64, hour: i64, minute: i64, second: i64| -> i64 {
        (days * 24 * 3600 * 1000) + (hour * 3600 * 1000) + (minute * 60 * 1000) + (second * 1000)
    };

    let tests = vec![
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::DateTime32(None), false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::YearMonth), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "datetime-add-year-month-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_seconds(2020, 2, 29, 10, 30, 00), /* 2020-2-29-10:30:00 */
                            to_seconds(2000, 1, 31, 15, 00, 00),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::DateTime32(None), false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            12i64,       /* 1 year */
                            20 * 12 + 1, /* 20 years and 1 month */
                        ])
                        .into(),
                        DataField::new(
                            "dummy_1",
                            DataType::Interval(IntervalUnit::YearMonth),
                            false,
                        ),
                    ),
                ],
                expect: Series::new(vec![
                    to_seconds(2021, 2, 28, 10, 30, 00), /* 2021-2-28-10:30:00 */
                    to_seconds(2020, 2, 29, 15, 00, 00),
                ])
                .into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::DateTime32(None), false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::YearMonth), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "datetime-add-year-month-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_seconds(2021, 2, 28, 10, 30, 00),
                            to_seconds(2020, 2, 29, 15, 00, 00),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::DateTime32(None), false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![-12i64 /* -1 year */, -1 /* -1 month */]).into(),
                        DataField::new(
                            "dummy_1",
                            DataType::Interval(IntervalUnit::YearMonth),
                            false,
                        ),
                    ),
                ],
                expect: Series::new(vec![
                    to_seconds(2020, 2, 28, 10, 30, 00),
                    to_seconds(2020, 1, 29, 15, 00, 00),
                ])
                .into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::DateTime32(None), false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::DayTime), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "datetime-add-day-time-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_seconds(2020, 3, 1, 10, 30, 00),
                            to_seconds(2020, 3, 1, 10, 30, 00),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::DateTime32(None), false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            daytime_to_ms(-1, 0, 0, 0),
                            daytime_to_ms(-1, -1, 0, 0),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::Interval(IntervalUnit::DayTime), false),
                    ),
                ],
                expect: Series::new(vec![
                    to_seconds(2020, 2, 29, 10, 30, 00),
                    to_seconds(2020, 2, 29, 9, 30, 00),
                ])
                .into(),
                error: "",
            },
        ),
        (
            ArithmeticMinusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::DateTime32(None), false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::DayTime), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "datetime-minus-day-time-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_seconds(2020, 2, 29, 10, 30, 00),
                            to_seconds(2020, 2, 29, 9, 30, 00),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::DateTime32(None), false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            daytime_to_ms(-1, 0, 0, 0),
                            daytime_to_ms(-1, -1, 0, 0),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::Interval(IntervalUnit::DayTime), false),
                    ),
                ],
                expect: Series::new(vec![
                    to_seconds(2020, 3, 1, 10, 30, 00),
                    to_seconds(2020, 3, 1, 10, 30, 00),
                ])
                .into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date32, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::YearMonth), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date32-plus-year-month",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![to_days(2020, 2, 29), to_days(2000, 1, 31)]).into(),
                        DataField::new("dummy_1", DataType::Date32, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            12i64,       /* 1 year */
                            20 * 12 + 1, /* 20 years and 1 month */
                        ])
                        .into(),
                        DataField::new(
                            "dummy_1",
                            DataType::Interval(IntervalUnit::YearMonth),
                            false,
                        ),
                    ),
                ],
                expect: Series::new(vec![to_days(2021, 2, 28), to_days(2020, 2, 29)]).into(),
                error: "",
            },
        ),
        (
            ArithmeticMinusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date32, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::YearMonth), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date32-minus-year-month",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![to_days(2020, 2, 29), to_days(2000, 1, 31)]).into(),
                        DataField::new("dummy_1", DataType::Date32, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            -12i64,         /* - 1 year */
                            -(20 * 12 + 1), /* - 20 years and 1 month */
                        ])
                        .into(),
                        DataField::new(
                            "dummy_1",
                            DataType::Interval(IntervalUnit::YearMonth),
                            false,
                        ),
                    ),
                ],
                expect: Series::new(vec![to_days(2021, 2, 28), to_days(2020, 2, 29)]).into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date32, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::DayTime), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date32-plus-day-time",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![to_days(2020, 3, 1), to_days(2021, 3, 1)]).into(),
                        DataField::new("dummy_1", DataType::Date32, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            daytime_to_ms(-1, 0, 0, 0),
                            daytime_to_ms(-1, -1, 0, 0),
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::Interval(IntervalUnit::DayTime), false),
                    ),
                ],
                expect: Series::new(vec![to_days(2020, 2, 29), to_days(2021, 2, 28)]).into(),
                error: "",
            },
        ),
        (
            ArithmeticMinusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date32, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::DayTime), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date32-minus-day-time",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![to_days(2020, 3, 1), to_days(2021, 3, 1)]).into(),
                        DataField::new("dummy_1", DataType::Date32, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![daytime_to_ms(1, 0, 0, 0), daytime_to_ms(1, 1, 0, 0)])
                            .into(),
                        DataField::new("dummy_1", DataType::Interval(IntervalUnit::DayTime), false),
                    ),
                ],
                expect: Series::new(vec![to_days(2020, 2, 29), to_days(2021, 2, 28)]).into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date16, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::YearMonth), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date16-plus-year-month",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_days(2020, 2, 29) as u16,
                            to_days(2000, 1, 31) as u16,
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::Date16, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![
                            12i64,       /* 1 year */
                            20 * 12 + 1, /* 20 years and 1 month */
                        ])
                        .into(),
                        DataField::new(
                            "dummy_1",
                            DataType::Interval(IntervalUnit::YearMonth),
                            false,
                        ),
                    ),
                ],
                expect: Series::new(vec![
                    to_days(2021, 2, 28) as u16,
                    to_days(2020, 2, 29) as u16,
                ])
                .into(),
                error: "",
            },
        ),
        (
            ArithmeticPlusFunction::try_create_func("", &[
                DataTypeAndNullable::create(&DataType::Date16, false),
                DataTypeAndNullable::create(&DataType::Interval(IntervalUnit::DayTime), false),
            ])?,
            ScalarFunctionTestWithType {
                name: "date16-plus-day-time",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![
                            to_days(2020, 2, 29) as u16,
                            to_days(2021, 2, 28) as u16,
                        ])
                        .into(),
                        DataField::new("dummy_1", DataType::Date16, false),
                    ),
                    DataColumnWithField::new(
                        Series::new(vec![daytime_to_ms(1, 0, 0, 0), daytime_to_ms(1, 1, 0, 0)])
                            .into(),
                        DataField::new("dummy_1", DataType::Interval(IntervalUnit::DayTime), false),
                    ),
                ],
                expect: Series::new(vec![to_days(2020, 3, 1) as u16, to_days(2021, 3, 1) as u16])
                    .into(),
                error: "",
            },
        ),
    ];

    for (test_function, test) in tests {
        test_scalar_functions_with_type(test_function, &[test])?;
    }

    Ok(())
}
 */
