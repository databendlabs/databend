// Copyright 2020 Datafuse Labs.
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

use common_datavalues::chrono;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

#[test]
fn test_arithmetic_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Int16, false),
    ]);

    let tests = vec![
        Test {
            name: "add-int64-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["a", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2, 1]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
                Series::new(vec![1i16, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![5i64, 5, 5, 5]).into(),
            error: "",
        },
        Test {
            name: "add-diff-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["c", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2, 1]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
                Series::new(vec![1i16, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![5i64, 5, 5, 5]).into(),
            error: "",
        },
        Test {
            name: "sub-int64-passed",
            display: "minus",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMinusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![3i64, 1, -1]).into(),
            error: "",
        },
        Test {
            name: "mul-int64-passed",
            display: "multiply",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMulFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![4i64, 6, 6]).into(),
            error: "",
        },
        Test {
            name: "div-int64-passed",
            display: "divide",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticDivFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![4.0, 1.5, 0.6666666666666666]).into(),
            error: "",
        },
        Test {
            name: "mod-int64-passed",
            display: "modulo",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticModuloFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![0i64, 1, 2]).into(),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        // Type check.
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        let func = t.func;
        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let expect_type = func.return_type(&args)?.clone();
        let v = &(func.eval(&columns, rows)?);
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v, &t.expect);
    }
    Ok(())
}

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

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new(
            "interval-day-time",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
        DataField::new(
            "interval-year-month",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
        DataField::new("datetime32", DataType::DateTime32(None), false),
        DataField::new("date32", DataType::Date32, false),
        DataField::new("date16", DataType::Date16, false),
    ]);

    let tests = vec![
        Test {
            name: "datetime-add-year-month-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["datetime32", "interval-year-month"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![
                    to_seconds(2020, 2, 29, 10, 30, 00), /* 2020-2-29-10:30:00 */
                    to_seconds(2000, 1, 31, 15, 00, 00),
                ])
                .into(),
                Series::new(vec![
                    12i64,       /* 1 year */
                    20 * 12 + 1, /* 20 years and 1 month */
                ])
                .into(),
            ],
            expect: Series::new(vec![
                to_seconds(2021, 2, 28, 10, 30, 00), /* 2021-2-28-10:30:00 */
                to_seconds(2020, 2, 29, 15, 00, 00),
            ])
            .into(),
            error: "",
        },
        Test {
            name: "datetime-add-year-month-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["datetime32", "interval-year-month"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![
                    to_seconds(2021, 2, 28, 10, 30, 00),
                    to_seconds(2020, 2, 29, 15, 00, 00),
                ])
                .into(),
                Series::new(vec![-12i64 /* -1 year */, -1 /* -1 month */]).into(),
            ],
            expect: Series::new(vec![
                to_seconds(2020, 2, 28, 10, 30, 00),
                to_seconds(2020, 1, 29, 15, 00, 00),
            ])
            .into(),
            error: "",
        },
        Test {
            name: "datetime-add-day-time-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["datetime32", "interval-day-time"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![
                    to_seconds(2020, 3, 1, 10, 30, 00),
                    to_seconds(2020, 3, 1, 10, 30, 00),
                ])
                .into(),
                Series::new(vec![
                    daytime_to_ms(-1, 0, 0, 0),
                    daytime_to_ms(-1, -1, 0, 0),
                ])
                .into(),
            ],
            expect: Series::new(vec![
                to_seconds(2020, 2, 29, 10, 30, 00),
                to_seconds(2020, 2, 29, 9, 30, 00),
            ])
            .into(),
            error: "",
        },
        Test {
            name: "datetime-minus-day-time-passed",
            display: "minus",
            nullable: false,
            arg_names: vec!["datetime32", "interval-day-time"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus)?,
            columns: vec![
                Series::new(vec![
                    to_seconds(2020, 2, 29, 10, 30, 00),
                    to_seconds(2020, 2, 29, 9, 30, 00),
                ])
                .into(),
                Series::new(vec![
                    daytime_to_ms(-1, 0, 0, 0),
                    daytime_to_ms(-1, -1, 0, 0),
                ])
                .into(),
            ],
            expect: Series::new(vec![
                to_seconds(2020, 3, 1, 10, 30, 00),
                to_seconds(2020, 3, 1, 10, 30, 00),
            ])
            .into(),
            error: "",
        },
        Test {
            name: "date32-plus-year-month",
            display: "plus",
            nullable: false,
            arg_names: vec!["date32", "interval-year-month"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![to_days(2020, 2, 29), to_days(2000, 1, 31)]).into(),
                Series::new(vec![
                    12i64,       /* 1 year */
                    20 * 12 + 1, /* 20 years and 1 month */
                ])
                .into(),
            ],
            expect: Series::new(vec![to_days(2021, 2, 28), to_days(2020, 2, 29)]).into(),
            error: "",
        },
        Test {
            name: "date32-minus-year-month",
            display: "minus",
            nullable: false,
            arg_names: vec!["date32", "interval-year-month"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus)?,
            columns: vec![
                Series::new(vec![to_days(2020, 2, 29), to_days(2000, 1, 31)]).into(),
                Series::new(vec![
                    -12i64,         /* - 1 year */
                    -(20 * 12 + 1), /* - 20 years and 1 month */
                ])
                .into(),
            ],
            expect: Series::new(vec![to_days(2021, 2, 28), to_days(2020, 2, 29)]).into(),
            error: "",
        },
        Test {
            name: "date32-plus-day-time",
            display: "plus",
            nullable: false,
            arg_names: vec!["date32", "interval-day-time"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![to_days(2020, 3, 1), to_days(2021, 3, 1)]).into(),
                Series::new(vec![
                    daytime_to_ms(-1, 0, 0, 0),
                    daytime_to_ms(-1, -1, 0, 0),
                ])
                .into(),
            ],
            expect: Series::new(vec![to_days(2020, 2, 29), to_days(2021, 2, 28)]).into(),
            error: "",
        },
        Test {
            name: "date32-minus-day-time",
            display: "minus",
            nullable: false,
            arg_names: vec!["date32", "interval-day-time"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus)?,
            columns: vec![
                Series::new(vec![to_days(2020, 3, 1), to_days(2021, 3, 1)]).into(),
                Series::new(vec![daytime_to_ms(1, 0, 0, 0), daytime_to_ms(1, 1, 0, 0)]).into(),
            ],
            expect: Series::new(vec![to_days(2020, 2, 29), to_days(2021, 2, 28)]).into(),
            error: "",
        },
        Test {
            name: "date16-plus-year-month",
            display: "plus",
            nullable: false,
            arg_names: vec!["date16", "interval-year-month"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![
                    to_days(2020, 2, 29) as u16,
                    to_days(2000, 1, 31) as u16,
                ])
                .into(),
                Series::new(vec![
                    12i64,       /* 1 year */
                    20 * 12 + 1, /* 20 years and 1 month */
                ])
                .into(),
            ],
            expect: Series::new(vec![
                to_days(2021, 2, 28) as u16,
                to_days(2020, 2, 29) as u16,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "date16-plus-day-time",
            display: "plus",
            nullable: false,
            arg_names: vec!["date16", "interval-day-time"],
            func: ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)?,
            columns: vec![
                Series::new(vec![
                    to_days(2020, 2, 29) as u16,
                    to_days(2021, 2, 28) as u16,
                ])
                .into(),
                Series::new(vec![daytime_to_ms(1, 0, 0, 0), daytime_to_ms(1, 1, 0, 0)]).into(),
            ],
            expect: Series::new(vec![to_days(2020, 3, 1) as u16, to_days(2021, 3, 1) as u16])
                .into(),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();
        // Type check.
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        let func = t.func;
        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, rows)?);
        assert_eq!(v, &t.expect);
    }
    Ok(())
}
