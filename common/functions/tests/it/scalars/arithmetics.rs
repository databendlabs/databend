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

use common_datavalues::chrono;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::scalar_function_test::test_scalar_functions;
use super::scalar_function_test::test_scalar_functions_with_type;
use super::scalar_function_test::ScalarFunctionTest;
use super::scalar_function_test::ScalarFunctionWithFieldTest;

#[test]
fn test_arithmetic_function() -> Result<()> {
    let tests = vec![
        ("+", ScalarFunctionTest {
            name: "add-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 1]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![5i64, 5, 5, 5]),
            error: "",
        }),
        ("plus", ScalarFunctionTest {
            name: "add-diff-passed",
            columns: vec![
                Series::from_data(vec![1i16, 2, 3, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![2i64, 4, 6, 8]),
            error: "",
        }),
        ("-", ScalarFunctionTest {
            name: "sub-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2]),
                Series::from_data(vec![1i64, 2, 3]),
            ],
            expect: Series::from_data(vec![3i64, 1, -1]),
            error: "",
        }),
        ("multiply", ScalarFunctionTest {
            name: "mul-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2]),
                Series::from_data(vec![1i64, 2, 3]),
            ],
            expect: Series::from_data(vec![4i64, 6, 6]),
            error: "",
        }),
        ("/", ScalarFunctionTest {
            name: "div-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2]),
                Series::from_data(vec![1i64, 2, 3]),
            ],
            expect: Series::from_data(vec![4.0, 1.5, 0.6666666666666666]),
            error: "",
        }),
        ("div", ScalarFunctionTest {
            name: "intdiv-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2]),
                Series::from_data(vec![1i64, 2, 3]),
            ],
            expect: Series::from_data(vec![4i64, 1, 0]),
            error: "",
        }),
        ("modulo", ScalarFunctionTest {
            name: "mod-int64-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2]),
                Series::from_data(vec![1i64, 2, 3]),
            ],
            expect: Series::from_data(vec![0i64, 1, 2]),
            error: "",
        }),
    ];

    for (op, test) in tests {
        test_scalar_functions(op, &[test])?;
    }

    Ok(())
}

#[test]
fn test_arithmetic_date_interval() -> Result<()> {
    let to_day32 = |y: i32, m: u32, d: u32| -> i32 {
        let d = chrono::NaiveDate::from_ymd(y, m, d)
            .signed_duration_since(chrono::NaiveDate::from_ymd(1970, 1, 1));
        d.num_days() as i32
    };

    let to_seconds = |y: i32, m: u32, d: u32, h: u32, min: u32, s: u32| -> i64 {
        let date_time = chrono::NaiveDate::from_ymd(y, m, d).and_hms(h, min, s);
        date_time.timestamp()
    };

    let to_milliseconds = |y: i32, m: u32, d: u32, h: u32, min: u32, sec: u32, milli: u32| -> i64 {
        let date_time = chrono::NaiveDate::from_ymd(y, m, d).and_hms_milli(h, min, sec, milli);
        date_time.timestamp_millis()
    };

    let to_microseconds = |y: i32, m: u32, d: u32, h: u32, min: u32, sec: u32, micro: u32| -> i64 {
        let date_time = chrono::NaiveDate::from_ymd(y, m, d).and_hms_micro(h, min, sec, micro);
        date_time.timestamp_nanos() / 1000
    };

    let tests = vec![
        ("+", ScalarFunctionWithFieldTest {
            name: "date-add-years-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2020, 2, 29), /* 2020-2-29 */
                        to_day32(2016, 2, 29), /* 2016-2-29 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-1i64, 4]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Year)),
                ),
            ],
            expect: Series::from_data(vec![
                to_day32(2019, 2, 28), /* 2019-2-28 */
                to_day32(2020, 2, 29), /* 2020-2-29 */
            ]),
            error: "",
        }),
        ("-", ScalarFunctionWithFieldTest {
            name: "date-sub-years-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2400, 2, 29), /* 2400-2-29 */
                        to_day32(1960, 2, 29), /* 1960-2-29 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![1i64, -4]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Year)),
                ),
            ],
            expect: Series::from_data(vec![
                to_day32(2399, 2, 28), /* 2399-2-28 */
                to_day32(1964, 2, 29), /* 1964-2-29 */
            ]),
            error: "",
        }),
        ("+", ScalarFunctionWithFieldTest {
            name: "datetime-add-years-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_seconds(2020, 2, 29, 10, 30, 00), /* 2020-2-29 10:30:00 */
                        to_seconds(2021, 2, 28, 10, 30, 00), /* 2021-2-28 10:30:00 */
                    ]),
                    DataField::new("dummy_0", TimestampType::arc(0, None)),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![1i64, -1]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Year)),
                ),
            ],
            expect: Series::from_data(vec![
                to_seconds(2021, 2, 28, 10, 30, 00), /* 2021-2-28 10:30:00 */
                to_seconds(2020, 2, 28, 10, 30, 00), /* 2020-2-28 10:30:00 */
            ]),
            error: "",
        }),
        ("-", ScalarFunctionWithFieldTest {
            name: "datetime-sub-years-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_milliseconds(2020, 2, 29, 10, 30, 00, 000), /* 2020-2-29 10:30:00.000 */
                        to_milliseconds(1960, 2, 29, 10, 30, 00, 000), /* 1960-2-29 10:30:00.000 */
                    ]),
                    DataField::new("dummy_0", TimestampType::arc(3, None)),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![1i64, -4]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Year)),
                ),
            ],
            expect: Series::from_data(vec![
                to_milliseconds(2019, 2, 28, 10, 30, 00, 000), /* 2019-2-28 10:30:00.000 */
                to_milliseconds(1964, 2, 29, 10, 30, 00, 000), /* 1964-2-29 10:30:00.000 */
            ]),
            error: "",
        }),
        ("+", ScalarFunctionWithFieldTest {
            name: "date-add-months-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2020, 3, 31), /* 2020-3-31 */
                        to_day32(2000, 1, 31), /* 2000-1-31 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-1i64, 241]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Month)),
                ),
            ],
            expect: Series::from_data(vec![
                to_day32(2020, 2, 29), /* 2020-2-29 */
                to_day32(2020, 2, 29), /* 2020-2-29 */
            ]),
            error: "",
        }),
        ("+", ScalarFunctionWithFieldTest {
            name: "datetime-add-months-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_seconds(2020, 3, 31, 10, 30, 00), /* 2020-3-31 10:30:00 */
                        to_seconds(2000, 1, 31, 10, 30, 00), /* 2000-1-31 10:30:00 */
                    ]),
                    DataField::new("dummy_0", TimestampType::arc(0, None)),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-1i64, 241]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Month)),
                ),
            ],
            expect: Series::from_data(vec![
                to_seconds(2020, 2, 29, 10, 30, 00), /* 2020-2-29 10:30:00 */
                to_seconds(2020, 2, 29, 10, 30, 00), /* 2020-2-29 10:30:00 */
            ]),
            error: "",
        }),
        ("-", ScalarFunctionWithFieldTest {
            name: "date-sub-days-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2400, 2, 29), /* 2400-2-29 */
                        to_day32(1960, 2, 29), /* 1960-2-29 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![30i64, -30]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Day)),
                ),
            ],
            expect: Series::from_data(vec![
                to_day32(2400, 1, 30), /* 2400-1-30 */
                to_day32(1960, 3, 30), /* 1960-3-30 */
            ]),
            error: "",
        }),
        ("+", ScalarFunctionWithFieldTest {
            name: "datetime-add-days-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_milliseconds(2020, 2, 29, 10, 30, 00, 000), /* 2020-2-29 10:30:00.000 */
                        to_milliseconds(1960, 2, 29, 10, 30, 00, 000), /* 1960-2-29 10:30:00.000 */
                    ]),
                    DataField::new("dummy_0", TimestampType::arc(3, None)),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-30i64, 30]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Day)),
                ),
            ],
            expect: Series::from_data(vec![
                to_milliseconds(2020, 1, 30, 10, 30, 00, 000), /* 2020-1-30 10:30:00.000 */
                to_milliseconds(1960, 3, 30, 10, 30, 00, 000), /* 1960-3-30 10:30:00.000 */
            ]),
            error: "",
        }),
        ("+", ScalarFunctionWithFieldTest {
            name: "date-add-hours-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2020, 3, 1),  /* 2020-3-31 */
                        to_day32(2000, 1, 31), /* 2000-1-31 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-1i64, 1]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Hour)),
                ),
            ],
            expect: Series::from_data(vec![
                to_microseconds(2020, 2, 29, 23, 00, 00, 00), /* 2020-2-29 23:00:00 */
                to_microseconds(2000, 1, 31, 1, 00, 00, 00),  /* 2000-1-31 1:00:00 */
            ]),
            error: "",
        }),
        ("-", ScalarFunctionWithFieldTest {
            name: "date-sub-minutes-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_day32(2100, 2, 28), /* 2400-2-29 */
                        to_day32(1960, 2, 29), /* 1960-2-29 */
                    ]),
                    DataField::new("dummy_0", DateType::arc()),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![61i64, -30]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Minute)),
                ),
            ],
            expect: Series::from_data(vec![
                to_microseconds(2100, 2, 27, 22, 59, 00, 000), /* 2400-2-28 22:59:00 */
                to_microseconds(1960, 2, 29, 00, 30, 00, 000), /* 1960-2-29 00:30:00 */
            ]),
            error: "",
        }),
        ("-", ScalarFunctionWithFieldTest {
            name: "datetime-sub-seconds-passed",
            columns: vec![
                ColumnWithField::new(
                    Series::from_data(vec![
                        to_microseconds(2020, 3, 31, 10, 30, 00, 00), /* 2020-3-31 10:30:00 */
                        to_microseconds(2000, 1, 31, 10, 30, 00, 00), /* 2000-1-31 10:30:00 */
                    ]),
                    DataField::new("dummy_0", TimestampType::arc(6, None)),
                ),
                ColumnWithField::new(
                    Series::from_data(vec![-120i64, 23]),
                    DataField::new("dummy_1", IntervalType::arc(IntervalKind::Second)),
                ),
            ],
            expect: Series::from_data(vec![
                to_microseconds(2020, 3, 31, 10, 32, 00, 00), /* 2020-3-31 10:32:00 */
                to_microseconds(2000, 1, 31, 10, 29, 37, 00), /* 2000-1-31 10:29:37 */
            ]),
            error: "",
        }),
    ];

    for (op, test) in tests {
        test_scalar_functions_with_type(op, &[test])?;
    }

    Ok(())
}
