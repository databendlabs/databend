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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    display: &'static str,
    arg_names: Vec<&'static str>,
    func: Box<dyn Function>,
    columns: Vec<DataColumn>,
    expect: DataColumn,
    error: &'static str,
    nullable: bool,
}

#[test]
fn test_toyyyymm_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_toyyyymm_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32, 1, 2, 3]).into()],
            nullable: false,
            expect: Series::new(vec![197001u32, 197001u32, 197001u32, 197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMFunction::try_create("a")?,
            columns: vec![Series::new(vec![0u32]).into()],
            nullable: false,
            expect: Series::new(vec![197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(DataValue::UInt32(Some(0u32)), 15)],
            nullable: false,
            expect: Series::new(vec![197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![Series::new(vec![1630833797u32]).into()],
            nullable: false,
            expect: Series::new(vec![20210905u32]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_toyyyymmdd_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_toyyyymmdd_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![Series::new(vec![1630833797u32]).into()],
            nullable: false,
            expect: Series::new(vec![20210905u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![19700101u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1630833797u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![20210905u32]).into(),
            error: "",
        },
    ];
    do_test(tests)
}

#[test]
fn test_toyyyymmddhhmmss_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_toyyyymmddhhmmss_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDhhmmssFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![19700101000000u64]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmddhhmmss_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDhhmmssFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![19700101000000u64]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmddhhmmss_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDhhmmssFunction::try_create("a")?,
            columns: vec![Series::new(vec![1630833797u32]).into()],
            nullable: false,
            expect: Series::new(vec![20210905092317u64]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmddhhmmss_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDhhmmssFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![19700101000000u64]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmddhhmmss_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDhhmmssFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![19700101000000u64]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmddhhmmss_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDhhmmssFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1630833797u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![20210905092317u64]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_tomonth_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_tomonth_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToMonthFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_tomonth_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToMonthFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_tomonth_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToMonthFunction::try_create("a")?,
            columns: vec![Series::new(vec![1633081817u32]).into()],
            nullable: false,
            expect: Series::new(vec![10u8]).into(),
            error: "",
        },
        Test {
            name: "test_tomonth_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToMonthFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_tomonth_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToMonthFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_tomonth_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToMonthFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1633081817u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![10u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_todayofyear_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_todayofyear_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfYearFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![1u16]).into(),
            error: "",
        },
        Test {
            name: "test_todayofyear_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfYearFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![1u16]).into(),
            error: "",
        },
        Test {
            name: "test_todayofyear_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfYearFunction::try_create("a")?,
            columns: vec![Series::new(vec![1633173324u32]).into()],
            nullable: false,
            expect: Series::new(vec![275u16]).into(),
            error: "",
        },
        Test {
            name: "test_todayofyear_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfYearFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![1u16]).into(),
            error: "",
        },
        Test {
            name: "test_todayofyear_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfYearFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![1u16]).into(),
            error: "",
        },
        Test {
            name: "test_todayofyear_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfYearFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1633173324u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![275u16]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_todatofweek_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_todayofweek_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfWeekFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![4u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofweek_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfWeekFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![4u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofweek_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfWeekFunction::try_create("a")?,
            columns: vec![Series::new(vec![1633173324u32]).into()],
            nullable: false,
            expect: Series::new(vec![6u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofweek_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfWeekFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![4u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofweek_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfWeekFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![4u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofweek_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfWeekFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1633173324u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![6u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_todayofmonth_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_todayofmonth_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfMonthFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofmonth_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfMonthFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofmonth_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfMonthFunction::try_create("a")?,
            columns: vec![Series::new(vec![1633173324u32]).into()],
            nullable: false,
            expect: Series::new(vec![2u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofmonth_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToDayOfMonthFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofmonth_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToDayOfMonthFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![1u8]).into(),
            error: "",
        },
        Test {
            name: "test_todayofmonth_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToDayOfMonthFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1633173324u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![2u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_tohour_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_tohour_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToHourFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tohour_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToHourFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tohour_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToHourFunction::try_create("a")?,
            columns: vec![Series::new(vec![1634551542u32]).into()],
            nullable: false,
            expect: Series::new(vec![10u8]).into(),
            error: "",
        },
        Test {
            name: "test_tohour_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToHourFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tohour_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToHourFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tohour_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToHourFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1634551542u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![10u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_tominute_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_tominute_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToMinuteFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tominute_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToMinuteFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tominute_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToMinuteFunction::try_create("a")?,
            columns: vec![Series::new(vec![1634551542u32]).into()],
            nullable: false,
            expect: Series::new(vec![5u8]).into(),
            error: "",
        },
        Test {
            name: "test_tominute_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToMinuteFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tominute_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToMinuteFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tominute_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToMinuteFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1634551542u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![5u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_tosecond_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_tosecond_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToSecondFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tosecond_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToSecondFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tosecond_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToSecondFunction::try_create("a")?,
            columns: vec![Series::new(vec![1634551542u32]).into()],
            nullable: false,
            expect: Series::new(vec![42u8]).into(),
            error: "",
        },
        Test {
            name: "test_tosecond_date16_constant",
            display: "c()",
            arg_names: vec!["c"],
            func: ToSecondFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tosecond_date32_constant",
            display: "b()",
            arg_names: vec!["b"],
            func: ToSecondFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![0u8]).into(),
            error: "",
        },
        Test {
            name: "test_tosecond_datetime_constant",
            display: "a()",
            arg_names: vec!["a"],
            func: ToSecondFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1634551542u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![42u8]).into(),
            error: "",
        },
    ];

    do_test(tests)
}

#[test]
fn test_tomonday_function() -> Result<()> {
    let tests = vec![
        Test {
            name: "test_tomonday_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToMondayFunction::try_create("c")?,
            columns: vec![Series::new(vec![18919u16]).into()],
            nullable: false,
            expect: Series::new(vec![18918u16]).into(),
            error: "",
        },
        Test {
            name: "test_tomonday_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToMondayFunction::try_create("b")?,
            columns: vec![Series::new(vec![18919i32]).into()],
            nullable: false,
            expect: Series::new(vec![18918u16]).into(),
            error: "",
        },
        Test {
            name: "test_tomonday_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToMondayFunction::try_create("a")?,
            columns: vec![Series::new(vec![1634614318u32]).into()],
            nullable: false,
            expect: Series::new(vec![18918u16]).into(),
            error: "",
        },
    ];

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::DateTime32(None), false),
        DataField::new("b", DataType::Date32, false),
        DataField::new("c", DataType::Date16, false),
    ]);

    for t in tests {
        let rows = t.columns[0].len();

        // Type check
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

        // Display check
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, rows)?);
        let actual_type = v.data_type().clone();
        assert_eq!(actual_type, DataType::UInt16);
        assert_eq!(v, &t.expect);
    }

    Ok(())
}

fn do_test(tests: Vec<Test>) -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::DateTime32(None), false),
        DataField::new("b", DataType::Date32, false),
        DataField::new("c", DataType::Date16, false),
    ]);

    for t in tests {
        let rows = t.columns[0].len();

        // Type check
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

        // Display check
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
