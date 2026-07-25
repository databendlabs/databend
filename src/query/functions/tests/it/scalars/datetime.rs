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

use std::io::Write;
use std::str::FromStr;

use databend_common_expression::Domain;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::*;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_date;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_timestamp;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_timestamp_tz;
use databend_common_expression::utils::auto_detect_datetime::parse_epoch_str;
use goldenfile::Mint;
use jiff::Timestamp;
use jiff::Unit;
use jiff::civil::date;
use jiff::tz::TimeZone;

use super::TestContext;
use super::run_ast;
use super::run_ast_with_context;

#[test]
fn test_datetime() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("datetime.txt").unwrap();

    test_to_timestamp(file);
    test_to_date(file);
    test_date_add_subtract(file);
    test_timestamp_add_subtract(file);
    test_date_date_add_sub(file);
    test_timestamp_date_add_sub(file);
    test_date_arith(file);
    test_timestamp_arith(file);
    test_date_domain_overflow(file);
    test_to_number(file);
    test_rounder_functions(file);
    test_date_date_diff(file);
    test_current_time(file);
    test_date_from_parts(file);
    test_timestamp_from_parts(file);
}

fn test_to_timestamp(file: &mut impl Write) {
    run_ast(file, "to_timestamp(-30610224000000001)", &[]);
    run_ast(file, "to_timestamp(-315360000000000)", &[]);
    run_ast(file, "to_timestamp(-315360000000)", &[]);
    run_ast(file, "to_timestamp(-100)", &[]);
    run_ast(file, "to_timestamp(-0)", &[]);
    run_ast(file, "to_timestamp(0)", &[]);
    run_ast(file, "to_timestamp(100)", &[]);
    run_ast(file, "to_timestamp(315360000000)", &[]);
    run_ast(file, "to_timestamp('2023-01-11')", &[]);
    run_ast(file, "to_timestamp('2023-01-11')::int64::timestamp", &[]);
    run_ast(file, "to_timestamp(315360000000000)", &[]);
    run_ast(file, "to_timestamp(253402300800000000)", &[]);
    run_ast(file, "to_timestamp(a)", &[(
        "a",
        Int64Type::from_data(vec![
            -315360000000000i64,
            315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);

    run_ast(file, "to_timestamp(a) > '2020-01-01'", &[(
        "a",
        Int64Type::from_data(vec![i64::MIN, i64::MAX]),
    )]);

    run_ast(file, "to_timestamp(b)", &[(
        "b",
        StringType::from_data(vec!["2020-01-01", "2020-01-02", "2020-01-03", "2029-01-01"]),
    )]);
}

fn test_to_date(file: &mut impl Write) {
    run_ast(file, "to_date(-354286)", &[]);
    run_ast(file, "to_date(-354285)", &[]);
    run_ast(file, "to_date(-100)", &[]);
    run_ast(file, "to_date(-0)", &[]);
    run_ast(file, "to_date(0)", &[]);
    run_ast(file, "to_date('2023-01-11')", &[]);
    run_ast(file, "to_int32(to_date('2023-01-11'))::date", &[]);
    run_ast(file, "to_date(100)", &[]);
    run_ast(file, "to_date(2932896)", &[]);
    run_ast(file, "to_date(2932897)", &[]);
    run_ast(file, "to_date(a)", &[(
        "a",
        Int32Type::from_data(vec![-354285, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_date(b)", &[(
        "b",
        StringType::from_data(vec!["2020-01-01", "2020-01-02", "2020-01-03", "2029-01-01"]),
    )]);
}

fn test_date_add_subtract(file: &mut impl Write) {
    run_ast(file, "add_years(to_date(0), 10000)", &[]); // failed
    run_ast(file, "add_years(to_date(0), 100)", &[]);
    run_ast(file, "add_months(to_date(0), 100)", &[]);
    run_ast(file, "add_days(to_date(0), 100)", &[]);
    run_ast(file, "add(to_date(0), 100)", &[]);
    run_ast(file, "add(to_date(0), 10000000)", &[]);
    run_ast(file, "subtract_years(to_date(0), 100)", &[]);
    run_ast(file, "subtract_quarters(to_date(0), 100)", &[]);
    run_ast(file, "subtract_months(to_date(0), 100)", &[]);
    run_ast(file, "subtract_days(to_date(0), 100)", &[]);
    run_ast(file, "subtract(to_date(0), 100)", &[]);
    run_ast(file, "subtract(to_date(0), 10000000)", &[]);
    run_ast(file, "add_years(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_quarters(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_months(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_days(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_years(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_quarters(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_months(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_days(a, b)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

fn test_timestamp_add_subtract(file: &mut impl Write) {
    run_ast(file, "add_years(to_timestamp(0), 10000)", &[]); // failed
    run_ast(file, "add_years(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_quarters(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_months(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_days(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_hours(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_minutes(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_seconds(to_timestamp(0), 100)", &[]);
    run_ast(file, "add(to_timestamp(0), 100000000000000)", &[]);
    run_ast(file, "add(to_timestamp(0), 1000000000000000000)", &[]);
    run_ast(file, "subtract_years(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_quarters(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_months(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_days(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_hours(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_minutes(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract_seconds(to_timestamp(0), 100)", &[]);
    run_ast(file, "subtract(to_timestamp(0), 100000000000000)", &[]);
    run_ast(file, "subtract(to_timestamp(0), 1000000000000000000)", &[]);
    run_ast(file, "add_years(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_quarters(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_months(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_days(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_hours(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_minutes(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "add_seconds(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_years(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_quarters(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_months(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_days(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_hours(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_minutes(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "subtract_seconds(a, b)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

fn test_date_date_add_sub(file: &mut impl Write) {
    run_ast(file, "date_add(year, 10000, to_date(0))", &[]); // failed
    run_ast(file, "date_add(year, 100, to_date(0))", &[]);
    run_ast(file, "date_add(quarter, 100, to_date(0))", &[]);
    run_ast(file, "date_add(month, 100, to_date(0))", &[]);
    run_ast(file, "date_add(day, 100, to_date(0))", &[]);
    run_ast(file, "date_sub(year, 100, to_date(0))", &[]);
    run_ast(file, "date_sub(quarter, 100, to_date(0))", &[]);
    run_ast(file, "date_sub(month, 100, to_date(0))", &[]);
    run_ast(file, "date_sub(day, 100, to_date(0))", &[]);
    run_ast(file, "date_add(year, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(quarter, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(month, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(day, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(year, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(quarter, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(month, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(day, b, a)", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

fn test_timestamp_date_add_sub(file: &mut impl Write) {
    run_ast(file, "date_add(year, 10000, to_timestamp(0))", &[]); // failed
    run_ast(file, "date_add(year, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(quarter, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(month, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(day, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(hour, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(minute, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(second, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(year, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(quarter, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(month, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(day, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(hour, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(minute, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_sub(second, 100, to_timestamp(0))", &[]);
    run_ast(file, "date_add(year, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(quarter, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(month, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(day, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(hour, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(minute, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_add(second, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(year, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(quarter, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(month, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(day, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(hour, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(minute, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "date_sub(second, b, a)", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

fn test_date_arith(file: &mut impl Write) {
    run_ast(file, "to_date(0) + interval 10000 year", &[]); // failed
    run_ast(file, "to_date(0) + interval 100 year", &[]);
    run_ast(file, "to_date(0) + interval 100 quarter", &[]);
    run_ast(file, "to_date(0) + interval 100 month", &[]);
    run_ast(file, "to_date(0) + interval 100 day", &[]);
    run_ast(file, "to_date(0) - interval 100 year", &[]);
    run_ast(file, "to_date(0) - interval 100 quarter", &[]);
    run_ast(file, "to_date(0) - interval 100 month", &[]);
    run_ast(file, "to_date(0) - interval 100 day", &[]);
    run_ast(file, "a + interval b year", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b quarter", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b month", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b day", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b year", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b quarter", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b month", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b day", &[
        ("a", DateType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

fn test_timestamp_arith(file: &mut impl Write) {
    run_ast(file, "to_timestamp(0) + interval 10000 year", &[]); // failed
    run_ast(file, "to_timestamp(0) + interval 100 year", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 quarter", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 month", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 day", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 hour", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 minute", &[]);
    run_ast(file, "to_timestamp(0) + interval 100 second", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 year", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 quarter", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 month", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 day", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 hour", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 minute", &[]);
    run_ast(file, "to_timestamp(0) - interval 100 second", &[]);
    run_ast(file, "a + interval b year", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b quarter", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b month", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b day", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b hour", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b minute", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a + interval b second", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b year", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b quarter", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b month", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b day", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b hour", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b minute", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
    run_ast(file, "a - interval b second", &[
        ("a", TimestampType::from_data(vec![-100, 0, 100])),
        ("b", Int32Type::from_data(vec![1, 2, 3])),
    ]);
}

// Regression test for issue #20134: date/timestamp domain calculation with overflow
fn test_date_domain_overflow(file: &mut impl Write) {
    // Date plus: domain crosses upper valid date range
    run_ast_with_context(file, "a + b", TestContext {
        entries: &[
            ("a", DateType::from_data(vec![1, 1, 1]).into()),
            ("b", Int64Type::from_data(vec![-1, 0, 2147483647]).into()),
        ],
        input_domains: Some(&[
            ("a", Domain::Date(SimpleDomain { min: 1, max: 1 })),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: -1,
                    max: 2147483647,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Date plus: domain entirely above valid range
    run_ast_with_context(file, "a + b", TestContext {
        entries: &[
            ("a", DateType::from_data(vec![100]).into()),
            ("b", Int64Type::from_data(vec![2932897]).into()),
        ],
        input_domains: Some(&[
            ("a", Domain::Date(SimpleDomain { min: 100, max: 100 })),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: 2932897,
                    max: 2932897,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Date minus: domain crosses lower valid date range
    run_ast_with_context(file, "a - b", TestContext {
        entries: &[
            ("a", DateType::from_data(vec![0, 0, 0]).into()),
            ("b", Int64Type::from_data(vec![-1, 0, 2147483647]).into()),
        ],
        input_domains: Some(&[
            ("a", Domain::Date(SimpleDomain { min: 0, max: 0 })),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: -1,
                    max: 2147483647,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Timestamp plus: domain crosses upper valid range
    run_ast_with_context(file, "a + b", TestContext {
        entries: &[
            ("a", TimestampType::from_data(vec![0, 0, 0]).into()),
            ("b", Int64Type::from_data(vec![-1, 0, i64::MAX]).into()),
        ],
        input_domains: Some(&[
            ("a", Domain::Timestamp(SimpleDomain { min: 0, max: 0 })),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: -1,
                    max: i64::MAX,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Timestamp minus: domain crosses lower valid range
    run_ast_with_context(file, "a - b", TestContext {
        entries: &[
            ("a", TimestampType::from_data(vec![0, 0, 0]).into()),
            ("b", Int64Type::from_data(vec![-1, 0, i64::MAX]).into()),
        ],
        input_domains: Some(&[
            ("a", Domain::Timestamp(SimpleDomain { min: 0, max: 0 })),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: -1,
                    max: i64::MAX,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Date plus: i64 saturating_add actually triggers (DATE_MIN + i64::MIN wraps without saturating)
    run_ast_with_context(file, "a + b", TestContext {
        entries: &[
            ("a", DateType::from_data(vec![DATE_MIN]).into()),
            ("b", Int64Type::from_data(vec![-DATE_MIN as _]).into()),
        ],
        input_domains: Some(&[
            (
                "a",
                Domain::Date(SimpleDomain {
                    min: DATE_MIN,
                    max: DATE_MIN,
                }),
            ),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: i64::MIN,
                    max: 719162,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });

    // Date minus: i64 saturating_sub triggers (DATE_MIN - i64::MAX wraps without saturating)
    run_ast_with_context(file, "a - b", TestContext {
        entries: &[
            ("a", DateType::from_data(vec![DATE_MIN]).into()),
            ("b", Int64Type::from_data(vec![0]).into()),
        ],
        input_domains: Some(&[
            (
                "a",
                Domain::Date(SimpleDomain {
                    min: DATE_MIN,
                    max: DATE_MIN,
                }),
            ),
            (
                "b",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: 0,
                    max: i64::MAX,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });
}

fn test_to_number(file: &mut impl Write) {
    // date
    run_ast(file, "to_yyyymm(to_date(18875))", &[]);
    run_ast(file, "to_yyyymmdd(to_date(18875))", &[]);
    run_ast(file, "to_yyyymmddhhmmss(to_date(18875))", &[]);
    run_ast(file, "to_year(to_date(18875))", &[]);
    run_ast(file, "to_quarter(to_date(18875))", &[]);
    run_ast(file, "to_month(to_date(18875))", &[]);
    run_ast(file, "to_day_of_year(to_date(18875))", &[]);
    run_ast(file, "to_day_of_month(to_date(18875))", &[]);
    run_ast(file, "to_day_of_week(to_date(18875))", &[]);
    run_ast(file, "to_week_of_year(to_date(18875))", &[]);
    run_ast(file, "to_yyyymm(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_yyyymmdd(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_yyyymmddhhmmss(a)", &[(
        "a",
        DateType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_iso_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_quarter(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_month(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_day_of_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_day_of_month(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_day_of_week(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "dayofweek(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "yearweek(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "millennium(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    let millennium_days = [999, 1000, 1001, 1999, 2000, 2001].map(|year| {
        date(year, 1, 1)
            .since((Unit::Day, date(1970, 1, 1)))
            .unwrap()
            .get_days()
    });
    run_ast(file, "millennium(a)", &[(
        "a",
        DateType::from_data(millennium_days.to_vec()),
    )]);
    run_ast(file, "millennium(a)", &[(
        "a",
        TimestampType::from_data(
            millennium_days
                .map(|days| days as i64 * 86_400_000_000)
                .to_vec(),
        ),
    )]);
    run_ast(file, "to_week_of_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);

    // timestamp
    run_ast(file, "to_yyyymm(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_yyyymmdd(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_yyyymmddhhmmss(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_year(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_quarter(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_month(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_day_of_year(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_day_of_month(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_day_of_week(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_week_of_year(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_hour(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_minute(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_second(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_yyyymm(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_yyyymmdd(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_yyyymmddhhmmss(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_year(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_quarter(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_month(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_day_of_year(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_day_of_month(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_day_of_week(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_week_of_year(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_hour(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_minute(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_second(a)", &[(
        "a",
        TimestampType::from_data(vec![-100, 0, 100]),
    )]);
    run_ast(file, "to_unix_timestamp(a)", &[(
        "a",
        TimestampType::from_data(vec![-1_000_001, -1, 0, 1, 1_000_001]),
    )]);
}

fn test_rounder_functions(file: &mut impl Write) {
    run_ast(file, "to_monday(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_start_of_week(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN + 7, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_start_of_month(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_start_of_quarter(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_start_of_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_start_of_iso_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_last_of_week(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX - 7]),
    )]);
    run_ast(file, "to_last_of_month(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_last_of_quarter(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_last_of_year(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_previous_monday(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);
    run_ast(file, "to_next_monday(a)", &[(
        "a",
        DateType::from_data(vec![DATE_MIN, -100, 0, 100, DATE_MAX]),
    )]);

    run_ast(file, "to_start_of_second(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_minute(to_timestamp(1630812366))", &[]);
    run_ast(
        file,
        "to_start_of_five_minutes(to_timestamp(1630812366))",
        &[],
    );
    run_ast(
        file,
        "to_start_of_ten_minutes(to_timestamp(1630812366))",
        &[],
    );
    run_ast(
        file,
        "to_start_of_fifteen_minutes(to_timestamp(1630812366))",
        &[],
    );
    run_ast(file, "to_start_of_hour(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_day(to_timestamp(1630812366))", &[]);
    run_ast(file, "time_slot(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_monday(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_week(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_week(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_week(to_timestamp(1630812366), 1)", &[]);
    run_ast(file, "to_start_of_month(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_quarter(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_year(to_timestamp(1630812366))", &[]);
    run_ast(file, "to_start_of_iso_year(to_timestamp(1630812366))", &[]);

    run_ast(file, "date_trunc(year, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(quarter, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(month, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(day, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(hour, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(minute, to_timestamp(1630812366))", &[]);
    run_ast(file, "date_trunc(second, to_timestamp(1630812366))", &[]);

    run_ast(file, "last_day(to_timestamp(1630812366), year)", &[]);
    run_ast(file, "last_day(to_timestamp(1630812366), quarter)", &[]);
    run_ast(file, "last_day(to_timestamp(1630812366), month)", &[]);
    run_ast(file, "last_day(to_timestamp(1630812366), week)", &[]);

    run_ast(file, "previous_day(to_timestamp(1630812366), monday)", &[]);
    run_ast(file, "previous_day(to_timestamp(1630812366), tuesday)", &[]);
    run_ast(
        file,
        "previous_day(to_timestamp(1630812366), wednesday)",
        &[],
    );
    run_ast(file, "previous_day(to_timestamp(1630812366), thursday)", &[
    ]);
    run_ast(file, "previous_day(to_timestamp(1630812366), friday)", &[]);
    run_ast(file, "previous_day(to_timestamp(1630812366), saturday)", &[
    ]);
    run_ast(file, "previous_day(to_timestamp(1630812366), sunday)", &[]);

    run_ast(file, "next_day(to_timestamp(1630812366), monday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), tuesday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), wednesday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), thursday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), friday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), saturday)", &[]);
    run_ast(file, "next_day(to_timestamp(1630812366), sunday)", &[]);
}

fn test_date_date_diff(file: &mut impl Write) {
    run_ast(file, "date_diff(year, to_date(0), to_date(10000))", &[]);
    run_ast(file, "date_diff(year, to_date(10000), to_date(0))", &[]);
    run_ast(
        file,
        "date_diff(year, to_date('2000-01-01'), to_date('2024-12-31'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(year, to_date('2023-12-31'), to_date('2024-01-01'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(year, to_date('2024-01-01'), to_date('2023-12-31'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(year, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(year, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(file, "date_diff(month, to_date(0), to_date(10000))", &[]);
    run_ast(file, "date_diff(month, to_date(10000), to_date(0))", &[]);
    run_ast(
        file,
        "date_diff(month, to_date('2000-01-01'), to_date('2024-12-31'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(month, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(month, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(file, "date_diff(day, to_date(0), to_date(10000))", &[]);
    run_ast(file, "date_diff(day, to_date(10000), to_date(0))", &[]);
    run_ast(
        file,
        "date_diff(day, to_date('2000-01-01'), to_date('2024-12-31'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(day, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(day, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(hour, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(hour, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(minute, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(minute, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(second, to_timestamp('2023-11-12 09:38:18.165575'), to_timestamp('2025-03-27 21:01:35.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(second, to_timestamp('2020-02-29 23:59:59.165575'), to_timestamp('2019-02-28 23:59:59.423179'))",
        &[],
    );
    run_ast(
        file,
        "date_diff(second, to_timestamp('2020-02-29 23:59:59'), to_timestamp('2019-02-28 23:59:59'))",
        &[],
    );
}

fn test_current_time(file: &mut impl Write) {
    let tz = TimeZone::UTC;
    let now = Timestamp::from_str("2024-02-03T04:05:06.789123Z")
        .unwrap()
        .to_zoned(tz.clone());
    let func_ctx = FunctionContext {
        tz: tz.clone(),
        now,
        ..FunctionContext::default()
    };
    let ctx = TestContext {
        func_ctx,
        ..TestContext::default()
    };

    run_ast_with_context(file, "typeof(current_time())", ctx.clone());
    run_ast_with_context(file, "current_time()", ctx.clone());
    run_ast_with_context(file, "current_time(3)", ctx.clone());
    run_ast_with_context(file, "current_time(10)", ctx);
}

fn test_date_from_parts(file: &mut impl Write) {
    // Basic date construction
    run_ast(file, "date_from_parts(1977, 8, 7)", &[]);
    // Alias
    run_ast(file, "datefromparts(2023, 1, 15)", &[]);
    // Day overflow: 100th day from Jan 1, 2010
    run_ast(file, "date_from_parts(2010, 1, 100)", &[]);
    // Month overflow: +24 months from Jan 2010
    run_ast(file, "date_from_parts(2010, 25, 1)", &[]);
    // Zero month: Dec of previous year
    run_ast(file, "date_from_parts(2004, 0, 1)", &[]);
    // Negative month: Nov of previous year
    run_ast(file, "date_from_parts(2004, -1, 1)", &[]);
    // Zero day: one day before 1st
    run_ast(file, "date_from_parts(2004, 2, 0)", &[]);
    // Negative day
    run_ast(file, "date_from_parts(2004, 2, -1)", &[]);
    // Both negative
    run_ast(file, "date_from_parts(2004, -1, -1)", &[]);
}

fn test_timestamp_from_parts(file: &mut impl Write) {
    // Basic 6-arg timestamp
    run_ast(file, "timestamp_from_parts(2013, 4, 5, 12, 0, 0)", &[]);
    // 7-arg with nanoseconds
    run_ast(
        file,
        "timestamp_from_parts(2013, 4, 5, 12, 0, 0, 987654321)",
        &[],
    );
    // Alias
    run_ast(file, "timestampfromparts(2013, 4, 5, 12, 0, 0)", &[]);
    // Negative seconds (overflow handling)
    run_ast(file, "timestamp_from_parts(2013, 4, 5, 12, 0, -3600)", &[]);
}

// ===================================================================
// Unit tests for AUTO datetime format detection
// ===================================================================

#[test]
fn test_auto_detect_date() {
    // DD-MON-YYYY
    assert_eq!(auto_detect_date("17-DEC-1980"), Some(4003));
    assert_eq!(auto_detect_date("01-JAN-2000"), Some(10957));

    // DD-MON-YYYY lowercase
    assert_eq!(auto_detect_date("17-dec-1980"), Some(4003));
    assert_eq!(auto_detect_date("01-jan-2000"), Some(10957));
    assert_eq!(auto_detect_date("29-feb-2024"), Some(19782));

    // MM/DD/YYYY
    assert_eq!(auto_detect_date("12/17/1980"), Some(4003));
    assert_eq!(auto_detect_date("3/05/2023"), Some(19421));

    // Leap year
    assert_eq!(auto_detect_date("29-FEB-2024"), Some(19782));
    assert_eq!(auto_detect_date("02/29/2024"), Some(19782));

    // Month > 12 rejected
    assert_eq!(auto_detect_date("13/01/2024"), None);

    // Numeric strings are no longer handled by auto_detect_date (caller's job)
    assert_eq!(auto_detect_date("57600"), None);
    assert_eq!(auto_detect_date("1487654321"), None);
    assert_eq!(auto_detect_date("-86400"), None);

    // Invalid
    assert_eq!(auto_detect_date("not-a-date"), None);
    assert_eq!(auto_detect_date(""), None);
}

#[test]
fn test_auto_detect_timestamp() {
    let tz = TimeZone::UTC;

    // DD-MON-YYYY
    assert!(auto_detect_timestamp("17-DEC-1980 10:30:00", &tz).is_some());
    assert!(auto_detect_timestamp("01-JAN-2000 23:59:59.123456", &tz).is_some());

    // DD-MON-YYYY lowercase
    assert_eq!(
        auto_detect_timestamp("17-dec-1980 10:30:00", &tz),
        auto_detect_timestamp("17-DEC-1980 10:30:00", &tz)
    );

    // MM/DD/YYYY
    assert!(auto_detect_timestamp("12/17/1980 10:30:00", &tz).is_some());
    assert!(auto_detect_timestamp("2/18/2008 02:36:48", &tz).is_some());
    assert!(auto_detect_timestamp("2/18/2008 02:36:48.123", &tz).is_some());

    // RFC 2822 (24h, with tz) — should convert +0200 to UTC
    let ts = auto_detect_timestamp("Thu, 21 Dec 2000 16:01:07 +0200", &tz).unwrap();
    let ts_no_tz = auto_detect_timestamp("Thu, 21 Dec 2000 14:01:07", &tz).unwrap();
    assert_eq!(ts, ts_no_tz); // 16:01:07+0200 == 14:01:07 UTC

    // RFC 2822 (24h, no tz)
    assert!(auto_detect_timestamp("Thu, 21 Dec 2000 16:01:07", &tz).is_some());
    assert!(auto_detect_timestamp("Thu, 21 Dec 2000 16:01:07.999", &tz).is_some());

    // RFC 2822 (12h AM/PM, with tz)
    let ts_12h = auto_detect_timestamp("Thu, 21 Dec 2000 04:01:07 PM +0200", &tz).unwrap();
    assert_eq!(ts_12h, ts); // same as 24h version

    // RFC 2822 (12h AM/PM, no tz)
    assert!(auto_detect_timestamp("Thu, 21 Dec 2000 04:01:07 PM", &tz).is_some());
    assert!(auto_detect_timestamp("Thu, 21 Dec 2000 11:30:00 AM", &tz).is_some());

    // AM/PM boundary: 12:00 AM = midnight, 12:00 PM = noon
    let midnight = auto_detect_timestamp("Thu, 21 Dec 2000 12:00:00 AM", &tz).unwrap();
    let noon = auto_detect_timestamp("Thu, 21 Dec 2000 12:00:00 PM", &tz).unwrap();
    let zero_h = auto_detect_timestamp("Thu, 21 Dec 2000 00:00:00", &tz).unwrap();
    let twelve_h = auto_detect_timestamp("Thu, 21 Dec 2000 12:00:00", &tz).unwrap();
    assert_eq!(midnight, zero_h);
    assert_eq!(noon, twelve_h);

    // Leap year
    assert!(auto_detect_timestamp("29-FEB-2024 12:00:00", &tz).is_some());
    assert!(auto_detect_timestamp("02/29/2024 12:00:00", &tz).is_some());

    // Unix date
    assert!(auto_detect_timestamp("Mon Jul 08 18:09:51 +0000 2013", &tz).is_some());

    // Epoch is no longer handled by auto_detect_timestamp (caller's job)
    assert_eq!(auto_detect_timestamp("1487654321", &tz), None);
    assert_eq!(auto_detect_timestamp("1487654321321", &tz), None);
    assert_eq!(auto_detect_timestamp("20240305", &tz), None);
    assert_eq!(auto_detect_timestamp("-86400", &tz), None);

    // Invalid
    assert_eq!(auto_detect_timestamp("not-a-timestamp", &tz), None);
    assert_eq!(auto_detect_timestamp("", &tz), None);
}

#[test]
fn test_auto_detect_timestamp_tz_unit() {
    let tz = TimeZone::UTC;

    // RFC 2822 with offset — offset should be preserved
    let ts_tz = auto_detect_timestamp_tz("Thu, 21 Dec 2000 16:01:07 +0200", &tz).unwrap();
    assert_eq!(ts_tz.seconds_offset(), 7200); // +0200 = 7200s

    // No offset — should use session tz (UTC → 0)
    let ts_tz = auto_detect_timestamp_tz("17-DEC-1980 10:30:00", &tz).unwrap();
    assert_eq!(ts_tz.seconds_offset(), 0);
}

#[test]
fn test_parse_epoch_str_unit() {
    // seconds
    assert_eq!(parse_epoch_str("1487654321"), Some(1_487_654_321_000_000));
    // milliseconds (> 31536000000)
    assert_eq!(
        parse_epoch_str("1487654321321"),
        Some(1_487_654_321_321_000)
    );
    // boundary: sec vs ms
    assert_eq!(parse_epoch_str("31535999999"), Some(31_535_999_999_000_000));
    assert_eq!(parse_epoch_str("31536000001"), Some(31_536_000_001_000));
    // negative
    assert_eq!(parse_epoch_str("-86400"), Some(-86_400_000_000));
    // not a number
    assert_eq!(parse_epoch_str("abc"), None);
    assert_eq!(parse_epoch_str(""), None);
}
