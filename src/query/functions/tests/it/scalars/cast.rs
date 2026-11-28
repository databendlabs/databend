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

use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::type_check;
use databend_common_expression::types::*;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_io::HybridBitmap;
use goldenfile::Mint;
use itertools::Itertools;

use super::TestContext;
use super::parser;
use super::run_ast;
use super::run_ast_with_context;
use super::write_file_heading;
use super::write_section;

/// How the cases of a cast golden file are listed.
const CAST_NOTE: &str = "Each case runs as CAST. TRY_CAST follows a case only when CAST fails at runtime, to show which rows become NULL. Where CAST succeeds, TRY_CAST is checked to return the same values and is not listed; where CAST fails type checking, TRY_CAST is checked to fail it too.";

#[test]
fn test_cast_number() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_number.txt").unwrap();
    write_file_heading(file, "Casts between numbers, NULL and booleans", CAST_NOTE);
    test_cast_primitive(file);
    test_cast_between_number_and_boolean(file);
}

#[test]
fn test_cast_string() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_string.txt").unwrap();
    write_file_heading(
        file,
        "Casts between strings and numbers, booleans and binary",
        CAST_NOTE,
    );
    test_cast_between_number_and_string(file);
    test_cast_between_boolean_and_string(file);
    test_cast_between_binary_and_string(file);
}

#[test]
fn test_cast_decimal() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_decimal.txt").unwrap();
    write_file_heading(file, "Casts to decimals", CAST_NOTE);
    test_cast_between_string_and_decimal(file);
    test_to_decimal_with_precision_args(file);
}

#[test]
fn test_cast_datetime() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_datetime.txt").unwrap();
    write_file_heading(file, "Casts of dates and timestamps", CAST_NOTE);
    test_cast_number_to_timestamp(file);
    test_cast_number_to_date(file);
    test_cast_between_date_and_timestamp(file);
    test_cast_between_string_and_timestamp(file);
    test_cast_between_string_and_date(file);
}

#[test]
fn test_cast_variant() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_variant.txt").unwrap();
    write_file_heading(file, "Casts to Variant", CAST_NOTE);
    test_cast_to_variant(file);
}

#[test]
fn test_cast_nested() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_nested.txt").unwrap();
    write_file_heading(file, "Casts of nested types and vectors", CAST_NOTE);
    test_cast_to_nested_type(file);
    test_cast_to_vector(file);
}

/// Runs `template` as `CAST`, with `{prefix}` and `{try_}` removed, and checks `TRY_CAST`, with
/// `{prefix}` as `TRY_` and `{try_}` as `try_`. `TRY_CAST` is written only when `CAST` fails at
/// runtime; otherwise it must return what `CAST` returns, or fail type checking as `CAST` does.
fn run_cast(file: &mut impl Write, template: &str, columns: &[(&str, Column)]) {
    let cast = template.replace("{prefix}", "").replace("{try_}", "");
    let try_cast = template
        .replace("{prefix}", "TRY_")
        .replace("{try_}", "try_");
    let mut cast_output = Vec::new();
    run_ast(&mut cast_output, &cast, columns);
    file.write_all(&cast_output).unwrap();
    let mut try_cast_output = Vec::new();
    run_ast(&mut try_cast_output, &try_cast, columns);

    let cast_output = String::from_utf8(cast_output).unwrap();
    let try_cast_output = String::from_utf8(try_cast_output).unwrap();
    if !cast_output.starts_with("error") {
        assert_eq!(
            output_values(&try_cast_output),
            output_values(&cast_output),
            "`{try_cast}` returns another value than `{cast}`"
        );
    } else if type_checks(&cast, columns) {
        file.write_all(try_cast_output.as_bytes()).unwrap();
    } else {
        assert!(
            !type_checks(&try_cast, columns),
            "`{try_cast}` passes type checking but `{cast}` does not"
        );
    }
}

/// The output values that `run_ast` writes, one per row.
fn output_values(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter_map(|line| {
            if let Some(value) = line.strip_prefix("output         : ") {
                return Some(value);
            }
            line.starts_with("| Row ").then(|| {
                line.trim_end_matches('|')
                    .rsplit('|')
                    .next()
                    .unwrap()
                    .trim()
            })
        })
        .collect()
}

fn type_checks(text: &str, columns: &[(&str, Column)]) -> bool {
    let columns = columns
        .iter()
        .map(|(name, column)| (*name, column.data_type()))
        .collect::<Vec<_>>();
    let raw_expr = parser::parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS);
    type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).is_ok()
}

fn test_cast_primitive(file: &mut impl Write) {
    write_section(
        file,
        "Numbers and NULL",
        "A number converts to another number type when its value fits, and fails otherwise; a float drops its fraction under the default function context. NULL converts to NULL of a nullable type, and CAST to a type that is not nullable fails on it. Arrays and tuples convert element by element.",
    );
    run_cast(file, "{prefix}CAST(0 AS UINT8)", &[]);
    run_cast(file, "{prefix}CAST(0 AS UINT8 NULL)", &[]);
    run_cast(file, "{prefix}CAST('str' AS STRING)", &[]);
    run_cast(file, "{prefix}CAST('str' AS STRING NULL)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS UINT8)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS UINT8 NULL)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS STRING)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS STRING NULL)", &[]);
    run_cast(file, "{prefix}CAST(1024 AS UINT8)", &[]);
    run_cast(file, "{prefix}CAST(a AS UINT8)", &[(
        "a",
        UInt16Type::from_data(vec![0u16, 64, 255, 512, 1024]),
    )]);
    run_cast(file, "{prefix}CAST(a AS UINT16)", &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_cast(file, "{prefix}CAST(a AS INT64)", &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_cast(
        file,
        "({prefix}CAST(a AS FLOAT32), {prefix}CAST(a AS INT32), {prefix}CAST(b AS FLOAT32), {prefix}CAST(b AS INT32))",
        &[
            (
                "a",
                UInt64Type::from_data(vec![
                    0,
                    1,
                    u8::MAX as u64,
                    u16::MAX as u64,
                    u32::MAX as u64,
                    u64::MAX,
                ]),
            ),
            (
                "b",
                Float64Type::from_data(vec![
                    0.0,
                    u32::MAX as f64,
                    u64::MAX as f64,
                    f64::MIN,
                    f64::MAX,
                    f64::INFINITY,
                ]),
            ),
        ],
    );
    run_cast(
        file,
        "{prefix}CAST([[a, b], NULL, NULL] AS Array(Array(Int8)))",
        &[
            ("a", Int16Type::from_data(vec![0i16, 1, 2, 127, 255])),
            ("b", Int16Type::from_data(vec![0i16, -1, -127, -128, -129])),
        ],
    );
    run_cast(
        file,
        "{prefix}CAST((a, b, NULL) AS TUPLE(Int8, UInt8, Boolean NULL))",
        &[
            ("a", Int16Type::from_data(vec![0i16, 1, 2, 127, 256])),
            ("b", Int16Type::from_data(vec![0i16, 1, -127, -128, -129])),
        ],
    );
    run_cast(file, "{prefix}CAST(a AS INT16)", &[(
        "a",
        Float64Type::from_data(vec![0.0f64, 1.1, 2.2, 3.3, -4.4]),
    )]);
    run_cast(file, "{prefix}CAST(b AS INT16)", &[(
        "b",
        Int8Type::from_data(vec![0i8, 1, 2, 3, -4]),
    )]);

    run_cast(file, "{prefix}CAST(a AS UINT16)", &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);

    run_cast(file, "{prefix}CAST(c AS INT16)", &[(
        "c",
        Int64Type::from_data(vec![0i64, 11111111111, 2, 3, -4]),
    )]);
}

fn test_cast_to_variant(file: &mut impl Write) {
    write_section(
        file,
        "Values to Variant",
        "A value converts to the JSON value of the same kind: numbers, booleans and strings as themselves, arrays as JSON arrays, tuples as objects keyed by field position, and timestamps as strings. A string converts by parsing it as JSON text, so a string that is not JSON fails.",
    );
    run_cast(file, "{prefix}CAST(NULL AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(0 AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(-1 AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(1.1 AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST('🍦 が美味しい' AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST([0, 1, 2] AS VARIANT)", &[]);
    run_cast(
        file,
        "{prefix}CAST([0::VARIANT, '\"a\"'::VARIANT] AS VARIANT)",
        &[],
    );
    run_cast(file, "{prefix}CAST(to_timestamp(1000000) AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(false AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(true AS VARIANT)", &[]);
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST('\"🍦 が美味しい\"' AS VARIANT) AS VARIANT)",
        &[],
    );
    run_cast(file, "{prefix}CAST((1,) AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST((1, 2) AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST((false, true) AS VARIANT)", &[]);
    run_cast(file, "{prefix}CAST(('a',) AS VARIANT)", &[]);
    run_cast(
        file,
        "{prefix}CAST((1, 2, (false, true, ('a',))) AS VARIANT)",
        &[],
    );

    run_cast(file, "{prefix}CAST(a AS VARIANT)", &[(
        "a",
        StringType::from_data_with_validity(vec!["true", "{\"k\":\"v\"}", "[1,2,3]"], vec![
            true, false, true,
        ]),
    )]);

    run_cast(file, "{prefix}CAST(a AS VARIANT)", &[(
        "a",
        gen_bitmap_data(),
    )]);

    run_cast(file, "{prefix}CAST(a AS VARIANT NULL)", &[(
        "a",
        Int64Type::from_data_with_validity(vec![1, 2, 3], vec![true, false, true]),
    )]);
}

fn test_cast_number_to_timestamp(file: &mut impl Write) {
    write_section(
        file,
        "Numbers and timestamps",
        "An integer is a number of seconds, milliseconds or microseconds, by its magnitude: 315360000000 is milliseconds and 315360000000000 microseconds. A value out of the timestamp range becomes the minimum timestamp. A timestamp converts to its number of microseconds.",
    );
    run_cast(file, "{prefix}CAST(-30610224000000001 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(-315360000000000 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(-315360000000 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(-100 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(-0 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(0 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(100 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(315360000000 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(315360000000000 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(253402300800000000 AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(a AS TIMESTAMP)", &[(
        "a",
        Int64Type::from_data(vec![
            -315360000000000i64,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);

    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(-315360000000000) AS INT64)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(-315360000000) AS INT64)",
        &[],
    );
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(-100) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(-0) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(0) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(100) AS INT64)", &[]);
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(315360000000) AS INT64)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(315360000000000) AS INT64)",
        &[],
    );
    run_cast(file, "{prefix}CAST(a AS INT64)", &[(
        "a",
        TimestampType::from_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_cast_number_to_date(file: &mut impl Write) {
    write_section(
        file,
        "Numbers and dates",
        "An integer is a number of days since 1970-01-01. A value out of the date range becomes the minimum date, 0001-01-01, for CAST and TRY_CAST alike. A date converts to its number of days.",
    );
    run_cast(file, "{prefix}CAST(-354286 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(-354285 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(-100 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(-0 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(0 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(100 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(2932896 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(2932897 AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(a AS DATE)", &[(
        "a",
        Int32Type::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);

    run_cast(file, "{prefix}CAST(TO_DATE(-354285) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(-100) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(-0) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(0) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(100) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(2932896) AS INT64)", &[]);
    run_cast(file, "{prefix}CAST(a AS INT64)", &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_between_number_and_boolean(file: &mut impl Write) {
    write_section(
        file,
        "Numbers and booleans",
        "A number is true when it is not zero. A boolean converts to 1 or 0.",
    );
    run_cast(file, "{prefix}CAST(0 AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST(1 AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST(false AS UINT64)", &[]);
    run_cast(file, "{prefix}CAST(true AS INT64)", &[]);

    run_cast(file, "{prefix}CAST(0.0 AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST(1.0 AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST(false AS FLOAT32)", &[]);
    run_cast(file, "{prefix}CAST(true AS FLOAT64)", &[]);
    run_cast(file, "{prefix}CAST(false AS DECIMAL(4,3))", &[]);
    run_cast(file, "{prefix}CAST(true AS DECIMAL(4,2))", &[]);

    run_cast(file, "{prefix}CAST(num AS BOOLEAN)", &[(
        "num",
        Int64Type::from_data(vec![0i64, -1, 1, 2]),
    )]);
    run_cast(file, "{prefix}CAST(num AS BOOLEAN)", &[(
        "num",
        UInt64Type::from_data(vec![0u64, 1, 2]),
    )]);
    run_cast(file, "{prefix}CAST(bool AS UINT64)", &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
    run_cast(file, "{prefix}CAST(bool AS INT64)", &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
}

fn test_cast_between_number_and_string(file: &mut impl Write) {
    write_section(
        file,
        "Numbers and strings",
        "A string parses as a number, and fails when it is not a number of the type or does not fit. A number renders as text.",
    );
    run_cast(file, "{prefix}CAST('foo' AS UINT64)", &[]);
    run_cast(file, "{prefix}CAST('1foo' AS INT32)", &[]);
    run_cast(file, "{prefix}CAST('-1' AS UINT64)", &[]);
    run_cast(file, "{prefix}CAST('256' AS UINT8)", &[]);
    run_cast(file, "{prefix}CAST('1' AS UINT64)", &[]);
    run_cast(file, "{prefix}CAST(str AS INT64)", &[(
        "str",
        StringType::from_data(vec![
            "-9223372036854775808",
            "-1",
            "0",
            "1",
            "9223372036854775807",
        ]),
    )]);
    run_cast(file, "{prefix}CAST(str AS INT64)", &[(
        "str",
        StringType::from_data_with_validity(vec!["foo", "foo", "0", "0"], vec![
            true, false, true, false,
        ]),
    )]);

    run_cast(file, "{prefix}CAST(num AS STRING)", &[(
        "num",
        Int64Type::from_data(vec![i64::MIN, -1, 0, 1, i64::MAX]),
    )]);
    run_cast(file, "{prefix}CAST(num AS STRING)", &[(
        "num",
        UInt64Type::from_data(vec![0, 1, u64::MAX]),
    )]);
    run_cast(
        file,
        "{prefix}CAST(CAST('3.40282347e38' AS FLOAT64) AS STRING)",
        &[],
    );
    run_cast(file, "{prefix}CAST(num AS STRING)", &[(
        "num",
        Float64Type::from_data(vec![3.40282347e38]),
    )]);
}

fn test_cast_between_boolean_and_string(file: &mut impl Write) {
    write_section(
        file,
        "Booleans and strings",
        "`true` and `false` in any letter case, `1` and `0` parse as booleans; other strings, such as `t`, fail. A boolean renders as `true` or `false`.",
    );
    run_cast(file, "{prefix}CAST('t' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('f' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('0' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('1' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('true' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('false' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('TRUE' AS BOOLEAN)", &[]);
    run_cast(file, "{prefix}CAST('FaLse' AS BOOLEAN)", &[]);

    run_cast(file, "{prefix}CAST(bool AS STRING)", &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
}

fn test_cast_between_date_and_timestamp(file: &mut impl Write) {
    write_section(
        file,
        "Dates and timestamps",
        "A date converts to the timestamp at the start of its day, and a timestamp to the date of its day.",
    );
    run_cast(file, "{prefix}CAST(TO_DATE(1) AS TIMESTAMP)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(1) AS DATE)", &[]);
    run_cast(file, "{prefix}CAST(a AS DATE)", &[(
        "a",
        TimestampType::from_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
    run_cast(file, "{prefix}CAST(a AS TIMESTAMP)", &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
    run_cast(file, "{prefix}CAST(TO_DATE(a) AS TIMESTAMP)", &[(
        "a",
        Int32Type::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
    run_cast(file, "{prefix}CAST(a AS TIMESTAMP)", &[(
        "a",
        Int64Type::from_data(vec![i64::MAX]),
    )]);
    run_cast(file, "{prefix}CAST(a AS DATE)", &[(
        "a",
        Int64Type::from_data(vec![i64::MAX]),
    )]);
}

fn test_cast_between_string_and_timestamp(file: &mut impl Write) {
    write_section(
        file,
        "Strings and timestamps",
        "A string parses as a date with an optional time and time zone offset; the offset converts the time to the session time zone. A string that is not a timestamp fails. A timestamp renders with microseconds.",
    );
    run_cast(file, "{prefix}TO_TIMESTAMP('2022')", &[]);
    run_cast(file, "{prefix}TO_TIMESTAMP('2022-01')", &[]);
    run_cast(file, "{prefix}TO_TIMESTAMP('2022-01-02')", &[]);
    run_cast(file, "{prefix}TO_TIMESTAMP('A NON-TIMESTAMP STR')", &[]);
    run_cast(
        file,
        "{prefix}TO_TIMESTAMP('2022-01-02T03:25:02.868894-07:00')",
        &[],
    );
    run_cast(file, "{prefix}TO_TIMESTAMP('2022-01-02 02:00:11')", &[]);
    run_cast(file, "{prefix}TO_TIMESTAMP('2022-01-02T02:00:22')", &[]);
    run_cast(
        file,
        "{prefix}TO_TIMESTAMP('2022-01-02T01:12:00-07:00')",
        &[],
    );
    run_cast(file, "{prefix}TO_TIMESTAMP('2022-01-02T01')", &[]);
    run_cast(file, "{prefix}TO_TIMESTAMP(a)", &[(
        "a",
        StringType::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(-315360000000000) AS VARCHAR)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(-315360000000) AS VARCHAR)",
        &[],
    );
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(-100) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(-0) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(0) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_TIMESTAMP(100) AS VARCHAR)", &[]);
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(315360000000) AS VARCHAR)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST(TO_TIMESTAMP(315360000000000) AS VARCHAR)",
        &[],
    );
    run_cast(file, "{prefix}CAST(a AS VARCHAR)", &[(
        "a",
        TimestampType::from_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_cast_between_string_and_date(file: &mut impl Write) {
    write_section(
        file,
        "Strings and dates",
        "A string parses as a date. A time after the date is dropped, and its time zone offset does not move the date. A date renders as `YYYY-MM-DD`.",
    );
    run_cast(file, "{prefix}TO_DATE('2022')", &[]);
    run_cast(file, "{prefix}TO_DATE('2022-01')", &[]);
    run_cast(file, "{prefix}TO_DATE('2022-01-02')", &[]);
    run_cast(file, "{prefix}TO_DATE('A NON-DATE STR')", &[]);
    run_cast(
        file,
        "{prefix}TO_DATE('2022-01-02T03:25:02.868894-07:00')",
        &[],
    );
    run_cast(file, "{prefix}TO_DATE('2022-01-02 02:00:11')", &[]);
    run_cast(file, "{prefix}TO_DATE('2022-01-02T02:00:22')", &[]);
    run_cast(file, "{prefix}TO_DATE('2022-01-02T01:12:00-07:00')", &[]);
    run_cast(file, "{prefix}TO_DATE('2022-01-02T01')", &[]);
    run_cast(file, "{prefix}TO_DATE(a)", &[(
        "a",
        StringType::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_cast(file, "{prefix}CAST(TO_DATE(-354285) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(-100) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(-0) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(0) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(100) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(TO_DATE(2932896) AS VARCHAR)", &[]);
    run_cast(file, "{prefix}CAST(a AS VARCHAR)", &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_to_nested_type(file: &mut impl Write) {
    write_section(
        file,
        "Tuples and arrays",
        "A tuple or an array converts field by field or element by element; a tuple converts only to a tuple with the same number of fields. TRY_CAST makes each field and element nullable, so only the field or element that fails is NULL.",
    );
    run_cast(file, "{prefix}CAST((1, TRUE) AS Tuple(STRING))", &[]);
    run_cast(file, "{prefix}CAST(('a',) AS Tuple(INT))", &[]);
    run_cast(
        file,
        "{prefix}CAST(((1, TRUE), 1) AS Tuple(Tuple(INT, INT), INT))",
        &[],
    );
    run_cast(file, "{prefix}CAST(TRY_CAST(1 AS INT32) AS INT32)", &[]);
    run_cast(
        file,
        "{prefix}CAST(((1, 'a'), 1) AS Tuple(Tuple(INT, INT NULL), INT))",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST(((1, 'a'), 1) AS Tuple(Tuple(INT, INT), INT) NULL)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST([(1,TRUE),(2,FALSE)] AS Array(Tuple(INT, INT)))",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST([(1,'a'),(2,'a')] AS Array(Tuple(INT, INT)) NULL)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST([(1,'a'),(2,'a')] AS Array(Tuple(INT, INT NULL)))",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST([[TRUE], [FALSE, TRUE]] AS Array(Array(INT)))",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST([['a'], ['b', 'c']] AS Array(Array(INT) NULL))",
        &[],
    );
}

fn test_cast_between_string_and_decimal(file: &mut impl Write) {
    write_section(
        file,
        "Strings to decimals",
        "A string parses as a decimal of the target precision and scale, with or without an exponent. A value that needs more digits than the precision allows fails.",
    );
    run_cast(file, "{prefix}CAST('010.010' AS DECIMAL(5,3))", &[]);
    run_cast(file, "{prefix}CAST('010.010' AS DECIMAL(5,4))", &[]);
    run_cast(file, "{prefix}CAST('010.010' AS DECIMAL(5,2))", &[]);
    run_cast(file, "{prefix}CAST('010.010' AS DECIMAL(4,3))", &[]);
    run_cast(file, "{prefix}CAST('010.010' AS DECIMAL(4,2))", &[]);
    run_cast(file, "{prefix}CAST('-1010.010' AS DECIMAL(7,3))", &[]);
    run_cast(file, "{prefix}CAST('00' AS DECIMAL(2,1))", &[]);
    run_cast(file, "{prefix}CAST('0.0' AS DECIMAL(2,0))", &[]);
    run_cast(file, "{prefix}CAST('.0' AS DECIMAL(1,0))", &[]);
    run_cast(file, "{prefix}CAST('+1.0e-10' AS DECIMAL(11, 10))", &[]);
    run_cast(file, "{prefix}CAST('-1.0e+10' AS DECIMAL(11, 0))", &[]);
    run_cast(file, "{prefix}CAST('-0.000000' AS DECIMAL(11, 0))", &[]);
}

fn test_cast_between_binary_and_string(file: &mut impl Write) {
    write_section(
        file,
        "Binary and strings",
        "A string converts to its UTF-8 bytes, and the bytes convert back to the same string.",
    );
    run_cast(file, "{prefix}CAST('Abc' AS BINARY)", &[]);
    run_cast(file, "{prefix}CAST('Dobrý den' AS BINARY)", &[]);
    run_cast(file, "{prefix}CAST('ß😀山' AS BINARY)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS BINARY)", &[]);
    run_cast(file, "{prefix}CAST(NULL AS BINARY NULL)", &[]);
    run_cast(file, "{prefix}CAST(a AS BINARY)", &[(
        "a",
        StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
    )]);
    run_cast(file, "{prefix}CAST(a AS BINARY)", &[(
        "a",
        StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
            true, true, false,
        ]),
    )]);
    run_cast(file, "{prefix}CAST(a AS BINARY NULL)", &[(
        "a",
        StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
            true, true, false,
        ]),
    )]);
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST('Abc' AS BINARY) AS STRING)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST('Dobrý den' AS BINARY) AS STRING)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST('ß😀山' AS BINARY) AS STRING)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST(NULL AS BINARY) AS STRING)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST(NULL AS BINARY NULL) AS STRING NULL)",
        &[],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST(a AS BINARY) AS STRING)",
        &[(
            "a",
            StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
        )],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST(a AS BINARY) AS STRING)",
        &[(
            "a",
            StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
                true, true, false,
            ]),
        )],
    );
    run_cast(
        file,
        "{prefix}CAST({prefix}CAST(a AS BINARY NULL) AS STRING NULL)",
        &[(
            "a",
            StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
                true, true, false,
            ]),
        )],
    );
}

fn test_cast_to_vector(file: &mut impl Write) {
    write_section(
        file,
        "Vectors",
        "A string or a JSON array of numbers converts to a vector of the declared dimension.",
    );
    run_cast(file, "{prefix}CAST('[1,2,3,4]' AS VECTOR(4))", &[]);
    run_cast(
        file,
        "{prefix}CAST(parse_json('[1,2,3,4]') AS VECTOR(4))",
        &[],
    );
    run_cast(file, "{prefix}CAST(a AS VECTOR(4))", &[(
        "a",
        StringType::from_data(vec![
            " [1,2,3,4] ",
            "[4.5, 5.5, 6.5, 7.5]",
            "[8.1,8.2,8.3,8.4]",
        ]),
    )]);
    run_cast(file, "{prefix}CAST(parse_json(a) AS VECTOR(4))", &[(
        "a",
        StringType::from_data(vec![
            " [1,2,3,4] ",
            "[4.5, 5.5, 6.5, 7.5]",
            "[8.1,8.2,8.3,8.4]",
        ]),
    )]);
}

fn test_to_decimal_with_precision_args(file: &mut impl Write) {
    write_section(
        file,
        "to_decimal(precision, scale)",
        "`to_decimal(p, s)(x)` converts a number, boolean or decimal to `Decimal(p, s)`. A float converts from its binary value, so `123.45::FLOAT32` is `123.44` with scale 2.",
    );
    // Constant numeric inputs with explicit precision/scale arguments.
    run_cast(file, "{try_}to_decimal(10, 3)(123.456)", &[]);
    run_cast(file, "{try_}to_decimal(10, 3)(-123.456)", &[]);
    run_cast(file, "{try_}to_decimal(12, 1)(-9876.5)", &[]);
    run_cast(file, "{try_}to_decimal(6, 2)(5)", &[]);
    run_cast(file, "{try_}to_decimal(6, 2)(-5)", &[]);
    run_cast(file, "{try_}to_decimal(6, 2)(true)", &[]);
    run_cast(file, "{try_}to_decimal(10, 2)(123.45::FLOAT32)", &[]);
    run_cast(file, "{try_}to_decimal(10, 2)(-123.45::FLOAT32)", &[]);
    run_cast(file, "{try_}to_decimal(12, 4)(123.456::DECIMAL(9,3))", &[]);
    run_cast(file, "{try_}to_decimal(12, 4)(-123.456::DECIMAL(9,3))", &[]);

    // Column based conversions for floats, integers, and decimals.
    run_cast(file, "{try_}to_decimal(12, 4)(a)", &[(
        "a",
        Float64Type::from_data(vec![0.1, -2.3456, 987.6543]),
    )]);
    run_cast(file, "{try_}to_decimal(8, 0)(b)", &[(
        "b",
        Int32Type::from_data(vec![0, 1, -2, 123_456]),
    )]);
    run_cast(file, "{try_}to_decimal(9, 3)(c)", &[(
        "c",
        Float32Type::from_data(vec![0.25f32, -1.125, 123.5]),
    )]);
    run_cast(file, "{try_}to_decimal(15, 3)(d)", &[(
        "d",
        Decimal128Type::from_data_with_size(
            [123_450i128, -9_999, 1_000_000, -76_543],
            Some(DecimalSize::new_unchecked(10, 2)),
        ),
    )]);
}

#[test]
fn test_decimal_to_decimal() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("decimal_to_decimal_cast.txt").unwrap();

    run_ast_with_context(file, "a::decimal(15,2)", TestContext {
        entries: &[("a", UInt64Type::from_data(vec![0_u64, 100]).into())],
        input_domains: Some(&[(
            "a",
            Domain::Number(NumberDomain::UInt64(UInt64Type::full_domain())),
        )]),
        func_ctx: FunctionContext::default(),
        strict_eval: false,
    });

    for is_try in [false, true] {
        test_cast_decimal_basic(file, is_try);
        test_cast_decimal_scale_reduction(file, is_try, true);
        test_cast_decimal_scale_reduction(file, is_try, false);
        test_cast_decimal_precision_reduction(file, is_try);
        test_cast_decimal_cross_type(file, is_try);
        test_cast_decimal_with_columns(file, is_try);
    }
}

fn test_cast_decimal_basic(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    // Test basic decimal to decimal casting with same precision/scale
    run_ast(
        file,
        format!("{prefix}CAST(123.45::DECIMAL(5,2) AS DECIMAL(5,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(0.00::DECIMAL(3,2) AS DECIMAL(3,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(-999.99::DECIMAL(5,2) AS DECIMAL(5,2))"),
        &[],
    );

    // Test expanding precision (same scale)
    run_ast(
        file,
        format!("{prefix}CAST(123.45::DECIMAL(5,2) AS DECIMAL(10,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(-123.45::DECIMAL(5,2) AS DECIMAL(15,2))"),
        &[],
    );

    // Test expanding scale (same precision)
    run_ast(
        file,
        format!("{prefix}CAST(123.45::DECIMAL(5,2) AS DECIMAL(5,4))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(123::DECIMAL(3,0) AS DECIMAL(3,2))"),
        &[],
    );

    // Test expanding both precision and scale
    run_ast(
        file,
        format!("{prefix}CAST(123.45::DECIMAL(5,2) AS DECIMAL(10,4))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(1.2::DECIMAL(2,1) AS DECIMAL(20,10))"),
        &[],
    );

    // Test edge cases with zero
    run_ast(
        file,
        format!("{prefix}CAST(0::DECIMAL(1,0) AS DECIMAL(10,5))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(0.00000::DECIMAL(6,5) AS DECIMAL(2,1))"),
        &[],
    );
}

fn test_cast_decimal_scale_reduction(file: &mut impl Write, is_try: bool, rounding_mode: bool) {
    let prefix = if is_try { "TRY_" } else { "" };
    let func_ctx = FunctionContext {
        rounding_mode,
        ..FunctionContext::default()
    };
    let test_ctx = TestContext {
        func_ctx: func_ctx.clone(),
        ..TestContext::default()
    };

    // Test shrinking scale (same precision) - precision loss with rounding
    run_ast_with_context(
        file,
        format!("{prefix}CAST(123.456::DECIMAL(6,3) AS DECIMAL(6,1))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(123.999::DECIMAL(6,3) AS DECIMAL(6,2))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(999.995::DECIMAL(6,3) AS DECIMAL(6,1))"),
        test_ctx.clone(),
    );

    // Test scale reduction with different rounding scenarios
    run_ast_with_context(
        file,
        format!("{prefix}CAST(12.345::DECIMAL(5,3) AS DECIMAL(5,0))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(12.567::DECIMAL(5,3) AS DECIMAL(5,1))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(-12.345::DECIMAL(6,3) AS DECIMAL(6,1))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(-12.567::DECIMAL(6,3) AS DECIMAL(6,2))"),
        test_ctx.clone(),
    );

    // Test scale reduction with edge cases
    run_ast_with_context(
        file,
        format!("{prefix}CAST(0.999::DECIMAL(4,3) AS DECIMAL(4,0))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(0.001::DECIMAL(4,3) AS DECIMAL(4,1))"),
        test_ctx.clone(),
    );

    // Test scale reduction with column data
    let test_ctx = TestContext {
        entries: &[(
            "c",
            Decimal128Type::from_data_with_size(
                [12345i128, 67890, -11111, 99999, 0, -99999, 123456],
                Some(DecimalSize::new_unchecked(8, 3)),
            )
            .into(),
        )],
        func_ctx,
        ..TestContext::default()
    };

    run_ast_with_context(
        file,
        format!("{prefix}CAST(c AS DECIMAL(8,0))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(c AS DECIMAL(8,1))"),
        test_ctx.clone(),
    );
    run_ast_with_context(
        file,
        format!("{prefix}CAST(c AS DECIMAL(8,2))"),
        test_ctx.clone(),
    );
}

fn test_cast_decimal_precision_reduction(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    // Test shrinking precision (same scale) - potential overflow
    run_ast(
        file,
        format!("{prefix}CAST(12345.67::DECIMAL(7,2) AS DECIMAL(5,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(999.99::DECIMAL(5,2) AS DECIMAL(4,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(99.9::DECIMAL(3,1) AS DECIMAL(2,1))"),
        &[],
    );

    // Test negative numbers with precision reduction
    run_ast(
        file,
        format!("{prefix}CAST(-999.999::DECIMAL(6,3) AS DECIMAL(4,1))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(-123.45::DECIMAL(5,2) AS DECIMAL(4,2))"),
        &[],
    );

    // Test edge cases - maximum values for target precision
    run_ast(
        file,
        format!("{prefix}CAST(99.99::DECIMAL(4,2) AS DECIMAL(3,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(9.99::DECIMAL(3,2) AS DECIMAL(2,2))"),
        &[],
    );
}

fn test_cast_decimal_cross_type(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    // Test cross-type casting (different decimal storage types)
    run_ast(
        file,
        format!("{prefix}CAST(123.45::DECIMAL(5,2) AS DECIMAL(38,10))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(123456789.123456789::DECIMAL(18,9) AS DECIMAL(38,20))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(12345678901234567890.123456789::DECIMAL(38,9) AS DECIMAL(18,4))"),
        &[],
    );

    // Test maximum values for different precisions
    run_ast(
        file,
        format!("{prefix}CAST(99.99::DECIMAL(4,2) AS DECIMAL(6,2))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(999999.999::DECIMAL(9,3) AS DECIMAL(15,6))"),
        &[],
    );
}

fn test_cast_decimal_with_columns(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    // Test with column data - similar to comparison.rs test_decimal pattern
    let decimal_128_data = vec![0i128, 1, -1, 123456789, -987654321, 999999];
    let decimal_256_data = vec![
        i256::zero(),
        2.into(),
        (-2).into(),
        123456780.into(),
        (-987654320).into(),
        1000000.into(),
    ];

    let decimal_columns = &[
        (
            "a",
            Decimal128Type::from_data_with_size(
                &decimal_128_data,
                Some(DecimalSize::new_unchecked(10, 2)),
            ),
        ),
        (
            "b",
            Decimal256Type::from_data_with_size(
                &decimal_256_data,
                Some(DecimalSize::new_unchecked(15, 3)),
            ),
        ),
    ];

    // Test casting between different decimal column types
    run_ast(
        file,
        format!("{prefix}CAST(a AS DECIMAL(15,3))"),
        decimal_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(a AS DECIMAL(20,5))"),
        decimal_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(b AS DECIMAL(10,2))"),
        decimal_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(b AS DECIMAL(38,10))"),
        decimal_columns,
    );

    // Test casting with potential overflow/underflow
    run_ast(
        file,
        format!("{prefix}CAST(a AS DECIMAL(5,1))"),
        decimal_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(b AS DECIMAL(8,2))"),
        decimal_columns,
    );

    // Test edge cases with very large numbers
    let large_decimal_data = vec![
        i256::from(999999999999999999i128),
        i256::from(-999999999999999999i128),
        i256::zero(),
        i256::from(123456789012345i128),
        i256::from(-123456789012345i128),
    ];
    let large_columns = &[(
        "d",
        Decimal256Type::from_data_with_size(
            &large_decimal_data,
            Some(DecimalSize::new_unchecked(38, 10)),
        ),
    )];

    run_ast(
        file,
        format!("{prefix}CAST(d AS DECIMAL(20,5))"),
        large_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(d AS DECIMAL(38,15))"),
        large_columns,
    );
    run_ast(
        file,
        format!("{prefix}CAST(d AS DECIMAL(15,2))"),
        large_columns,
    );
}

fn gen_bitmap_data() -> Column {
    // construct bitmap column with 4 row:
    // 0..5, 1..6, 2..7, 3..8
    const N: u64 = 4;
    let rbs_iter = (0..N).map(|i| {
        let mut rb = HybridBitmap::new();
        for value in i..(i + 5) {
            rb.insert(value);
        }
        rb
    });

    let rbs = rbs_iter
        .map(|rb| {
            let mut data = Vec::new();
            rb.serialize_into(&mut data).unwrap();
            data
        })
        .collect_vec();

    BitmapType::from_data(rbs)
}

#[test]
fn test_decimal_cast_precision_and_partial_errors() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("decimal_cast_precision.txt").unwrap();
    for expression in [
        "CAST(9::Int64 AS DECIMAL(1,0))",
        "CAST(10::Int64 AS DECIMAL(1,0))",
        "CAST(-10::Int64 AS DECIMAL(1,0))",
        "CAST(9223372036854775807::Int64 AS DECIMAL(1,0))",
        "CAST(18446744073709551615::UInt64 AS DECIMAL(1,0))",
        "TRY_CAST(10::Int64 AS DECIMAL(1,0))",
        "TRY_CAST(18446744073709551615::UInt64 AS DECIMAL(1,0))",
        "CAST(FALSE AS DECIMAL(1,1))",
        "CAST(TRUE AS DECIMAL(1,1))",
        "TRY_CAST(TRUE AS DECIMAL(1,1))",
        "CAST(parse_json('true') AS DECIMAL(1,1))",
        "TRY_CAST(parse_json('true') AS DECIMAL(1,1))",
    ] {
        run_ast(file, expression, &[]);
    }
    let strings = [("s", StringType::from_data(vec!["10", "100", "2"]))];
    run_ast(file, "CAST(s AS DECIMAL(3,0)) <= 10", &strings);
    let strings = [("s", StringType::from_data(vec!["10", "100x", "2"]))];
    run_ast(file, "CAST(s AS DECIMAL(3,0)) <= 10", &strings);
    let columns = [("a", Int64Type::from_data(vec![-10, -9, 0, 9, 10]))];
    run_ast(file, "CAST(a AS DECIMAL(1,0))", &columns);
    run_ast(file, "TRY_CAST(a AS DECIMAL(1,0))", &columns);
    let columns = [("f", Float64Type::from_data(vec![1.5, f64::NAN]))];
    run_ast(file, "CAST(f AS DECIMAL(38,10))", &columns);
    let columns = [("active", BooleanType::from_data(vec![false, true, false]))];
    run_ast(
        file,
        "if(active, CAST(10::Int64 AS DECIMAL(1,0)), NULL)",
        &columns,
    );
    run_ast(
        file,
        "if(active, TRY_CAST(10::Int64 AS DECIMAL(1,0)), NULL)",
        &columns,
    );
    for expression in [
        "CAST(1e30::Float64 AS DECIMAL(1,0))",
        "CAST('bad' AS DECIMAL(1,0))",
        "CAST(10::DECIMAL(2,0) AS DECIMAL(1,0))",
        "CAST(TRUE AS DECIMAL(1,1))",
        "CAST(parse_json('10') AS DECIMAL(1,0))",
        "CAST(parse_json('true') AS DECIMAL(1,1))",
    ] {
        run_ast(file, format!("if(active, {expression}, NULL)"), &columns);
    }
    run_ast(
        file,
        "if(active, CAST(parse_json('7') AS DECIMAL(1,0)), NULL)",
        &columns,
    );
    let columns = [("active", BooleanType::from_data(vec![false, false, false]))];
    run_ast(
        file,
        "if(active, CAST(10::Int64 AS DECIMAL(1,0)), 1::DECIMAL(1,0))",
        &columns,
    );
    // TRY over Variant writes NULL for every failure kind without an error.
    let columns = [
        (
            "s",
            StringType::from_data(vec!["1.5", "1000", "\"x\"", "null"]),
        ),
        (
            "active",
            BooleanType::from_data(vec![true, false, true, true]),
        ),
    ];
    for expression in [
        "CAST(parse_json(s) AS DECIMAL(3,1))",
        "TRY_CAST(parse_json(s) AS DECIMAL(3,1))",
        "if(active, CAST(parse_json(s) AS DECIMAL(3,1)), NULL)",
        "if(active, TRY_CAST(parse_json(s) AS DECIMAL(3,1)), NULL)",
    ] {
        run_ast(file, expression, &columns);
    }
    // A scalar string with a format column follows each visible row's format.
    let columns = [
        ("f", StringType::from_data(vec!["9999", "999"])),
        ("active", BooleanType::from_data(vec![false, true])),
    ];
    run_ast(file, "to_decimal(3, 0)('123', f)", &columns);
    run_ast(
        file,
        "if(active, to_decimal(3, 0)('123', f), NULL)",
        &columns,
    );
}
