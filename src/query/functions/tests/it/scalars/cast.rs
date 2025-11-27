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

use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use goldenfile::Mint;
use itertools::Itertools;
use roaring::RoaringTreemap;

use super::run_ast;
use super::run_ast_with_context;
use super::TestContext;

#[test]
fn test_cast() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast.txt").unwrap();

    for is_try in [false, true] {
        test_cast_primitive(file, is_try);
        test_cast_to_variant(file, is_try);
        test_cast_number_to_timestamp(file, is_try);
        test_cast_number_to_date(file, is_try);
        test_cast_between_number_and_string(file, is_try);
        test_cast_between_boolean_and_string(file, is_try);
        test_cast_between_string_and_decimal(file, is_try);
        test_cast_between_number_and_boolean(file, is_try);
        test_cast_between_date_and_timestamp(file, is_try);
        test_cast_between_string_and_timestamp(file, is_try);
        test_cast_between_string_and_date(file, is_try);
        test_cast_to_nested_type(file, is_try);
        test_cast_between_binary_and_string(file, is_try);
    }
}

fn test_cast_primitive(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST(0 AS UINT8)"), &[]);
    run_ast(file, format!("{prefix}CAST(0 AS UINT8 NULL)"), &[]);
    run_ast(file, format!("{prefix}CAST('str' AS STRING)"), &[]);
    run_ast(file, format!("{prefix}CAST('str' AS STRING NULL)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS UINT8)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS UINT8 NULL)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS STRING)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS STRING NULL)"), &[]);
    run_ast(file, format!("{prefix}CAST(1024 AS UINT8)"), &[]);
    run_ast(file, format!("{prefix}CAST(a AS UINT8)"), &[(
        "a",
        UInt16Type::from_data(vec![0u16, 64, 255, 512, 1024]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS UINT16)"), &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS INT64)"), &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(
        file,
        format!(
            "({prefix}CAST(a AS FLOAT32), {prefix}CAST(a AS INT32), {prefix}CAST(b AS FLOAT32), {prefix}CAST(b AS INT32))"
        ),
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
    run_ast(
        file,
        format!("{prefix}CAST([[a, b], NULL, NULL] AS Array(Array(Int8)))"),
        &[
            ("a", Int16Type::from_data(vec![0i16, 1, 2, 127, 255])),
            ("b", Int16Type::from_data(vec![0i16, -1, -127, -128, -129])),
        ],
    );
    run_ast(
        file,
        format!("{prefix}CAST((a, b, NULL) AS TUPLE(Int8, UInt8, Boolean NULL))"),
        &[
            ("a", Int16Type::from_data(vec![0i16, 1, 2, 127, 256])),
            ("b", Int16Type::from_data(vec![0i16, 1, -127, -128, -129])),
        ],
    );
    run_ast(file, format!("{prefix}CAST(a AS INT16)"), &[(
        "a",
        Float64Type::from_data(vec![0.0f64, 1.1, 2.2, 3.3, -4.4]),
    )]);
    run_ast(file, format!("{prefix}CAST(b AS INT16)"), &[(
        "b",
        Int8Type::from_data(vec![0i8, 1, 2, 3, -4]),
    )]);

    run_ast(file, format!("{prefix}CAST(a AS UINT16)"), &[(
        "a",
        Int16Type::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);

    run_ast(file, format!("{prefix}CAST(c AS INT16)"), &[(
        "c",
        Int64Type::from_data(vec![0i64, 11111111111, 2, 3, -4]),
    )]);
}

fn test_cast_to_variant(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST(NULL AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST(0 AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST(-1 AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST(1.1 AS VARIANT)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST('🍦 が美味しい' AS VARIANT)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST([0, 1, 2] AS VARIANT)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST([0::VARIANT, '\"a\"'::VARIANT] AS VARIANT)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(to_timestamp(1000000) AS VARIANT)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(false AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST(true AS VARIANT)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST('\"🍦 が美味しい\"' AS VARIANT) AS VARIANT)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST((1,) AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST((1, 2) AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST((false, true) AS VARIANT)"), &[]);
    run_ast(file, format!("{prefix}CAST(('a',) AS VARIANT)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST((1, 2, (false, true, ('a',))) AS VARIANT)"),
        &[],
    );

    run_ast(file, format!("{prefix}CAST(a AS VARIANT)"), &[(
        "a",
        StringType::from_data_with_validity(vec!["true", "{\"k\":\"v\"}", "[1,2,3]"], vec![
            true, false, true,
        ]),
    )]);

    run_ast(file, format!("{prefix}CAST(a AS VARIANT)"), &[(
        "a",
        gen_bitmap_data(),
    )]);
}

fn test_cast_number_to_timestamp(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(
        file,
        format!("{prefix}CAST(-30610224000000001 AS TIMESTAMP)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(-315360000000000 AS TIMESTAMP)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(-315360000000 AS TIMESTAMP)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(-100 AS TIMESTAMP)"), &[]);
    run_ast(file, format!("{prefix}CAST(-0 AS TIMESTAMP)"), &[]);
    run_ast(file, format!("{prefix}CAST(0 AS TIMESTAMP)"), &[]);
    run_ast(file, format!("{prefix}CAST(100 AS TIMESTAMP)"), &[]);
    run_ast(file, format!("{prefix}CAST(315360000000 AS TIMESTAMP)"), &[
    ]);
    run_ast(
        file,
        format!("{prefix}CAST(315360000000000 AS TIMESTAMP)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(253402300800000000 AS TIMESTAMP)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(a AS TIMESTAMP)"), &[(
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

    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-315360000000000) AS INT64)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-315360000000) AS INT64)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-100) AS INT64)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(TO_TIMESTAMP(-0) AS INT64)"), &[
    ]);
    run_ast(file, format!("{prefix}CAST(TO_TIMESTAMP(0) AS INT64)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(100) AS INT64)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(315360000000) AS INT64)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(315360000000000) AS INT64)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(a AS INT64)"), &[(
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

fn test_cast_number_to_date(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST(-354286 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(-354285 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(-100 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(-0 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(0 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(100 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(2932896 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(2932897 AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(a AS DATE)"), &[(
        "a",
        Int32Type::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);

    run_ast(file, format!("{prefix}CAST(TO_DATE(-354285) AS INT64)"), &[
    ]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(-100) AS INT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(-0) AS INT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(0) AS INT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(100) AS INT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(2932896) AS INT64)"), &[
    ]);
    run_ast(file, format!("{prefix}CAST(a AS INT64)"), &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_between_number_and_boolean(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST(0 AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST(1 AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST(false AS UINT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(true AS INT64)"), &[]);

    run_ast(file, format!("{prefix}CAST(0.0 AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST(1.0 AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST(false AS FLOAT32)"), &[]);
    run_ast(file, format!("{prefix}CAST(true AS FLOAT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(false AS DECIMAL(4,3))"), &[]);
    run_ast(file, format!("{prefix}CAST(true AS DECIMAL(4,2))"), &[]);

    run_ast(file, format!("{prefix}CAST(num AS BOOLEAN)"), &[(
        "num",
        Int64Type::from_data(vec![0i64, -1, 1, 2]),
    )]);
    run_ast(file, format!("{prefix}CAST(num AS BOOLEAN)"), &[(
        "num",
        UInt64Type::from_data(vec![0u64, 1, 2]),
    )]);
    run_ast(file, format!("{prefix}CAST(bool AS UINT64)"), &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
    run_ast(file, format!("{prefix}CAST(bool AS INT64)"), &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
}

fn test_cast_between_number_and_string(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST('foo' AS UINT64)"), &[]);
    run_ast(file, format!("{prefix}CAST('1foo' AS INT32)"), &[]);
    run_ast(file, format!("{prefix}CAST('-1' AS UINT64)"), &[]);
    run_ast(file, format!("{prefix}CAST('256' AS UINT8)"), &[]);
    run_ast(file, format!("{prefix}CAST('1' AS UINT64)"), &[]);
    run_ast(file, format!("{prefix}CAST(str AS INT64)"), &[(
        "str",
        StringType::from_data(vec![
            "-9223372036854775808",
            "-1",
            "0",
            "1",
            "9223372036854775807",
        ]),
    )]);
    run_ast(file, format!("{prefix}CAST(str AS INT64)"), &[(
        "str",
        StringType::from_data_with_validity(vec!["foo", "foo", "0", "0"], vec![
            true, false, true, false,
        ]),
    )]);

    run_ast(file, format!("{prefix}CAST(num AS STRING)"), &[(
        "num",
        Int64Type::from_data(vec![i64::MIN, -1, 0, 1, i64::MAX]),
    )]);
    run_ast(file, format!("{prefix}CAST(num AS STRING)"), &[(
        "num",
        UInt64Type::from_data(vec![0, 1, u64::MAX]),
    )]);
}

fn test_cast_between_boolean_and_string(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST('t' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('f' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('0' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('1' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('true' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('false' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('TRUE' AS BOOLEAN)"), &[]);
    run_ast(file, format!("{prefix}CAST('FaLse' AS BOOLEAN)"), &[]);

    run_ast(file, format!("{prefix}CAST(bool AS STRING)"), &[(
        "bool",
        BooleanType::from_data(vec![false, true]),
    )]);
}

fn test_cast_between_date_and_timestamp(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST(TO_DATE(1) AS TIMESTAMP)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_TIMESTAMP(1) AS DATE)"), &[]);
    run_ast(file, format!("{prefix}CAST(a AS DATE)"), &[(
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
    run_ast(file, format!("{prefix}CAST(a AS TIMESTAMP)"), &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(a) AS TIMESTAMP)"), &[(
        "a",
        Int32Type::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS TIMESTAMP)"), &[(
        "a",
        Int64Type::from_data(vec![i64::MAX]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS DATE)"), &[(
        "a",
        Int64Type::from_data(vec![i64::MAX]),
    )]);
}

fn test_cast_between_string_and_timestamp(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}TO_TIMESTAMP('2022')"), &[]);
    run_ast(file, format!("{prefix}TO_TIMESTAMP('2022-01')"), &[]);
    run_ast(file, format!("{prefix}TO_TIMESTAMP('2022-01-02')"), &[]);
    run_ast(
        file,
        format!("{prefix}TO_TIMESTAMP('A NON-TIMESTAMP STR')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}TO_TIMESTAMP('2022-01-02T03:25:02.868894-07:00')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}TO_TIMESTAMP('2022-01-02 02:00:11')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}TO_TIMESTAMP('2022-01-02T02:00:22')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}TO_TIMESTAMP('2022-01-02T01:12:00-07:00')"),
        &[],
    );
    run_ast(file, format!("{prefix}TO_TIMESTAMP('2022-01-02T01')"), &[]);
    run_ast(file, format!("{prefix}TO_TIMESTAMP(a)"), &[(
        "a",
        StringType::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-315360000000000) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-315360000000) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-100) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(-0) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(0) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(100) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(315360000000) AS VARCHAR)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TO_TIMESTAMP(315360000000000) AS VARCHAR)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(a AS VARCHAR)"), &[(
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

fn test_cast_between_string_and_date(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}TO_DATE('2022')"), &[]);
    run_ast(file, format!("{prefix}TO_DATE('2022-01')"), &[]);
    run_ast(file, format!("{prefix}TO_DATE('2022-01-02')"), &[]);
    run_ast(file, format!("{prefix}TO_DATE('A NON-DATE STR')"), &[]);
    run_ast(
        file,
        format!("{prefix}TO_DATE('2022-01-02T03:25:02.868894-07:00')"),
        &[],
    );
    run_ast(file, format!("{prefix}TO_DATE('2022-01-02 02:00:11')"), &[]);
    run_ast(file, format!("{prefix}TO_DATE('2022-01-02T02:00:22')"), &[]);
    run_ast(
        file,
        format!("{prefix}TO_DATE('2022-01-02T01:12:00-07:00')"),
        &[],
    );
    run_ast(file, format!("{prefix}TO_DATE('2022-01-02T01')"), &[]);
    run_ast(file, format!("{prefix}TO_DATE(a)"), &[(
        "a",
        StringType::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(
        file,
        format!("{prefix}CAST(TO_DATE(-354285) AS VARCHAR)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(TO_DATE(-100) AS VARCHAR)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(-0) AS VARCHAR)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(0) AS VARCHAR)"), &[]);
    run_ast(file, format!("{prefix}CAST(TO_DATE(100) AS VARCHAR)"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST(TO_DATE(2932896) AS VARCHAR)"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(a AS VARCHAR)"), &[(
        "a",
        DateType::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_to_nested_type(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(
        file,
        format!("{prefix}CAST((1, TRUE) AS Tuple(STRING))"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST(('a',) AS Tuple(INT))"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST(((1, TRUE), 1) AS Tuple(Tuple(INT, INT), INT))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(TRY_CAST(1 AS INT32) AS INT32)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(((1, 'a'), 1) AS Tuple(Tuple(INT, INT NULL), INT))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST(((1, 'a'), 1) AS Tuple(Tuple(INT, INT), INT) NULL)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST([(1,TRUE),(2,FALSE)] AS Array(Tuple(INT, INT)))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST([(1,'a'),(2,'a')] AS Array(Tuple(INT, INT)) NULL)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST([(1,'a'),(2,'a')] AS Array(Tuple(INT, INT NULL)))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST([[TRUE], [FALSE, TRUE]] AS Array(Array(INT)))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST([['a'], ['b', 'c']] AS Array(Array(INT) NULL))"),
        &[],
    );
}

fn test_cast_between_string_and_decimal(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST('010.010' AS DECIMAL(5,3))"), &[
    ]);
    run_ast(file, format!("{prefix}CAST('010.010' AS DECIMAL(5,4))"), &[
    ]);
    run_ast(file, format!("{prefix}CAST('010.010' AS DECIMAL(5,2))"), &[
    ]);
    run_ast(file, format!("{prefix}CAST('010.010' AS DECIMAL(4,3))"), &[
    ]);
    run_ast(file, format!("{prefix}CAST('010.010' AS DECIMAL(4,2))"), &[
    ]);
    run_ast(
        file,
        format!("{prefix}CAST('-1010.010' AS DECIMAL(7,3))"),
        &[],
    );
    run_ast(file, format!("{prefix}CAST('00' AS DECIMAL(2,1))"), &[]);
    run_ast(file, format!("{prefix}CAST('0.0' AS DECIMAL(2,0))"), &[]);
    run_ast(file, format!("{prefix}CAST('.0' AS DECIMAL(1,0))"), &[]);
    run_ast(
        file,
        format!("{prefix}CAST('+1.0e-10' AS DECIMAL(11, 10))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST('-1.0e+10' AS DECIMAL(11, 0))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST('-0.000000' AS DECIMAL(11, 0))"),
        &[],
    );
}

fn test_cast_between_binary_and_string(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}CAST('Abc' AS BINARY)"), &[]);
    run_ast(file, format!("{prefix}CAST('Dobrý den' AS BINARY)"), &[]);
    run_ast(file, format!("{prefix}CAST('ß😀山' AS BINARY)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS BINARY)"), &[]);
    run_ast(file, format!("{prefix}CAST(NULL AS BINARY NULL)"), &[]);
    run_ast(file, format!("{prefix}CAST(a AS BINARY)"), &[(
        "a",
        StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS BINARY)"), &[(
        "a",
        StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
            true, true, false,
        ]),
    )]);
    run_ast(file, format!("{prefix}CAST(a AS BINARY NULL)"), &[(
        "a",
        StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
            true, true, false,
        ]),
    )]);
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST('Abc' AS BINARY) AS STRING)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST('Dobrý den' AS BINARY) AS STRING)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST('ß😀山' AS BINARY) AS STRING)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST(NULL AS BINARY) AS STRING)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST(NULL AS BINARY NULL) AS STRING NULL)"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST(a AS BINARY) AS STRING)"),
        &[(
            "a",
            StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
        )],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST(a AS BINARY) AS STRING)"),
        &[(
            "a",
            StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
                true, true, false,
            ]),
        )],
    );
    run_ast(
        file,
        format!("{prefix}CAST({prefix}CAST(a AS BINARY NULL) AS STRING NULL)"),
        &[(
            "a",
            StringType::from_data_with_validity(vec!["Abc", "Dobrý den", "ß😀山"], vec![
                true, true, false,
            ]),
        )],
    );
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
        let mut rb = RoaringTreemap::new();
        rb.insert_range(i..(i + 5));
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
