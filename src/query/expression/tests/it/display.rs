// Copyright 2023 Datafuse Labs.
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

use std::borrow::Cow;

use databend_common_expression::Column;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::type_check;
use databend_common_expression::types::*;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use proptest::prelude::*;

#[test]
fn test_fmt_expr() {
    let mut fake_registry = FunctionRegistry::empty();
    fake_registry.register_2_arg_core::<StringType, StringType, StringType, _, _>(
        "test_fn",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar("x".to_string()),
    );

    let raw_expr = RawExpr::Cast {
        span: None,
        is_try: false,
        expr: Box::new(RawExpr::ColumnRef {
            span: None,
            id: 1,
            data_type: BooleanType::data_type(),
            display_name: "aaa".to_string(),
        }),
        dest_type: StringType::data_type(),
    };

    let raw_expr = RawExpr::FunctionCall {
        span: None,
        name: "test_fn".to_string(),
        params: vec![Scalar::Null],
        args: vec![raw_expr.clone(), raw_expr.clone()],
    };

    let expr = type_check::check(&raw_expr, &fake_registry).unwrap();

    assert_eq!(
        "test_fn<String, String>(CAST<Boolean>(aaa AS String), CAST<Boolean>(aaa AS String))",
        format!("{expr}")
    );
    assert_eq!(
        "test_fn<String, String>(CAST<Boolean>(aaa (#1) AS String), CAST<Boolean>(aaa (#1) AS String))",
        format!("{}", expr.fmt_with_options(true))
    );
}

fn fold(sql: &str) -> Expr {
    fold_with_columns(sql, &[])
}

fn fold_with_columns(sql: &str, columns: &[(&str, DataType)]) -> Expr {
    let raw_expr = parse_raw_expr(sql, columns, &BUILTIN_FUNCTIONS);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    ConstantFolder::fold(
        Cow::Owned(expr),
        &FunctionContext::default(),
        &BUILTIN_FUNCTIONS,
    )
    .0
    .into_owned()
}

/// Renders a constant with `to_sql_string` and rebinds the text under a cast to its own
/// type, as a stored DEFAULT is rebound under the column type. Returns the SQL text and
/// the rebound constant.
fn sql_round_trip(scalar: Scalar, data_type: DataType) -> (String, Scalar) {
    let sql = Expr::<usize>::Constant(Constant {
        span: None,
        scalar,
        data_type: data_type.clone(),
    })
    .to_sql_string();
    let restored = fold(&format!("CAST({sql} AS {data_type})"));
    let restored = restored
        .as_constant()
        .unwrap_or_else(|| panic!("`{sql}` does not fold to a constant"))
        .scalar
        .clone();
    (sql, restored)
}

fn check_f64_sql_round_trip(value: f64) -> String {
    let (sql, restored) = sql_round_trip(
        Scalar::Number(NumberScalar::Float64(value.into())),
        Float64Type::data_type(),
    );
    let Scalar::Number(NumberScalar::Float64(restored)) = restored else {
        panic!("`{sql}` rebinds to {restored:?}");
    };
    assert_float_restored(value, *restored, value.to_bits(), restored.to_bits(), &sql);
    sql
}

fn check_f32_sql_round_trip(value: f32) -> String {
    let (sql, restored) = sql_round_trip(
        Scalar::Number(NumberScalar::Float32(value.into())),
        Float32Type::data_type(),
    );
    let Scalar::Number(NumberScalar::Float32(restored)) = restored else {
        panic!("`{sql}` rebinds to {restored:?}");
    };
    assert_float_restored(
        value as f64,
        *restored as f64,
        value.to_bits() as u64,
        restored.to_bits() as u64,
        &sql,
    );
    sql
}

/// Floats must rebind bit-identically, except for the members of the zero and NaN
/// equality classes, which only need to stay in their class
/// (docs/designs/20260921-float-value-semantics.md).
fn assert_float_restored(value: f64, restored: f64, bits: u64, restored_bits: u64, sql: &str) {
    if value.is_nan() {
        assert!(restored.is_nan(), "{value:?} -> `{sql}` -> {restored:?}");
    } else if value == 0.0 {
        assert_eq!(restored, 0.0, "{value:?} -> `{sql}`");
    } else {
        assert_eq!(restored_bits, bits, "{value:?} -> `{sql}` -> {restored:?}");
    }
}

#[test]
fn test_float_to_sql_string_round_trip() {
    // Adjacent floats, mantissas beyond 2^53 or scales beyond 10^22 that a naive
    // decimal-to-float division rounds twice, subnormals, and the extremes.
    let cases = [
        (0.0_f64, "0"),
        (-0.0, "-0"),
        (0.3, "0.3"),
        (0.30000000000000004, "0.30000000000000004"),
        (0.37349299999999996, "0.37349299999999996"),
        (1.5990461000457081, "1.5990461000457081"),
        (955.3820000000001, "955.3820000000001"),
        (-529455.1535166176, "-529455.1535166176"),
        (123456789.12345679, "123456789.12345679"),
        (0.000001, "0.000001"),
        (1.5e-7, "1.5e-7"),
        (1.5e-16, "1.5e-16"),
        (-1.5e-16, "-1.5e-16"),
        (-3.443861187166828e-233, "-3.443861187166828e-233"),
        (1.2345678901234568e20, "123456789012345680000"),
        (1e21, "1e21"),
        (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
        (f64::from_bits(1), "5e-324"),
        (f64::MAX, "1.7976931348623157e308"),
        (f64::MIN, "-1.7976931348623157e308"),
        (f64::NAN, "'nan'::Float64"),
        (f64::INFINITY, "'inf'::Float64"),
        (f64::NEG_INFINITY, "'-inf'::Float64"),
    ];
    for (value, expected) in cases {
        assert_eq!(check_f64_sql_round_trip(value), expected, "{value:?}");
    }

    let cases = [
        (0.0_f32, "0"),
        (-0.0, "-0"),
        (0.3, "0.3"),
        (f32::from_bits(0.3_f32.to_bits() + 1), "0.30000004"),
        (0.33333334, "0.33333334"),
        (12345.678, "12345.678"),
        (1e-20, "1e-20"),
        (1.5e-16, "1.5e-16"),
        (-1.5e-16, "-1.5e-16"),
        (f32::MIN_POSITIVE, "1.1754944e-38"),
        (f32::from_bits(1), "1e-45"),
        (f32::MAX, "3.4028235e38"),
        (f32::MIN, "-3.4028235e38"),
        (f32::NAN, "'nan'::Float32"),
        (f32::INFINITY, "'inf'::Float32"),
        (f32::NEG_INFINITY, "'-inf'::Float32"),
    ];
    for (value, expected) in cases {
        assert_eq!(check_f32_sql_round_trip(value), expected, "{value:?}");
    }
}

#[test]
fn test_nested_float_sql_round_trip() {
    let array = Float64Type::from_data(vec![1.5e-16, 0.30000000000000004, f64::NAN]);
    let cases = [
        (
            Scalar::Array(array.clone()),
            DataType::Array(Box::new(Float64Type::data_type())),
            "[1.5e-16, 0.30000000000000004, 'nan'::Float64]",
        ),
        (
            Scalar::Tuple(vec![
                Scalar::Number(NumberScalar::Float64(1.5e-16.into())),
                Scalar::Number(NumberScalar::Float32(0.33333334.into())),
            ]),
            DataType::Tuple(vec![Float64Type::data_type(), Float32Type::data_type()]),
            "(1.5e-16, 0.33333334)",
        ),
        (
            Scalar::Map(Column::Tuple(vec![
                StringType::from_data(vec!["a", "b"]),
                Float64Type::from_data(vec![0.37349299999999996, f64::INFINITY]),
            ])),
            DataType::Map(Box::new(DataType::Tuple(vec![
                StringType::data_type(),
                Float64Type::data_type(),
            ]))),
            "{'a':0.37349299999999996, 'b':'inf'::Float64}",
        ),
    ];
    for (scalar, data_type, expected) in cases {
        let (sql, restored) = sql_round_trip(scalar.clone(), data_type);
        assert_eq!(sql, expected);
        assert_eq!(restored, scalar, "`{sql}`");
    }
}

/// `sql_display` is for people and truncates floats like block output, at any depth;
/// `to_sql_string` keeps them through casts and function arguments.
#[test]
fn test_float_sql_display_and_to_sql_string() {
    let columns = [("a", Float64Type::data_type())];
    let cases = [
        (
            "a + CAST('0.30000000000000004' AS DOUBLE)",
            "a + 0.3",
            "a + 0.30000000000000004",
        ),
        (
            "CAST(a AS FLOAT) < CAST('0.30000004' AS FLOAT)",
            "CAST(a AS Float32) < 0.3",
            "CAST(a AS Float32) < 0.30000004",
        ),
        (
            "pow(a, CAST('1.5e-16' AS DOUBLE))",
            "pow(a, 0)",
            "pow(a, 1.5e-16)",
        ),
        (
            "contains([CAST('1.5e-16' AS DOUBLE)], a)",
            "contains([0], a)",
            "contains([1.5e-16], a)",
        ),
        (
            "a = CAST('nan' AS DOUBLE)",
            "a = 'nan'::Float64",
            "a = 'nan'::Float64",
        ),
        (
            "a > CAST('-inf' AS DOUBLE)",
            "a > '-inf'::Float64",
            "a > '-inf'::Float64",
        ),
    ];
    for (sql, display, round_trip) in cases {
        let expr = fold_with_columns(sql, &columns);
        assert_eq!(expr.sql_display(), display, "{sql}");
        assert_eq!(expr.to_sql_string(), round_trip, "{sql}");
    }
}

#[test]
fn test_float_block_display_unchanged() {
    let block = DataBlock::new_from_columns(vec![Float64Type::from_data(vec![
        0.30000000000000004,
        1.5e-16,
    ])]);
    let display = block.to_string();
    assert!(display.contains("0.3"));
    assert!(!display.contains("0.30000000000000004"));
    assert!(!display.contains("0.00000000000000015"));
    assert!(format!("{block:?}").contains("Float64([0.3, 0])"));
}

#[test]
fn test_folded_float_sql_round_trip() {
    for sql in [
        "CAST('0.30000000000000004' AS DOUBLE)",
        "CAST('0.37349299999999996' AS DOUBLE)",
        "CAST('1.5e-16' AS DOUBLE)",
        "CAST('0.30000004' AS FLOAT)",
        "CAST('1.5e-16' AS FLOAT)",
        "CAST('0.1' AS DOUBLE) + CAST('0.2' AS DOUBLE)",
        // Literals bind as decimals, so these exercise decimal-to-float conversion
        // before the value is displayed.
        "CAST(0.37349299999999996 AS DOUBLE)",
        "CAST(-3.443861187166828e-233 AS DOUBLE)",
        "CAST(0.33333334 AS FLOAT)",
        "CAST(1e-20 AS FLOAT)",
    ] {
        let folded = fold(sql);
        let folded = folded.as_constant().unwrap();
        let (text, restored) = sql_round_trip(folded.scalar.clone(), folded.data_type.clone());
        assert_eq!(restored, folded.scalar, "{sql} -> `{text}`");
    }
}

proptest! {
    #[test]
    fn test_float64_to_sql_string_round_trip(bits in any::<u64>()) {
        check_f64_sql_round_trip(f64::from_bits(bits));
    }

    #[test]
    fn test_float32_to_sql_string_round_trip(bits in any::<u32>()) {
        check_f32_sql_round_trip(f32::from_bits(bits));
    }
}
