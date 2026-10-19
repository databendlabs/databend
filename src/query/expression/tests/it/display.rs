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

macro_rules! check_float_sql_display {
    ($float:ident, $variant:ident, $number_type:ident, $value:expr) => {{
        let value: $float = $value;
        let expected = value.to_string();
        let number = NumberScalar::$variant(value.into());
        let expr = Expr::<usize>::Constant(Constant {
            span: None,
            scalar: Scalar::Number(number),
            data_type: $number_type::data_type(),
        });
        let sql = expr.sql_display();
        assert_eq!(sql, expected);
        assert_eq!(sql.parse::<$float>().unwrap().to_bits(), value.to_bits());
    }};
}

#[test]
fn test_float_sql_display_preserves_precision() {
    // Include adjacent floats, values below the former ten-decimal-place limit,
    // and values outside rust_decimal's range. Signed zero must also survive.
    for value in [
        0.0_f64,
        -0.0,
        0.3,
        0.30000000000000004,
        1.5e-16,
        -1.5e-16,
        123456789.12345679,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        f64::MAX,
        f64::MIN,
    ] {
        check_float_sql_display!(f64, Float64, Float64Type, value);
    }
    for value in [
        0.0_f32,
        -0.0,
        0.3,
        f32::from_bits(0.3_f32.to_bits() + 1),
        1.5e-16,
        -1.5e-16,
        12345.678,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        f32::MAX,
        f32::MIN,
    ] {
        check_float_sql_display!(f32, Float32, Float32Type, value);
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
    let fold = |sql: &str| {
        let raw_expr = parse_raw_expr(sql, &[], &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        ConstantFolder::fold(
            Cow::Owned(expr),
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        )
        .0
    };

    for sql in [
        "CAST('0.30000000000000004' AS DOUBLE)",
        "CAST('1.5e-16' AS DOUBLE)",
        "CAST('0.30000004' AS FLOAT)",
        "CAST('1.5e-16' AS FLOAT)",
        "CAST('0.1' AS DOUBLE) + CAST('0.2' AS DOUBLE)",
    ] {
        let folded = fold(sql);
        // A stored default's column type supplies the cast when it is rebound.
        let restored = fold(&format!(
            "CAST({} AS {})",
            folded.sql_display(),
            folded.data_type()
        ));
        assert_eq!(
            restored.as_constant().unwrap().scalar,
            folded.as_constant().unwrap().scalar,
            "{sql}"
        );
    }
}

proptest! {
    #[test]
    fn test_float64_sql_display_round_trip(bits in any::<u64>()) {
        let value = f64::from_bits(bits);
        prop_assume!(value.is_finite());
        check_float_sql_display!(f64, Float64, Float64Type, value);
    }

    #[test]
    fn test_float32_sql_display_round_trip(bits in any::<u32>()) {
        let value = f32::from_bits(bits);
        prop_assume!(value.is_finite());
        check_float_sql_display!(f32, Float32, Float32Type, value);
    }
}
