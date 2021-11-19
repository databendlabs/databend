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

use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::scalars::Monotonicity;
use common_functions::scalars::Range;
use common_planners::*;

use crate::optimizers::MonotonicityCheckVisitor;

#[test]
fn test_check_expression_monotonicity_without_range() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expect_mono: Monotonicity,
        column: String,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "f(x) = x + 12",
            expr: Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
            },
            column: String::from("x"),
        },
        Test {
            name: "f(x) = -x + 12",
            expr: Expression::create_binary_expression("+", vec![
                Expression::create_unary_expression("-", vec![col("x")]),
                lit(12i32),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
            },
            column: String::from("x"),
        },
        Test {
            name: "f(x) = (-x + 12) - x + (1 - x)",
            expr: Expression::create_binary_expression("+", vec![
                Expression::create_binary_expression("-", vec![
                    // -x + 12
                    Expression::create_binary_expression("+", vec![
                        Expression::create_unary_expression("-", vec![col("x")]),
                        lit(12i32),
                    ]),
                    col("x"),
                ]),
                // 1 - x
                Expression::create_unary_expression("-", vec![lit(1i64), col("x")]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
            },
            column: String::from("x"),
        },
        Test {
            name: "f(x) = (x + 12) - x + (1 - x)",
            expr: Expression::create_binary_expression("+", vec![
                Expression::create_binary_expression("-", vec![
                    // x + 12
                    Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
                    col("x"),
                ]),
                // 1 - x
                Expression::create_unary_expression("-", vec![lit(1i64), col("x")]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: false,
            },
            column: String::from("x"),
        },
        Test {
            name: "f(x) = abs(x + 12)",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
            },
            column: String::from("x"),
        },
    ];

    for t in tests.into_iter() {
        let (mono, column) = MonotonicityCheckVisitor::check_expression(&t.expr, None)?;
        assert_eq!(mono.is_monotonic, t.expect_mono.is_monotonic, "{}", t.name);
        if mono.is_monotonic {
            assert_eq!(mono.is_positive, t.expect_mono.is_positive, "{}", t.name);
            assert_eq!(column, t.column, "{}", t.name);
        }
    }

    Ok(())
}

#[test]
fn test_check_expression_monotonicity_with_range() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expect_mono: Monotonicity,
        column: String,
        range: Range,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "f(x) = abs(x) where x >= 0",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
            },
            column: String::from("x"),
            range: Range {
                begin: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(0)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
                end: None,
            },
        },
        Test {
            name: "f(x) = abs(x) where x <= 0",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
            },
            column: String::from("x"),
            range: Range {
                begin: None,
                end: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(0)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
            },
        },
        Test {
            name: "f(x) = abs(x) where x <= 5", // should NOT be monotonic
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: false,
            },
            column: String::from("x"),
            range: Range {
                begin: None,
                end: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(5)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
            },
        },
        Test {
            name: "f(x) = abs(x + 12) where x >= -12",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
            },
            column: String::from("x"),
            range: Range {
                begin: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(-12)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
                end: None,
            },
        },
        Test {
            name: "f(x) = abs(x + 12) where x >= -14", // should NOT be monotonic
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
            },
            column: String::from("x"),
            range: Range {
                begin: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(-14)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
                end: None,
            },
        },
        Test {
            name: "f(x) = abs( (x - 2) + (x - 3) ) where x >= 5",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![
                    Expression::create_binary_expression("-", vec![col("x"), lit(2_i32)]),
                    Expression::create_binary_expression("-", vec![col("x"), lit(3_i32)]),
                ]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
            },
            column: String::from("x"),
            range: Range {
                begin: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(5)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
                end: None,
            },
        },
        Test {
            name: "f(x) = abs( (x - 2) + (x - 3) ) where x >= 4", // should NOT be monotonic
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![
                    Expression::create_binary_expression("-", vec![col("x"), lit(2_i32)]),
                    Expression::create_binary_expression("-", vec![col("x"), lit(3_i32)]),
                ]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
            },
            column: String::from("x"),
            range: Range {
                begin: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(4)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
                end: None,
            },
        },
        Test {
            name: "f(x) = abs( (-x + 8) - x) where x <= 4",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("-", vec![
                    Expression::create_binary_expression("+", vec![
                        Expression::create_unary_expression("-", vec![col("x")]),
                        lit(8_i64),
                    ]),
                    col("x"),
                ]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
            },
            column: String::from("x"),
            range: Range {
                begin: None,
                end: Some(DataColumnWithField::new(
                    DataColumn::Constant(DataValue::Int8(Some(4)), 1),
                    DataField::new("x", DataType::Int8, false),
                )),
            },
        },
    ];

    for t in tests.into_iter() {
        let (mono, column) = MonotonicityCheckVisitor::check_expression(&t.expr, Some(t.range))?;
        assert_eq!(mono.is_monotonic, t.expect_mono.is_monotonic, "{}", t.name);
        if mono.is_monotonic {
            assert_eq!(mono.is_positive, t.expect_mono.is_positive, "{}", t.name);
            assert_eq!(column, t.column, "{}", t.name);
        }
    }

    Ok(())
}
