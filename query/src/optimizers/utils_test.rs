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

use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::scalars::Monotonicity;
use common_planners::*;

use crate::optimizers::MonotonicityCheckVisitor;

#[test]
fn test_check_expression_monotonicity_without_range() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expect_mono: Monotonicity,
        column: &'static str,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "f(x) = x + 12",
            expr: Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
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
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
        },
        Test {
            name: "f(x,y) = x + y", // multi-variable function is not supported,
            expr: Expression::create_binary_expression("+", vec![col("x"), col("y")]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
            column: "",
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
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
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
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
        },
        Test {
            name: "f(x) = abs(x + 12)",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
        },
    ];

    for t in tests.into_iter() {
        let (mono, column) = MonotonicityCheckVisitor::check_expression(&t.expr, None, None)?;
        assert_eq!(
            mono.is_monotonic, t.expect_mono.is_monotonic,
            "{} is_monotonic",
            t.name
        );
        if mono.is_monotonic {
            assert_eq!(
                mono.is_positive, t.expect_mono.is_positive,
                "{} is_positive",
                t.name
            );
            assert_eq!(column, t.column, "{} column", t.name);
        }
    }

    Ok(())
}

#[test]
fn test_check_expression_monotonicity_with_range() -> Result<()> {
    let create_data = |d: i64| -> Option<DataColumnWithField> {
        let data_field = DataField::new("x", DataType::Int64, false);
        let data_column = DataColumn::Constant(DataValue::Int64(Some(d)), 1);
        Some(DataColumnWithField::new(data_column, data_field))
    };

    let extract_data = |data_column_field: DataColumnWithField| -> Result<i64> {
        let column = data_column_field.column();
        column.to_values()?[0].as_i64()
    };

    struct Test {
        name: &'static str,
        expr: Expression,
        expect_mono: Monotonicity,
        column: &'static str,
        left: Option<DataColumnWithField>,
        right: Option<DataColumnWithField>,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "f(x) = abs(x) where  0 <= x <= 10",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_data(0),
                right: create_data(10),
            },
            column: "x",
            left: create_data(0),
            right: create_data(10),
        },
        Test {
            name: "f(x) = abs(x) where  -10 <= x <= -2",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: create_data(10),
                right: create_data(2),
            },
            column: "x",
            left: create_data(-10),
            right: create_data(-2),
        },
        Test {
            name: "f(x) = abs(x) where -5 <= x <= 5", // should NOT be monotonic
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: false,
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
            left: create_data(-5),
            right: create_data(5),
        },
        Test {
            name: "f(x) = abs(x + 12) where -12 <= x <= 1000",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_data(0),
                right: create_data(1012),
            },
            column: "x",
            left: create_data(-12),
            right: create_data(1000),
        },
        Test {
            name: "f(x) = abs(x + 12) where -14 <=  x <= 20", // should NOT be monotonic
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![col("x"), lit(12i32)]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
            column: "x",
            left: create_data(-14),
            right: create_data(20),
        },
        Test {
            name: "f(x) = abs( (x - 7) + (x - 3) ) where 5 <= x <= 100",
            expr: Expression::create_scalar_function("abs", vec![
                Expression::create_binary_expression("+", vec![
                    Expression::create_binary_expression("-", vec![col("x"), lit(7_i32)]),
                    Expression::create_binary_expression("-", vec![col("x"), lit(3_i32)]),
                ]),
            ]),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_data(0),
                right: create_data(190),
            },
            column: "x",
            left: create_data(5),
            right: create_data(100),
        },
        Test {
            name: "f(x) = abs( (-x + 8) - x) where -100 <= x <= 4",
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
                is_constant: false,
                left: create_data(208),
                right: create_data(0),
            },
            column: "x",
            left: create_data(-100),
            right: create_data(4),
        },
    ];

    for t in tests.into_iter() {
        let (mono, column) = MonotonicityCheckVisitor::check_expression(&t.expr, t.left, t.right)?;
        assert_eq!(
            mono.is_monotonic, t.expect_mono.is_monotonic,
            "{} is_monotonic",
            t.name
        );
        if mono.is_monotonic {
            assert_eq!(
                mono.is_positive, t.expect_mono.is_positive,
                "{} is_positive",
                t.name
            );
            assert_eq!(column, t.column, "{} column", t.name);
            assert_eq!(
                extract_data(mono.left.unwrap())?,
                extract_data(t.expect_mono.left.unwrap())?,
                "{} left",
                t.name
            );
            assert_eq!(
                extract_data(mono.right.unwrap())?,
                extract_data(t.expect_mono.right.unwrap())?,
                "{} right",
                t.name
            );
        }
    }

    Ok(())
}
