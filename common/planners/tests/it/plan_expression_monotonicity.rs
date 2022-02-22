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

use std::collections::HashMap;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::Monotonicity;
use common_planners::*;

struct Test {
    name: &'static str,
    expr: Expression,
    column: &'static str,
    left: Option<ColumnWithField>,
    right: Option<ColumnWithField>,
    expect_mono: Monotonicity,
}

fn create_f64(d: f64) -> Option<ColumnWithField> {
    let data_field = DataField::new("x", f64::to_data_type());
    let col = data_field
        .data_type()
        .create_constant_column(&DataValue::Float64(d), 1)
        .unwrap();
    Some(ColumnWithField::new(col, data_field))
}

fn create_u8(d: u8) -> Option<ColumnWithField> {
    let data_field = DataField::new("x", u8::to_data_type());
    let col = data_field
        .data_type()
        .create_constant_column(&DataValue::UInt64(d as u64), 1)
        .unwrap();

    Some(ColumnWithField::new(col, data_field))
}

fn create_datetime(d: u32) -> Option<ColumnWithField> {
    let data_field = DataField::new("x", DateTime32Type::arc(None));
    let col = data_field
        .data_type()
        .create_constant_column(&DataValue::UInt64(d as u64), 1)
        .unwrap();

    Some(ColumnWithField::new(col, data_field))
}

fn verify_test(t: Test) -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("x", f64::to_data_type()),
        DataField::new("y", i64::to_data_type()),
        DataField::new("z", DateTime32Type::arc(None)),
    ]);

    let mut variables = HashMap::new();
    variables.insert(t.column.to_string(), (t.left.clone(), t.right.clone()));

    let mut single_point = false;
    if t.left.is_some() && t.right.is_some() {
        let left = t.left.unwrap().column().get_checked(0)?;
        let right = t.right.unwrap().column().get_checked(0)?;
        if left == right {
            single_point = true;
        }
    }

    let mono =
        ExpressionMonotonicityVisitor::check_expression(schema, &t.expr, variables, single_point);

    assert_eq!(
        mono.is_monotonic, t.expect_mono.is_monotonic,
        "{} is_monotonic",
        t.name
    );
    assert_eq!(
        mono.is_constant, t.expect_mono.is_constant,
        "{} is_constant",
        t.name
    );

    if t.expect_mono.is_monotonic {
        assert_eq!(
            mono.is_positive, t.expect_mono.is_positive,
            "{} is_positive",
            t.name
        );
    }

    if t.expect_mono.is_monotonic || t.expect_mono.is_constant {
        let left = mono.left;
        let right = mono.right;

        let expected_left = t.expect_mono.left;
        let expected_right = t.expect_mono.right;

        if expected_left.is_none() {
            assert!(left.is_none(), "{} left", t.name);
        } else {
            let left_val = left.unwrap().column().get_checked(0)?;
            let expected_left_val = expected_left.unwrap().column().get_checked(0)?;
            assert!(left_val == expected_left_val, "{}", t.name);
        }

        if expected_right.is_none() {
            assert!(right.is_none(), "{} right", t.name);
        } else {
            let right_val = right.unwrap().column().get_checked(0)?;
            let expected_right_val = expected_right.unwrap().column().get_checked(0)?;
            assert!(right_val == expected_right_val, "{}", t.name);
        }
    }
    Ok(())
}

#[test]
fn test_arithmetic_plus_minus() -> Result<()> {
    let test_suite = vec![
        Test {
            name: "f(x) = x + 12",
            expr: add(col("x"), lit(12i32)),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            name: "f(x) = -x + 12",
            expr: add(neg(col("x")), lit(12)),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            // Cannot find the column name 'y'.
            name: "f(x,y) = x + y",
            expr: add(col("x"), col("y")),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: false,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            name: "f(x) = (-x + 12) - x + (1 - x)",
            expr: add(
                sub(add(neg(col("x")), lit(12)), col("x")),
                sub(lit(1), col("x")),
            ),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            // Function '-' is not monotonic in the variables range.
            name: "f(x) = (x + 12) - x + (1 - x)",
            expr: add(sub(add(col("x"), lit(12)), col("x")), sub(lit(1), col("x"))),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity::default(),
        },
    ];

    for t in test_suite.into_iter() {
        verify_test(t)?;
    }
    Ok(())
}

#[test]
fn test_arithmetic_mul_div() -> Result<()> {
    let test_suite = vec![
        Test {
            name: "f(x) = -5 * x",
            expr: Expression::create_binary_expression("*", vec![lit(-5_i8), col("x")]),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            name: "f(x) = -1/x",
            expr: Expression::create_binary_expression("/", vec![lit(-1_i8), col("x")]),
            column: "x",
            left: create_f64(5.0),
            right: create_f64(10.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(-0.2),
                right: create_f64(-0.1),
            },
        },
        Test {
            name: "f(x) = x/10",
            expr: Expression::create_binary_expression("/", vec![col("x"), lit(10_i8)]),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            // Function '*' is not monotonic in the variables range.
            name: "f(x) = x * (x-12) where x in [10-1000]",
            expr: Expression::create_binary_expression("*", vec![
                col("x"),
                sub(col("x"), lit(12_i64)),
            ]),
            column: "x",
            left: create_f64(10.0),
            right: create_f64(1000.0),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = x * (x-12) where x in [12, 100]",
            expr: Expression::create_binary_expression("*", vec![
                col("x"),
                sub(col("x"), lit(12_i64)),
            ]),
            column: "x",
            left: create_f64(12.0),
            right: create_f64(100.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(0.0),
                right: create_f64(8800.0),
            },
        },
        Test {
            name: "f(x) = x/(1/x) where  x >= 1",
            expr: Expression::create_binary_expression("/", vec![
                col("x"),
                Expression::create_binary_expression("/", vec![lit(1_i8), col("x")]),
            ]),
            column: "x",
            left: create_f64(1.0),
            right: create_f64(2.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(1.0),
                right: create_f64(4.0),
            },
        },
        Test {
            // Function '/' is not monotonic in the variables range.
            name: "f(x) = -x/(2/(x-2)) where  x in [0-10]",
            expr: Expression::create_binary_expression("/", vec![
                neg(col("x")),
                Expression::create_binary_expression("/", vec![
                    lit(2_i8),
                    sub(col("x"), lit(2_i8)),
                ]),
            ]),
            column: "x",
            left: create_f64(0.0),
            right: create_f64(10.0),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = -x/(2/(x-2)) where  x in [4-10]",
            expr: Expression::create_binary_expression("/", vec![
                neg(col("x")),
                Expression::create_binary_expression("/", vec![
                    lit(2_i8),
                    sub(col("x"), lit(2_i8)),
                ]),
            ]),
            column: "x",
            left: create_f64(4.0),
            right: create_f64(10.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: create_f64(-4.0),
                right: create_f64(-40.0),
            },
        },
    ];

    for t in test_suite.into_iter() {
        verify_test(t)?;
    }
    Ok(())
}

#[test]
fn test_abs_function() -> Result<()> {
    let test_suite = vec![
        Test {
            // Function 'abs' is not monotonic in the variables range.
            name: "f(x) = abs(x + 12)",
            expr: Expression::create_scalar_function("abs", vec![add(col("x"), lit(12i32))]),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = abs(x) where  0 <= x <= 10",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            column: "x",
            left: create_f64(0.0),
            right: create_f64(10.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(0.0),
                right: create_f64(10.0),
            },
        },
        Test {
            name: "f(x) = abs(x) where  -10 <= x <= -2",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            column: "x",
            left: create_f64(-10.0),
            right: create_f64(-2.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: create_f64(10.0),
                right: create_f64(2.0),
            },
        },
        Test {
            // Function 'abs' is not monotonic in the variables range.
            name: "f(x) = abs(x) where -5 <= x <= 5",
            expr: Expression::create_scalar_function("abs", vec![col("x")]),
            column: "x",
            left: create_f64(-5.0),
            right: create_f64(5.0),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = abs(x + 12) where -12 <= x <= 1000",
            expr: Expression::create_scalar_function("abs", vec![add(col("x"), lit(12i32))]),
            column: "x",
            left: create_f64(-12.0),
            right: create_f64(1000.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(0.0),
                right: create_f64(1012.0),
            },
        },
        Test {
            // Function 'abs' is not monotonic in the variables range.
            name: "f(x) = abs(x + 12) where -14 <=  x <= 20",
            expr: Expression::create_scalar_function("abs", vec![add(col("x"), lit(12i32))]),
            column: "x",
            left: create_f64(-14.0),
            right: create_f64(20.0),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = abs( (x - 7) + (x - 3) ) where 5 <= x <= 100",
            expr: Expression::create_scalar_function("abs", vec![add(
                sub(col("x"), lit(7_i32)),
                sub(col("x"), lit(3_i32)),
            )]),
            column: "x",
            left: create_f64(5.0),
            right: create_f64(100.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_f64(0.0),
                right: create_f64(190.0),
            },
        },
        Test {
            name: "f(x) = abs( (-x + 8) - x) where -100 <= x <= 4",
            expr: Expression::create_scalar_function("abs", vec![sub(
                add(neg(col("x")), lit(8)),
                col("x"),
            )]),
            column: "x",
            left: create_f64(-100.0),
            right: create_f64(4.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: false,
                is_constant: false,
                left: create_f64(208.0),
                right: create_f64(0.0),
            },
        },
    ];

    for t in test_suite.into_iter() {
        verify_test(t)?;
    }
    Ok(())
}

#[test]
fn test_dates_function() -> Result<()> {
    let test_suite = vec![
        Test {
            name: "f(x) = toStartOfWeek(z+12)",
            expr: Expression::create_scalar_function("toStartOfWeek", vec![add(
                col("z"),
                lit(12i32),
            )]),
            column: "z",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            name: "f(x) = toMonday(x)",
            expr: Expression::create_scalar_function("toMonday", vec![col("x")]),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
        Test {
            // Function 'toSecond' is not monotonic in the variables range.
            name: "f(x) = toSecond(x)",
            expr: Expression::create_scalar_function("toSecond", vec![col("x")]),
            column: "x",
            left: None,
            right: None,
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(z) = toSecond(z)",
            expr: Expression::create_scalar_function("toSecond", vec![col("z")]),
            column: "z",
            left: create_datetime(1638288000),
            right: create_datetime(1638288059),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: create_u8(0),
                right: create_u8(59),
            },
        },
        Test {
            // Function 'toDayOfYear' is not monotonic in the variables range.
            name: "f(z) = toDayOfYear(z)",
            expr: Expression::create_scalar_function("toDayOfYear", vec![col("z")]),
            column: "z",
            left: create_datetime(1606752119),
            right: create_datetime(1638288059),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(z) = toStartOfHour(z)",
            expr: Expression::create_scalar_function("toStartOfHour", vec![col("z")]),
            column: "z",
            left: None,
            right: None,
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: false,
                left: None,
                right: None,
            },
        },
    ];

    for t in test_suite.into_iter() {
        verify_test(t)?;
    }
    Ok(())
}

#[test]
fn test_single_point() -> Result<()> {
    let test_suite = vec![
        Test {
            // Function 'rand' is not monotonic in the variables range.
            name: "f(x) = x + rand()",
            expr: add(col("x"), Expression::create_scalar_function("rand", vec![])),
            column: "x",
            left: create_f64(1.0),
            right: create_f64(1.0),
            expect_mono: Monotonicity::default(),
        },
        Test {
            name: "f(x) = x * (12 - x)",
            expr: Expression::create_binary_expression("*", vec![
                col("x"),
                sub(lit(12_i64), col("x")),
            ]),
            column: "x",
            left: create_f64(1.0),
            right: create_f64(1.0),
            expect_mono: Monotonicity {
                is_monotonic: true,
                is_positive: true,
                is_constant: true,
                left: create_f64(11.0),
                right: create_f64(11.0),
            },
        },
    ];

    for t in test_suite.into_iter() {
        verify_test(t)?;
    }
    Ok(())
}
