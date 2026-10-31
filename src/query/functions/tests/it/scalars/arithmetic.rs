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
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::ScalarRef;
use databend_common_expression::type_check;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::*;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;

use super::TestContext;
use super::run_ast;
use super::run_ast_with_context;

#[test]
fn test_arithmetic() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("arithmetic.txt").unwrap();

    let columns = &[
        ("a", Int8Type::from_data(vec![1i8, 2, 3])),
        (
            "a2",
            UInt8Type::from_data_with_validity(vec![1u8, 2, 3], vec![true, true, false]),
        ),
        ("b", Int16Type::from_data(vec![2i16, 4, 6])),
        ("c", UInt32Type::from_data(vec![10u32, 20, 30])),
        ("d", Float64Type::from_data(vec![10f64, -20f64, 30f64])),
        (
            "d2",
            UInt8Type::from_data_with_validity(vec![1u8, 0, 3], vec![true, false, true]),
        ),
        (
            "e",
            Column::Decimal(DecimalColumn::Decimal64(
                vec![31, 335, 1888].into(),
                DecimalSize::new_unchecked(10, 1),
            )),
        ),
        (
            "f",
            Column::Decimal(DecimalColumn::Decimal256(
                vec![i256::from(50), i256::from(92), i256::from(1234)].into(),
                DecimalSize::new_unchecked(76, 2),
            )),
        ),
        ("g", Int64Type::from_data(vec![i64::MAX, i64::MIN, 0])),
    ];
    test_add(file, columns);
    test_minus(file, columns);
    test_unary_minus(file, columns);
    test_mul(file, columns);
    test_div(file, columns);
    test_intdiv(file, columns);
    test_modulo(file, columns);
    test_to_string(file, columns);
    test_carte(file, columns);
    test_square(file, columns);
    test_cube(file, columns);
    test_abs(file, columns);
    test_factorial(file, columns);
    test_bitwise_xor(file, columns);
    test_bitwise_and(file, columns);
    test_bitwise_or(file, columns);
    test_bitwise_not(file, columns);
    test_bitwise_shift_left(file, columns);
    test_bitwise_shift_right(file, columns);
}

fn test_add(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a + b", columns);
    run_ast(file, "a2 + 10", columns);
    run_ast(file, "a2 + c", columns);
    run_ast(file, "c + 0.5", columns);
    run_ast(file, "c + b", columns);
    run_ast(file, "c + d", columns);
    run_ast(file, "c + e", columns);
    run_ast(file, "d + e", columns);
    run_ast(file, "d2 + e", columns);
    run_ast(file, "d2 + f", columns);
    run_ast(file, "e + f", columns);
}

fn test_minus(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a - b", columns);
    run_ast(file, "a2 - 10", columns);
    run_ast(file, "a2 - c", columns);
    run_ast(file, "c - 0.5", columns);
    run_ast(file, "c - b", columns);
    run_ast(file, "c - d", columns);
    run_ast(file, "c - e", columns);
    run_ast(file, "d - e", columns);
    run_ast(file, "d2 - e", columns);
    run_ast(file, "d2 - f", columns);
    run_ast(file, "e - f", columns);
}

fn test_unary_minus(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "-a", columns);
    run_ast(file, "-a2", columns);
    run_ast(file, "-b", columns);
    run_ast(file, "-c", columns);
    run_ast(file, "-d", columns);
    run_ast(file, "-d2", columns);
    run_ast(file, "-e", columns);
    run_ast(file, "-f", columns);
    run_ast(file, "-g", columns);
}

fn test_mul(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a  * b", columns);
    run_ast(file, "a2 * 10", columns);
    run_ast(file, "a2 * c", columns);
    run_ast(file, "c * 0.5", columns);
    run_ast(file, "c * b", columns);
    run_ast(file, "c * d", columns);
    run_ast(file, "c * e", columns);
    run_ast(file, "d * e", columns);
    run_ast(file, "d2 * e", columns);
    run_ast(file, "d2 * f", columns);
    run_ast(file, "e * f", columns);
    run_ast(file, "e * e", columns);
    run_ast(file, "f * f", columns);
    run_ast(file, "e * 0.5", columns);
}

fn test_div(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a / b", columns);
    run_ast(file, "a2 / 10", columns);
    run_ast(file, "a2 / c", columns);
    run_ast(file, "c / 0.5", columns);
    run_ast(file, "divide(c, b)", columns);
    run_ast(file, "c / d", columns);
    run_ast(file, "b / d2", columns);
    run_ast(file, "2.0 / 0", columns);
    run_ast(file, "c / e", columns);
    run_ast(file, "d / e", columns);
    run_ast(file, "d2 / e", columns);
    run_ast(file, "d2 / f", columns);
    run_ast(file, "e / f", columns);
}

fn test_intdiv(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a  div b", columns);
    run_ast(file, "a2 div 10", columns);
    run_ast(file, "a2 div c", columns);
    run_ast(file, "c div 0.5", columns);
    run_ast(file, "c div b", columns);
    run_ast(file, "c div d", columns);
    run_ast(file, "c div d2", columns);
    run_ast(file, "c div 0", columns);
}

fn test_modulo(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "(a + 3)  % b", columns);
    run_ast(file, "a2 % 4", columns);
    run_ast(file, "(a2 + 4) % c", columns);
    run_ast(file, "c % (b + 3)", columns);
    run_ast(file, "c % (d - 3)", columns);
    run_ast(file, "c % 0", columns);
    run_ast(file, "c % d2", columns);
    run_ast_with_context(file, "lhs_i8 % rhs_i8", TestContext {
        entries: &[
            ("lhs_i8", Int8Type::from_data(vec![-127i8, -2, -1]).into()),
            ("rhs_i8", Int8Type::from_data(vec![i8::MIN, -2, -1]).into()),
        ],
        input_domains: Some(&[
            (
                "lhs_i8",
                Domain::Number(NumberDomain::Int8(SimpleDomain {
                    min: i8::MIN + 1,
                    max: -1,
                })),
            ),
            (
                "rhs_i8",
                Domain::Number(NumberDomain::Int8(SimpleDomain {
                    min: i8::MIN,
                    max: -1,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });
    run_ast_with_context(file, "lhs_i64 % rhs_i64", TestContext {
        entries: &[
            (
                "lhs_i64",
                Int64Type::from_data(vec![i64::MIN + 1, -2, -1]).into(),
            ),
            (
                "rhs_i64",
                Int64Type::from_data(vec![i64::MIN, -2, -1]).into(),
            ),
        ],
        input_domains: Some(&[
            (
                "lhs_i64",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: i64::MIN + 1,
                    max: -1,
                })),
            ),
            (
                "rhs_i64",
                Domain::Number(NumberDomain::Int64(SimpleDomain {
                    min: i64::MIN,
                    max: -1,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });
    run_ast_with_context(file, "lhs_u8_wrap % rhs_i8_wrap", TestContext {
        entries: &[
            ("lhs_u8_wrap", UInt8Type::from_data(vec![1u8, 2, 3]).into()),
            (
                "rhs_i8_wrap",
                Int8Type::from_data(vec![-1i8, -1, -1]).into(),
            ),
        ],
        input_domains: Some(&[
            (
                "lhs_u8_wrap",
                Domain::Number(NumberDomain::UInt8(SimpleDomain { min: 127, max: 128 })),
            ),
            (
                "rhs_i8_wrap",
                Domain::Number(NumberDomain::Int8(SimpleDomain { min: -1, max: -1 })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });
    run_ast(file, "lhs_u8_min_as_i8 % rhs_i8_minus_one", &[
        ("lhs_u8_min_as_i8", UInt8Type::from_data(vec![128u8])),
        ("rhs_i8_minus_one", Int8Type::from_data(vec![-1i8])),
    ]);
    run_ast_with_context(file, "lhs_i8_wrap % rhs_u8_wrap", TestContext {
        entries: &[
            (
                "lhs_i8_wrap",
                Int8Type::from_data(vec![-1i8, -2, -3]).into(),
            ),
            ("rhs_u8_wrap", UInt8Type::from_data(vec![1u8, 1, 1]).into()),
        ],
        input_domains: Some(&[
            (
                "lhs_i8_wrap",
                Domain::Number(NumberDomain::Int8(SimpleDomain {
                    min: i8::MIN,
                    max: i8::MIN + 1,
                })),
            ),
            (
                "rhs_u8_wrap",
                Domain::Number(NumberDomain::UInt8(SimpleDomain {
                    min: u8::MAX - 1,
                    max: u8::MAX,
                })),
            ),
        ]),
        func_ctx: FunctionContext::default(),
        strict_eval: true,
    });
    run_ast(file, "lhs_i64_min % rhs_i64_minus_one", &[
        ("lhs_i64_min", Int64Type::from_data(vec![i64::MIN])),
        ("rhs_i64_minus_one", Int64Type::from_data(vec![-1i64])),
    ]);
    run_ast(file, "wide_lhs % narrow_rhs", &[
        ("wide_lhs", UInt16Type::from_data(vec![256u16, 511, 512])),
        ("narrow_rhs", UInt8Type::from_data(vec![255u8, 255, 255])),
    ]);
    run_ast(file, "small_lhs % wide_rhs", &[
        ("small_lhs", Int64Type::from_data(vec![-2i64, 0, 2])),
        ("wide_rhs", Int64Type::from_data(vec![100i64, 101, 102])),
    ]);
}

fn test_to_string(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "to_string(a)", columns);
    run_ast(file, "to_string(a2)", columns);
    run_ast(file, "to_string(b)", columns);
    run_ast(file, "to_string(c)", columns);
    run_ast(file, "to_string(d)", columns);
    run_ast(file, "to_string(d2)", columns);
    run_ast(file, "to_string(e)", columns);
    run_ast(file, "to_string(f)", columns);
}

fn test_carte(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a ^ 2", columns);
    run_ast(file, "a ^ a", columns);
    run_ast(file, "a ^ a2", columns);
    run_ast(file, "c ^ 0", columns);
    run_ast(file, "c ^ d", columns);
}

fn test_square(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "|/a", columns);
    run_ast(file, "|/a2", columns);
    run_ast(file, "|/b", columns);
    run_ast(file, "|/c", columns);
    run_ast(file, "|/d", columns);
    run_ast(file, "|/d2", columns);
}

fn test_cube(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "||/a", columns);
    run_ast(file, "||/a2", columns);
    run_ast(file, "||/b", columns);
    run_ast(file, "||/c", columns);
    run_ast(file, "||/d", columns);
    run_ast(file, "||/d2", columns);
}

fn test_factorial(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a!", columns);
    run_ast(file, "b!", columns);
    run_ast(file, "12!", columns);
    run_ast(file, "30!", columns);
}

fn test_abs(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "@ a", columns);
    run_ast(file, "@ a2", columns);
    run_ast(file, "@ b", columns);
    run_ast(file, "@ c", columns);
    run_ast(file, "@ d", columns);
    run_ast(file, "@ d2", columns);
}

fn test_bitwise_and(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a & b", columns);
    run_ast(file, "a2 & 10", columns);
    run_ast(file, "a2 & c", columns);
    run_ast(file, "c & b", columns);
}

fn test_bitwise_or(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a | b", columns);
    run_ast(file, "a2 | 10", columns);
    run_ast(file, "a2 | c", columns);
    run_ast(file, "c | b", columns);
}

fn test_bitwise_xor(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a # b", columns);
    run_ast(file, "a2 # 10", columns);
    run_ast(file, "a2 # c", columns);
    run_ast(file, "c # b", columns);
}

fn test_bitwise_not(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "~a", columns);
    run_ast(file, "~a2", columns);
    run_ast(file, "~b", columns);
    run_ast(file, "~c", columns);
}

fn test_bitwise_shift_left(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a << 4", columns);
    run_ast(file, "a2 << 2", columns);
    run_ast(file, "a2 << 4", columns);
    run_ast(file, "c << 2", columns);
}

fn test_bitwise_shift_right(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a >> 1", columns);
    run_ast(file, "a2 >> 1", columns);
    run_ast(file, "a2 >> 2", columns);
    run_ast(file, "c >> 2", columns);
}

#[test]
fn test_decimal_multiply() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint
        .new_goldenfile("arithmetic_decimal_multiply.txt")
        .unwrap();
    let magnitude = 99_999_999_999_999_999;
    let columns = [
        (
            "a",
            Decimal64Type::from_data_with_size(
                [magnitude, -magnitude, magnitude, -magnitude],
                Some(DecimalSize::new_unchecked(17, 17)),
            ),
        ),
        (
            "b",
            Decimal64Type::from_data_with_size(
                [magnitude, magnitude, -magnitude, -magnitude],
                Some(DecimalSize::new_unchecked(17, 17)),
            ),
        ),
    ];
    run_ast(file, "a * b", &columns);
    run_ast(file, "a * '0.99999999999999999'::DECIMAL(17,17)", &columns);
    run_ast(file, "'0.99999999999999999'::DECIMAL(17,17) * b", &columns);
    run_ast(
        file,
        "'0.99999999999999999'::DECIMAL(17,17) * '0.99999999999999999'::DECIMAL(17,17)",
        &[],
    );
    // Output precision below 38 does not make an unscaled i128 product safe.
    run_ast(
        file,
        "'0.9999999999999999999999999999999999999'::DECIMAL(37,37) * '0.9999999999999999999999999999999999999'::DECIMAL(37,37)",
        &[],
    );
    run_ast(
        file,
        "'-0.9999999999999999999999999999999999999'::DECIMAL(37,37) * '0.9999999999999999999999999999999999999'::DECIMAL(37,37)",
        &[],
    );
    run_ast(file, "7::DECIMAL(8,0) * 3::DECIMAL(8,0)", &[]);
    run_ast(file, "7::DECIMAL(19,0) * 3::DECIMAL(19,0)", &[]);
    run_ast(file, "7::DECIMAL(39,0) * 3::DECIMAL(39,0)", &[]);
    run_ast(
        file,
        "'99999999999999999999999999999999999999'::DECIMAL(38,0) * '99999999999999999999999999999999999999'::DECIMAL(38,0)",
        &[],
    );
}

#[test]
fn test_decimal_similar_overflows() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint
        .new_goldenfile("arithmetic_decimal_overflows.txt")
        .unwrap();
    let nines = "9".repeat(75);
    for expression in [
        // Unscaled Decimal256 product exceeds its native width, final result fits.
        format!("'0.{nines}'::DECIMAL(75,75) * '0.{nines}'::DECIMAL(75,75)"),
        // A large divisor must not be narrowed to the small quotient's type.
        "1::DECIMAL(1,0) / '18446744073709551616'::DECIMAL(20,0)".to_owned(),
        "-1::DECIMAL(1,0) / '-18446744073709551616'::DECIMAL(20,0)".to_owned(),
        // Scaling fits, but the old add-half rounding intermediate overflowed.
        format!("'5.5'::DECIMAL(76,75) / '{nines}.9'::DECIMAL(76,1)"),
        // Alignment can exceed a decimal's precision before valid cancellation.
        "1::DECIMAL(38,0) - '0.99999999999999999999999999999999999999'::DECIMAL(38,38)".to_owned(),
        format!("1::DECIMAL(76,0) - '0.{}'::DECIMAL(76,76)", "9".repeat(76)),
        format!("1::DECIMAL(76,0) + '-0.{}'::DECIMAL(76,76)", "9".repeat(76)),
        // Remainders fit even if aligned operands need more than 76 digits.
        "'0.1'::DECIMAL(76,76) % 1::DECIMAL(76,0)".to_owned(),
        format!("1::DECIMAL(76,0) % '0.{}'::DECIMAL(76,76)", "9".repeat(76)),
        // Native arithmetic overflow and decimal precision overflow are errors.
        "'99999999999999999999999999999999999999'::DECIMAL(38,0) + '99999999999999999999999999999999999999'::DECIMAL(38,0)".to_owned(),
        "'-99999999999999999999999999999999999999'::DECIMAL(38,0) - '99999999999999999999999999999999999999'::DECIMAL(38,0)".to_owned(),
        "'99999999999999999999999999999999999999'::DECIMAL(38,0) / '0.1'::DECIMAL(1,1)".to_owned(),
        "1::DECIMAL(38,0) / 0::DECIMAL(38,0)".to_owned(),
    ] {
        run_ast(file, expression, &[]);
    }
    // Domain calculation must rescale the product too; all rows round to zero.
    let columns = [
        (
            "a",
            Decimal64Type::from_data_with_size([1, 2], Some(DecimalSize::new_unchecked(13, 13))),
        ),
        (
            "b",
            Decimal64Type::from_data_with_size([1, 2], Some(DecimalSize::new_unchecked(13, 13))),
        ),
    ];
    for expression in ["a * b", "(a * b) = 0", "a / b", "a % b", "a + b", "a - b"] {
        run_ast(file, expression, &columns);
    }
    // A failed domain must not fold `is_not_null` to true and drop the error.
    let nines = "9".repeat(76);
    run_ast(
        file,
        format!("is_not_null(a * '{nines}'::DECIMAL(76,0))"),
        &columns,
    );
    let nullable =
        |column: Column| column.wrap_nullable(Some([false, true, true].into_iter().collect()));
    let columns = [
        (
            "a",
            nullable(Decimal64Type::from_data_with_size(
                [0, 1, -1],
                Some(DecimalSize::new_unchecked(13, 13)),
            )),
        ),
        (
            "b",
            nullable(Decimal64Type::from_data_with_size(
                [0, 1, 2],
                Some(DecimalSize::new_unchecked(13, 13)),
            )),
        ),
    ];
    for expression in [
        "a * b",
        "a / b",
        "a % b",
        "a + b",
        "a - b",
        "if(false, a / (b - b), a)",
    ] {
        run_ast(file, expression, &columns);
    }
    // A masked payload need not fit its decimal precision. The narrow divisor
    // path must not evaluate MIN / -1 in i64 on this inactive row.
    let columns = [(
        "masked",
        nullable(Decimal64Type::from_data_with_size(
            [i64::MIN, 1, -1],
            Some(DecimalSize::new_unchecked(13, 13)),
        )),
    )];
    run_ast(file, "masked / '-1'::DECIMAL(38,0)", &columns);
    // A native prefix is not sufficient: one large value must prevent a
    // whole-batch narrowing of the divisor column.
    let columns = [
        (
            "numerator",
            Decimal64Type::from_data_with_size([1, 1, 1], Some(DecimalSize::new_unchecked(1, 0))),
        ),
        (
            "divisor",
            Decimal256Type::from_data_with_size(
                [i256::MINUS_ONE, i256::from_words(1, 0), i256::ONE],
                Some(DecimalSize::new_unchecked(76, 0)),
            ),
        ),
    ];
    run_ast(file, "numerator / divisor", &columns);
    // A scalar error belongs to every visible row, even when row 0 is hidden;
    // a fully hidden batch reports none.
    let columns = [("active", BooleanType::from_data(vec![false, true, false]))];
    run_ast(
        file,
        "if(active, 1::DECIMAL(38,0) / 0::DECIMAL(38,0), NULL)",
        &columns,
    );
    let columns = [("active", BooleanType::from_data(vec![false, false, false]))];
    run_ast(
        file,
        "if(active, 1::DECIMAL(38,0) / 0::DECIMAL(38,0), NULL)",
        &columns,
    );
    // An all-NULL operand domain stays NULL through domain calculation.
    let a = Decimal64Type::from_data_with_size([0, 0], Some(DecimalSize::new_unchecked(4, 0)))
        .wrap_nullable(Some([false, false].into_iter().collect()));
    let b = Decimal256Type::from_data_with_size(
        [i256::ONE, i256::from(2)],
        Some(DecimalSize::new_unchecked(76, 0)),
    );
    for expression in ["a / b", "a % b"] {
        run_ast_with_context(file, expression, TestContext {
            entries: &[("a", a.clone().into()), ("b", b.clone().into())],
            input_domains: Some(&[
                (
                    "a",
                    Domain::Nullable(NullableDomain {
                        has_null: true,
                        value: None,
                    }),
                ),
                ("b", b.domain()),
            ]),
            ..TestContext::default()
        });
    }
}

// Rows hidden by validity may hold payloads beyond the precision, here i128
// extremes the golden formatter cannot print. Both ways validity reaches a
// decimal kernel, a NULL row and an IF branch, must keep such rows from
// raising errors or disturbing visible rows.
#[test]
fn test_decimal_hidden_payloads() {
    let size = DecimalSize::new_unchecked(20, 0);
    let a = Decimal128Type::from_data_with_size([i128::MIN, 4, i128::MAX], Some(size));
    let b = Decimal128Type::from_data_with_size([i128::MAX, 2, -1], Some(size));
    let visible = [false, true, false];
    let nullable = |column: Column| column.wrap_nullable(Some(visible.into_iter().collect()));
    for (operation, expected) in [
        ("a + b", 6),
        ("a - b", 2),
        ("a * b", 8),
        ("a / b", 2),
        ("a % b", 0),
        ("round(-1)(a)", 0),
    ] {
        for (text, columns) in [
            (operation.to_owned(), vec![
                ("a", nullable(a.clone())),
                ("b", nullable(b.clone())),
            ]),
            (format!("if(active, {operation}, NULL)"), vec![
                ("a", a.clone()),
                ("b", b.clone()),
                ("active", BooleanType::from_data(visible.to_vec())),
            ]),
        ] {
            let types = columns
                .iter()
                .map(|(name, column)| (*name, column.data_type()))
                .collect::<Vec<_>>();
            let raw = super::parser::parse_raw_expr(&text, &types, &BUILTIN_FUNCTIONS);
            let expr = type_check::check(&raw, &BUILTIN_FUNCTIONS).unwrap();
            let block = DataBlock::new_from_columns(columns.into_iter().map(|(_, c)| c).collect());
            let context = FunctionContext::default();
            let result = Evaluator::new(&block, &context, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .unwrap_or_else(|error| panic!("{text}: {error}"));
            let output = *expr.data_type().remove_nullable().as_decimal().unwrap();
            let expected = i256::from(expected) * i256::e(output.scale());
            match result.index(1).unwrap() {
                ScalarRef::Decimal(row) => assert_eq!(row.as_decimal::<i256>(), expected, "{text}"),
                other => panic!("{text}: {other:?}"),
            }
            for hidden in [0, 2] {
                assert!(result.index(hidden).unwrap().is_null(), "{text}");
            }
        }
    }
}

#[test]
fn test_decimal() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("arithmetic_decimal.txt").unwrap();

    let columns = [
        (
            "l_extendedprice",
            Decimal64Type::from_data_with_size(
                [
                    7029081, 3988040, 2696083, 8576832, 3179020, 1941184, 6911822, 4874825,
                    4207270, 4425444,
                ],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
        (
            "l_discount",
            Decimal64Type::from_data_with_size(
                [8, 8, 0, 9, 7, 7, 7, 9, 9, 2],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
        (
            "ps_supplycost",
            Decimal64Type::from_data_with_size(
                [
                    32776, 34561, 52627, 45035, 65850, 11521, 34965, 79552, 42880, 48615,
                ],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
        (
            "l_quantity",
            Decimal64Type::from_data_with_size(
                [4300, 4000, 2300, 4800, 2000, 1600, 4600, 2500, 3700, 3600],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
    ];

    run_ast(
        file,
        "l_extendedprice + (1 - l_discount) - l_quantity",
        &columns,
    );

    run_ast(file, "1964831797.0000 - 0.0214642400000", &columns);

    // `%` on decimals is exact at the larger scale, keeps the dividend's sign,
    // and does not round through Float64 (5.5 % 0.1 was 0.0999999999999997).
    run_ast(file, "5.5 % 0.1", &columns);
    run_ast(file, "0.25 % 0.1", &columns);
    run_ast(file, "1.234 % 0.5", &columns);
    run_ast(file, "-7.5 % 2", &columns);
    run_ast(file, "7.5 % -2", &columns);
    run_ast(file, "7 % 2.5", &columns);
    run_ast(file, "0.001::decimal(38, 19) % 0.1", &columns);
    run_ast(file, "0.1::decimal(38, 38) % 1", &columns);
    run_ast(file, "1 % 0.3::decimal(38, 38)", &columns);
    run_ast(
        file,
        "1234567890123456789012345678901234567890::decimal(76, 0) % 7",
        &columns,
    );
    run_ast(file, "l_quantity % 7", &columns);
    run_ast(file, "l_extendedprice % 0.07", &columns);
    run_ast(file, "l_quantity % 0", &columns);
}
