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
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_math() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("math.txt").unwrap();

    test_abs(file);
    test_sign(file);
    test_trigonometric(file);
    test_ceil(file);
    test_exp(file);
    test_round(file);
    test_sqrt(file);
    test_cbrt(file);
    test_truncate(file);
    test_log_function(file);
    test_factorial(file);
}

fn test_abs(file: &mut impl Write) {
    run_ast(file, "abs(1)", &[]);
    run_ast(file, "abs(-1)", &[]);
    run_ast(file, "abs(null)", &[]);
    run_ast(file, "abs(a)", &[(
        "a",
        Int64Type::from_data(vec![1i64, -30, 1024]),
    )]);
}

fn test_sign(file: &mut impl Write) {
    run_ast(file, "sign(1)", &[]);
    run_ast(file, "sign(-1)", &[]);
    run_ast(file, "sign(null)", &[]);
    run_ast(file, "sign(a)", &[(
        "a",
        Int64Type::from_data(vec![1i64, -30, 1024]),
    )]);
}

fn test_trigonometric(file: &mut impl Write) {
    run_ast(file, "sin(1)", &[]);
    run_ast(file, "cos(1)", &[]);
    run_ast(file, "tan(1)", &[]);
    run_ast(file, "atan(0.5)", &[]);
    run_ast(file, "cot(-1.0)", &[]);
    run_ast(file, "asin(1)", &[]);
    run_ast(file, "acos(0)", &[]);
    run_ast(file, "atan(null)", &[]);
    run_ast(file, "atan2(a, 4)", &[(
        "a",
        Int64Type::from_data(vec![1i64, -1, 1024]),
    )]);
}

fn test_ceil(file: &mut impl Write) {
    run_ast(file, "ceil(5)", &[]);
    run_ast(file, "ceil(5.6)", &[]);
    run_ast(file, "ceil(a)", &[(
        "a",
        Float64Type::from_data(vec![1.23f64, -1.23]),
    )]);
}

fn test_exp(file: &mut impl Write) {
    run_ast(file, "exp(2)", &[]);
    run_ast(file, "exp(-2)", &[]);
    run_ast(file, "exp(0)", &[]);
    run_ast(file, "exp(a)", &[(
        "a",
        Int64Type::from_data(vec![4i64, -2, 10]),
    )]);
}

fn test_round(file: &mut impl Write) {
    run_ast(file, "round(0)(-1.23)", &[]);
    run_ast(file, "round(1)(1.298, 1)", &[]);
    run_ast(file, "round(0)(1.298, 0)", &[]);

    // Currently we don't support negative param
    // run_ast(file, "round(-1)(23.298, -1)", &[]);
    run_ast(
        file,
        "round(35)(0.12345678901234567890123456789012345, 35)",
        &[],
    );
    run_ast(file, "round(a)", &[(
        "a",
        Float64Type::from_data(vec![22.22f64, -22.23, 10.0]),
    )]);
}

fn test_sqrt(file: &mut impl Write) {
    run_ast(file, "sqrt(4)", &[]);
    run_ast(file, "sqrt(a)", &[(
        "a",
        Int64Type::from_data(vec![22i64, 1024, 10]),
    )]);
}

fn test_cbrt(file: &mut impl Write) {
    run_ast(file, "cbrt(8)", &[]);
    run_ast(file, "cbrt(a)", &[(
        "a",
        Int64Type::from_data(vec![22i64, 1024, 10]),
    )]);
}

fn test_factorial(file: &mut impl Write) {
    run_ast(file, "factorial(5)", &[]);
    run_ast(file, "factorial(30)", &[]);
    run_ast(file, "factorial(a)", &[(
        "a",
        Int64Type::from_data(vec![3, 12, 16]),
    )]);
}

fn test_truncate(file: &mut impl Write) {
    run_ast(file, "truncate(1)(1.223, 1)", &[]);
    run_ast(file, "truncate(0)(1.999)", &[]);
    run_ast(file, "truncate(1)(1.999, 1)", &[]);
    run_ast(file, "truncate(-2)(122::DECIMAL(3))", &[]);
    run_ast(file, "truncate(1+3-6)(122::DECIMAL(3))", &[]);
    run_ast(file, "truncate(122, -1)", &[]);
    run_ast(file, "truncate(0)(10.28*100, 0)", &[]);
    run_ast(file, "truncate(a, 1)", &[(
        "a",
        Float64Type::from_data(vec![22.22f64, -22.23, 10.0]),
    )]);
}

fn test_log_function(file: &mut impl Write) {
    run_ast(file, "log(2)", &[]);
    run_ast(file, "log(2, 65536)", &[]);
    run_ast(file, "log2(65536)", &[]);
    run_ast(file, "log10(100)", &[]);
    run_ast(file, "ln(2)", &[]);
    run_ast(file, "round(2, a)", &[(
        "a",
        Int64Type::from_data(vec![22i64, 65536, 10]),
    )]);
}
