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

use databend_common_expression::FromData;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt16Type;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_other() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("other.txt").unwrap();

    test_run_diff(file);
    test_humanize(file);
    test_typeof(file);
    test_ignore(file);
    test_assume_not_null(file);
    test_inet_aton(file);
    test_try_inet_aton(file);
    test_inet_ntoa(file);
    test_try_inet_ntoa(file);
}

#[test]
fn test_num_to_char() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("num_to_char.txt").unwrap();

    for column in [
        Int64Type::from_data(vec![-12, 0, 34]),
        Float32Type::from_data(vec![-12.5, 0.0, 34.25]),
        Float64Type::from_data(vec![-12.5, 0.0, 34.25]),
    ] {
        let columns = &[
            ("n", column),
            ("a", Int64Type::from_data(vec![1, 0, 1])),
            (
                "fmt",
                StringType::from_data(vec!["FM999.00", "FM000", "FM999.0"]),
            ),
            (
                "bad_fmt",
                StringType::from_data(vec!["FM999.00", "9.9.9", "FM999.0"]),
            ),
            (
                "null_fmt",
                StringType::from_data_with_validity(vec!["FM999.00", "9.9.9", "FM999.0"], vec![
                    true, false, true,
                ]),
            ),
        ];
        for expr in [
            "to_char(n, 'FM999.00')",
            "to_string(n, 'FM999.00')",
            "to_char(n, fmt)",
            "to_char(n, '9.9.9')",
            "to_char(n, bad_fmt)",
            "to_char(n, null_fmt)",
            "to_char(n, NULL)",
            "if(a = 0, 'skipped', to_char(n, bad_fmt))",
            "if(a >= 0, 'skipped', to_char(n, '9.9.9'))",
            "if(a = 0, 'skipped', to_char(n, '9.9.9'))",
        ] {
            run_ast(file, expr, columns);
        }
    }

    for column in [
        Int64Type::from_data_with_validity(vec![-12, 0, 34], vec![true, false, true]),
        Float32Type::from_data_with_validity(vec![-12.5, 0.0, 34.25], vec![true, false, true]),
        Float64Type::from_data_with_validity(vec![-12.5, 0.0, 34.25], vec![true, false, true]),
    ] {
        let columns = &[("n", column), ("a", Int64Type::from_data(vec![1, 0, 1]))];
        run_ast(file, "to_char(n, 'FM999.00')", columns);
        // The only selected row is NULL: the cached parse error must not escape.
        run_ast(file, "if(a = 1, 'skipped', to_char(n, '9.9.9'))", columns);
        run_ast(file, "to_char(if(a >= 0, NULL, n), '9.9.9')", columns);
    }

    for ty in ["Int64", "Float32", "Float64"] {
        run_ast(file, format!("to_char(12::{ty}, 'FM999.00')"), &[]);
        run_ast(file, format!("to_char(12::{ty}, fmt)"), &[(
            "fmt",
            StringType::from_data(vec!["FM999.00", "FM000", "FM999.0"]),
        )]);
        run_ast(file, format!("to_char(NULL::Nullable({ty}), '9.9.9')"), &[]);
        run_ast(
            file,
            format!("if(a >= 0, 'skipped', to_char(12::{ty}, '9.9.9'))"),
            &[("a", Int64Type::from_data(vec![1, 0, 1]))],
        );
    }
}

fn test_run_diff(file: &mut impl Write) {
    run_ast(file, "running_difference(-1)", &[]);
    run_ast(file, "running_difference(0.2)", &[]);
    run_ast(file, "running_difference(to_datetime(10000))", &[]);
    run_ast(file, "running_difference(to_date(10000))", &[]);
    run_ast(file, "running_difference(a)", &[(
        "a",
        UInt16Type::from_data(vec![224u16, 384, 512]),
    )]);
    run_ast(file, "running_difference(a)", &[(
        "a",
        Float64Type::from_data(vec![37.617673, 38.617673, 39.617673]),
    )]);
}

fn test_humanize(file: &mut impl Write) {
    run_ast(file, "humanize_size(100)", &[]);
    run_ast(file, "humanize_size(1024.33)", &[]);
    run_ast(file, "humanize_number(100)", &[]);
    run_ast(file, "humanize_number(1024.33)", &[]);
}

fn test_typeof(file: &mut impl Write) {
    run_ast(file, "typeof(humanize_size(100))", &[]);
    run_ast(file, "typeof(a)", &[(
        "a",
        Float64Type::from_data(vec![37.617673, 38.617673, 39.617673]),
    )]);
}

fn test_ignore(file: &mut impl Write) {
    run_ast(file, "typeof(ignore(100))", &[]);
    run_ast(file, "ignore(100)", &[]);
    run_ast(file, "ignore(100, 'str')", &[]);
}

fn test_assume_not_null(file: &mut impl Write) {
    run_ast(file, "assume_not_null(a2)", &[(
        "a2",
        UInt8Type::from_data_with_validity(vec![1u8, 2, 3], vec![true, true, false]),
    )]);
}

fn test_inet_aton(file: &mut impl Write) {
    run_ast(file, "inet_aton('1.2.3.4')", &[]);
}

fn test_try_inet_aton(file: &mut impl Write) {
    run_ast(file, "try_inet_aton('10.0.5.9000')", &[]);
    run_ast(file, "try_inet_aton('10.0.5.9')", &[]);
}

fn test_inet_ntoa(file: &mut impl Write) {
    run_ast(file, "inet_ntoa(16909060)", &[]);
}

fn test_try_inet_ntoa(file: &mut impl Write) {
    run_ast(file, "try_inet_ntoa(121211111111111)", &[]);
}
