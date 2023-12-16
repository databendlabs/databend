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

use databend_common_expression::types::Float64Type;
use databend_common_expression::types::UInt16Type;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_other() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("other.txt").unwrap();

    test_run_diff(file);
    test_humanize(file);
    test_typeof(file);
    test_sleep(file);
    test_ignore(file);
    test_assume_not_null(file);
    test_inet_aton(file);
    test_try_inet_aton(file);
    test_inet_ntoa(file);
    test_try_inet_ntoa(file);
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

fn test_sleep(file: &mut impl Write) {
    run_ast(file, "sleep(2)", &[]);
    run_ast(file, "sleep(300.2)", &[]);
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
