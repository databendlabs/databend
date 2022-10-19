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

use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_array() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("array.txt").unwrap();

    test_create(file);
    test_length(file);
    test_get(file);
    test_slice(file);
    test_remove_first(file);
    test_remove_last(file);
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "[]", &[]);
    run_ast(file, "[NULL, 8, -10]", &[]);
    run_ast(file, "[['a', 'b'], []]", &[]);
    run_ast(file, r#"['a', 1, parse_json('{"foo":"bar"}')]"#, &[]);
    run_ast(
        file,
        r#"[parse_json('[]'), parse_json('{"foo":"bar"}')]"#,
        &[],
    );
}

fn test_length(file: &mut impl Write) {
    run_ast(file, "length([])", &[]);
    run_ast(file, "length([1, 2, 3])", &[]);
    run_ast(file, "length([true, false])", &[]);
    run_ast(file, "length(['a', 'b', 'c', 'd'])", &[]);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "get([], 1)", &[]);
    run_ast(file, "get([], NULL)", &[]);
    run_ast(file, "get([true, false], 0)", &[]);
    run_ast(file, "get(['a', 'b', 'c'], 2)", &[]);
    run_ast(file, "get([1, 2, 3], 0)", &[]);
    run_ast(file, "get([1, 2, 3], 5)", &[]);
    run_ast(file, "get([1, null, 3], 0)", &[]);
    run_ast(file, "get([1, null, 3], 1)", &[]);
}

fn test_slice(file: &mut impl Write) {
    run_ast(file, "slice([], 1, 2)", &[]);
    run_ast(file, "slice([1], 1, 2)", &[]);
    run_ast(file, "slice([NULL, 1, 2, 3], 0, 2)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 1, 2)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 0, 2)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 2, 6)", &[]);
}

fn test_remove_first(file: &mut impl Write) {
    run_ast(file, "remove_first([])", &[]);
    run_ast(file, "remove_first([1])", &[]);
    run_ast(file, "remove_first([0, 1, 2, NULL])", &[]);
    run_ast(file, "remove_first([0, 1, 2, 3])", &[]);
    run_ast(file, "remove_first(['a', 'b', 'c', 'd'])", &[]);
}

fn test_remove_last(file: &mut impl Write) {
    run_ast(file, "remove_last([])", &[]);
    run_ast(file, "remove_last([1])", &[]);
    run_ast(file, "remove_last([0, 1, 2, NULL])", &[]);
    run_ast(file, "remove_last([0, 1, 2, 3])", &[]);
    run_ast(file, "remove_last(['a', 'b', 'c', 'd'])", &[]);
}
