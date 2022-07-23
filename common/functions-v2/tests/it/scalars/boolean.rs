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

use std::io::Write;

use common_expression::types::DataType;
use common_expression::Column;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_boolean() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("boolean.txt").unwrap();

    test_and(file);
    test_not(file);
    test_or(file);
    test_xor(file);
}

fn test_and(file: &mut impl Write) {
    run_ast(file, "true AND false", &[]);
    run_ast(file, "null AND false", &[]);
}

fn test_not(file: &mut impl Write) {
    run_ast(file, "NOT a", &[("a", DataType::Null, Column::Null {
        len: 5,
    })]);
    run_ast(file, "NOT a", &[(
        "a",
        DataType::Boolean,
        Column::Boolean(vec![true, false, true].into()),
    )]);
    run_ast(file, "NOT a", &[(
        "a",
        DataType::Nullable(Box::new(DataType::Boolean)),
        Column::Nullable {
            column: Box::new(Column::Boolean(vec![true, false, true].into())),
            validity: vec![false, true, false].into(),
        },
    )]);
    run_ast(file, "NOT a", &[(
        "a",
        DataType::Nullable(Box::new(DataType::Boolean)),
        Column::Nullable {
            column: Box::new(Column::Boolean(vec![false, false, false].into())),
            validity: vec![true, true, false].into(),
        },
    )]);
}

fn test_or(file: &mut impl Write) {
    run_ast(file, "true OR false", &[]);
    run_ast(file, "null OR false", &[]);
}

fn test_xor(file: &mut impl Write) {
    run_ast(file, "true XOR false", &[]);
    run_ast(file, "null XOR false", &[]);
}
