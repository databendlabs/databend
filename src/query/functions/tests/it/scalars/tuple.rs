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

use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_tuple() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("tuple.txt").unwrap();

    test_create(file);
    test_get(file);
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "(NULL,)", &[]);
    run_ast(file, "(NULL, NULL)", &[]);
    run_ast(file, "(1, 2, 'a')", &[]);
    run_ast(file, "(1, 2, ('a', 'b'))", &[]);
    run_ast(file, "(s, s)", &[(
        "s",
        StringType::from_data_with_validity(vec!["a", "b", "c", "d"], vec![
            true, true, false, true,
        ]),
    )]);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "get((NULL,))", &[]);
    run_ast(file, "(NULL,).0", &[]);
    run_ast(file, "(NULL,).1", &[]);
    run_ast(file, "(NULL,).2", &[]);
    run_ast(file, "(1, 'a').1", &[]);
    run_ast(file, "(1, 'a').2", &[]);
    run_ast(file, "(1, 2, ('a', 'b')).3", &[]);
    run_ast(file, "(1, 2, ('a', 'b')).3.1", &[]);
    run_ast(file, "(s, s).1", &[(
        "s",
        StringType::from_data_with_validity(vec!["a", "b", "c", "d"], vec![
            true, true, false, true,
        ]),
    )]);
    run_ast(file, "col.1", &[(
        "col",
        NullableColumn::new_column(
            Column::Tuple(vec![StringType::from_data_with_validity(
                vec!["a", "b", "c", "d"],
                vec![true, true, false, false],
            )]),
            vec![true, false, true, false].into(),
        ),
    )]);
}
