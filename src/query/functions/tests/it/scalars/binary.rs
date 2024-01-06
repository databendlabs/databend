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

use databend_common_expression::types::BinaryType;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_binary() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("binary.txt").unwrap();

    test_length(file);
}

fn test_length(file: &mut impl Write) {
    // todo!("new string")
    // run_ast(file, "length(to_binary('latin'))", &[]);
    // run_ast(file, "length(to_binary(NULL))", &[]);
    run_ast(file, "length(a)", &[(
        "a",
        BinaryType::from_data(vec![b"latin", "кириллица".as_bytes(), &[
            0xDE, 0xAD, 0xBE, 0xEF,
        ]]),
    )]);
}
