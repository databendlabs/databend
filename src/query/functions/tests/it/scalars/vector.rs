// Copyright 2023 Datafuse Labs.
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
fn test_vector() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("vector.txt").unwrap();

    test_vector_cosine_distance(file);
}

fn test_vector_cosine_distance(file: &mut impl Write) {
    run_ast(file, "cosine_distance([a], [b])", &[
        ("a", Float32Type::from_data(vec![0f32, 1.0, 2.0])),
        ("b", Float32Type::from_data(vec![3f32, 4.0, 5.0])),
    ]);
}
