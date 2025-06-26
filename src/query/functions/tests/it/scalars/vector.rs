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

use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_vector() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("vector.txt").unwrap();

    test_vector_cosine_distance(file);
    test_vector_l1_distance(file);
    test_vector_l2_distance(file);
}

fn test_vector_cosine_distance(file: &mut impl Write) {
    run_ast(file, "cosine_distance([1,0,0], [1,0,0])", &[]);
    run_ast(file, "cosine_distance([1,0,0], [-1,0,0])", &[]);
    run_ast(file, "cosine_distance([1,2,3], [4,5,6])", &[]);
    run_ast(file, "cosine_distance([0,0,0], [1,2,3])", &[]);
    run_ast(
        file,
        "cosine_distance([1,-2,3]::vector(3), [-4,5,-6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "cosine_distance([0.1,0.2,0.3]::vector(3), [0.4,0.5,0.6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "cosine_distance([1,0]::vector(2), [0,1]::vector(2))",
        &[],
    );
}

fn test_vector_l1_distance(file: &mut impl Write) {
    run_ast(file, "l1_distance([1,2,3], [1,2,3])", &[]);
    run_ast(file, "l1_distance([1,2,3], [4,5,6])", &[]);
    run_ast(file, "l1_distance([0,0,0], [1,2,3])", &[]);
    run_ast(
        file,
        "l1_distance([1,-2,3]::vector(3), [-4,5,-6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "l1_distance([0.1,0.2,0.3]::vector(3), [0.4,0.5,0.6]::vector(3))",
        &[],
    );
    run_ast(file, "l1_distance([1,2]::vector(2), [3,4]::vector(2))", &[]);
}

fn test_vector_l2_distance(file: &mut impl Write) {
    run_ast(file, "l2_distance([1,2,3], [1,2,3])", &[]);
    run_ast(file, "l2_distance([1,2,3], [4,5,6])", &[]);
    run_ast(file, "l2_distance([0,0,0], [1,2,3])", &[]);
    run_ast(
        file,
        "l2_distance([1,-2,3]::vector(3), [-4,5,-6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "l2_distance([0.1,0.2,0.3]::vector(3), [0.4,0.5,0.6]::vector(3))",
        &[],
    );
    run_ast(file, "l2_distance([1,2]::vector(2), [3,4]::vector(2))", &[]);
}
