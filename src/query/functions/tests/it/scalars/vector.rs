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

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_vector() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("vector.txt").unwrap();

    test_vector_cosine_distance(file);
    test_vector_l1_distance(file);
    test_vector_l2_distance(file);
    test_vector_inner_product(file);
    test_vector_vector_dims(file);
    test_vector_vector_norm(file);
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
    run_ast(
        file,
        "cosine_distance([a, b]::vector(2), [c, d]::vector(2))",
        &[
            ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
            ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
            ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
            ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
        ],
    );
    run_ast(file, "cosine_distance([a, b], [c, d])", &[
        ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
        ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
        ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
        ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
    ]);
    run_ast(file, "cosine_distance([a, b], [c, d])", &[
        (
            "a",
            Float32Type::from_data_with_validity(vec![1.0f32, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float32Type::from_data_with_validity(vec![2.0f32, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float32Type::from_data_with_validity(vec![3.0f32, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float32Type::from_data_with_validity(vec![4.0f32, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
    run_ast(file, "cosine_distance([a, b], [c, d])", &[
        ("a", Float64Type::from_data(vec![1.0f64, 5.1, 9.4])),
        ("b", Float64Type::from_data(vec![2.0f64, 6.2, 10.6])),
        ("c", Float64Type::from_data(vec![3.0f64, 7.3, 11.1])),
        ("d", Float64Type::from_data(vec![4.0f64, 8.4, 12.3])),
    ]);
    run_ast(file, "cosine_distance([a, b], [c, d])", &[
        (
            "a",
            Float64Type::from_data_with_validity(vec![1.0f64, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float64Type::from_data_with_validity(vec![2.0f64, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float64Type::from_data_with_validity(vec![3.0f64, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float64Type::from_data_with_validity(vec![4.0f64, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
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
    run_ast(
        file,
        "l1_distance([a, b]::vector(2), [c, d]::vector(2))",
        &[
            ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
            ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
            ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
            ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
        ],
    );
    run_ast(file, "l1_distance([a, b], [c, d])", &[
        ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
        ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
        ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
        ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
    ]);
    run_ast(file, "l1_distance([a, b], [c, d])", &[
        (
            "a",
            Float32Type::from_data_with_validity(vec![1.0f32, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float32Type::from_data_with_validity(vec![2.0f32, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float32Type::from_data_with_validity(vec![3.0f32, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float32Type::from_data_with_validity(vec![4.0f32, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
    run_ast(file, "l1_distance([a, b], [c, d])", &[
        ("a", Float64Type::from_data(vec![1.0f64, 5.1, 9.4])),
        ("b", Float64Type::from_data(vec![2.0f64, 6.2, 10.6])),
        ("c", Float64Type::from_data(vec![3.0f64, 7.3, 11.1])),
        ("d", Float64Type::from_data(vec![4.0f64, 8.4, 12.3])),
    ]);
    run_ast(file, "l1_distance([a, b], [c, d])", &[
        (
            "a",
            Float64Type::from_data_with_validity(vec![1.0f64, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float64Type::from_data_with_validity(vec![2.0f64, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float64Type::from_data_with_validity(vec![3.0f64, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float64Type::from_data_with_validity(vec![4.0f64, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
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
    run_ast(
        file,
        "l2_distance([a, b]::vector(2), [c, d]::vector(2))",
        &[
            ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
            ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
            ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
            ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
        ],
    );
    run_ast(file, "l2_distance([a, b], [c, d])", &[
        ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
        ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
        ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
        ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
    ]);
    run_ast(file, "l2_distance([a, b], [c, d])", &[
        (
            "a",
            Float32Type::from_data_with_validity(vec![1.0f32, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float32Type::from_data_with_validity(vec![2.0f32, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float32Type::from_data_with_validity(vec![3.0f32, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float32Type::from_data_with_validity(vec![4.0f32, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
    run_ast(file, "l2_distance([a, b], [c, d])", &[
        ("a", Float64Type::from_data(vec![1.0f64, 5.1, 9.4])),
        ("b", Float64Type::from_data(vec![2.0f64, 6.2, 10.6])),
        ("c", Float64Type::from_data(vec![3.0f64, 7.3, 11.1])),
        ("d", Float64Type::from_data(vec![4.0f64, 8.4, 12.3])),
    ]);
    run_ast(file, "l2_distance([a, b], [c, d])", &[
        (
            "a",
            Float64Type::from_data_with_validity(vec![1.0f64, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float64Type::from_data_with_validity(vec![2.0f64, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float64Type::from_data_with_validity(vec![3.0f64, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float64Type::from_data_with_validity(vec![4.0f64, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
}

fn test_vector_inner_product(file: &mut impl Write) {
    run_ast(file, "inner_product([1,0,0], [1,0,0])", &[]);
    run_ast(file, "inner_product([1,0,0], [-1,0,0])", &[]);
    run_ast(file, "inner_product([1,2,3], [4,5,6])", &[]);
    run_ast(file, "inner_product([0,0,0], [1,2,3])", &[]);
    run_ast(
        file,
        "inner_product([1,-2,3]::vector(3), [-4,5,-6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "inner_product([0.1,0.2,0.3]::vector(3), [0.4,0.5,0.6]::vector(3))",
        &[],
    );
    run_ast(
        file,
        "inner_product([1,0]::vector(2), [0,1]::vector(2))",
        &[],
    );
    run_ast(
        file,
        "inner_product([a, b]::vector(2), [c, d]::vector(2))",
        &[
            ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
            ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
            ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
            ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
        ],
    );
    run_ast(file, "inner_product([a, b], [c, d])", &[
        ("a", Float32Type::from_data(vec![1.0f32, 5.1, 9.4])),
        ("b", Float32Type::from_data(vec![2.0f32, 6.2, 10.6])),
        ("c", Float32Type::from_data(vec![3.0f32, 7.3, 11.1])),
        ("d", Float32Type::from_data(vec![4.0f32, 8.4, 12.3])),
    ]);
    run_ast(file, "inner_product([a, b], [c, d])", &[
        (
            "a",
            Float32Type::from_data_with_validity(vec![1.0f32, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float32Type::from_data_with_validity(vec![2.0f32, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float32Type::from_data_with_validity(vec![3.0f32, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float32Type::from_data_with_validity(vec![4.0f32, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
    run_ast(file, "inner_product([a, b], [c, d])", &[
        ("a", Float64Type::from_data(vec![1.0f64, 5.1, 9.4])),
        ("b", Float64Type::from_data(vec![2.0f64, 6.2, 10.6])),
        ("c", Float64Type::from_data(vec![3.0f64, 7.3, 11.1])),
        ("d", Float64Type::from_data(vec![4.0f64, 8.4, 12.3])),
    ]);
    run_ast(file, "inner_product([a, b], [c, d])", &[
        (
            "a",
            Float64Type::from_data_with_validity(vec![1.0f64, 5.1, 9.4], vec![true, true, true]),
        ),
        (
            "b",
            Float64Type::from_data_with_validity(vec![2.0f64, 6.2, 10.6], vec![true, true, true]),
        ),
        (
            "c",
            Float64Type::from_data_with_validity(vec![3.0f64, 7.3, 11.1], vec![true, true, true]),
        ),
        (
            "d",
            Float64Type::from_data_with_validity(vec![4.0f64, 8.4, 12.3], vec![true, true, true]),
        ),
    ]);
}

fn test_vector_vector_dims(file: &mut impl Write) {
    run_ast(file, "vector_dims([1,-2]::vector(2))", &[]);
    run_ast(file, "vector_dims([0.1,0.2,0.3]::vector(3))", &[]);
    run_ast(file, "vector_dims([a, b, c]::vector(3))", &[
        ("a", Float32Type::from_data(vec![1.0f32, 4.0, 7.0])),
        ("b", Float32Type::from_data(vec![2.0f32, 5.0, 8.0])),
        ("c", Float32Type::from_data(vec![3.0f32, 6.0, 9.0])),
    ]);
}

fn test_vector_vector_norm(file: &mut impl Write) {
    run_ast(file, "vector_norm([1,-2]::vector(2))", &[]);
    run_ast(file, "vector_norm([0.1,0.2,0.3]::vector(3))", &[]);
    run_ast(file, "vector_norm([a, b, c]::vector(3))", &[
        ("a", Float32Type::from_data(vec![1.0f32, 4.0, 7.0])),
        ("b", Float32Type::from_data(vec![2.0f32, 5.0, 8.0])),
        ("c", Float32Type::from_data(vec![3.0f32, 6.0, 9.0])),
    ]);
}
