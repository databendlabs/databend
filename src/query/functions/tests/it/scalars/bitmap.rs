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
fn test_bitmap() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("bitmap.txt").unwrap();

    test_build_bitmap(file);
    test_to_bitmap(file);
    test_bitmap_contains(file);
    test_bitmap_count(file);
    test_bitmap_has_all(file);
    test_bitmap_has_any(file);
    test_bitmap_max(file);
    test_bitmap_min(file);
    test_sub_bitmap(file);
    test_bitmap_subset_limit(file);
    test_bitmap_subset_in_range(file);
    test_bitmap_op(file);
}

fn test_build_bitmap(file: &mut impl Write) {
    run_ast(file, "build_bitmap([NULL, 8])", &[]);
    run_ast(file, "build_bitmap([7, 8])", &[]);
    run_ast(file, "build_bitmap([7, -8])", &[]);
    run_ast(file, "build_bitmap([a, b])", &[
        ("a", UInt16Type::from_data(vec![1u16, 2, 3])),
        ("b", UInt16Type::from_data(vec![1u16, 2, 3])),
    ]);
}

fn test_to_bitmap(file: &mut impl Write) {
    run_ast(file, "to_bitmap('0, 1, 2')", &[]);
    run_ast(file, "to_bitmap(1024)", &[]);
}

fn test_bitmap_contains(file: &mut impl Write) {
    run_ast(file, "bitmap_contains(build_bitmap([1,4,5]), 1)", &[]);
}

fn test_bitmap_count(file: &mut impl Write) {
    run_ast(file, "bitmap_count(build_bitmap([1,2,5]))", &[]);
}

fn test_bitmap_has_all(file: &mut impl Write) {
    run_ast(
        file,
        "bitmap_has_all(build_bitmap([1,4,5]), build_bitmap([1]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_has_all(build_bitmap([1,4,5]), build_bitmap([1,2]))",
        &[],
    );
}

fn test_bitmap_has_any(file: &mut impl Write) {
    run_ast(
        file,
        "bitmap_has_any(build_bitmap([1,4,5]), build_bitmap([1,2]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_has_any(build_bitmap([1,4,5]), build_bitmap([2,3]))",
        &[],
    );
}

fn test_bitmap_max(file: &mut impl Write) {
    run_ast(file, "bitmap_max(build_bitmap([1,4,5]))", &[]);
}

fn test_bitmap_min(file: &mut impl Write) {
    run_ast(file, "bitmap_min(build_bitmap([1,4,5]))", &[]);
}

fn test_sub_bitmap(file: &mut impl Write) {
    run_ast(file, "sub_bitmap(build_bitmap([1, 2, 3, 4, 5]), 1, 3)", &[]);
}

fn test_bitmap_subset_limit(file: &mut impl Write) {
    run_ast(file, "bitmap_subset_limit(build_bitmap([3,5,7]), 4, 2)", &[
    ]);
}

fn test_bitmap_subset_in_range(file: &mut impl Write) {
    run_ast(
        file,
        "bitmap_subset_in_range(build_bitmap([5,7,9]), 6, 9)",
        &[],
    );
}

fn test_bitmap_op(file: &mut impl Write) {
    run_ast(
        file,
        "bitmap_or(build_bitmap([1,4,5]), build_bitmap([1,5]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_and(build_bitmap([1,3,5]), build_bitmap([2,4,6]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_xor(build_bitmap([1,3,5]), build_bitmap([2,4,6]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_not(build_bitmap([1,3,5]), build_bitmap([1,5]))",
        &[],
    );
    run_ast(
        file,
        "bitmap_and_not(build_bitmap([1,3,5]), build_bitmap([1,5]))",
        &[],
    );
}
