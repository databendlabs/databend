// Copyright 2021 Datafuse Labs
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

use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
use databend_common_pipeline_transforms::sorts::core::SimpleRowsDesc;

/// Checks the in-place comparisons of `R` against comparing the items, for every pair of rows,
/// with each side in a column of its own.
fn assert_row_comparisons_match_items<R: Rows>(lhs: &Column, rhs: &Column) {
    let lhs = R::from_column(lhs).unwrap();
    let rhs = R::from_column(rhs).unwrap();
    for i in 0..lhs.len() {
        for j in 0..rhs.len() {
            let (a, b) = (lhs.row(i), rhs.row(j));
            assert_eq!(
                lhs.cmp_rows(i, &rhs, j),
                a.cmp(&b),
                "cmp_rows({a:?}, {b:?})"
            );
            assert_eq!(lhs.eq_rows(i, &rhs, j), a == b, "eq_rows({a:?}, {b:?})");
        }
    }
    for j in 0..rhs.len() {
        let bound = R::owned_item(rhs.row(j));
        let b = rhs.row(j);
        let expected = (0..lhs.len()).map(|i| lhs.row(i) <= b).collect::<Vec<_>>();
        let actual = lhs.rows_le_bound(&bound).iter().collect::<Vec<_>>();
        assert_eq!(actual, expected, "rows_le_bound({b:?})");
    }
    for start in 0..=lhs.len() {
        for j in 0..rhs.len() {
            let b = rhs.row(j);
            let le = (start..lhs.len()).take_while(|&i| lhs.row(i) <= b).count();
            let eq = (start..lhs.len()).take_while(|&i| lhs.row(i) == b).count();
            assert_eq!(
                lhs.leading_rows_le(start..lhs.len(), &rhs, j),
                le,
                "leading_rows_le({start}.., {b:?})"
            );
            assert_eq!(
                lhs.leading_rows_eq(start..lhs.len(), &rhs, j),
                eq,
                "leading_rows_eq({start}.., {b:?})"
            );
        }
    }
}

#[test]
fn test_string_row_comparisons() {
    // Sorted, as merged rows are, with runs of equal values, values around the 12-byte inline
    // limit, embedded `\0`, and long values sharing a prefix. The model below does not rely on
    // the order.
    let mut values = vec![
        "",
        "",
        "\0",
        "a",
        "a\0",
        "a\0b",
        "ab",
        "abcd",
        "abcd",
        "abcd\0",
        "abce",
        "abcdefghijkl",
        "abcdefghijkl\0",
        "abcdefghijklm",
        "abcdefghijklm",
        "abcdefghijkmm",
        "https://www.example.com/a",
        "https://www.example.com/ab",
        "https://www.example.com/b",
        "\u{e9}",
    ];
    values.sort();
    let asc = StringType::from_data(values.clone());
    // The same values in another column, reversed so each sits at another buffer offset.
    let reversed = StringType::from_data(values.iter().rev().copied().collect::<Vec<_>>());
    assert_row_comparisons_match_items::<SimpleRowsAsc<StringType>>(&asc, &reversed);
    assert_row_comparisons_match_items::<SimpleRowsDesc<StringType>>(&reversed, &asc);
}

#[test]
fn test_number_row_comparisons() {
    let asc = Int32Type::from_data(vec![-3, -3, 0, 1, 1, 1, 7]);
    assert_row_comparisons_match_items::<SimpleRowsAsc<Int32Type>>(&asc, &asc);
    let desc = Int32Type::from_data(vec![7, 1, 1, 1, 0, -3, -3]);
    assert_row_comparisons_match_items::<SimpleRowsDesc<Int32Type>>(&desc, &desc);
}
