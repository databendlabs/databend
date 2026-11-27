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

use databend_common_expression::Scalar;
use databend_common_storages_fuse::statistics::reduce_block_statistics;
use databend_common_storages_fuse::statistics::reducers::merge_statistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

fn int(value: i32) -> Scalar {
    Scalar::Number(value.into())
}

fn string(value: &str) -> Scalar {
    Scalar::String(value.to_string())
}

fn stats(min: Scalar, max: Scalar, null_count: u64) -> ColumnStatistics {
    ColumnStatistics::new(min, max, null_count, 4, Some(1))
}

fn bounds(stats: &ColumnStatistics) -> (&Scalar, &Scalar) {
    (stats.min(), stats.max())
}

fn summary(row_count: u64, col_stats: StatisticsOfColumns) -> Statistics {
    Statistics {
        row_count,
        block_count: 1,
        col_stats,
        ..Default::default()
    }
}

/// A string whose upper bound could not be stored has a null upper bound. It
/// is larger than any bound the other blocks have.
#[test]
fn test_reduce_keeps_an_unknown_upper_bound() {
    let open = StatisticsOfColumns::from([(0, stats(string("a"), Scalar::Null, 0))]);
    let closed = StatisticsOfColumns::from([(0, stats(string("b"), string("c"), 2))]);

    for inputs in [[&open, &closed], [&closed, &open]] {
        let reduced = reduce_block_statistics(&inputs);
        assert_eq!(bounds(&reduced[&0]), (&string("a"), &Scalar::Null));
        assert_eq!(reduced[&0].null_count, 2);
    }
}

/// Rows without a non-null value add to the null count and leave the range
/// alone.
#[test]
fn test_reduce_rows_without_values_leave_the_range_alone() {
    let nulls = StatisticsOfColumns::from([(0, stats(Scalar::Null, Scalar::Null, 3))]);
    let values = StatisticsOfColumns::from([(0, stats(int(7), int(9), 0))]);

    let reduced = reduce_block_statistics(&[&nulls, &values]);
    assert_eq!(bounds(&reduced[&0]), (&int(7), &int(9)));
    assert_eq!(reduced[&0].null_count, 3);

    let reduced = reduce_block_statistics(&[&nulls, &nulls]);
    assert_eq!(bounds(&reduced[&0]), (&Scalar::Null, &Scalar::Null));
    assert_eq!(reduced[&0].null_count, 6);
}

/// The statistics of the elements of an Array count elements. Three segments
/// of one row each hold `[]`, `[NULL, NULL, 5]` and `[7, 8, 9]`. Merging their
/// summaries one after the other, as segment compaction does, must keep the 5
/// in the range of the elements.
#[test]
fn test_merge_statistics_keeps_the_range_of_array_elements() {
    let segment = |min, max, null_count| {
        summary(
            1,
            StatisticsOfColumns::from([(0, stats(min, max, null_count))]),
        )
    };
    let empty = segment(Scalar::Null, Scalar::Null, 0);
    let nulls_and_five = segment(int(5), int(5), 2);
    let seven_to_nine = segment(int(7), int(9), 0);

    let merged = merge_statistics(empty, &nulls_and_five, None);
    let merged = merge_statistics(merged, &seven_to_nine, None);

    assert_eq!(merged.row_count, 3);
    assert_eq!(bounds(&merged.col_stats[&0]), (&int(5), &int(9)));
    assert_eq!(merged.col_stats[&0].null_count, 2);
}
