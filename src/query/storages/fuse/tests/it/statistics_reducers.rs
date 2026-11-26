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
use databend_common_storages_fuse::statistics::reducers::deduct_statistics;
use databend_common_storages_fuse::statistics::reducers::merge_statistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

fn int(value: i32) -> Scalar {
    Scalar::Number(value.into())
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

/// A block written before `ALTER TABLE .. ADD COLUMN` has no statistics for
/// the new column. Its rows read as the column's default, which the range of
/// the blocks written afterwards says nothing about.
#[test]
fn test_reduce_leaves_out_a_column_unknown_in_an_input() {
    let before = StatisticsOfColumns::from([(0, stats(int(1), int(1), 0))]);
    let after = StatisticsOfColumns::from([
        (0, stats(int(2), int(2), 0)),
        (1, stats(int(42), int(42), 0)),
    ]);

    for inputs in [[(1, &before), (1, &after)], [(1, &after), (1, &before)]] {
        let reduced = reduce_block_statistics(&inputs);
        assert_eq!(reduced.len(), 1);
        assert_eq!(bounds(&reduced[&0]), (&int(1), &int(2)));
    }
}

#[test]
fn test_reduce_ignores_inputs_without_rows() {
    let values = StatisticsOfColumns::from([(0, stats(int(3), int(5), 1))]);
    let empty = StatisticsOfColumns::new();
    let stale = StatisticsOfColumns::from([(0, stats(int(100), int(200), 0))]);

    for no_rows in [&empty, &stale] {
        let reduced = reduce_block_statistics(&[(0, no_rows), (7, &values)]);
        assert_eq!(reduced, values);
    }
    assert!(reduce_block_statistics(&[(0, &stale)]).is_empty());
    assert!(reduce_block_statistics::<StatisticsOfColumns>(&[]).is_empty());
}

/// Rows that are all null, such as those of a block whose column defaults to
/// NULL, hold no value: they add to the null count and leave the range alone.
#[test]
fn test_reduce_all_null_rows_have_no_values() {
    let nulls = StatisticsOfColumns::from([(0, stats(Scalar::Null, Scalar::Null, 3))]);
    let values = StatisticsOfColumns::from([(0, stats(int(7), int(9), 0))]);

    let reduced = reduce_block_statistics(&[(3, &nulls), (2, &values)]);
    assert_eq!(bounds(&reduced[&0]), (&int(7), &int(9)));
    assert_eq!(reduced[&0].null_count, 3);

    let reduced = reduce_block_statistics(&[(3, &nulls), (3, &nulls)]);
    assert_eq!(bounds(&reduced[&0]), (&Scalar::Null, &Scalar::Null));
    assert_eq!(reduced[&0].null_count, 6);
}

#[test]
fn test_merge_statistics_leaves_out_a_column_unknown_on_either_side() {
    let before = summary(
        1,
        StatisticsOfColumns::from([(0, stats(int(1), int(1), 0))]),
    );
    let after = summary(
        1,
        StatisticsOfColumns::from([
            (0, stats(int(2), int(2), 0)),
            (1, stats(int(42), int(42), 0)),
        ]),
    );

    for (l, r) in [(&before, &after), (&after, &before)] {
        let merged = merge_statistics(l.clone(), r, None);
        assert_eq!(merged.row_count, 2);
        assert_eq!(merged.col_stats.len(), 1);
        assert_eq!(bounds(&merged.col_stats[&0]), (&int(1), &int(2)));
    }

    // A summary without rows takes the other side as it is.
    let merged = merge_statistics(Statistics::default(), &after, None);
    assert_eq!(merged.col_stats, after.col_stats);
    let merged = merge_statistics(after.clone(), &Statistics::default(), None);
    assert_eq!(merged.col_stats, after.col_stats);
}

#[test]
fn test_deduct_statistics_leaves_out_a_column_unknown_in_the_removed_rows() {
    let all = summary(
        3,
        StatisticsOfColumns::from([
            (0, stats(int(1), int(9), 2)),
            (1, stats(Scalar::Null, Scalar::Null, 3)),
        ]),
    );
    let removed = summary(
        1,
        StatisticsOfColumns::from([(0, stats(int(1), int(1), 1))]),
    );

    let left = deduct_statistics(&all, &removed);
    assert_eq!(left.row_count, 2);
    // How many of the nulls of column 1 went away with the removed row is not
    // known, so neither is what remains.
    assert_eq!(left.col_stats.len(), 1);
    assert_eq!(left.col_stats[&0].null_count, 1);

    let left = deduct_statistics(&all, &Statistics::default());
    assert_eq!(left.col_stats, all.col_stats);
}
