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

use std::collections::HashMap;

use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;
use databend_common_storages_fuse::statistics::reduce_block_statistics;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;

fn int_stats(min: i32, max: i32) -> ColumnStatistics {
    let int = |v| Scalar::Number(NumberScalar::Int32(v));
    ColumnStatistics::new(int(min), int(max), 0, 4, Some(1))
}

fn string_stats(value: &str) -> ColumnStatistics {
    let value = Scalar::String(value.to_string());
    ColumnStatistics::new(value.clone(), value, 0, 3, Some(1))
}

#[test]
fn test_reduce_block_statistics_requires_every_input() {
    let left = HashMap::from([
        (0, int_stats(1, 2)),
        (1, int_stats(3, 4)),
        (2, string_stats("abc")),
    ]);
    let right = HashMap::from([(0, int_stats(5, 6)), (2, string_stats("xyz"))]);

    let reduced = reduce_block_statistics(&[&left, &right]);
    // Column 1 has no range in `right`, whose rows may hold any value.
    assert_eq!(reduced.len(), 2);
    assert_eq!(
        reduced[&0],
        ColumnStatistics::new(
            Scalar::Number(NumberScalar::Int32(1)),
            Scalar::Number(NumberScalar::Int32(6)),
            0,
            8,
            Some(2),
        )
    );
    assert_eq!(reduced[&2].min(), &Scalar::String("abc".to_string()));
    assert_eq!(reduced[&2].max(), &Scalar::String("xyz".to_string()));

    // An input without any statistics leaves every column out.
    assert!(reduce_block_statistics(&[&left, &HashMap::new(), &right]).is_empty());
}

#[test]
fn test_merge_statistics_keeps_ranges_covering_every_row() {
    let mut summary = Statistics {
        row_count: 2,
        col_stats: HashMap::from([(0, int_stats(1, 2)), (1, string_stats("abc"))]),
        ..Default::default()
    };
    let expected = summary.col_stats.clone();

    // Merging an empty summary must not drop the ranges.
    merge_statistics_mut(&mut summary, &Statistics::default(), None);
    assert_eq!(summary.col_stats, expected);

    // Rows without a range for column 1 drop it from the merged summary.
    let appended = Statistics {
        row_count: 1,
        col_stats: HashMap::from([(0, int_stats(7, 7))]),
        ..Default::default()
    };
    merge_statistics_mut(&mut summary, &appended, None);
    assert_eq!(summary.row_count, 3);
    assert_eq!(summary.col_stats.len(), 1);
    assert_eq!(
        summary.col_stats[&0].max(),
        &Scalar::Number(NumberScalar::Int32(7))
    );
}
