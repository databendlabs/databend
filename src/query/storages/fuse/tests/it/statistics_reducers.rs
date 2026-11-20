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
use databend_common_storages_fuse::statistics::reduce_block_statistics;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;

fn string_stats(value: &str) -> ColumnStatistics {
    let value = Scalar::String(value.to_string());
    ColumnStatistics::new(value.clone(), value, 0, 0, Some(1))
}

#[test]
fn test_reduce_block_statistics_requires_every_input() {
    let left = HashMap::from([(0, string_stats("a")), (1, string_stats("left"))]);
    let right = HashMap::from([(0, string_stats("z")), (2, string_stats("right"))]);
    let result = reduce_block_statistics(&[&left, &right]);

    assert_eq!(result.len(), 1);
    assert_eq!(result[&0].min(), &Scalar::String("a".to_string()));
    assert_eq!(result[&0].max(), &Scalar::String("z".to_string()));
    assert!(reduce_block_statistics(&[&left, &HashMap::new(), &right]).is_empty());
}

#[test]
fn test_merge_statistics_distinguishes_empty_rows_from_missing_stats() {
    let mut left = Statistics {
        row_count: 1,
        col_stats: HashMap::from([(0, string_stats("abc")), (1, string_stats("xyz"))]),
        ..Default::default()
    };
    let mut right = Statistics::default();
    let expected = left.col_stats.clone();
    merge_statistics_mut(&mut left, &right, None);
    assert_eq!(left.col_stats, expected);

    right.row_count = 1;
    right.col_stats.insert(1, string_stats("xyz"));
    merge_statistics_mut(&mut left, &right, None);
    assert_eq!(left.row_count, 2);
    assert_eq!(left.col_stats.len(), 1);
    assert_eq!(left.col_stats[&1].distinct_of_values, Some(2));
}
