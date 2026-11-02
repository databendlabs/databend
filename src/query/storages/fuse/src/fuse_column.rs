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

use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_expression::ColumnId;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnTopN;

/// A column statistics provider for fuse table.
#[derive(Default)]
pub struct FuseTableColumnStatisticsProvider {
    stats_row_count: u64,
    row_count: u64,
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
    // Columns whose exact null count equals the snapshot row count.
    all_null_counts: HashMap<ColumnId, u64>,
    histograms: HashMap<ColumnId, Histogram>,
    top_n: BlockTopN,
    count_min_sketch: BlockCountMinSketch,
}

impl FuseTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, FuseColumnStatistics>,
        histograms: HashMap<ColumnId, Histogram>,
        top_n: BlockTopN,
        count_min_sketch: BlockCountMinSketch,
        column_distinct_values: Option<HashMap<ColumnId, u64>>,
        stats_row_count: u64,
        row_count: u64,
    ) -> Self {
        let distinct_map = column_distinct_values.as_ref();
        // Snapshot null counts are exact sums of block statistics. An all-NULL
        // column has no min/max and is dropped by `get_useful_stat`, so keep
        // this fact separately.
        let all_null_counts = column_stats
            .iter()
            .filter(|(_, stat)| row_count > 0 && stat.null_count == row_count)
            .map(|(column_id, stat)| (*column_id, stat.null_count))
            .collect();
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                // `get_useful_stat` fills a missing NDV from the row count and
                // marks it as inferred.
                let ndv = distinct_map
                    .and_then(|map| map.get(&column_id).cloned())
                    .or(stat.distinct_of_values);
                let stat = BasicColumnStatistics {
                    min: stat.min.to_datum(),
                    max: stat.max.to_datum(),
                    ndv,
                    null_count: stat.null_count,
                    in_memory_size: stat.in_memory_size,
                    ndv_is_inferred: false,
                };
                (column_id, stat.get_useful_stat(row_count, stats_row_count))
            })
            .collect();
        Self {
            column_stats,
            all_null_counts,
            histograms,
            top_n,
            count_min_sketch,
            stats_row_count,
            row_count,
        }
    }
}

impl ColumnStatisticsProvider for FuseTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id).and_then(|s| s.as_ref())
    }

    fn num_rows(&self) -> Option<u64> {
        Some(self.row_count)
    }

    fn stats_num_rows(&self) -> Option<u64> {
        Some(self.stats_row_count)
    }

    fn average_size(&self, column_id: ColumnId) -> Option<u64> {
        self.column_stats.get(&column_id).and_then(|v| {
            v.as_ref()
                .and_then(|s| s.in_memory_size.checked_div(self.row_count))
        })
    }

    fn histogram(&self, column_id: ColumnId) -> Option<Histogram> {
        self.histograms.get(&column_id).cloned()
    }

    fn top_n(&self, column_id: ColumnId) -> Option<ColumnTopN> {
        self.top_n.get(&column_id).cloned()
    }

    fn count_min_sketch(&self, column_id: ColumnId) -> Option<ColumnCountMinSketch> {
        self.count_min_sketch.get(&column_id).cloned()
    }

    fn all_null_count(&self, column_id: ColumnId) -> Option<u64> {
        self.all_null_counts.get(&column_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;
    use databend_common_statistics::Datum;

    use super::*;

    fn int_column_stat(distinct_of_values: Option<u64>) -> FuseColumnStatistics {
        FuseColumnStatistics::new(
            Scalar::Number(NumberScalar::Int64(1)),
            Scalar::Number(NumberScalar::Int64(1_000_000)),
            0,
            8_000,
            distinct_of_values,
        )
    }

    #[test]
    fn test_missing_ndv_is_marked_as_inferred() {
        let provider = FuseTableColumnStatisticsProvider::new(
            HashMap::from([(0, int_column_stat(None)), (1, int_column_stat(Some(500)))]),
            HashMap::new(),
            Default::default(),
            Default::default(),
            None,
            1_000,
            1_000,
        );

        let missing = provider.column_statistics(0).unwrap();
        assert_eq!(missing.min, Some(Datum::Int(1)));
        assert_eq!(missing.ndv, Some(1_000));
        assert!(missing.ndv_is_inferred);

        let collected = provider.column_statistics(1).unwrap();
        assert_eq!(collected.ndv, Some(500));
        assert!(!collected.ndv_is_inferred);
    }
}
