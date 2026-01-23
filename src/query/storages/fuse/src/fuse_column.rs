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
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;

/// A column statistics provider for fuse table.
#[derive(Default)]
pub struct FuseTableColumnStatisticsProvider {
    stats_row_count: u64,
    row_count: u64,
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
    histograms: HashMap<ColumnId, Histogram>,
}

impl FuseTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, FuseColumnStatistics>,
        histograms: HashMap<ColumnId, Histogram>,
        column_distinct_values: Option<HashMap<ColumnId, u64>>,
        stats_row_count: u64,
        row_count: u64,
    ) -> Self {
        let distinct_map = column_distinct_values.as_ref();
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                let ndv = distinct_map
                    .and_then(|map| map.get(&column_id).cloned())
                    .or(stat.distinct_of_values)
                    .unwrap_or(row_count);
                let stat = BasicColumnStatistics {
                    min: stat.min.to_datum(),
                    max: stat.max.to_datum(),
                    ndv: Some(ndv),
                    null_count: stat.null_count,
                    in_memory_size: stat.in_memory_size,
                };
                (column_id, stat.get_useful_stat(row_count, stats_row_count))
            })
            .collect();
        Self {
            column_stats,
            histograms,
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
}
