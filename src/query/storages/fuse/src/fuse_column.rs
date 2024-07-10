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
use databend_common_storage::Datum;
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;

/// A column statistics provider for fuse table.
#[derive(Default)]
pub struct FuseTableColumnStatisticsProvider {
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
}

impl FuseTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, FuseColumnStatistics>,
        column_distinct_values: Option<HashMap<ColumnId, u64>>,
        row_count: u64,
    ) -> Self {
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                let ndv = column_distinct_values.as_ref().map_or(row_count, |map| {
                    map.get(&column_id).map_or(row_count, |v| *v)
                });
                let stat = BasicColumnStatistics {
                    min: Datum::from_scalar(stat.min),
                    max: Datum::from_scalar(stat.max),
                    ndv: Some(ndv),
                    null_count: stat.null_count,
                };
                (column_id, stat.get_useful_stat(row_count))
            })
            .collect();
        Self { column_stats }
    }
}

impl ColumnStatisticsProvider for FuseTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id).and_then(|s| s.as_ref())
    }

    fn num_rows(&self) -> Option<u64> {
        None
    }
}
