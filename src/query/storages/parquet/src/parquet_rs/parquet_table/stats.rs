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
use std::sync::Arc;

use databend_common_catalog::plan::FullParquetMeta;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ParquetTableColumnStatisticsProvider;

pub fn create_stats_provider(
    metas: &[Arc<FullParquetMeta>],
    num_columns: usize,
) -> ParquetTableColumnStatisticsProvider {
    let mut num_rows = 0;
    let mut basic_column_stats = vec![BasicColumnStatistics::new_null(); num_columns];

    for meta in metas {
        num_rows += meta
            .meta
            .row_groups()
            .iter()
            .map(|r| r.num_rows() as u64)
            .sum::<u64>();
        if let Some(stats) = &meta.row_group_level_stats {
            for rg_stat in stats {
                for (column_id, col_stat) in rg_stat {
                    let column_id = *column_id as usize;
                    let col_stat = col_stat.clone().into();
                    basic_column_stats[column_id].merge(col_stat);
                }
            }
        }
    }

    let mut column_stats = HashMap::with_capacity(basic_column_stats.len());
    for (column_id, col_stat) in basic_column_stats.into_iter().enumerate() {
        column_stats.insert(column_id as u32, col_stat.get_useful_stat(num_rows));
    }

    ParquetTableColumnStatisticsProvider::new(column_stats, num_rows)
}
