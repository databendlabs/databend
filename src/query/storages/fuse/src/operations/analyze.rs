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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use tracing::warn;

use crate::io::SegmentsIO;
use crate::FuseTable;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_analyze(&self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        // 1. Read table snapshot.
        let r = self.read_table_snapshot().await;
        let snapshot_opt = match r {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                warn!(
                    "concurrent statistic: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc().await?,
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        if let Some(snapshot) = snapshot_opt {
            // 2. Iterator segments and blocks to estimate statistics.
            let mut sum_map = HashMap::new();
            let mut row_count_sum = 0;
            let mut block_count_sum: u64 = 0;

            let segments_io = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
            let segments = segments_io.read_segments(&snapshot.segments, true).await?;
            for segment in segments {
                let segment = segment?;
                segment.blocks.iter().for_each(|block| {
                    let block = block.as_ref();
                    let row_count = block.row_count;
                    if row_count != 0 {
                        block_count_sum += 1;
                        row_count_sum += row_count;
                        for (i, col_stat) in block.col_stats.iter() {
                            let density = match col_stat.distinct_of_values {
                                Some(ndv) => ndv as f64 / row_count as f64,
                                None => 0.0,
                            };

                            match sum_map.get_mut(i) {
                                Some(sum) => {
                                    *sum += density;
                                }
                                None => {
                                    let _ = sum_map.insert(*i, density);
                                }
                            }
                        }
                    }
                });
            }
            let mut ndv_map = HashMap::new();
            for (i, sum) in sum_map.iter() {
                let density_avg = *sum / block_count_sum as f64;
                ndv_map.insert(*i, (density_avg * row_count_sum as f64) as u64);
            }

            // 3. Generate new table statistics
            let table_statistics = TableSnapshotStatistics::new(ndv_map);
            let table_statistics_location = self
                .meta_location_generator
                .snapshot_statistics_location_from_uuid(
                    &table_statistics.snapshot_id,
                    table_statistics.format_version(),
                )?;

            // 4. Save table statistics
            let mut new_snapshot = TableSnapshot::from_previous(&snapshot);
            new_snapshot.table_statistics_location = Some(table_statistics_location);
            FuseTable::commit_to_meta_server(
                ctx.as_ref(),
                &self.table_info,
                &self.meta_location_generator,
                new_snapshot,
                Some(table_statistics),
                &None,
                &self.operator,
            )
            .await?;
        }

        Ok(())
    }
}
