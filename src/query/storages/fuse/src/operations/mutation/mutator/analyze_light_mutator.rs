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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use opendal::Operator;

use crate::io::SegmentsIO;
use crate::operations::acquire_task_permit;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::FuseTable;

pub struct AnalyzeLightMutator {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    base_snapshot: Arc<TableSnapshot>,
    table_meta_timestamps: TableMetaTimestamps,
    distinct_columns: HashSet<ColumnId>,
    cluster_key_id: Option<u32>,

    col_stats: HashMap<ColumnId, ColumnStatistics>,
    cluster_stats: Option<ClusterStatistics>,
}

impl AnalyzeLightMutator {
    pub fn try_new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        base_snapshot: Arc<TableSnapshot>,
        cluster_key_id: Option<u32>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Option<Self> {
        let mut distinct_columns = HashSet::new();
        // Only track columns whose NDV is not yet computed.
        for (col_id, stats) in &base_snapshot.summary.col_stats {
            if stats.distinct_of_values.is_none() {
                distinct_columns.insert(*col_id);
            }
        }
        if distinct_columns.is_empty() {
            return None;
        }
        Some(AnalyzeLightMutator {
            ctx,
            operator,
            base_snapshot,
            table_meta_timestamps,
            distinct_columns,
            cluster_key_id,
            col_stats: HashMap::new(),
            cluster_stats: None,
        })
    }

    #[async_backtrace::framed]
    pub async fn target_select(&mut self) -> Result<()> {
        let segment_locations = &self.base_snapshot.segments;
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.operator.clone(),
            Arc::new(self.base_snapshot.schema.clone()),
        );

        let chunk_size = max_threads * 4;
        let max_concurrency = std::cmp::max(max_threads * 2, 10);
        let runtime = GlobalIORuntime::instance();
        let semaphore = Arc::new(Semaphore::new(max_concurrency));

        let mut result_ndvs: HashMap<ColumnId, u64> =
            self.distinct_columns.iter().map(|&k| (k, 0)).collect();
        let mut stats_of_columns = Vec::with_capacity(segment_locations.len());
        let mut cluster_stats_list = Vec::with_capacity(segment_locations.len());
        for chunk in segment_locations.chunks(chunk_size) {
            let segment_infos = segments_io
                .read_segments::<Arc<CompactSegmentInfo>>(chunk, false)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            let mut handlers = Vec::with_capacity(segment_infos.len());
            for segment_info in segment_infos {
                stats_of_columns.push(segment_info.summary.col_stats.clone());
                cluster_stats_list.push(segment_info.summary.cluster_stats.clone());
                // Determine which columns still need NDV computation in this segment.
                let columns_to_process = self
                    .distinct_columns
                    .iter()
                    .filter(|&col_id| {
                        segment_info
                            .summary
                            .col_stats
                            .get(col_id)
                            .is_some_and(|stats| stats.distinct_of_values.is_none())
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                // If all needed columns already have segment-level NDV stats, use them directly.
                if columns_to_process.is_empty() {
                    for (col_id, stats) in &segment_info.summary.col_stats {
                        if let Some(ndv) = stats.distinct_of_values {
                            if let Some(acc) = result_ndvs.get_mut(col_id) {
                                *acc += ndv;
                            }
                        }
                    }
                    continue;
                }

                // Otherwise, fall back to computing from block-level stats.
                let permit = acquire_task_permit(semaphore.clone()).await?;
                let handler = runtime.spawn(async move {
                    let mut ndvs: HashMap<ColumnId, u64> = columns_to_process
                        .iter()
                        .map(|&col_id| (col_id, 0))
                        .collect();

                    let blocks = segment_info.block_metas()?;
                    for block in blocks {
                        for col_id in &columns_to_process {
                            if let Some(col_stats) = block.col_stats.get(col_id) {
                                if let Some(block_ndv) = col_stats.distinct_of_values {
                                    *ndvs.entry(*col_id).or_insert(0) += block_ndv;
                                }
                            }
                        }
                    }

                    drop(permit);
                    Ok::<_, ErrorCode>(ndvs)
                });
                handlers.push(handler);
            }

            if !handlers.is_empty() {
                let joint = futures::future::try_join_all(handlers).await.map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "[ANALYZE-TABLE] Failed to process block statistics: {}",
                        e
                    ))
                })?;

                for ndvs in joint {
                    let ndvs = ndvs?;
                    for (col_id, ndv) in ndvs {
                        *result_ndvs.get_mut(&col_id).unwrap() += ndv;
                    }
                }
            }
        }

        // Generate new column statistics for snapshot
        self.col_stats = reduce_block_statistics(&stats_of_columns);
        for (col_id, ndv) in result_ndvs {
            if let Some(stats) = self.col_stats.get_mut(&col_id) {
                stats.distinct_of_values = Some(ndv);
            }
        }
        self.cluster_stats = reduce_cluster_statistics(&cluster_stats_list, self.cluster_key_id);
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_commit(&self, table: &FuseTable) -> Result<()> {
        let mut statistics = self.base_snapshot.summary.clone();
        statistics.col_stats = self.col_stats.clone();
        statistics.cluster_stats = self.cluster_stats.clone();
        table
            .commit_mutation(
                &self.ctx,
                self.base_snapshot.clone(),
                &self.base_snapshot.segments,
                statistics,
                self.table_meta_timestamps,
                None,
            )
            .await
    }
}
