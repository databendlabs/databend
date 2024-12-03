// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;
use databend_common_storages_fuse::DEFAULT_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_enterprise_hilbert_clustering::HilbertClusteringHandler;
use databend_enterprise_hilbert_clustering::HilbertClusteringHandlerWrapper;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;

pub struct RealHilbertClusteringHandler {}

#[async_trait::async_trait]
impl HilbertClusteringHandler for RealHilbertClusteringHandler {
    #[async_backtrace::framed]
    async fn do_hilbert_clustering(
        &self,
        table: Arc<dyn Table>,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<(ReclusterInfoSideCar, Arc<TableSnapshot>)>> {
        let Some((cluster_key_id, _)) = table.cluster_key_meta() else {
            return Ok(None);
        };

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let block_per_seg =
            fuse_table.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let block_thresholds = fuse_table.get_block_thresholds();
        let thresholds = BlockThresholds {
            max_rows_per_block: block_per_seg * block_thresholds.max_rows_per_block,
            min_rows_per_block: block_per_seg * block_thresholds.min_rows_per_block,
            max_bytes_per_block: block_per_seg * block_thresholds.max_bytes_per_block,
        };
        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let chunk_size = max_threads * 4;
        let mut target_segments = vec![];
        let mut total_rows = 0;
        let mut total_size = 0;
        let mut stable = false;
        'FOR: for chunk in segment_locations.chunks(chunk_size) {
            // read segments.
            let compact_segments = FuseTable::segment_pruning(
                &ctx,
                fuse_table.schema_with_stream(),
                fuse_table.get_operator(),
                &push_downs,
                fuse_table.get_storage_format(),
                chunk.to_vec(),
            )
            .await?;

            if compact_segments.is_empty() {
                continue;
            }

            for (location, segment) in compact_segments.into_iter() {
                total_rows += segment.summary.row_count as usize;
                total_size += segment.summary.uncompressed_byte_size as usize;
                if !thresholds.check_large_enough(total_rows, total_size) {
                    // totals < N
                    target_segments.push((location, segment.clone()));
                    continue;
                }

                if thresholds.check_for_compact(total_rows, total_size) {
                    // N <= totals < 2N
                    target_segments.push((location, segment.clone()));
                } else {
                    // totals >= 2N
                    let new_target_segments = vec![(location, segment)];
                    if Self::check_for_recluster(&new_target_segments, cluster_key_id, stable) {
                        target_segments = new_target_segments;
                        stable = true;
                        break 'FOR;
                    }
                };

                if Self::check_for_recluster(&target_segments, cluster_key_id, stable) {
                    stable = true;
                    break 'FOR;
                }
                target_segments.clear();
                total_rows = 0;
                total_size = 0;
            }
        }

        if !stable && !Self::check_for_recluster(&target_segments, cluster_key_id, stable) {
            return Ok(None);
        }

        let rows_per_block = block_thresholds.calc_rows_per_block(total_size, total_rows) as u64;
        let block_size = ctx.get_settings().get_max_block_size()?;
        ctx.get_settings()
            .set_max_block_size(rows_per_block.min(block_size))?;

        let mut removed_statistics = Statistics::default();
        let mut removed_segment_indexes = Vec::with_capacity(target_segments.len());
        for (segment_loc, segment) in target_segments {
            ctx.add_selected_segment_location(segment_loc.location);
            removed_segment_indexes.push(segment_loc.segment_idx);
            merge_statistics_mut(
                &mut removed_statistics,
                &segment.summary,
                Some(cluster_key_id),
            );
        }

        let recluster_info = ReclusterInfoSideCar {
            merged_blocks: vec![],
            removed_segment_indexes,
            removed_statistics,
        };
        Ok(Some((recluster_info, snapshot)))
    }
}

impl RealHilbertClusteringHandler {
    pub fn init() -> Result<()> {
        let handler = RealHilbertClusteringHandler {};
        let wrapper = HilbertClusteringHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn check_for_recluster(
        segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        default_cluster_id: u32,
        stable: bool,
    ) -> bool {
        match segments.len() {
            0 => false,
            1 => segments[0]
                .1
                .summary
                .cluster_stats
                .as_ref()
                .map_or(true, |stats| {
                    stats.cluster_key_id != default_cluster_id || (stats.level != -1 && stable)
                }),
            _ => true,
        }
    }
}
