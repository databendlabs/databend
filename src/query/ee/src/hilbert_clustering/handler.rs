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
use databend_enterprise_hilbert_clustering::HilbertClusteringHandler;
use databend_enterprise_hilbert_clustering::HilbertClusteringHandlerWrapper;
use databend_storages_common_table_meta::meta::ClusterStatistics;
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

        let block_thresholds = fuse_table.get_block_thresholds();
        let thresholds = BlockThresholds {
            max_rows_per_block: block_thresholds.block_per_segment
                * block_thresholds.max_rows_per_block,
            min_rows_per_block: block_thresholds.block_per_segment
                * block_thresholds.min_rows_per_block,
            max_bytes_per_block: block_thresholds.block_per_segment
                * block_thresholds.max_bytes_per_block,
            max_bytes_per_file: block_thresholds.block_per_segment
                * block_thresholds.max_bytes_per_file,
            block_per_segment: block_thresholds.block_per_segment,
        };
        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let chunk_size = max_threads * 4;
        let mut checker = ReclusterChecker::new(
            cluster_key_id,
            thresholds,
            push_downs.as_ref().is_none_or(|v| v.filters.is_none()),
        );
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

            for (location, segment) in compact_segments.into_iter() {
                if checker.add(location, segment) {
                    break 'FOR;
                }
            }
        }

        let target_segments = checker.finalize();
        if target_segments.is_empty() {
            return Ok(None);
        }

        let rows_per_block =
            block_thresholds.calc_rows_per_block(checker.total_size, checker.total_rows, 0) as u64;
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
}

struct ReclusterChecker {
    segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    default_cluster_id: u32,
    thresholds: BlockThresholds,

    total_rows: usize,
    total_size: usize,

    finished: bool,
    // Whether the target segments is at the head of snapshot.
    head_of_snapshot: bool,
}

impl ReclusterChecker {
    fn new(default_cluster_id: u32, thresholds: BlockThresholds, head_of_snapshot: bool) -> Self {
        Self {
            segments: vec![],
            default_cluster_id,
            thresholds,
            total_rows: 0,
            total_size: 0,
            finished: false,
            head_of_snapshot,
        }
    }

    fn add(&mut self, location: SegmentLocation, segment: Arc<CompactSegmentInfo>) -> bool {
        let row_count = segment.summary.row_count as usize;
        let byte_size = segment.summary.uncompressed_byte_size as usize;
        self.total_rows += row_count;
        self.total_size += byte_size;
        if !self
            .thresholds
            .check_large_enough(self.total_rows, self.total_size)
        {
            // totals < N
            self.segments.push((location, segment));
            return false;
        }

        let segment_should_recluster = self.should_recluster(&segment, |v| {
            v.cluster_key_id != self.default_cluster_id || v.level != -1
        });
        let mut retained = false;
        if !self.head_of_snapshot || segment_should_recluster {
            if self
                .thresholds
                .check_for_compact(self.total_rows, self.total_size)
            {
                // N <= totals < 2N
                self.segments.push((location, segment));
                retained = true;
            } else if segment_should_recluster {
                // totals >= 2N
                self.segments = vec![(location, segment)];
                self.total_rows = row_count;
                self.total_size = byte_size;
                self.finished = true;
                return true;
            }
        }

        if self.check_for_recluster() {
            if !retained {
                self.total_rows -= row_count;
                self.total_size -= byte_size;
            }
            self.finished = true;
            return true;
        }

        self.reset();
        false
    }

    fn finalize(&mut self) -> Vec<(SegmentLocation, Arc<CompactSegmentInfo>)> {
        if !self.finished && !self.check_for_recluster() {
            return vec![];
        }
        std::mem::take(&mut self.segments)
    }

    fn check_for_recluster(&self) -> bool {
        match self.segments.len() {
            0 => false,
            1 => self.should_recluster(&self.segments[0].1, |v| {
                v.cluster_key_id != self.default_cluster_id
            }),
            _ => true,
        }
    }

    fn should_recluster<F>(&self, segment: &CompactSegmentInfo, pred: F) -> bool
    where F: Fn(&ClusterStatistics) -> bool {
        segment.summary.cluster_stats.as_ref().is_none_or(pred)
    }

    fn reset(&mut self) {
        self.total_rows = 0;
        self.total_size = 0;
        self.head_of_snapshot = false;
        self.segments.clear();
    }
}
