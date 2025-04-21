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
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
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

        let block_per_seg =
            fuse_table.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let hilbert_clustering_min_bytes =
            ctx.get_settings().get_hilbert_clustering_min_bytes()? as usize;
        let max_bytes_per_block = fuse_table.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_BUFFER_SIZE,
        );
        let hilbert_min_bytes = std::cmp::max(
            hilbert_clustering_min_bytes,
            max_bytes_per_block * block_per_seg,
        );
        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let chunk_size = max_threads * 4;
        let mut checker = ReclusterChecker::new(
            cluster_key_id,
            hilbert_min_bytes,
            push_downs.as_ref().is_none_or(|v| v.filters.is_none()),
        );
        'FOR: for chunk in segment_locations.chunks(chunk_size) {
            // read segments.
            let compact_segments = FuseTable::segment_pruning(
                &ctx,
                fuse_table.schema_with_stream(),
                fuse_table.get_operator(),
                &push_downs,
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
    last_segment: Option<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    default_cluster_id: u32,

    hilbert_min_bytes: usize,
    total_bytes: usize,

    finished: bool,
    // Whether the target segments is at the head of snapshot.
    head_of_snapshot: bool,
}

impl ReclusterChecker {
    fn new(default_cluster_id: u32, hilbert_min_bytes: usize, head_of_snapshot: bool) -> Self {
        Self {
            segments: vec![],
            last_segment: None,
            default_cluster_id,
            hilbert_min_bytes,
            total_bytes: 0,
            finished: false,
            head_of_snapshot,
        }
    }

    fn add(&mut self, location: SegmentLocation, segment: Arc<CompactSegmentInfo>) -> bool {
        let segment_should_recluster = self.should_recluster(&segment, |v| {
            v.cluster_key_id != self.default_cluster_id || v.level != -1
        });

        if segment_should_recluster || !self.head_of_snapshot {
            self.total_bytes += segment.summary.uncompressed_byte_size as usize;
            self.segments.push((location.clone(), segment.clone()));
        }

        if !segment_should_recluster || self.total_bytes >= self.hilbert_min_bytes {
            if self.check_for_recluster() {
                self.finished = true;
                return true;
            }
            self.last_segment = Some((location, segment));
            self.reset();
        }

        false
    }

    fn finalize(&mut self) -> Vec<(SegmentLocation, Arc<CompactSegmentInfo>)> {
        if !self.finished {
            if let Some((location, segment)) = self.last_segment.take() {
                self.segments.push((location, segment));
            }
            if !self.check_for_recluster() {
                return vec![];
            }
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
        self.total_bytes = 0;
        self.head_of_snapshot = false;
        self.segments.clear();
    }
}
