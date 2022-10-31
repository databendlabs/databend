//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use common_catalog::table::Table;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use metrics::gauge;
use opendal::Operator;

use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::CompactOptions;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;

// The rough idea goes like this:
// 1. `target_select` will working and the base_snapshot: compact segments and keep the compactions
// 2. `try_commit` will try to commit the compacted snapshot to the meta store.
//     in condition of reconcilable conflicts detected, compactor will try to resolve the conflict and try again.
//     after MAX_RETRIES attempts, if still failed, abort the operation even if conflict resolvable.
//
// example of a merge process:
// - segments of base_snapshot:
//  [a, b, c, d, e, f, g]
//  suppose a and d are compacted enough, and others are not.
// - after `select_target`:
//  unchanged_segments : [a, d]
//  compacted_segments : [x, y], where
//     [b, c] compacted into x
//     [e, f, g] compacted into y
// - if there is a concurrent tx, committed BEFORE compact segments,
//   and suppose the segments of the tx's snapshot is
//  [a, b, c, d, e, f, g, h, i]
//  compare with the base_snapshot's segments
//  [a, b, c, d, e, f, g]
// we know that tx is appended(only) on the base of base_snapshot
// and the `appended_segments` is [h, i]
//
// The final merged segments should be
// [x, y, a, d, h, i]
//
// note that
// 1. the concurrently appended(and committed) segment will NOT be compacted during tx
//     conflicts resolving to make the committing of compact operation agile
// 2. in the implementation, the order of segment in the vector is arranged in reversed order
//    - compaction starts from the head of the snapshot's segment vector.
//    - newly compacted segments will be added to the end of snapshot's segment list

pub struct SegmentCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,

    // summarised statistics of all the accumulated segments(compacted, and unchanged)
    merged_segment_statistics: Statistics,
    // locations all the accumulated segments(compacted, and unchanged)
    merged_segments_locations: Vec<Location>,
    // paths all the newly created segments (which are compacted)
    new_segment_paths: Vec<String>,
}

impl SegmentCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        compact_params: CompactOptions,
        location_generator: TableMetaLocationGenerator,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            compact_params,
            data_accessor: operator,
            location_generator,
            merged_segment_statistics: Statistics::default(),
            merged_segments_locations: vec![],
            new_segment_paths: vec![],
        })
    }

    fn need_compaction(&self) -> bool {
        !self.merged_segments_locations.is_empty()
    }
}

#[async_trait::async_trait]
impl TableMutator for SegmentCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let select_begin = Instant::now();

        let fuse_segment_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let base_segment_locations = &self.compact_params.base_snapshot.segments;
        let base_segments = fuse_segment_io
            .read_segments(&self.compact_params.base_snapshot.segments)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let number_segments = base_segments.len();

        let blocks_per_segment_threshold = self.compact_params.block_per_seg;

        let mut segments_tobe_compacted = Vec::with_capacity(number_segments / 2);

        let mut unchanged_segment_locations = Vec::with_capacity(number_segments / 2);

        let mut unchanged_segment_statistics = Statistics::default();

        let mut start = 0;
        let limit = self.compact_params.limit.unwrap_or(number_segments);
        loop {
            let end = std::cmp::min(start + limit, number_segments);

            for idx in start..end {
                let number_blocks = base_segments[idx].blocks.len();
                if number_blocks >= blocks_per_segment_threshold {
                    // skip if current segment is large enough, mark it as unchanged
                    unchanged_segment_locations.push(base_segment_locations[idx].clone());
                    unchanged_segment_statistics = merge_statistics(
                        &unchanged_segment_statistics,
                        &base_segments[idx].summary,
                    )?;
                    continue;
                } else {
                    // if number of blocks meets the threshold, mark them down
                    segments_tobe_compacted.push(&base_segments[idx])
                }
            }

            // check if necessary to compact the segment meta: at least two segments were marked as segments_tobe_compacted
            if segments_tobe_compacted.len() > 1 {
                if end < number_segments {
                    unchanged_segment_locations = base_segment_locations[end..]
                        .iter()
                        .chain(unchanged_segment_locations.iter())
                        .cloned()
                        .collect();
                    for segment in &base_segments[end..] {
                        unchanged_segment_statistics =
                            merge_statistics(&unchanged_segment_statistics, &segment.summary)?;
                    }
                }
                break;
            }

            if end == number_segments {
                return Ok(false);
            }
            start = end;
        }

        // flatten the block metas of segments being compacted
        let blocks_of_new_segments: Vec<Arc<BlockMeta>> = segments_tobe_compacted
            .iter()
            .flat_map(|v| v.blocks.iter())
            .cloned()
            .collect();

        // split the block metas into chunks of blocks, with chunk size set to block_per_seg
        let chunk_of_blocks = blocks_of_new_segments.chunks(blocks_per_segment_threshold);
        // Build new segments which are compacted according to the setting of `block_per_seg`
        // note that the newly segments will be persistent into storage, such that if retry
        // happens during the later `try_commit` phase, they do not need to be written again.
        let mut compacted_segment_statistics = Statistics::default();
        let mut compacted_segment_locations = Vec::with_capacity(number_segments / 2);
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let segment_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );
        for chunk in chunk_of_blocks {
            let stats = reduce_block_metas(chunk)?;
            compacted_segment_statistics = merge_statistics(&compacted_segment_statistics, &stats)?;
            let new_segment = SegmentInfo::new(chunk.to_vec(), stats);
            let location = segment_writer.write_segment(new_segment).await?;
            compacted_segment_locations.push(location);
        }

        // collect all the paths of newly created segments
        self.new_segment_paths = compacted_segment_locations
            .iter()
            .map(|(path, _version)| path.clone())
            .collect();

        self.merged_segment_statistics =
            merge_statistics(&compacted_segment_statistics, &unchanged_segment_statistics)?;

        self.merged_segments_locations = {
            // place the newly generated compacted segment to the tail of the vector
            unchanged_segment_locations.append(&mut compacted_segment_locations);
            unchanged_segment_locations
        };

        gauge!(
            "fuse_compact_segments_select_duration_second",
            select_begin.elapsed(),
        );
        Ok(self.need_compaction())
    }

    async fn try_commit(self: Box<Self>, table: Arc<dyn Table>) -> Result<()> {
        if !self.need_compaction() {
            // defensive checking
            return Ok(());
        }

        let abort_action = AbortOperation {
            segments: self.new_segment_paths,
            ..Default::default()
        };

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .commit_mutation(
                &self.ctx,
                self.compact_params.base_snapshot,
                self.merged_segments_locations,
                self.merged_segment_statistics,
                abort_action,
            )
            .await
    }
}
