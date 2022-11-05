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

use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use common_storages_table_meta::meta::Versioned;
use opendal::Operator;

use crate::io::BlockCompactThresholds;
use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_set_segments_memory_usage;
use crate::metrics::metrics_set_selected_blocks_memory_usage;
use crate::operations::mutation::AbortOperation;
use crate::operations::AppendOperationLogEntry;
use crate::operations::CompactOptions;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;

pub struct FullCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    block_compactor: BlockCompactThresholds,
    location_generator: TableMetaLocationGenerator,
    selected_blocks: Vec<Arc<BlockMeta>>,
    // summarised statistics of all the accumulated segments(segment compacted, and unchanged)
    merged_segment_statistics: Statistics,
    // locations all the accumulated segments(segment compacted, and unchanged)
    merged_segments_locations: Vec<Location>,
    // paths the newly created segments (which are segment compacted)
    new_segment_paths: Vec<String>,
    // is_cluster indicates whether the table contains cluster key.
    is_cluster: bool,
}

impl FullCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        compact_params: CompactOptions,
        block_compactor: BlockCompactThresholds,
        location_generator: TableMetaLocationGenerator,
        is_cluster: bool,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            compact_params,
            data_accessor: operator,
            block_compactor,
            location_generator,
            selected_blocks: Vec::new(),
            merged_segment_statistics: Statistics::default(),
            merged_segments_locations: Vec::new(),
            new_segment_paths: Vec::new(),
            is_cluster,
        })
    }

    pub fn partitions_total(&self) -> usize {
        self.compact_params.base_snapshot.summary.block_count as usize
    }

    pub fn selected_blocks(&self) -> Vec<Arc<BlockMeta>> {
        self.selected_blocks.clone()
    }

    pub fn get_storage_operator(&self) -> Operator {
        self.data_accessor.clone()
    }
}

#[async_trait::async_trait]
impl TableMutator for FullCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let snapshot = self.compact_params.base_snapshot.clone();
        let segment_locations = &snapshot.segments;

        // Read all segments information in parallel.
        let segments_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let segments = segments_io
            .read_segments(segment_locations)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let number_segments = segments.len();

        // todo: add real metrics
        metrics_set_segments_memory_usage(0.0);

        let limit = self.compact_params.limit.unwrap_or(number_segments);

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;

        let mut unchanged_segment_locations = Vec::with_capacity(limit / 2);

        let mut unchanged_segment_statistics = Statistics::default();

        let mut start = 0;
        // Blocks that need to be reorganized into new segments.
        let mut remain_blocks = Vec::new();
        loop {
            let end = std::cmp::min(start + limit, number_segments);
            for i in start..end {
                let mut need_merge = false;
                let mut remains = Vec::new();

                segments[i].blocks.iter().for_each(|b| {
                    if self.is_cluster
                        || self
                            .block_compactor
                            .check_perfect_block(b.row_count as usize, b.block_size as usize)
                    {
                        remains.push(b.clone());
                    } else {
                        self.selected_blocks.push(b.clone());
                        need_merge = true;
                    }
                });

                // todo: add real metrics
                metrics_set_selected_blocks_memory_usage(0.0);

                // If the number of blocks of segment meets block_per_seg, and the blocks in segments donot need to be compacted,
                // then record the segment information.
                if !need_merge && segments[i].blocks.len() >= self.compact_params.block_per_seg {
                    unchanged_segment_locations.push(segment_locations[i].clone());
                    unchanged_segment_statistics =
                        merge_statistics(&unchanged_segment_statistics, &segments[i].summary)?;
                    continue;
                }

                remain_blocks.append(&mut remains);
            }

            // Because the pipeline is used, a threshold for the number of blocks is added to resolve
            // https://github.com/datafuselabs/databend/issues/8316
            if self.selected_blocks.len() >= max_threads * 2 {
                if end < number_segments {
                    self.merged_segments_locations = segment_locations[end..].to_vec();
                    for segment in &segments[end..] {
                        unchanged_segment_statistics =
                            merge_statistics(&unchanged_segment_statistics, &segment.summary)?;
                    }
                }

                self.merged_segments_locations
                    .extend(unchanged_segment_locations.into_iter());
                self.merged_segment_statistics = unchanged_segment_statistics;
                break;
            }

            if end == number_segments {
                self.selected_blocks.clear();
                return Ok(false);
            }
            start = end;
        }

        // Create new segments.
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );
        let chunks = remain_blocks.chunks(self.compact_params.block_per_seg);
        for chunk in chunks {
            let new_summary = reduce_block_metas(chunk)?;
            self.merged_segment_statistics =
                merge_statistics(&self.merged_segment_statistics, &new_summary)?;
            let new_segment = SegmentInfo::new(chunk.to_vec(), new_summary);
            let new_segment_location = seg_writer.write_segment(new_segment).await?;
            self.merged_segments_locations
                .push(new_segment_location.clone());
            self.new_segment_paths.push(new_segment_location.0);
        }

        Ok(true)
    }

    async fn try_commit(self: Box<FullCompactMutator>, table: Arc<dyn Table>) -> Result<()> {
        let mut segments = self.merged_segments_locations;
        let mut summary = self.merged_segment_statistics;
        let mut abort_operation = AbortOperation {
            segments: self.new_segment_paths,
            ..Default::default()
        };

        let append_entries = self.ctx.consume_precommit_blocks();
        let append_log_entries = append_entries
            .iter()
            .map(AppendOperationLogEntry::try_from)
            .collect::<Result<Vec<AppendOperationLogEntry>>>()?;

        let (merged_segments, merged_summary) =
            FuseTable::merge_append_operations(&append_log_entries)?;

        for entry in append_log_entries {
            for block in &entry.segment_info.blocks {
                abort_operation = abort_operation.add_block(block);
            }
            abort_operation = abort_operation.add_segment(entry.segment_location);
        }

        segments.extend(
            merged_segments
                .into_iter()
                .map(|loc| (loc, SegmentInfo::VERSION)),
        );
        summary = merge_statistics(&summary, &merged_summary)?;

        let table = FuseTable::try_from_table(table.as_ref())?;
        table
            .commit_mutation(
                &self.ctx,
                self.compact_params.base_snapshot,
                segments,
                summary,
                abort_operation,
            )
            .await
    }
}
