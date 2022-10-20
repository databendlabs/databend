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
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use opendal::Operator;

use crate::io::BlockCompactor;
use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::AppendOperationLogEntry;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::reducers::reduce_statistics;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;

#[derive(Clone)]
pub struct FullCompactMutator {
    ctx: Arc<dyn TableContext>,
    base_snapshot: Arc<TableSnapshot>,
    data_accessor: Operator,
    block_compactor: BlockCompactor,
    location_generator: TableMetaLocationGenerator,
    selected_blocks: Vec<BlockMeta>,
    // Blocks that need to be reorganized into new segments.
    remain_blocks: Vec<BlockMeta>,
    segments: Vec<Location>,
    summary: Statistics,
    block_per_seg: usize,
    // is_cluster indicates whether the table contains cluster key.
    is_cluster: bool,
}

impl FullCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        block_compactor: BlockCompactor,
        location_generator: TableMetaLocationGenerator,
        block_per_seg: usize,
        is_cluster: bool,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            base_snapshot,
            data_accessor: operator,
            block_compactor,
            location_generator,
            selected_blocks: Vec::new(),
            remain_blocks: Vec::new(),
            segments: Vec::new(),
            summary: Statistics::default(),
            block_per_seg,
            is_cluster,
        })
    }

    pub fn partitions_total(&self) -> usize {
        self.base_snapshot.summary.block_count as usize
    }

    pub fn selected_blocks(&self) -> Vec<BlockMeta> {
        self.selected_blocks.clone()
    }

    pub fn get_storage_operator(&self) -> Operator {
        self.data_accessor.clone()
    }
}

#[async_trait::async_trait]
impl TableMutator for FullCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let snapshot = self.base_snapshot.clone();
        let segment_locations = &snapshot.segments;
        let mut summarys = Vec::new();

        // Read all segments information in parallel.
        let segments_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let segments = segments_io.read_segments(segment_locations).await?;
        for (idx, segment) in segments.iter().enumerate() {
            let mut need_merge = false;
            let mut remains = Vec::new();
            let segment = segment.clone()?;

            segment.blocks.iter().for_each(|b| {
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

            // If the number of blocks of segment meets block_per_seg, and the blocks in segments donot need to be compacted,
            // then record the segment information.
            if !need_merge && segment.blocks.len() == self.block_per_seg {
                let location = segment_locations[idx].clone();
                self.segments.push(location);
                summarys.push(segment.summary.clone());
                continue;
            }

            self.remain_blocks.append(&mut remains);
        }

        if self.selected_blocks.is_empty()
            && (self.remain_blocks.is_empty() || snapshot.segments.len() <= self.segments.len() + 1)
        {
            return Ok(false);
        }

        // update the summary of new snapshot
        self.summary = reduce_statistics(&summarys)?;
        Ok(true)
    }

    async fn try_commit(self: Box<FullCompactMutator>, table: Arc<dyn Table>) -> Result<()> {
        let ctx = self.ctx.clone();
        let mut segments = self.segments;
        let mut summary = self.summary;
        let mut abort_operation = AbortOperation::default();

        // Create new segments.
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );
        let chunks = self.remain_blocks.chunks(self.block_per_seg);
        for chunk in chunks {
            let new_summary = reduce_block_metas(chunk)?;
            let new_segment = SegmentInfo::new(chunk.to_vec(), new_summary.clone());
            let new_segment_location = seg_writer.write_segment(new_segment).await?;
            segments.push(new_segment_location.clone());
            summary = merge_statistics(&summary, &new_summary)?;

            abort_operation = abort_operation.add_segment(new_segment_location.0);
        }

        let append_entries = ctx.consume_precommit_blocks();
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
            .commit_mutation(&ctx, self.base_snapshot, segments, summary, abort_operation)
            .await
    }
}
