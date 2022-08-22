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

use crate::io::write_meta;
use crate::io::MetaReaders;
use crate::io::SegmentWriter;
use crate::operations::AppendOperationLogEntry;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::reducers::reduce_statistics;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD;
use crate::DEFAULT_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;

#[derive(Clone)]
pub struct CompactMutator {
    ctx: Arc<dyn TableContext>,
    pub(crate) base_snapshot: Arc<TableSnapshot>,
    pub(crate) data_accessor: Operator,
    pub(crate) selected_blocks: Vec<BlockMeta>,
    segments: Vec<Location>,
    summary: Statistics,
    table: Arc<FuseTable>,
}

impl CompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        table: &FuseTable,
    ) -> Result<Self> {
        let data_accessor = ctx.get_storage_operator()?;

        Ok(Self {
            ctx,
            base_snapshot,
            data_accessor,
            selected_blocks: Vec::new(),
            segments: Vec::new(),
            summary: Statistics::default(),
            table: Arc::new(table.clone()),
        })
    }
}

#[async_trait::async_trait]
impl TableMutator for CompactMutator {
    async fn blocks_select(&mut self) -> Result<bool> {
        let snapshot = self.base_snapshot.clone();
        // Blocks that need to be reorganized into new segments.
        let mut remain_blocks = Vec::new();
        let mut summarys = Vec::new();
        let reader = MetaReaders::segment_info_reader(self.ctx.as_ref());

        let max_rows_per_block = self
            .table
            .get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let min_rows_per_block = (max_rows_per_block as f64 * 0.8) as usize;
        let max_bytes_per_block = self.table.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        );

        let block_per_seg = self
            .table
            .get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let is_cluster = self.table.cluster_key_meta.is_some();
        for segment_location in &snapshot.segments {
            let (x, ver) = (segment_location.0.clone(), segment_location.1);
            let mut need_merge = false;
            let mut remains = Vec::new();
            let segment = reader.read(x, None, ver).await?;
            segment.blocks.iter().for_each(|b| {
                if is_cluster
                    || b.check_perfect_block(
                        max_rows_per_block,
                        min_rows_per_block,
                        max_bytes_per_block,
                    )
                {
                    remains.push(b.clone());
                } else {
                    self.selected_blocks.push(b.clone());
                    need_merge = true;
                }
            });

            // If the number of blocks of segment meets block_per_seg, and the blocks in segments donot need to be compacted,
            // then record the segment information.
            if !need_merge && segment.blocks.len() == block_per_seg {
                self.segments.push(segment_location.clone());
                summarys.push(segment.summary.clone());
                continue;
            }

            remain_blocks.append(&mut remains);
        }

        if self.selected_blocks.is_empty()
            && (remain_blocks.is_empty() || snapshot.segments.len() <= self.segments.len() + 1)
        {
            return Ok(false);
        }

        // Create new segments.
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.table.meta_location_generator,
            &segment_info_cache,
        );
        let chunks = remain_blocks.chunks(block_per_seg);
        for chunk in chunks {
            let new_summary = reduce_block_metas(chunk)?;
            let new_segment = SegmentInfo::new(chunk.to_vec(), new_summary.clone());
            let new_segment_location = seg_writer.write_segment(new_segment).await?;
            self.segments.push(new_segment_location);
            summarys.push(new_summary);
        }

        // update the summary of new snapshot
        self.summary = reduce_statistics(&summarys)?;
        Ok(true)
    }

    async fn try_commit(&self, catalog_name: &str) -> Result<()> {
        let ctx = self.ctx.clone();
        let snapshot = self.base_snapshot.clone();
        let mut new_snapshot = TableSnapshot::from_previous(&snapshot);
        new_snapshot.segments = self.segments.clone();

        let append_entries = ctx.consume_precommit_blocks();
        let append_log_entries = append_entries
            .iter()
            .map(AppendOperationLogEntry::try_from)
            .collect::<Result<Vec<AppendOperationLogEntry>>>()?;

        let (merged_segments, merged_summary) =
            FuseTable::merge_append_operations(&append_log_entries)?;

        let mut merged_segments = merged_segments
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();
        new_snapshot.segments.append(&mut merged_segments);
        new_snapshot.summary = merge_statistics(&self.summary, &merged_summary)?;

        // write down the new snapshot
        let snapshot_loc = self
            .table
            .meta_location_generator
            .snapshot_location_from_uuid(
                &new_snapshot.snapshot_id,
                new_snapshot.format_version(),
            )?;
        write_meta(&self.data_accessor, &snapshot_loc, &new_snapshot).await?;

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            catalog_name,
            &self.table.table_info,
            snapshot_loc,
            &new_snapshot.summary,
        )
        .await?;
        Ok(())
    }
}
