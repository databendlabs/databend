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

use common_datablocks::DataBlock;
use common_exception::Result;
use opendal::Operator;

use super::block_filter::all_the_columns_ids;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockCompactor;
use crate::storages::fuse::io::BlockWriter;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::io::SegmentWriter;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::statistics::reducers::reduce_block_metas;
use crate::storages::fuse::statistics::reducers::reduce_statistics;
use crate::storages::fuse::FuseTable;

pub struct CompactMutator<'a> {
    ctx: &'a Arc<QueryContext>,
    location_generator: &'a TableMetaLocationGenerator,
    base_snapshot: &'a TableSnapshot,
    data_accessor: Operator,
    row_per_block: usize,
    block_per_seg: usize,
}

impl<'a> CompactMutator<'a> {
    pub fn try_create(
        ctx: &'a Arc<QueryContext>,
        location_generator: &'a TableMetaLocationGenerator,
        base_snapshot: &'a TableSnapshot,
        row_per_block: usize,
        block_per_seg: usize,
    ) -> Result<Self> {
        let data_accessor = ctx.get_storage_operator()?;
        Ok(Self {
            ctx,
            location_generator,
            base_snapshot,
            data_accessor,
            row_per_block,
            block_per_seg,
        })
    }

    pub async fn compact(&mut self, table: &FuseTable) -> Result<TableSnapshot> {
        let snapshot = self.base_snapshot;
        // Blocks that need to be reorganized into new segments.
        let mut remain_blocks = Vec::new();
        // Blocks that need to be compacted.
        let mut merged_blocks = Vec::new();
        // The new segments.
        let mut segments = Vec::new();
        let mut summarys = Vec::new();
        let reader = MetaReaders::segment_info_reader(self.ctx);
        for segment_location in &snapshot.segments {
            let (x, ver) = (segment_location.0.clone(), segment_location.1);
            let mut need_merge = false;
            let mut remains = Vec::new();
            let segment = reader.read(x, None, ver).await?;
            segment.blocks.iter().for_each(|b| {
                if b.row_count != self.row_per_block as u64 {
                    merged_blocks.push(b.clone());
                    need_merge = true;
                } else {
                    remains.push(b.clone());
                }
            });

            // If the number of blocks of segment meets block_per_seg, and the blocks in segments donot need to be compacted,
            // then record the segment information.
            if !need_merge && segment.blocks.len() == self.block_per_seg {
                segments.push(segment_location.clone());
                summarys.push(segment.summary.clone());
                continue;
            }

            remain_blocks.append(&mut remains);
        }

        // Compact the blocks.
        let col_ids = all_the_columns_ids(table);
        let mut compactor = BlockCompactor::new(self.row_per_block);
        let block_writer = BlockWriter::new(&self.data_accessor, self.location_generator);
        for block_meta in &merged_blocks {
            let block_reader = table.create_block_reader(self.ctx, col_ids.clone())?;
            let data_block = block_reader.read_with_block_meta(block_meta).await?;

            let res = compactor.compact(data_block)?;
            Self::write_block(&block_writer, res, &mut remain_blocks).await?;
        }
        let remains = compactor.finish()?;
        Self::write_block(&block_writer, remains, &mut remain_blocks).await?;

        // Create new segments.
        let segment_info_cache = self
            .ctx
            .get_storage_cache_manager()
            .get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            self.location_generator,
            &segment_info_cache,
        );
        let chunks = remain_blocks.chunks(self.block_per_seg);
        for chunk in chunks {
            let new_summary = reduce_block_metas(chunk)?;
            let new_segment = SegmentInfo::new(chunk.to_vec(), new_summary.clone());
            let new_segment_location = seg_writer.write_segment(new_segment).await?;
            segments.push(new_segment_location);
            summarys.push(new_summary);
        }

        let mut new_snapshot = TableSnapshot::from_previous(snapshot);
        new_snapshot.segments = segments;
        // update the summary of new snapshot
        let new_summary = reduce_statistics(&summarys)?;
        new_snapshot.summary = new_summary;
        Ok(new_snapshot)
    }

    async fn write_block(
        writer: &BlockWriter<'_>,
        blocks: Option<Vec<DataBlock>>,
        metas: &mut Vec<BlockMeta>,
    ) -> Result<()> {
        if let Some(blocks) = blocks {
            for block in blocks {
                let new_block_meta = writer.write(block).await?;
                metas.push(new_block_meta);
            }
        }
        Ok(())
    }
}
