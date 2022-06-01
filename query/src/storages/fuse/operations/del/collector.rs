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

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::write_block;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::Compression;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::meta::Versioned;
use crate::storages::fuse::operations::util::column_metas;
use crate::storages::fuse::statistics::reducers::reduce_block_metas;
use crate::storages::fuse::statistics::StatisticsAccumulator;

pub enum Deletion {
    NothingDeleted,
    Remains(DataBlock),
}

#[allow(dead_code)]
pub struct Replacement {
    original_block_loc: Location,
    new_block_meta: BlockMeta,
}

pub type SegmentIndex = usize;

pub struct DeletionCollector<'a> {
    mutations: HashMap<SegmentIndex, Vec<Replacement>>,
    ctx: &'a QueryContext,
    location_generator: &'a TableMetaLocationGenerator,
    base_snapshot: &'a TableSnapshot,
}

impl<'a> DeletionCollector<'a> {
    pub fn new(
        ctx: &'a QueryContext,
        location_generator: &'a TableMetaLocationGenerator,
        base_snapshot: &'a TableSnapshot,
    ) -> Self {
        Self {
            mutations: HashMap::new(),
            ctx,
            location_generator,
            base_snapshot,
        }
    }

    pub async fn new_snapshot(self) -> Result<TableSnapshot> {
        let snapshot = self.base_snapshot;
        let mut new_snapshot = snapshot.clone();
        let seg_reader = MetaReaders::segment_info_reader(self.ctx);
        for (seg_idx, replacements) in self.mutations {
            let seg_loc = &snapshot.segments[seg_idx];
            let segment = seg_reader.read(&seg_loc.0, None, seg_loc.1).await?;
            let mut new_segment = SegmentInfo::new(segment.blocks.clone(), segment.summary.clone());
            for replacement in replacements {
                let position = new_segment
                    .blocks
                    .iter()
                    .position(|v| v.location == replacement.original_block_loc)
                    .unwrap();
                new_segment.blocks[position] = replacement.new_block_meta
            }

            let new_summary = reduce_block_metas(&new_segment.blocks)?;
            new_segment.summary = new_summary;

            let new_seg_loc = self.location_generator.gen_segment_info_location();
            new_snapshot.segments[seg_idx] = (new_seg_loc, SegmentInfo::VERSION);
        }
        // TODO update the summary of snapshot
        // write the new segment out (and keep it in undo log)
        Ok(new_snapshot)
    }
    ///Replaces the block located at `block_meta.location`,
    /// of segment indexed by `seg_idx`, with a new block `r`
    pub async fn replace_with(
        &mut self,
        seg_idx: usize,
        block_location: &Location,
        replace_with: DataBlock,
    ) -> Result<()> {
        // write new block, and keep the mutations
        let new_block_meta = self.write_new_block(replace_with).await?;
        let original_block_loc = block_location.clone();
        self.mutations
            .entry(seg_idx)
            .or_default()
            .push(Replacement {
                original_block_loc,
                new_block_meta,
            });
        Ok(())
    }

    async fn write_new_block(&mut self, block: DataBlock) -> Result<BlockMeta> {
        let location = self.location_generator.gen_block_location();
        let data_accessor = self.ctx.get_storage_operator()?;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let col_stats = StatisticsAccumulator::acc_columns(&block)?;
        let (file_size, file_meta_data) = write_block(block, data_accessor, &location).await?;
        let col_metas = column_metas(&file_meta_data)?;
        let block_meta = BlockMeta {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats: None, // TODO confirm this with zhyass
            location: (location, DataBlock::VERSION),
            compression: Compression::Lz4,
        };
        Ok(block_meta)
    }
}
