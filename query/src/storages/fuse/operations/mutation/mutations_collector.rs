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
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockWriter;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::io::SegmentWriter;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::statistics::reducers::reduce_block_metas;
use crate::storages::fuse::statistics::reducers::reduce_statistics;

pub enum Deletion {
    NothingDeleted,
    Remains(DataBlock),
}

pub struct Replacement {
    original_block_loc: Location,
    new_block_meta: Option<BlockMeta>,
}

pub type SegmentIndex = usize;

pub struct DeletionCollector<'a> {
    mutations: HashMap<SegmentIndex, Vec<Replacement>>,
    ctx: &'a QueryContext,
    location_generator: &'a TableMetaLocationGenerator,
    base_snapshot: &'a TableSnapshot,
    data_accessor: Operator,
}

impl<'a> DeletionCollector<'a> {
    pub fn try_create(
        ctx: &'a QueryContext,
        location_generator: &'a TableMetaLocationGenerator,
        base_snapshot: &'a TableSnapshot,
    ) -> Result<Self> {
        let data_accessor = ctx.get_storage_operator()?;
        Ok(Self {
            mutations: HashMap::new(),
            ctx,
            location_generator,
            base_snapshot,
            data_accessor,
        })
    }

    pub async fn into_new_snapshot(self) -> Result<(TableSnapshot, String)> {
        // TODO docs!!!
        let snapshot = self.base_snapshot;
        let mut new_snapshot = TableSnapshot::from_previous(snapshot);
        let segment_reader = MetaReaders::segment_info_reader(self.ctx);

        let segment_info_cache = self
            .ctx
            .get_storage_cache_manager()
            .get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            self.location_generator,
            &segment_info_cache,
        );

        for (seg_idx, replacements) in self.mutations {
            let seg_loc = &snapshot.segments[seg_idx];
            let segment = segment_reader.read(&seg_loc.0, None, seg_loc.1).await?;

            let block_positions = segment
                .blocks
                .iter()
                .enumerate()
                .map(|(idx, meta)| (&meta.location, idx))
                .collect::<HashMap<_, _>>();

            let mut new_segment = SegmentInfo::new(segment.blocks.clone(), segment.summary.clone());

            for replacement in replacements {
                let position = block_positions
                    .get(&replacement.original_block_loc)
                    .ok_or_else(|| {
                        ErrorCode::LogicalError(format!(
                            "block location not found {:?}",
                            &replacement.original_block_loc
                        ))
                    })?;
                if let Some(block_meta) = replacement.new_block_meta {
                    new_segment.blocks[*position] = block_meta;
                } else {
                    new_segment.blocks.remove(*position);
                }
            }

            // TODO test this (in UT)
            if new_segment.blocks.is_empty() {
                // remove the segment if no blocks there
                new_snapshot.segments.remove(seg_idx);
            } else {
                let new_summary = reduce_block_metas(&new_segment.blocks)?;
                new_segment.summary = new_summary;
                let new_segment_location = seg_writer.write_segment(new_segment).await?;
                new_snapshot.segments[seg_idx] = new_segment_location;
            }
        }

        let mut new_segment_summaries = vec![];
        for (loc, ver) in &new_snapshot.segments {
            let seg = segment_reader.read(loc, None, *ver).await?;
            // only need the summary, drop the reference to segment ASAP
            new_segment_summaries.push(seg.summary.clone())
        }

        // update the summary of new snapshot
        let new_summary = reduce_statistics(&new_segment_summaries)?;
        new_snapshot.summary = new_summary;

        // write the new segment out (and keep it in undo log)
        let snapshot_loc = self.location_generator.snapshot_location_from_uuid(
            &new_snapshot.snapshot_id,
            new_snapshot.format_version(),
        )?;
        let bytes = serde_json::to_vec(&new_snapshot)?;
        self.data_accessor
            .object(&snapshot_loc)
            .write(bytes)
            .await?;
        Ok((new_snapshot, snapshot_loc))
    }

    /// Replaces
    ///  the block located at `block_location` of segment indexed by `seg_idx`
    /// With a new block `r`
    pub async fn replace_with(
        &mut self,
        seg_idx: usize,
        location_of_block_to_be_replaced: Location,
        replace_with: DataBlock,
    ) -> Result<()> {
        // write new block, and keep the mutations
        let new_block_meta = if replace_with.num_rows() == 0 {
            None
        } else {
            let block_writer = BlockWriter::new(&self.data_accessor, self.location_generator);
            Some(block_writer.write_block(replace_with).await?)
        };
        let original_block_loc = location_of_block_to_be_replaced;
        self.mutations
            .entry(seg_idx)
            .or_default()
            .push(Replacement {
                original_block_loc,
                new_block_meta,
            });
        Ok(())
    }
}
