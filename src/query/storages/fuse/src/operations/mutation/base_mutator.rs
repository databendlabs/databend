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

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::BlockCompactThresholds;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use common_storages_table_meta::meta::TableSnapshot;
use opendal::Operator;

use super::AbortOperation;
use crate::io::MetaReaders;
use crate::io::SegmentWriter;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::reducers::reduce_statistics;

#[derive(Clone)]
pub struct Replacement {
    pub(crate) original_block_loc: Location,
    pub(crate) new_block_meta: Option<BlockMeta>,
}

pub type SegmentIndex = usize;

#[derive(Clone)]
pub struct BaseMutator {
    pub(crate) mutations: HashMap<SegmentIndex, Vec<Replacement>>,
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) location_generator: TableMetaLocationGenerator,
    pub(crate) data_accessor: Operator,
    pub(crate) base_snapshot: Arc<TableSnapshot>,
    pub(crate) thresholds: BlockCompactThresholds,
}

impl BaseMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        location_generator: TableMetaLocationGenerator,
        base_snapshot: Arc<TableSnapshot>,
        thresholds: BlockCompactThresholds,
    ) -> Result<Self> {
        Ok(Self {
            mutations: HashMap::new(),
            ctx,
            location_generator,
            data_accessor: op,
            base_snapshot,
            thresholds,
        })
    }

    pub fn add_mutation(
        &mut self,
        seg_idx: SegmentIndex,
        original_block_loc: Location,
        new_block_meta: Option<BlockMeta>,
    ) {
        self.mutations
            .entry(seg_idx)
            .or_default()
            .push(Replacement {
                original_block_loc,
                new_block_meta,
            });
    }

    pub async fn generate_segments(&self) -> Result<(Vec<Location>, Statistics, AbortOperation)> {
        let mut abort_operation = AbortOperation::default();
        let segments = self.base_snapshot.segments.clone();
        let mut segments_editor =
            HashMap::<_, _, RandomState>::from_iter(segments.clone().into_iter().enumerate());

        let segment_reader = MetaReaders::segment_info_reader(self.data_accessor.clone());

        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let seg_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );

        // apply mutations
        for (seg_idx, replacements) in self.mutations.clone() {
            let segment = {
                let (path, version) = &segments[seg_idx];
                segment_reader.read(&path, None, *version).await?
            };

            // collects the block locations of the segment being modified
            let block_positions = segment
                .blocks
                .iter()
                .enumerate()
                .map(|(idx, meta)| (&meta.location, idx))
                .collect::<HashMap<_, _>>();

            // prepare the new segment
            let mut new_segment = SegmentInfo::new(segment.blocks.clone(), segment.summary.clone());

            // take away the blocks, they are being mutated
            let mut block_editor = HashMap::<_, _, RandomState>::from_iter(
                std::mem::take(&mut new_segment.blocks)
                    .into_iter()
                    .enumerate(),
            );

            for replacement in replacements {
                let position = block_positions
                    .get(&replacement.original_block_loc)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "block location not found {:?}",
                            &replacement.original_block_loc
                        ))
                    })?;
                if let Some(block_meta) = replacement.new_block_meta {
                    abort_operation.add_block(&block_meta);
                    block_editor.insert(*position, Arc::new(block_meta));
                } else {
                    block_editor.remove(position);
                }
            }
            // assign back the mutated blocks to segment
            new_segment.blocks = block_editor.into_values().collect();

            if new_segment.blocks.is_empty() {
                // remove the segment if no blocks there
                segments_editor.remove(&seg_idx);
            } else {
                // re-calculate the segment statistics
                let new_summary = reduce_block_metas(&new_segment.blocks, self.thresholds)?;
                new_segment.summary = new_summary;
                // write down new segment
                let new_segment_location = seg_writer.write_segment(new_segment).await?;
                segments_editor.insert(seg_idx, new_segment_location.clone());
                abort_operation.add_segment(new_segment_location.0);
            }
        }

        // assign back the mutated segments to snapshot
        let new_segments = segments_editor.into_values().collect::<Vec<_>>();

        let mut new_segment_summaries = Vec::with_capacity(new_segments.len());
        for (loc, ver) in &new_segments {
            let seg = segment_reader.read(loc, None, *ver).await?;
            new_segment_summaries.push(seg.summary.clone())
        }

        // update the summary of new snapshot
        let new_summary = reduce_statistics(&new_segment_summaries)?;
        Ok((new_segments, new_summary, abort_operation))
    }
}
