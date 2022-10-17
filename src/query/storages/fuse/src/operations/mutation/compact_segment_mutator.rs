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

use std::ops::Range;
use std::sync::Arc;

use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use common_meta_app::schema::TableInfo;
use opendal::Operator;

use crate::io::SegmentWriter;
use crate::io::TableMetaLocationGenerator;
use crate::statistics::reducers::reduce_block_metas;
use crate::FuseSegmentIO;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;

pub struct CompactSegmentMutator {
    ctx: Arc<dyn TableContext>,
    base_snapshot: Arc<TableSnapshot>,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,
    block_per_seg: usize,
    new_segments: Vec<(Location, Arc<SegmentInfo>)>,
    // is_cluster indicates whether the table contains cluster key.
    is_cluster: bool,
}

impl CompactSegmentMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        location_generator: TableMetaLocationGenerator,
        blocks_per_seg: usize,
        is_cluster: bool,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            base_snapshot,
            data_accessor: operator,
            location_generator,
            block_per_seg: blocks_per_seg,
            new_segments: vec![],
            is_cluster,
        })
    }
}

#[async_trait::async_trait]
impl TableMutator for CompactSegmentMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let fuse_segment_io = FuseSegmentIO::create(self.ctx.clone(), self.data_accessor.clone());
        let base_segments = fuse_segment_io
            .read_segments(&self.base_snapshot.segments)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let blocks_per_segment_threshold = self.block_per_seg;
        let mut segments_tobe_compacted = vec![];
        for segment in &base_segments {
            let number_blocks = segment.blocks.len();
            // skip if current segment is large enough
            if number_blocks >= blocks_per_segment_threshold {
                // this should not happen, warn it
                continue;
            } else {
                // if number of blocks meets the threshold, mark them down
                segments_tobe_compacted.push(segment)
            }
        }

        let mut blocks_of_new_segments: Vec<&BlockMeta> = vec![];
        for segment in segments_tobe_compacted {
            for x in &segment.blocks {
                blocks_of_new_segments.push(x);
            }
        }

        let chunk_of_blocks = blocks_of_new_segments.chunks(self.block_per_seg);
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let segment_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );

        let mut compacted_new_segments = vec![];
        for chunk in chunk_of_blocks {
            let stats = reduce_block_metas(chunk)?;
            let blocks: Vec<BlockMeta> = chunk.iter().map(|block| Clone::clone(*block)).collect();
            let new_segment = SegmentInfo::new(blocks, stats);
            let location = segment_writer.write_segment_new(new_segment).await?;
            compacted_new_segments.push(location);
        }

        self.new_segments = compacted_new_segments;
        Ok(!self.new_segments.is_empty())
    }

    async fn try_commit(&self, table_info: &TableInfo) -> Result<()> {
        let ctx = &self.ctx;
        let base_snapshot = &self.base_snapshot;
        let catalog = ctx.get_catalog(table_info.catalog())?;
        let mut table = catalog.get_table_by_info(table_info)?;
        let mut latest_snapshot;
        let mut retires = 0;
        // used in conflict reconciliation, for concurrent append only operations
        let mut appended_segments_locations: &[Location] = &[];

        loop {
            let fuse_segment_io =
                FuseSegmentIO::create(self.ctx.clone(), self.data_accessor.clone());

            let append_segments_infos = fuse_segment_io
                .read_segments(appended_segments_locations)
                .await?
                .into_iter()
                .collect::<Result<Vec<Arc<SegmentInfo>>>>()?;

            let appended_segments_with_locations = appended_segments_locations.iter().zip(
                append_segments_infos
                    .iter()
                    .map(|segment_info| segment_info.as_ref()),
            );

            let compacted_segments = self.new_segments.iter().map(|(l, s)| (l, s.as_ref()));
            // chain compacted_segments with appended_segments
            let merged_segment = appended_segments_with_locations.chain(compacted_segments);

            let (merged_segments, merged_summary) = FuseTable::merge_segments_new(merged_segment)?;

            let merged_segments = merged_segments
                .into_iter()
                .map(|loc| (loc, SegmentInfo::VERSION)) // TODO fix me
                .collect();

            let mut snapshot_tobe_committed = TableSnapshot::from_previous(base_snapshot.as_ref());
            snapshot_tobe_committed.segments = merged_segments;
            snapshot_tobe_committed.summary = merged_summary;

            match FuseTable::commit_to_meta_server(
                ctx.as_ref(),
                table_info,
                &self.location_generator,
                snapshot_tobe_committed,
                &self.data_accessor,
            )
            .await
            {
                Err(e) => {
                    if e.code() == ErrorCode::table_version_mismatched_code() {
                        table = table.refresh(ctx.as_ref()).await?;
                        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                        // TODO remove this unwrap
                        latest_snapshot =
                            fuse_table.read_table_snapshot(ctx.clone()).await?.unwrap();
                        match detect_conflicts(base_snapshot, &latest_snapshot) {
                            Conflict::UnResolvable => {
                                break Err(ErrorCode::StorageOther(
                                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                                ));
                            }
                            Conflict::ResolvableWithAppend(r) => {
                                appended_segments_locations = &latest_snapshot.segments[r];
                            }
                        }

                        retires += 1;
                        if retires > 10 {
                            return Err(ErrorCode::StorageOther(format!(
                                "compact segment failed after {} retries",
                                retires
                            )));
                        }
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                Ok(_) => return Ok(()),
            }
        }
    }
}

enum Conflict {
    UnResolvable,
    ResolvableWithAppend(Range<usize>),
}

fn detect_conflicts(base: &TableSnapshot, latest: &TableSnapshot) -> Conflict {
    let base_segments = &base.segments;
    let latest_segments = &latest.segments;

    let base_segments_len = base_segments.len();
    let latest_segments_len = latest_segments.len();

    if latest_segments_len >= base_segments_len
        && base_segments[base_segments_len - 1] == latest_segments[latest_segments_len - 1]
        && base_segments[0] == latest_segments[latest_segments_len - base_segments_len]
    {
        return Conflict::UnResolvable;
    };

    Conflict::ResolvableWithAppend(0..(latest_segments_len - base_segments_len))
}
