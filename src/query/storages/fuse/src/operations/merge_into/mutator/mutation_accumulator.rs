// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::TableSchemaRef;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use tracing::info;

use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::merge_into::mutation_meta::AppendOperationLogEntry;
use crate::operations::merge_into::mutation_meta::CommitMeta;
use crate::operations::merge_into::mutation_meta::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::Replacement;
use crate::operations::merge_into::mutation_meta::ReplacementLogEntry;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::statistics::reducers::deduct_statistics_mut;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

#[derive(Default)]
struct BlockMutations {
    replaced_blocks: Vec<(BlockIndex, Arc<BlockMeta>)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl BlockMutations {
    fn new_replacement(block_idx: BlockIndex, block_meta: Arc<BlockMeta>) -> Self {
        BlockMutations {
            replaced_blocks: vec![(block_idx, block_meta)],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        BlockMutations {
            replaced_blocks: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(&mut self, block_idx: BlockIndex, block_meta: Arc<BlockMeta>) {
        self.replaced_blocks.push((block_idx, block_meta));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

pub struct MutationAccumulator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    thresholds: BlockThresholds,

    mutations: HashMap<SegmentIndex, BlockMutations>,
    // (path, segment_info)
    appended_segments: Vec<(String, Arc<SegmentInfo>, FormatVersion)>,
    base_segments: Vec<Location>,

    abort_operation: AbortOperation,
    summary: Statistics,
}

impl MutationAccumulator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        thresholds: BlockThresholds,
        base_segments: Vec<Location>,
        summary: Statistics,
    ) -> Self {
        MutationAccumulator {
            ctx,
            schema,
            dal,
            location_gen,
            thresholds,
            mutations: HashMap::new(),
            appended_segments: vec![],
            base_segments,
            abort_operation: AbortOperation::default(),
            summary,
        }
    }

    pub fn accumulate_log_entry(&mut self, log_entry: &MutationLogEntry) {
        match log_entry {
            MutationLogEntry::Replacement(mutation) => self.accumulate_mutation(mutation),
            MutationLogEntry::Append(append) => self.accumulate_append(append),
        }
    }

    fn accumulate_mutation(&mut self, meta: &ReplacementLogEntry) {
        match &meta.op {
            Replacement::Replaced(block_meta) => {
                self.mutations
                    .entry(meta.index.segment_idx)
                    .and_modify(|v| v.push_replaced(meta.index.block_idx, block_meta.clone()))
                    .or_insert(BlockMutations::new_replacement(
                        meta.index.block_idx,
                        block_meta.clone(),
                    ));
                self.abort_operation.add_block(block_meta);
            }
            Replacement::Deleted => {
                self.mutations
                    .entry(meta.index.segment_idx)
                    .and_modify(|v| v.push_deleted(meta.index.block_idx))
                    .or_insert(BlockMutations::new_deletion(meta.index.block_idx));
            }
            Replacement::DoNothing => (),
        }
    }

    fn accumulate_append(&mut self, append_log_entry: &AppendOperationLogEntry) {
        for block_meta in &append_log_entry.segment_info.blocks {
            self.abort_operation.add_block(block_meta);
        }
        // TODO can we avoid this clone?
        self.abort_operation
            .add_segment(append_log_entry.segment_location.clone());

        self.appended_segments.push((
            append_log_entry.segment_location.clone(),
            append_log_entry.segment_info.clone(),
            append_log_entry.format_version,
        ))
    }
}

impl MutationAccumulator {
    pub async fn apply(&mut self) -> Result<CommitMeta> {
        let mut recalc_stats = false;
        if self.mutations.len() == self.base_segments.len() {
            self.summary = Statistics::default();
            recalc_stats = true;
        }

        let start = Instant::now();
        let mut count = 0;

        let segment_locations = self.base_segments.clone();
        let mut segments_editor =
            BTreeMap::<_, _>::from_iter(segment_locations.into_iter().enumerate());

        let chunk_size = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let segment_indices = self.mutations.keys().cloned().collect::<Vec<_>>();
        for chunk in segment_indices.chunks(chunk_size) {
            let results = self.partial_apply(chunk.to_vec()).await?;
            for result in results {
                if let Some((location, summary)) = result.new_segment_info {
                    // replace the old segment location with the new one.
                    self.abort_operation.add_segment(location.clone());
                    segments_editor.insert(result.index, (location.clone(), SegmentInfo::VERSION));
                    merge_statistics_mut(&mut self.summary, &summary)?;
                } else {
                    // remove the old segment location.
                    segments_editor.remove(&result.index);
                }

                if !recalc_stats {
                    // deduct the old segment summary from the merged summary.
                    deduct_statistics_mut(&mut self.summary, &result.origin_summary);
                }
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "mutation: generate new segment files:{}/{}, cost:{} sec",
                    count,
                    segment_indices.len(),
                    start.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
                info!(status);
            }
        }

        for (_path, new_segment, _format_version) in &self.appended_segments {
            merge_statistics_mut(&mut self.summary, &new_segment.summary)?;
        }

        let updated_segments = segments_editor.into_values();

        // with newly appended segments
        let new_segments = self
            .appended_segments
            .iter()
            .map(|(path, _segment, format_version)| (path.clone(), *format_version))
            .chain(updated_segments)
            .collect();

        let meta = CommitMeta::new(
            new_segments,
            self.summary.clone(),
            self.abort_operation.clone(),
        );
        Ok(meta)
    }

    async fn partial_apply(&mut self, segment_indices: Vec<usize>) -> Result<Vec<SegmentLite>> {
        let thresholds = self.thresholds;
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let segment_mutation = self.mutations.remove(&index).unwrap();
            let location = self.base_segments[index].clone();
            let schema = self.schema.clone();
            let op = self.dal.clone();
            let location_gen = self.location_gen.clone();

            tasks.push(async move {
                // read the old segment
                let segment_info =
                    SegmentsIO::read_segment(op.clone(), location, schema, false).await?;
                // prepare the new segment
                let mut new_segment =
                    SegmentInfo::new(segment_info.blocks.clone(), segment_info.summary.clone());
                // take away the blocks, they are being mutated
                let mut block_editor = BTreeMap::<_, _>::from_iter(
                    std::mem::take(&mut new_segment.blocks)
                        .into_iter()
                        .enumerate(),
                );

                for (idx, new_meta) in segment_mutation.replaced_blocks {
                    block_editor.insert(idx, new_meta);
                }
                for idx in segment_mutation.deleted_blocks {
                    block_editor.remove(&idx);
                }

                // assign back the mutated blocks to segment
                new_segment.blocks = block_editor.into_values().collect();
                if !new_segment.blocks.is_empty() {
                    // re-calculate the segment statistics
                    let new_summary = reduce_block_metas(&new_segment.blocks, thresholds)?;
                    new_segment.summary = new_summary.clone();

                    // write the segment info.
                    let location = location_gen.gen_segment_info_location();
                    let serialized_segment = SerializedSegment {
                        path: location.clone(),
                        segment: Arc::new(new_segment),
                    };
                    SegmentsIO::write_segment(op, serialized_segment).await?;

                    Ok(SegmentLite {
                        index,
                        new_segment_info: Some((location, new_summary)),
                        origin_summary: segment_info.summary.clone(),
                    })
                } else {
                    Ok(SegmentLite {
                        index,
                        new_segment_info: None,
                        origin_summary: segment_info.summary.clone(),
                    })
                }
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }
}

struct SegmentLite {
    // segment index.
    index: usize,
    // new segment location and summary.
    new_segment_info: Option<(String, Statistics)>,
    // origin segment summary.
    origin_summary: Statistics,
}
