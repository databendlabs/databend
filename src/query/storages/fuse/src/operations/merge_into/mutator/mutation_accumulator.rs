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

use common_exception::Result;
use common_expression::BlockThresholds;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;

use crate::io::TableMetaLocationGenerator;
use crate::operations::merge_into::mutation_meta::mutation_log::AppendOperationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_log::CommitMeta;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_log::Replacement;
use crate::operations::merge_into::mutation_meta::mutation_log::ReplacementLogEntry;
use crate::operations::mutation::base_mutator::BlockIndex;
use crate::operations::mutation::base_mutator::SegmentIndex;
use crate::operations::mutation::AbortOperation;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

pub struct SerializedSegment {
    pub raw_data: Vec<u8>,
    pub path: String,
    pub segment: Arc<SegmentInfo>,
}

#[derive(Default)]
struct BlockMutations {
    replaced_segments: Vec<(BlockIndex, Arc<BlockMeta>)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl BlockMutations {
    fn new_replacement(block_idx: BlockIndex, block_meta: Arc<BlockMeta>) -> Self {
        BlockMutations {
            replaced_segments: vec![(block_idx, block_meta)],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        BlockMutations {
            replaced_segments: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(&mut self, block_idx: BlockIndex, block_meta: Arc<BlockMeta>) {
        self.replaced_segments.push((block_idx, block_meta));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

#[derive(Default)]
pub struct MutationAccumulator {
    mutations: HashMap<SegmentIndex, BlockMutations>,
    // (path, segment_info)
    appended_segments: Vec<(String, Arc<SegmentInfo>, FormatVersion)>,
}

impl MutationAccumulator {
    pub fn accumulate_mutation(&mut self, meta: &ReplacementLogEntry) {
        match &meta.op {
            Replacement::Replaced(block_meta) => {
                self.mutations
                    .entry(meta.index.segment_idx)
                    .and_modify(|v| v.push_replaced(meta.index.block_idx, block_meta.clone()))
                    .or_insert(BlockMutations::new_replacement(
                        meta.index.block_idx,
                        block_meta.clone(),
                    ));
            }
            Replacement::Deleted => {
                self.mutations
                    .entry(meta.index.segment_idx)
                    .and_modify(|v| v.push_deleted(meta.index.block_idx))
                    .or_insert(BlockMutations::new_deletion(meta.index.block_idx));
            }
        }
    }

    pub fn accumulate_append(&mut self, append_log_entry: &AppendOperationLogEntry) {
        self.appended_segments.push((
            append_log_entry.segment_location.clone(),
            append_log_entry.segment_info.clone(),
            append_log_entry.format_version,
        ))
    }

    pub fn accumulate_log_entry(&mut self, log_entry: &MutationLogEntry) {
        match log_entry {
            MutationLogEntry::Replacement(mutation) => self.accumulate_mutation(mutation),
            MutationLogEntry::Append(append) => self.accumulate_append(append),
        }
    }
}

impl MutationAccumulator {
    pub fn apply(
        &self,
        base_segment_paths: Vec<Location>,
        segment_infos: &[Arc<SegmentInfo>],
        thresholds: BlockThresholds,
        location_gen: &TableMetaLocationGenerator,
    ) -> Result<(CommitMeta, Vec<SerializedSegment>)> {
        let mut abort_operation: AbortOperation = AbortOperation::default();
        let mut serialized_segments = Vec::new();
        let mut segments_editor =
            BTreeMap::<_, _>::from_iter(base_segment_paths.into_iter().enumerate());
        let mut table_statistics = Statistics::default();

        // 1. apply segment mutations
        for (seg_idx, seg_info) in segment_infos.iter().enumerate() {
            let segment_mutation = self.mutations.get(&seg_idx);
            if let Some(BlockMutations {
                replaced_segments,
                deleted_blocks,
            }) = segment_mutation
            {
                let replaced = replaced_segments;
                let deleted = deleted_blocks;

                // prepare the new segment
                let mut new_segment =
                    SegmentInfo::new(seg_info.blocks.clone(), seg_info.summary.clone());

                // take away the blocks, they are being mutated
                let mut block_editor = BTreeMap::<_, _>::from_iter(
                    std::mem::take(&mut new_segment.blocks)
                        .into_iter()
                        .enumerate(),
                );

                // update replaced blocks
                for (idx, replaced_with_meta) in replaced {
                    block_editor.insert(*idx, replaced_with_meta.clone());
                    // old block meta is replaced with `replaced_with_meta` (update/partial deletion)
                    abort_operation.add_block(replaced_with_meta);
                }

                // remove deleted blocks
                for idx in deleted {
                    block_editor.remove(idx);
                }

                // assign back the mutated blocks to segment
                new_segment.blocks = block_editor.into_values().collect();
                if new_segment.blocks.is_empty() {
                    segments_editor.remove(&seg_idx);
                } else {
                    // re-calculate the segment statistics
                    let new_summary = reduce_block_metas(&new_segment.blocks, thresholds)?;
                    merge_statistics_mut(&mut table_statistics, &new_summary)?;
                    new_segment.summary = new_summary;

                    let location = location_gen.gen_segment_info_location();
                    abort_operation.add_segment(location.clone());
                    // for newly created segment, always use the latest version
                    segments_editor.insert(seg_idx, (location.clone(), SegmentInfo::VERSION));
                    serialized_segments.push(SerializedSegment {
                        raw_data: new_segment.to_bytes()?,
                        path: location,
                        segment: Arc::new(new_segment),
                    });
                }
            } else {
                // accumulate the original segment statistics
                merge_statistics_mut(&mut table_statistics, &seg_info.summary)?;
            }
        }

        for (path, new_segment, _format_version) in &self.appended_segments {
            merge_statistics_mut(&mut table_statistics, &new_segment.summary)?;
            for block_meta in &new_segment.blocks {
                abort_operation.add_block(block_meta);
            }
            abort_operation.add_segment(path.clone());
        }

        let updated_segments = segments_editor.into_values();

        // with newly appended segments
        let new_segments = self
            .appended_segments
            .iter()
            .map(|(path, _segment, format_version)| (path.clone(), *format_version))
            .chain(updated_segments)
            .collect();

        let meta = CommitMeta::new(new_segments, table_statistics, abort_operation);
        Ok((meta, serialized_segments))
    }
}
