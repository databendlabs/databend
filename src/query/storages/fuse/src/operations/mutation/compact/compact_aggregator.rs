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

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRefExt;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;

use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::AbortOperation;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::SnapshotChanges;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

pub struct CompactAggregator {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    thresholds: BlockThresholds,
    default_cluster_key_id: Option<u32>,

    // locations all the merged blocks.
    merge_blocks: HashMap<usize, BTreeMap<usize, Arc<BlockMeta>>>,
    abort_operation: AbortOperation,

    removed_segment_indexes: Vec<usize>,
    removed_statistics: Statistics,

    start_time: Instant,
}

impl CompactAggregator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Self {
        Self {
            ctx,
            dal,
            location_gen,
            default_cluster_key_id,
            merge_blocks: HashMap::new(),
            thresholds,
            abort_operation: AbortOperation::default(),
            removed_segment_indexes: vec![],
            removed_statistics: Statistics::default(),
            start_time: Instant::now(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for CompactAggregator {
    const NAME: &'static str = "CompactAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        // gather the input data.
        if let Some(meta) = data.get_owned_meta().and_then(MutationLogs::downcast_from) {
            for entry in meta.entries.into_iter() {
                match entry {
                    MutationLogEntry::ReplacedBlock { index, block_meta } => {
                        self.abort_operation.add_block(&block_meta);
                        self.merge_blocks
                            .entry(index.segment_idx)
                            .and_modify(|v| {
                                v.insert(index.block_idx, block_meta.clone());
                            })
                            .or_insert(BTreeMap::from([(index.block_idx, block_meta)]));

                        // Refresh status
                        {
                            let status = format!(
                                "compact: run compact tasks:{}, cost:{} sec",
                                self.abort_operation.blocks.len(),
                                self.start_time.elapsed().as_secs()
                            );
                            self.ctx.set_status_info(&status);
                        }
                    }
                    MutationLogEntry::CompactExtras { extras } => {
                        match self.merge_blocks.entry(extras.segment_index) {
                            Entry::Occupied(mut v) => {
                                v.get_mut().extend(extras.unchanged_blocks.into_iter());
                            }
                            Entry::Vacant(v) => {
                                v.insert(extras.unchanged_blocks);
                            }
                        }
                        self.removed_segment_indexes
                            .extend(extras.removed_segment_indexes);
                        merge_statistics_mut(
                            &mut self.removed_statistics,
                            &extras.removed_segment_summary,
                            self.default_cluster_key_id,
                        );
                    }
                    _ => return Err(ErrorCode::Internal("It's a bug.")),
                }
            }
        }
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mut serialized_segments = Vec::with_capacity(self.merge_blocks.len());
        let mut merged_statistics = Statistics::default();
        let mut replaced_segments = HashMap::with_capacity(self.merge_blocks.len());
        for (segment_idx, block_map) in std::mem::take(&mut self.merge_blocks) {
            // generate the new segment.
            let blocks: Vec<_> = block_map.into_values().collect();
            let new_summary =
                reduce_block_metas(&blocks, self.thresholds, self.default_cluster_key_id);
            merge_statistics_mut(
                &mut merged_statistics,
                &new_summary,
                self.default_cluster_key_id,
            );
            let new_segment = SegmentInfo::new(blocks, new_summary);
            let location = self.location_gen.gen_segment_info_location();
            self.abort_operation.add_segment(location.clone());
            replaced_segments.insert(segment_idx, (location.clone(), SegmentInfo::VERSION));
            serialized_segments.push(SerializedSegment {
                path: location,
                segment: Arc::new(new_segment),
            });
        }

        let start = Instant::now();
        // Refresh status
        {
            let status = format!(
                "compact: begin to write new segments:{}",
                serialized_segments.len()
            );
            self.ctx.set_status_info(&status);
        }
        // write segments, schema in segments_io is useless here.
        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.dal.clone(),
            TableSchemaRefExt::create(vec![]),
        );
        segments_io.write_segments(serialized_segments).await?;

        // Refresh status
        self.ctx.set_status_info(&format!(
            "compact: end to write new segments, cost:{} sec",
            start.elapsed().as_secs()
        ));
        let ctx = ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
            appended_segments: vec![],
            replaced_segments,
            removed_segment_indexes: self.removed_segment_indexes.clone(),
            merged_statistics,
            removed_statistics: std::mem::take(&mut self.removed_statistics),
        });
        let meta = CommitMeta::new(ctx, std::mem::take(&mut self.abort_operation));
        Ok(Some(DataBlock::empty_with_meta(Box::new(meta))))
    }
}
