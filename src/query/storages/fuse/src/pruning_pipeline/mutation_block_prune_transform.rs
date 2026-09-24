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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_channel::Sender;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::RangeIndexInput;
use databend_storages_common_pruner::RangePruner;

use crate::FuseTable;
use crate::operations::DeletedSegmentInfo;
use crate::operations::Mutation;
use crate::operations::MutationPartInfo;
use crate::pruning::BlockPruner;
use crate::pruning::PruningContext;
use crate::pruning_pipeline::ExtractSegmentTransform;
use crate::pruning_pipeline::PrunedCompactSegmentMeta;

/// Counters of the deletion pruning pipeline, shared by all of its processors.
#[derive(Default)]
pub struct MutationPruneStats {
    pub num_parts: AtomicUsize,
    pub num_whole_block_mutation: AtomicUsize,
    pub num_whole_segment_mutation: AtomicUsize,
}

pub struct MutationPartsMeta {
    pub parts: Vec<PartInfoPtr>,
}

impl MutationPartsMeta {
    pub fn create(parts: Vec<PartInfoPtr>) -> BlockMetaInfoPtr {
        Box::new(MutationPartsMeta { parts })
    }
}

impl Debug for MutationPartsMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutationPartsMeta").finish()
    }
}

local_block_meta_serde!(MutationPartsMeta);

#[typetag::serde(name = "mutation_parts_meta")]
impl BlockMetaInfo for MutationPartsMeta {}

/// Turns a pruned segment into deletion tasks, mirroring the delete branch of
/// `FusePruner::pruning`:
/// - a segment that the inverted filter rejects entirely is deleted as a whole;
/// - otherwise its blocks are pruned by the filter, and the blocks that the
///   inverted filter rejects are marked as whole block deletions.
pub struct MutationBlockPruneTransform {
    block_pruner: Arc<BlockPruner>,
    pruning_ctx: Arc<PruningContext>,
    inverse_range_index: Option<RangeIndex>,
    schema: TableSchemaRef,
    stats: Arc<MutationPruneStats>,
}

impl MutationBlockPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_pruner: Arc<BlockPruner>,
        pruning_ctx: Arc<PruningContext>,
        inverse_range_index: Option<RangeIndex>,
        schema: TableSchemaRef,
        stats: Arc<MutationPruneStats>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            MutationBlockPruneTransform {
                block_pruner,
                pruning_ctx,
                inverse_range_index,
                schema,
                stats,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for MutationBlockPruneTransform {
    const NAME: &'static str = "MutationBlockPruneTransform";

    async fn transform(&mut self, mut data: DataBlock) -> Result<Option<DataBlock>> {
        let Some(meta) = data
            .take_meta()
            .and_then(PrunedCompactSegmentMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "Cannot downcast meta to PrunedCompactSegmentMeta",
            ));
        };
        let (segment_location, info) = meta.segments;

        if let Some(range_index) = &self.inverse_range_index {
            let range_input =
                RangeIndexInput::new(&info.summary.col_stats, info.summary.spatial_stats.as_ref());
            if !range_index.should_keep(&range_input, None) {
                self.stats
                    .num_whole_segment_mutation
                    .fetch_add(1, Ordering::Relaxed);
                self.stats
                    .num_whole_block_mutation
                    .fetch_add(info.summary.block_count as usize, Ordering::Relaxed);
                self.stats.num_parts.fetch_add(1, Ordering::Relaxed);
                let part: PartInfoPtr = Arc::new(Box::new(Mutation::MutationDeletedSegment(
                    DeletedSegmentInfo {
                        index: segment_location.segment_idx,
                        summary: info.summary.clone(),
                    },
                )));
                return Ok(Some(DataBlock::empty_with_meta(MutationPartsMeta::create(
                    vec![part],
                ))));
            }
        }

        // Do not populate the block meta cache for deletion operations, since block metas
        // touched by deletion are not likely to be accessed soon.
        let block_metas = ExtractSegmentTransform::extract_block_metas(
            &segment_location.location.0,
            &info,
            false,
            &self.pruning_ctx.pruning_cost,
        )?;
        let projected_virtual_schema = self
            .pruning_ctx
            .project_virtual_segment_schema(info.summary.virtual_segment_schema.as_ref());
        let block_metas = self
            .block_pruner
            .pruning(segment_location, block_metas, projected_virtual_schema)
            .await?;
        if block_metas.is_empty() {
            return Ok(None);
        }

        let mut parts = Vec::with_capacity(block_metas.len());
        for (index, block_meta) in block_metas {
            let whole_block_mutation = self.inverse_range_index.as_ref().is_some_and(|index| {
                let range_input = RangeIndexInput::from_block_meta(block_meta.as_ref(), None, None);
                !index.should_keep(&range_input, None)
            });
            if whole_block_mutation {
                self.stats
                    .num_whole_block_mutation
                    .fetch_add(1, Ordering::Relaxed);
            }
            let inner_part = FuseTable::all_columns_part(
                Some(&self.schema),
                &Some(index.clone()),
                &None,
                &block_meta,
            );
            let part: PartInfoPtr =
                Arc::new(Box::new(Mutation::MutationPartInfo(MutationPartInfo {
                    index,
                    cluster_stats: block_meta.cluster_stats.clone(),
                    inner_part,
                    whole_block_mutation,
                })));
            parts.push(part);
        }
        self.stats
            .num_parts
            .fetch_add(parts.len(), Ordering::Relaxed);
        Ok(Some(DataBlock::empty_with_meta(MutationPartsMeta::create(
            parts,
        ))))
    }
}

/// Streams the deletion tasks produced by the pruning pipeline to the mutation sources.
pub struct SendMutationPartSink {
    sender: Option<Sender<Result<PartInfoPtr>>>,
}

impl SendMutationPartSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Result<PartInfoPtr>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            SendMutationPartSink {
                sender: Some(sender),
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for SendMutationPartSink {
    const NAME: &'static str = "SendMutationPartSink";

    async fn on_finish(&mut self) -> Result<()> {
        // Close the channel so that the mutation sources know all tasks have been sent.
        drop(self.sender.take());
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let Some(meta) = data_block
            .take_meta()
            .and_then(MutationPartsMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "Cannot downcast meta to MutationPartsMeta",
            ));
        };

        let Some(sender) = &self.sender else {
            return Ok(true);
        };
        for part in meta.parts {
            // The receivers are gone if the query is killed or finished early.
            if sender.send(Ok(part)).await.is_err() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
