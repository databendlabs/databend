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

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::OwnedSemaphorePermit;
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::F32;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures_util::future;
use log::info;

use crate::pruning::PruningContext;
use crate::pruning_pipeline::meta_info::BlockPruningResult;
use crate::pruning_pipeline::meta_info::ExtractSegmentResult;

/// CompactReadTransform Workflow:
/// 1. Read the compact segment from the location (Async)
/// 2. Prune the segment with the range pruner
pub struct AsyncBlockPruningTransform {
    pub pruning_ctx: Arc<PruningContext>,
}

impl AsyncBlockPruningTransform {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            AsyncBlockPruningTransform { pruning_ctx },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for AsyncBlockPruningTransform {
    const NAME: &'static str = "AsyncBlockPruningTransform";

    #[async_backtrace::framed]
    async fn transform(
        &mut self,
        data: DataBlock,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        if let Some(ptr) = data.get_meta() {
            if let Some(meta) = ExtractSegmentResult::downcast_from(ptr.clone()) {
                let pruning_runtime = &self.pruning_ctx.pruning_runtime;
                let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
                let limit_pruner = self.pruning_ctx.limit_pruner.clone();
                let range_pruner = self.pruning_ctx.range_pruner.clone();
                let page_pruner = self.pruning_ctx.page_pruner.clone();
                let bloom_pruner = self.pruning_ctx.bloom_pruner.clone();
                let inverted_index_pruner = self.pruning_ctx.inverted_index_pruner.clone();

                let block_meta_indexes = self.internal_column_pruning(&meta.block_metas);
                let mut block_meta_indexes = block_meta_indexes.into_iter();
                let pruning_tasks = std::iter::from_fn(|| {
                    // check limit speculatively
                    if limit_pruner.exceeded() {
                        return None;
                    }

                    type BlockPruningFutureReturn = Pin<
                        Box<
                            dyn Future<Output = databend_common_exception::Result<BlockPruneResult>>
                                + Send,
                        >,
                    >;
                    type BlockPruningFuture = Box<
                        dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn
                            + Send
                            + 'static,
                    >;

                    block_meta_indexes.next().map(|(block_idx, block_meta)| {
                        let mut prune_result = BlockPruneResult::new(
                            block_idx,
                            block_meta.location.0.clone(),
                            false,
                            None,
                            None,
                        );
                        let block_meta = block_meta.clone();
                        let row_count = block_meta.row_count;
                        let should_keep = range_pruner
                            .should_keep(&block_meta.col_stats, Some(&block_meta.col_metas));
                        if should_keep {
                            // not pruned by block zone map index,
                            let bloom_pruner = bloom_pruner.clone();
                            let limit_pruner = limit_pruner.clone();
                            let page_pruner = page_pruner.clone();
                            let inverted_index_pruner = inverted_index_pruner.clone();
                            let block_location = block_meta.location.clone();
                            let index_location = block_meta.bloom_filter_index_location.clone();
                            let index_size = block_meta.bloom_filter_index_size;
                            let column_ids =
                                block_meta.col_metas.keys().cloned().collect::<Vec<_>>();

                            let v: BlockPruningFuture =
                                Box::new(move |permit: OwnedSemaphorePermit| {
                                    Box::pin(async move {
                                        let _permit = permit;
                                        let keep = if let Some(bloom_pruner) = bloom_pruner {
                                            let keep_by_bloom = bloom_pruner
                                                .should_keep(
                                                    &index_location,
                                                    index_size,
                                                    &block_meta.col_stats,
                                                    column_ids,
                                                    &block_meta,
                                                )
                                                .await;

                                            let keep = keep_by_bloom
                                                && limit_pruner.within_limit(row_count);
                                            keep
                                        } else {
                                            limit_pruner.within_limit(row_count)
                                        };
                                        if keep {
                                            let (keep, range) =
                                                page_pruner.should_keep(&block_meta.cluster_stats);

                                            prune_result.keep = keep;
                                            prune_result.range = range;

                                            if keep {
                                                if let Some(inverted_index_pruner) =
                                                    inverted_index_pruner
                                                {
                                                    info!("using inverted index");
                                                    let matched_rows = inverted_index_pruner
                                                        .should_keep(&block_location.0, row_count)
                                                        .await?;
                                                    prune_result.keep = matched_rows.is_some();
                                                    prune_result.matched_rows = matched_rows;
                                                }
                                            }
                                        }
                                        Ok(prune_result)
                                    })
                                });
                            v
                        } else {
                            let v: BlockPruningFuture =
                                Box::new(move |permit: OwnedSemaphorePermit| {
                                    Box::pin(async move {
                                        let _permit = permit;
                                        Ok(prune_result)
                                    })
                                });
                            v
                        }
                    })
                });

                let join_handlers = pruning_runtime
                    .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), pruning_tasks)
                    .await?;

                let joint = future::try_join_all(join_handlers).await.map_err(|e| {
                    ErrorCode::StorageOther(format!("block pruning failure, {}", e))
                })?;

                let mut result = Vec::with_capacity(joint.len());
                let block_num = meta.block_metas.len();
                for prune_result in joint {
                    let prune_result = prune_result?;
                    if prune_result.keep {
                        let block = meta.block_metas[prune_result.block_idx].clone();

                        debug_assert_eq!(prune_result.block_location, block.location.0);

                        result.push((
                            Some(BlockMetaIndex {
                                segment_idx: meta.segment_location.segment_idx,
                                block_idx: prune_result.block_idx,
                                range: prune_result.range,
                                page_size: block.page_size() as usize,
                                block_id: block_id_in_segment(block_num, prune_result.block_idx),
                                block_location: prune_result.block_location.clone(),
                                segment_location: meta.segment_location.location.0.clone(),
                                snapshot_location: meta.segment_location.snapshot_loc.clone(),
                                matched_rows: prune_result.matched_rows.clone(),
                            }),
                            block,
                        ))
                    }
                }

                return Ok(Some(DataBlock::empty_with_meta(
                    BlockPruningResult::create(result),
                )));
            }
        }

        Err(ErrorCode::Internal(
            "Cannot downcast meta to ExtractSegmentResult",
        ))
    }
}

// result of block pruning
struct BlockPruneResult {
    // the block index in segment
    block_idx: usize,
    // the location of the block
    block_location: String,
    // whether keep the block after pruning
    keep: bool,
    // the page ranges should keeped in the block
    range: Option<Range<usize>>,
    // the matched rows and scores should keeped in the block
    // only used by inverted index search
    matched_rows: Option<Vec<(usize, Option<F32>)>>,
}

impl BlockPruneResult {
    fn new(
        block_idx: usize,
        block_location: String,
        keep: bool,
        range: Option<Range<usize>>,
        matched_rows: Option<Vec<(usize, Option<F32>)>>,
    ) -> Self {
        Self {
            block_idx,
            keep,
            range,
            block_location,
            matched_rows,
        }
    }
}

impl AsyncBlockPruningTransform {
    /// Apply internal column pruning at block level
    fn internal_column_pruning(
        &self,
        block_metas: &[Arc<BlockMeta>],
    ) -> Vec<(usize, Arc<BlockMeta>)> {
        match &self.pruning_ctx.internal_column_pruner {
            Some(pruner) => block_metas
                .iter()
                .enumerate()
                .filter(|(_, block_meta)| {
                    pruner.should_keep(BLOCK_NAME_COL_NAME, &block_meta.location.0)
                })
                .map(|(index, block_meta)| (index, block_meta.clone()))
                .collect(),
            None => block_metas
                .iter()
                .enumerate()
                .map(|(index, block_meta)| (index, block_meta.clone()))
                .collect(),
        }
    }
}
