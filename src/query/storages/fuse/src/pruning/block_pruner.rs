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
use std::time::Instant;

use databend_common_catalog::plan::block_id_in_segment;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::*;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures_util::future;
use log::info;
use tokio::sync::OwnedSemaphorePermit;

use super::SegmentLocation;
use crate::pruning::PruningContext;
use crate::pruning::PruningCostKind;

pub struct BlockPruner {
    pub pruning_ctx: Arc<PruningContext>,
}

impl BlockPruner {
    pub fn create(pruning_ctx: Arc<PruningContext>) -> Result<BlockPruner> {
        Ok(BlockPruner { pruning_ctx })
    }

    #[async_backtrace::framed]
    pub async fn pruning(
        &self,
        segment_location: SegmentLocation,
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        // Apply internal column pruning.
        let block_meta_indexes = self.internal_column_pruning(&block_metas);

        // Apply block pruning.
        if self.pruning_ctx.bloom_pruner.is_some()
            || self.pruning_ctx.inverted_index_pruner.is_some()
            || self.pruning_ctx.virtual_column_pruner.is_some()
        {
            // async pruning with bloom index, inverted index or virtual columns.
            self.block_pruning(segment_location, block_metas, block_meta_indexes)
                .await
        } else {
            // sync pruning without a bloom index, inverted index and virtual columns.
            self.block_pruning_sync(segment_location, block_metas, block_meta_indexes)
        }
    }

    /// Apply internal column pruning.
    pub fn internal_column_pruning(
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

    // async pruning with bloom index, inverted index or virtual columns.
    #[async_backtrace::framed]
    pub async fn block_pruning(
        &self,
        segment_location: SegmentLocation,
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
        block_meta_indexes: Vec<(usize, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_cost = self.pruning_ctx.pruning_cost.clone();
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();
        let bloom_pruner = self.pruning_ctx.bloom_pruner.clone();
        let inverted_index_pruner = self.pruning_ctx.inverted_index_pruner.clone();
        let virtual_column_pruner = self.pruning_ctx.virtual_column_pruner.clone();

        let mut block_meta_indexes = block_meta_indexes.into_iter();
        let pruning_tasks = std::iter::from_fn(|| {
            // check limit speculatively
            if limit_pruner.exceeded() {
                return None;
            }

            type BlockPruningFutureReturn =
                Pin<Box<dyn Future<Output = Result<BlockPruneResult>> + Send>>;
            type BlockPruningFuture =
                Box<dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn + Send + 'static>;

            let pruning_stats = pruning_stats.clone();
            block_meta_indexes.next().map(|(block_idx, block_meta)| {
                // Perf.
                {
                    metrics_inc_blocks_range_pruning_before(1);
                    metrics_inc_bytes_block_range_pruning_before(block_meta.block_size);

                    pruning_stats.set_blocks_range_pruning_before(1);
                }

                let mut prune_result =
                    BlockPruneResult::new(block_idx, block_meta.location.0.clone());
                let block_meta = block_meta.clone();
                let row_count = block_meta.row_count;
                prune_result.keep = pruning_cost.measure(PruningCostKind::BlocksRange, || {
                    range_pruner.should_keep(&block_meta.col_stats, Some(&block_meta.col_metas))
                });
                if prune_result.keep {
                    // Perf.
                    {
                        metrics_inc_blocks_range_pruning_after(1);
                        metrics_inc_bytes_block_range_pruning_after(block_meta.block_size);

                        pruning_stats.set_blocks_range_pruning_after(1);
                    }

                    // not pruned by block zone map index,
                    let bloom_pruner = bloom_pruner.clone();
                    let limit_pruner = limit_pruner.clone();
                    let page_pruner = page_pruner.clone();
                    let inverted_index_pruner = inverted_index_pruner.clone();
                    let virtual_column_pruner = virtual_column_pruner.clone();
                    let block_location = block_meta.location.clone();
                    let index_location = block_meta.bloom_filter_index_location.clone();
                    let index_size = block_meta.bloom_filter_index_size;
                    let column_ids = block_meta.col_metas.keys().cloned().collect::<Vec<_>>();

                    let pruning_cost = pruning_cost.clone();
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            if let Some(bloom_pruner) = bloom_pruner {
                                // Perf.
                                {
                                    metrics_inc_blocks_bloom_pruning_before(1);
                                    metrics_inc_bytes_block_bloom_pruning_before(
                                        block_meta.block_size,
                                    );

                                    pruning_stats.set_blocks_bloom_pruning_before(1);
                                }

                                let keep_by_bloom = pruning_cost
                                    .measure_async(
                                        PruningCostKind::BlocksBloom,
                                        bloom_pruner.should_keep(
                                            &index_location,
                                            index_size,
                                            &block_meta.col_stats,
                                            column_ids,
                                            &block_meta.as_ref().into(),
                                        ),
                                    )
                                    .await;
                                prune_result.keep =
                                    keep_by_bloom && limit_pruner.within_limit(row_count);
                                if prune_result.keep {
                                    // Perf.
                                    {
                                        metrics_inc_blocks_bloom_pruning_after(1);
                                        metrics_inc_bytes_block_bloom_pruning_after(
                                            block_meta.block_size,
                                        );

                                        pruning_stats.set_blocks_bloom_pruning_after(1);
                                    }
                                }
                            } else {
                                prune_result.keep = limit_pruner.within_limit(row_count);
                            }
                            if prune_result.keep {
                                let (keep, range) =
                                    page_pruner.should_keep(&block_meta.cluster_stats);

                                prune_result.keep = keep;
                                prune_result.range = range;
                            }
                            if prune_result.keep {
                                if let Some(inverted_index_pruner) = inverted_index_pruner {
                                    // Perf.
                                    {
                                        metrics_inc_blocks_inverted_index_pruning_before(1);
                                        metrics_inc_bytes_block_inverted_index_pruning_before(
                                            block_meta.block_size,
                                        );

                                        pruning_stats.set_blocks_inverted_index_pruning_before(1);
                                    }
                                    let matched_rows = pruning_cost
                                        .measure_async(
                                            PruningCostKind::BlocksInverted,
                                            inverted_index_pruner
                                                .should_keep(&block_location.0, row_count),
                                        )
                                        .await?;
                                    if let Some((rows, scores)) = matched_rows {
                                        prune_result.keep = true;
                                        prune_result.matched_rows = Some(rows);
                                        prune_result.matched_scores = scores;
                                    } else {
                                        prune_result.keep = false;
                                    }

                                    if prune_result.keep {
                                        // Perf.
                                        {
                                            metrics_inc_blocks_inverted_index_pruning_after(1);
                                            metrics_inc_bytes_block_inverted_index_pruning_after(
                                                block_meta.block_size,
                                            );
                                            pruning_stats
                                                .set_blocks_inverted_index_pruning_after(1);
                                        }
                                    }
                                }
                            }
                            if prune_result.keep {
                                if let Some(virtual_column_pruner) = virtual_column_pruner {
                                    // Check whether can read virtual columns,
                                    // and ignore the source columns.
                                    let virtual_block_meta = virtual_column_pruner
                                        .prune_virtual_columns(&block_meta.virtual_block_meta)
                                        .await?;
                                    prune_result.virtual_block_meta = virtual_block_meta;
                                }
                            }
                            Ok(prune_result)
                        })
                    });
                    v
                } else {
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            Ok(prune_result)
                        })
                    });
                    v
                }
            })
        });

        let start = Instant::now();

        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), pruning_tasks)
            .await?;

        let joint = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        let mut result = Vec::with_capacity(joint.len());
        let block_num = block_metas.len();
        for prune_result in joint {
            let prune_result = prune_result?;
            if prune_result.keep {
                let block = block_metas[prune_result.block_idx].clone();

                debug_assert_eq!(prune_result.block_location, block.location.0);

                result.push((
                    BlockMetaIndex {
                        segment_idx: segment_location.segment_idx,
                        block_idx: prune_result.block_idx,
                        range: prune_result.range,
                        page_size: block.page_size() as usize,
                        block_id: block_id_in_segment(block_num, prune_result.block_idx),
                        block_location: prune_result.block_location.clone(),
                        segment_location: segment_location.location.0.clone(),
                        snapshot_location: segment_location.snapshot_loc.clone(),
                        matched_rows: prune_result.matched_rows.clone(),
                        matched_scores: prune_result.matched_scores.clone(),
                        vector_scores: None,
                        virtual_block_meta: prune_result.virtual_block_meta.clone(),
                    },
                    block,
                ))
            }
        }

        // Perf
        let elapsed = start.elapsed().as_millis() as u64;
        {
            metrics_inc_pruning_milliseconds(elapsed);
        }
        info!("[FUSE-PRUNER] block prune elapsed: {elapsed}");

        Ok(result)
    }

    pub fn block_pruning_sync(
        &self,
        segment_location: SegmentLocation,
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
        block_meta_indexes: Vec<(usize, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_cost = self.pruning_ctx.pruning_cost.clone();
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let start = Instant::now();

        let mut result = Vec::with_capacity(block_meta_indexes.len());
        let block_num = block_metas.len();
        for (block_idx, block_meta) in block_meta_indexes {
            // Perf.
            {
                metrics_inc_blocks_range_pruning_before(1);
                metrics_inc_bytes_block_range_pruning_before(block_meta.block_size);

                pruning_stats.set_blocks_range_pruning_before(1);
            }

            // check limit speculatively
            if limit_pruner.exceeded() {
                break;
            }
            let row_count = block_meta.row_count;
            let keep_by_range = pruning_cost.measure(PruningCostKind::BlocksRange, || {
                range_pruner.should_keep(&block_meta.col_stats, Some(&block_meta.col_metas))
            });
            if keep_by_range && limit_pruner.within_limit(row_count) {
                // Perf.
                {
                    metrics_inc_blocks_range_pruning_after(1);
                    metrics_inc_bytes_block_range_pruning_after(block_meta.block_size);

                    pruning_stats.set_blocks_range_pruning_after(1);
                }

                let (keep, range) = page_pruner.should_keep(&block_meta.cluster_stats);
                if keep {
                    result.push((
                        BlockMetaIndex {
                            segment_idx: segment_location.segment_idx,
                            block_idx,
                            range,
                            page_size: block_meta.page_size() as usize,
                            block_id: block_id_in_segment(block_num, block_idx),
                            block_location: block_meta.as_ref().location.0.clone(),
                            segment_location: segment_location.location.0.clone(),
                            snapshot_location: segment_location.snapshot_loc.clone(),
                            matched_rows: None,
                            matched_scores: None,
                            vector_scores: None,
                            virtual_block_meta: None,
                        },
                        block_meta.clone(),
                    ))
                }
            }
        }

        // Perf
        let elapsed = start.elapsed().as_millis() as u64;
        {
            metrics_inc_pruning_milliseconds(elapsed);
        }
        info!("[FUSE-PRUNER] sync block prune elapsed: {elapsed}");

        Ok(result)
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
    // the matched rows in the block (aligned with `matched_scores` when present)
    // only used by inverted index search
    matched_rows: Option<Vec<usize>>,
    // optional scores for the matched rows
    matched_scores: Option<Vec<F32>>,
    // the optional block meta of virtual columns
    virtual_block_meta: Option<VirtualBlockMetaIndex>,
}

impl BlockPruneResult {
    fn new(block_idx: usize, block_location: String) -> Self {
        Self {
            block_idx,
            block_location,
            keep: false,
            range: None,
            matched_rows: None,
            matched_scores: None,
            virtual_block_meta: None,
        }
    }
}
