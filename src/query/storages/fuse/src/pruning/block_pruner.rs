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
use databend_storages_common_pruner::RangeIndexInput;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures_util::future;
use log::info;
use tokio::sync::OwnedSemaphorePermit;

use super::SegmentLocation;
use crate::io::GranulePruningReadContext;
use crate::io::granule_index::GRANULE_BLOOM_INDEX_NAME;
use crate::io::num_granules_of;
use crate::pruning::PruningContext;
use crate::pruning::PruningCostKind;
use crate::pruning::RuntimeStatsPruner;

pub struct BlockPruner {
    pub pruning_ctx: Arc<PruningContext>,
}

pub(crate) struct GranulePrunedBlock {
    pub(crate) block_meta_index: BlockMetaIndex,
    pub(crate) block_meta: Arc<BlockMeta>,
    pub(crate) granule_bloom_applied: bool,
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
            || self.pruning_ctx.sparse_granule_index_pruner.is_some()
            || !self.pruning_ctx.granule_index_pruners.is_empty()
            || self.pruning_ctx.inverted_index_pruner.is_some()
            || self.pruning_ctx.spatial_index_pruner.is_some()
            || self.pruning_ctx.virtual_column_pruner.is_some()
        {
            // async pruning with bloom index, inverted index or virtual columns.
            self.block_pruning(segment_location, block_metas, block_meta_indexes, None)
                .await
        } else {
            // sync pruning without a bloom index, inverted index and virtual columns.
            self.block_pruning_sync(segment_location, block_metas, block_meta_indexes, None)
        }
    }

    #[async_backtrace::framed]
    pub async fn refine_pruning(
        &self,
        block_metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if self.pruning_ctx.bloom_pruner.is_none()
            && self.pruning_ctx.sparse_granule_index_pruner.is_none()
            && self.pruning_ctx.granule_index_pruners.is_empty()
            && self.pruning_ctx.inverted_index_pruner.is_none()
            && self.pruning_ctx.spatial_index_pruner.is_none()
            && self.pruning_ctx.virtual_column_pruner.is_none()
        {
            return Ok(block_metas);
        }

        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let pruning_ctx = self.pruning_ctx.clone();

        type BlockPruningFutureReturn =
            Pin<Box<dyn Future<Output = Result<Option<(BlockMetaIndex, Arc<BlockMeta>)>>> + Send>>;
        type BlockPruningFuture =
            Box<dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn + Send + 'static>;

        let pruning_tasks = block_metas
            .into_iter()
            .map(|(mut block_meta_index, block_meta)| {
                let pruning_ctx = pruning_ctx.clone();

                let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                    Box::pin(async move {
                        let _permit = permit;
                        let prune_result =
                            BlockPruneResult::from_block_meta_index(&block_meta_index);
                        let prune_result = Self::prune_after_range(
                            pruning_ctx,
                            prune_result,
                            block_meta.clone(),
                            block_meta.row_count,
                            true,
                        )
                        .await?;

                        let keep = prune_result.keep;
                        block_meta_index = prune_result.apply_to_block_meta_index(block_meta_index);

                        Ok(keep.then_some((block_meta_index, block_meta)))
                    })
                });
                v
            });

        let start = Instant::now();

        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), pruning_tasks)
            .await?;

        let joint = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        let result = joint
            .into_iter()
            .filter_map(|prune_result| prune_result.transpose())
            .collect::<Result<Vec<_>>>()?;

        let elapsed = start.elapsed().as_millis() as u64;
        metrics_inc_pruning_milliseconds(elapsed);
        info!("[FUSE-PRUNER] refine block prune elapsed: {elapsed}");

        Ok(result)
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
        runtime_stats_pruner: Option<Arc<RuntimeStatsPruner>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_cost = self.pruning_ctx.pruning_cost.clone();
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let pruning_ctx = self.pruning_ctx.clone();

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
            let runtime_stats_pruner = runtime_stats_pruner.clone();
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
                let range_input = RangeIndexInput::from_block_meta(block_meta.as_ref());
                prune_result.keep = pruning_cost.measure(PruningCostKind::BlocksRange, || {
                    range_pruner.should_keep(&range_input, Some(&block_meta.col_metas))
                });
                if prune_result.keep {
                    // Perf.
                    {
                        metrics_inc_blocks_range_pruning_after(1);
                        metrics_inc_bytes_block_range_pruning_after(block_meta.block_size);

                        pruning_stats.set_blocks_range_pruning_after(1);
                    }

                    if let Some(pruner) = runtime_stats_pruner.as_ref() {
                        if pruner.should_prune(Some(&block_meta.col_stats), row_count as usize) {
                            prune_result.keep = false;
                        }
                    }
                }

                if prune_result.keep {
                    // not pruned by block zone map index,
                    let pruning_ctx = pruning_ctx.clone();
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            Self::prune_after_range(
                                pruning_ctx,
                                prune_result,
                                block_meta,
                                row_count,
                                false,
                            )
                            .await
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
                        granule_ranges: prune_result.granule_ranges.clone(),
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

    /// Run range, runtime-statistics, and granule-index pruning synchronously.
    ///
    /// This entry point is used by the pruning pipeline so the CPU work and the
    /// blocking side of granule-index reads run on a pipeline executor thread.
    /// Actual OpenDAL reads are still dispatched by `OperatorRangeReader` to the
    /// global I/O runtime.
    pub(crate) fn range_and_granule_pruning(
        &self,
        segment_location: SegmentLocation,
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
        runtime_stats_pruner: Option<Arc<RuntimeStatsPruner>>,
    ) -> Result<Vec<GranulePrunedBlock>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_cost = self.pruning_ctx.pruning_cost.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let block_meta_indexes = self.internal_column_pruning(&block_metas);
        let block_num = block_metas.len();
        let start = Instant::now();
        let mut result = Vec::with_capacity(block_meta_indexes.len());

        for (block_idx, block_meta) in block_meta_indexes {
            metrics_inc_blocks_range_pruning_before(1);
            metrics_inc_bytes_block_range_pruning_before(block_meta.block_size);
            pruning_stats.set_blocks_range_pruning_before(1);

            let row_count = block_meta.row_count;
            let range_input = RangeIndexInput::from_block_meta(block_meta.as_ref());
            let keep_by_range = pruning_cost.measure(PruningCostKind::BlocksRange, || {
                range_pruner.should_keep(&range_input, Some(&block_meta.col_metas))
            });
            if !keep_by_range {
                continue;
            }

            metrics_inc_blocks_range_pruning_after(1);
            metrics_inc_bytes_block_range_pruning_after(block_meta.block_size);
            pruning_stats.set_blocks_range_pruning_after(1);

            if let Some(pruner) = runtime_stats_pruner.as_ref() {
                if pruner.should_prune(Some(&block_meta.col_stats), row_count as usize) {
                    continue;
                }
            }

            let outcome = Self::prune_granule_indexes(&self.pruning_ctx, &block_meta, None);
            if outcome.granule_ranges.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }

            result.push(GranulePrunedBlock {
                block_meta_index: BlockMetaIndex {
                    segment_idx: segment_location.segment_idx,
                    block_idx,
                    range: None,
                    granule_ranges: outcome.granule_ranges,
                    page_size: block_meta.page_size() as usize,
                    block_id: block_id_in_segment(block_num, block_idx),
                    block_location: block_meta.location.0.clone(),
                    segment_location: segment_location.location.0.clone(),
                    snapshot_location: segment_location.snapshot_loc.clone(),
                    matched_rows: None,
                    matched_scores: None,
                    vector_scores: None,
                    virtual_block_meta: None,
                },
                block_meta,
                granule_bloom_applied: outcome.granule_bloom_applied,
            });
        }

        let elapsed = start.elapsed().as_millis() as u64;
        metrics_inc_pruning_milliseconds(elapsed);
        info!("[FUSE-PRUNER] range and granule prune elapsed: {elapsed}");
        Ok(result)
    }

    /// Apply the ordinary asynchronous block indexes after pipeline-local
    /// range and granule pruning.
    pub(crate) async fn async_block_index_pruning(
        &self,
        blocks: Vec<GranulePrunedBlock>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let start = Instant::now();
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let pruning_ctx = self.pruning_ctx.clone();

        type PruningFutureReturn =
            Pin<Box<dyn Future<Output = Result<Option<(BlockMetaIndex, Arc<BlockMeta>)>>> + Send>>;
        type PruningFuture =
            Box<dyn FnOnce(OwnedSemaphorePermit) -> PruningFutureReturn + Send + 'static>;

        let tasks = blocks.into_iter().map(|block| {
            let pruning_ctx = pruning_ctx.clone();
            let task: PruningFuture = Box::new(move |permit| {
                Box::pin(async move {
                    let _permit = permit;
                    let mut prune_result =
                        BlockPruneResult::from_block_meta_index(&block.block_meta_index);
                    prune_result = Self::prune_after_granule_indexes(
                        prune_result,
                        &block.block_meta,
                        block.block_meta.row_count,
                        false,
                        block.granule_bloom_applied,
                        pruning_ctx,
                    )
                    .await?;
                    Ok(prune_result.keep.then(|| {
                        (
                            prune_result.apply_to_block_meta_index(block.block_meta_index),
                            block.block_meta,
                        )
                    }))
                })
            });
            task
        });

        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), tasks)
            .await?;
        let joined = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block index pruning failure, {e}")))?;
        let result = joined
            .into_iter()
            .filter_map(|result| result.transpose())
            .collect();
        let elapsed = start.elapsed().as_millis() as u64;
        metrics_inc_pruning_milliseconds(elapsed);
        info!("[FUSE-PRUNER] async block index prune elapsed: {elapsed}");
        result
    }

    async fn prune_after_range(
        pruning_ctx: Arc<PruningContext>,
        mut prune_result: BlockPruneResult,
        block_meta: Arc<BlockMeta>,
        row_count: u64,
        limit_before_bloom: bool,
    ) -> Result<BlockPruneResult> {
        if !prune_result.keep {
            return Ok(prune_result);
        }

        let outcome = Self::prune_granule_indexes(
            &pruning_ctx,
            &block_meta,
            prune_result.granule_ranges.take(),
        );
        prune_result.granule_ranges = outcome.granule_ranges;
        if prune_result
            .granule_ranges
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            prune_result.keep = false;
            return Ok(prune_result);
        }

        Self::prune_after_granule_indexes(
            prune_result,
            &block_meta,
            row_count,
            limit_before_bloom,
            outcome.granule_bloom_applied,
            pruning_ctx,
        )
        .await
    }

    fn prune_granule_indexes(
        pruning_ctx: &PruningContext,
        block_meta: &BlockMeta,
        input_ranges: Option<Vec<Range<usize>>>,
    ) -> GranulePruneOutcome {
        let sparse_pruner = pruning_ctx.sparse_granule_index_pruner.as_ref();
        let index_pruners = &pruning_ctx.granule_index_pruners;
        let has_granule_pipeline =
            sparse_pruner.is_some() || !index_pruners.is_empty() || input_ranges.is_some();
        let Some(granule_index) = block_meta
            .granule_index
            .as_ref()
            .filter(|_| has_granule_pipeline)
        else {
            return GranulePruneOutcome {
                granule_ranges: input_ranges,
                granule_bloom_applied: false,
            };
        };

        let num_granules = num_granules_of(
            block_meta.row_count as usize,
            granule_index.granule_rows as usize,
        );
        if num_granules == 0 {
            return GranulePruneOutcome {
                granule_ranges: input_ranges,
                granule_bloom_applied: false,
            };
        }

        #[allow(clippy::single_range_in_vec_init)]
        let mut survivors = input_ranges.unwrap_or_else(|| vec![0..num_granules]);
        let pruning_cost = &pruning_ctx.pruning_cost;

        if let Some(pruner) = sparse_pruner {
            match pruning_cost.measure(PruningCostKind::BlocksRange, || {
                pruner.select_granule_ranges(block_meta, granule_index, &survivors)
            }) {
                Ok(ranges) => survivors = ranges,
                Err(e) => log::warn!(
                    "[FUSE-PRUNER] sparse granule pruning failed for {}, preserving input ranges: {e}",
                    block_meta.location.0
                ),
            }
            if survivors.is_empty() {
                return GranulePruneOutcome {
                    granule_ranges: Some(survivors),
                    granule_bloom_applied: false,
                };
            }
        }

        let mut granule_bloom_applied = false;
        if !index_pruners.is_empty() {
            let mark_names = index_pruners
                .iter()
                .flat_map(|pruner| pruner.required_marks())
                .collect::<Vec<_>>();
            let read_ctx = match GranulePruningReadContext::load(
                &pruning_ctx.dal,
                &pruning_ctx.granule_read_settings,
                &granule_index.offsets,
                &mark_names,
                num_granules,
            ) {
                Ok(ctx) => ctx,
                Err(e) => {
                    log::warn!(
                        "[FUSE-PRUNER] granule marks load failed for {}, preserving input ranges: {e}",
                        block_meta.location.0
                    );
                    return GranulePruneOutcome {
                        granule_ranges: Some(survivors),
                        granule_bloom_applied: false,
                    };
                }
            };

            for pruner in index_pruners {
                match pruning_cost.measure(PruningCostKind::BlocksRange, || {
                    pruner.prune_granules(block_meta, &survivors, &read_ctx)
                }) {
                    Ok(ranges) => {
                        granule_bloom_applied |= pruner.name() == GRANULE_BLOOM_INDEX_NAME;
                        survivors = ranges;
                    }
                    Err(e) => log::warn!(
                        "[FUSE-PRUNER] granule index {} failed for {}, preserving input ranges: {e}",
                        pruner.name(),
                        block_meta.location.0
                    ),
                }
                if survivors.is_empty() {
                    break;
                }
            }
        }

        GranulePruneOutcome {
            granule_ranges: Some(survivors),
            granule_bloom_applied,
        }
    }

    async fn prune_after_granule_indexes(
        mut prune_result: BlockPruneResult,
        block_meta: &BlockMeta,
        row_count: u64,
        limit_before_bloom: bool,
        granule_bloom_applied: bool,
        pruning_ctx: Arc<PruningContext>,
    ) -> Result<BlockPruneResult> {
        let pruning_stats = pruning_ctx.pruning_stats.clone();
        let pruning_cost = pruning_ctx.pruning_cost.clone();
        let limit_pruner = pruning_ctx.limit_pruner.clone();
        let bloom_pruner = pruning_ctx.bloom_pruner.clone();
        let inverted_index_pruner = pruning_ctx.inverted_index_pruner.clone();
        let virtual_column_pruner = pruning_ctx.virtual_column_pruner.clone();
        let spatial_index_pruner = pruning_ctx.spatial_index_pruner.clone();

        if limit_before_bloom {
            prune_result.keep = limit_pruner.within_limit(row_count);
        }

        if prune_result.keep {
            if granule_bloom_applied {
                if !limit_before_bloom {
                    prune_result.keep = limit_pruner.within_limit(row_count);
                }
            } else if let Some(bloom_pruner) = bloom_pruner {
                metrics_inc_blocks_bloom_pruning_before(1);
                metrics_inc_bytes_block_bloom_pruning_before(block_meta.block_size);
                pruning_stats.set_blocks_bloom_pruning_before(1);

                let column_ids = block_meta.col_metas.keys().cloned().collect::<Vec<_>>();
                let keep_by_bloom = pruning_cost
                    .measure_async(
                        PruningCostKind::BlocksBloom,
                        bloom_pruner.should_keep(
                            &block_meta.bloom_filter_index_location,
                            block_meta.bloom_filter_index_size,
                            &block_meta.col_stats,
                            column_ids,
                            &block_meta.into(),
                        ),
                    )
                    .await;
                prune_result.keep = if limit_before_bloom {
                    keep_by_bloom
                } else {
                    keep_by_bloom && limit_pruner.within_limit(row_count)
                };

                if prune_result.keep {
                    metrics_inc_blocks_bloom_pruning_after(1);
                    metrics_inc_bytes_block_bloom_pruning_after(block_meta.block_size);
                    pruning_stats.set_blocks_bloom_pruning_after(1);
                }
            } else if !limit_before_bloom {
                prune_result.keep = limit_pruner.within_limit(row_count);
            }
        }

        if prune_result.keep {
            if let Some(inverted_index_pruner) = inverted_index_pruner {
                metrics_inc_blocks_inverted_index_pruning_before(1);
                metrics_inc_bytes_block_inverted_index_pruning_before(block_meta.block_size);
                pruning_stats.set_blocks_inverted_index_pruning_before(1);

                let matched_rows = pruning_cost
                    .measure_async(
                        PruningCostKind::BlocksInverted,
                        inverted_index_pruner.should_keep(&block_meta.location.0, row_count),
                    )
                    .await?;

                if let Some((rows, scores)) = matched_rows {
                    prune_result.matched_rows = Some(rows);
                    prune_result.matched_scores = scores;
                } else {
                    prune_result.keep = false;
                }

                if prune_result.keep {
                    metrics_inc_blocks_inverted_index_pruning_after(1);
                    metrics_inc_bytes_block_inverted_index_pruning_after(block_meta.block_size);
                    pruning_stats.set_blocks_inverted_index_pruning_after(1);
                }
            }
        }

        if prune_result.keep {
            if let (Some(spatial_index_pruner), Some(_)) = (
                spatial_index_pruner,
                block_meta.spatial_index_location.as_ref(),
            ) {
                metrics_inc_blocks_spatial_index_pruning_before(1);
                metrics_inc_bytes_block_spatial_index_pruning_before(block_meta.block_size);
                pruning_stats.set_blocks_spatial_index_pruning_before(1);

                let start = Instant::now();
                let should_prune = pruning_cost
                    .measure_async(
                        PruningCostKind::BlocksSpatial,
                        spatial_index_pruner.should_prune(block_meta),
                    )
                    .await?;
                let elapsed = start.elapsed();
                metrics_inc_block_spatial_index_pruning_milliseconds(elapsed.as_millis() as u64);
                prune_result.keep = !should_prune;

                if prune_result.keep {
                    metrics_inc_blocks_spatial_index_pruning_after(1);
                    metrics_inc_bytes_block_spatial_index_pruning_after(block_meta.block_size);
                    pruning_stats.set_blocks_spatial_index_pruning_after(1);
                }
            }
        }

        if prune_result.keep {
            if let Some(virtual_column_pruner) = virtual_column_pruner {
                prune_result.virtual_block_meta = virtual_column_pruner
                    .prune_virtual_columns(&block_meta.virtual_block_meta)
                    .await?;
            }
        }

        Ok(prune_result)
    }

    pub fn block_pruning_sync(
        &self,
        segment_location: SegmentLocation,
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
        block_meta_indexes: Vec<(usize, Arc<BlockMeta>)>,
        runtime_stats_pruner: Option<Arc<RuntimeStatsPruner>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_cost = self.pruning_ctx.pruning_cost.clone();
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();

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
            let range_input = RangeIndexInput::from_block_meta(block_meta.as_ref());
            let keep_by_range = pruning_cost.measure(PruningCostKind::BlocksRange, || {
                range_pruner.should_keep(&range_input, Some(&block_meta.col_metas))
            });
            if keep_by_range && limit_pruner.within_limit(row_count) {
                // Perf.
                {
                    metrics_inc_blocks_range_pruning_after(1);
                    metrics_inc_bytes_block_range_pruning_after(block_meta.block_size);

                    pruning_stats.set_blocks_range_pruning_after(1);
                }

                if let Some(pruner) = runtime_stats_pruner.as_ref() {
                    if pruner.should_prune(Some(&block_meta.col_stats), row_count as usize) {
                        continue;
                    }
                }

                result.push((
                    BlockMetaIndex {
                        segment_idx: segment_location.segment_idx,
                        block_idx,
                        range: None,
                        granule_ranges: None,
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

        // Perf
        let elapsed = start.elapsed().as_millis() as u64;
        {
            metrics_inc_pruning_milliseconds(elapsed);
        }
        info!("[FUSE-PRUNER] sync block prune elapsed: {elapsed}");

        Ok(result)
    }
}

struct GranulePruneOutcome {
    granule_ranges: Option<Vec<Range<usize>>>,
    granule_bloom_applied: bool,
}

// result of block pruning
struct BlockPruneResult {
    // the block index in segment
    block_idx: usize,
    // the location of the block
    block_location: String,
    // whether keep the block after pruning
    keep: bool,
    // the page ranges should be kept in the block
    range: Option<Range<usize>>,
    // the surviving sparse-granule-index granule runs (maximally coalesced) for the cluster-key predicate
    granule_ranges: Option<Vec<Range<usize>>>,
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
            granule_ranges: None,
            matched_rows: None,
            matched_scores: None,
            virtual_block_meta: None,
        }
    }

    fn from_block_meta_index(block_meta_index: &BlockMetaIndex) -> Self {
        Self {
            block_idx: block_meta_index.block_idx,
            block_location: block_meta_index.block_location.clone(),
            keep: true,
            range: block_meta_index.range.clone(),
            granule_ranges: block_meta_index.granule_ranges.clone(),
            matched_rows: block_meta_index.matched_rows.clone(),
            matched_scores: block_meta_index.matched_scores.clone(),
            virtual_block_meta: block_meta_index.virtual_block_meta.clone(),
        }
    }

    fn apply_to_block_meta_index(self, mut block_meta_index: BlockMetaIndex) -> BlockMetaIndex {
        block_meta_index.range = self.range;
        block_meta_index.granule_ranges = self.granule_ranges;
        block_meta_index.matched_rows = self.matched_rows;
        block_meta_index.matched_scores = self.matched_scores;
        block_meta_index.virtual_block_meta = self.virtual_block_meta;
        block_meta_index
    }
}
