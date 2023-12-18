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

use databend_common_base::base::tokio::sync::OwnedSemaphorePermit;
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_metrics::storage::*;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use futures_util::future;

use super::SegmentLocation;
use crate::pruning::BloomPruner;
use crate::pruning::PruningContext;

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
        segment_info: &CompactSegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if let Some(bloom_pruner) = &self.pruning_ctx.bloom_pruner {
            self.block_pruning(bloom_pruner, segment_location, segment_info)
                .await
        } else {
            // if no available filter pruners, just prune the blocks by
            // using zone map index, and do not spawn async tasks
            self.block_pruning_sync(segment_location, segment_info)
        }
    }

    // async pruning with bloom index.
    #[async_backtrace::framed]
    async fn block_pruning(
        &self,
        bloom_pruner: &Arc<dyn BloomPruner + Send + Sync>,
        segment_location: SegmentLocation,
        segment_info: &CompactSegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let segment_block_metas = segment_info.block_metas()?;

        let blocks = if let Some(internal_column_pruner) = &self.pruning_ctx.internal_column_pruner
        {
            segment_block_metas
                .iter()
                .enumerate()
                .filter(|(_, block)| {
                    internal_column_pruner.should_keep(BLOCK_NAME_COL_NAME, &block.location.0)
                })
                .collect::<Vec<_>>()
        } else {
            segment_block_metas.iter().enumerate().collect()
        };

        let mut blocks = blocks.into_iter();
        let pruning_tasks = std::iter::from_fn(|| {
            // check limit speculatively
            if limit_pruner.exceeded() {
                return None;
            }

            type BlockPruningFutureReturn =
                Pin<Box<dyn Future<Output = (usize, bool, Option<Range<usize>>, String)> + Send>>;
            type BlockPruningFuture =
                Box<dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn + Send + 'static>;

            let pruning_stats = pruning_stats.clone();
            blocks.next().map(|(block_idx, block_meta)| {
                // Perf.
                {
                    metrics_inc_blocks_range_pruning_before(1);
                    metrics_inc_bytes_block_range_pruning_before(block_meta.block_size);

                    pruning_stats.set_blocks_range_pruning_before(1);
                }

                let block_meta = block_meta.clone();
                let row_count = block_meta.row_count;
                if range_pruner.should_keep(&block_meta.col_stats, Some(&block_meta.col_metas)) {
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
                    let index_location = block_meta.bloom_filter_index_location.clone();
                    let index_size = block_meta.bloom_filter_index_size;
                    let column_ids = block_meta.col_metas.keys().cloned().collect::<Vec<_>>();

                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            // Perf.
                            {
                                metrics_inc_blocks_bloom_pruning_before(1);
                                metrics_inc_bytes_block_bloom_pruning_before(block_meta.block_size);

                                pruning_stats.set_blocks_bloom_pruning_before(1);
                            }

                            let _permit = permit;
                            let keep = bloom_pruner
                                .should_keep(&index_location, index_size, column_ids)
                                .await
                                && limit_pruner.within_limit(row_count);

                            if keep {
                                // Perf.
                                {
                                    metrics_inc_blocks_bloom_pruning_after(1);
                                    metrics_inc_bytes_block_bloom_pruning_after(
                                        block_meta.block_size,
                                    );

                                    pruning_stats.set_blocks_bloom_pruning_after(1);
                                }

                                let (keep, range) =
                                    page_pruner.should_keep(&block_meta.cluster_stats);
                                (block_idx, keep, range, block_meta.location.0.clone())
                            } else {
                                (block_idx, keep, None, block_meta.location.0.clone())
                            }
                        })
                    });
                    v
                } else {
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            (block_idx, false, None, block_meta.location.0.clone())
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
        let block_num = segment_info.summary.block_count as usize;
        for item in joint {
            let (block_idx, keep, range, block_location) = item;
            if keep {
                let block = segment_block_metas[block_idx].clone();

                debug_assert_eq!(block_location, block.location.0);

                result.push((
                    BlockMetaIndex {
                        segment_idx: segment_location.segment_idx,
                        block_idx,
                        range,
                        page_size: block.page_size() as usize,
                        block_id: block_id_in_segment(block_num, block_idx),
                        block_location: block_location.clone(),
                        segment_location: segment_location.location.0.clone(),
                        snapshot_location: segment_location.snapshot_loc.clone(),
                    },
                    block,
                ))
            }
        }

        // Perf
        {
            metrics_inc_pruning_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(result)
    }

    fn block_pruning_sync(
        &self,
        segment_location: SegmentLocation,
        segment_info: &CompactSegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let start = Instant::now();

        let segment_block_metas = segment_info.block_metas()?;
        let blocks = if let Some(internal_column_pruner) = &self.pruning_ctx.internal_column_pruner
        {
            segment_block_metas
                .iter()
                .enumerate()
                .filter(|(_, block)| {
                    internal_column_pruner.should_keep(BLOCK_NAME_COL_NAME, &block.location.0)
                })
                .collect::<Vec<_>>()
        } else {
            segment_block_metas.iter().enumerate().collect::<Vec<_>>()
        };
        let mut result = Vec::with_capacity(blocks.len());
        let block_num = segment_info.summary.block_count as usize;
        for (block_idx, block_meta) in blocks {
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
            if range_pruner.should_keep(&block_meta.col_stats, Some(&block_meta.col_metas))
                && limit_pruner.within_limit(row_count)
            {
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
                        },
                        block_meta.clone(),
                    ))
                }
            }
        }

        // Perf
        {
            metrics_inc_pruning_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(result)
    }
}
