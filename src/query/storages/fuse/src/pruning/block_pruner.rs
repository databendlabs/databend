//  Copyright 2023 Datafuse Labs.
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

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio::sync::OwnedSemaphorePermit;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::SegmentInfo;

use crate::metrics::*;
use crate::pruning::FuseBloomPruner;
use crate::pruning::PruningContext;

pub struct BlockPruner {
    pub pruning_ctx: PruningContext,
}

impl BlockPruner {
    pub fn create(pruning_ctx: PruningContext) -> Result<BlockPruner> {
        Ok(BlockPruner { pruning_ctx })
    }

    pub async fn pruning(
        &self,
        segment_idx: usize,
        segment_info: &SegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if let Some(filter_pruner) = &self.pruning_ctx.filter_pruner {
            self.block_pruning(filter_pruner, segment_idx, segment_info)
                .await
        } else {
            // if no available filter pruners, just prune the blocks by
            // using zone map index, and do not spawn async tasks
            self.block_pruning_sync(segment_idx, segment_info)
        }
    }

    async fn block_pruning(
        &self,
        filter_pruner: &Arc<dyn FuseBloomPruner + Send + Sync>,
        segment_idx: usize,
        segment_info: &SegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let mut blocks = segment_info.blocks.iter().enumerate();
        let pruning_tasks = std::iter::from_fn(|| {
            // check limit speculatively
            if limit_pruner.exceeded() {
                return None;
            }

            type BlockPruningFutureReturn =
                Pin<Box<dyn Future<Output = (usize, bool, Option<Range<usize>>)> + Send>>;
            type BlockPruningFuture =
                Box<dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn + Send + 'static>;

            blocks.next().map(|(block_idx, block_meta)| {
                let block_meta = block_meta.clone();
                let row_count = block_meta.row_count;
                if range_pruner.should_keep(&block_meta.col_stats) {
                    // not pruned by block zone map index,
                    let filter_pruner = filter_pruner.clone();
                    let limit_pruner = limit_pruner.clone();
                    let page_pruner = page_pruner.clone();
                    let index_location = block_meta.bloom_filter_index_location.clone();
                    let index_size = block_meta.bloom_filter_index_size;

                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            let keep = filter_pruner.should_keep(&index_location, index_size).await
                                && limit_pruner.within_limit(row_count);

                            if keep {
                                let (keep, range) =
                                    page_pruner.should_keep(&block_meta.cluster_stats);
                                (block_idx, keep, range)
                            } else {
                                (block_idx, keep, None)
                            }
                        })
                    });
                    v
                } else {
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            (block_idx, false, None)
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

        let mut result = Vec::with_capacity(segment_info.blocks.len());
        for item in joint {
            let (block_idx, keep, range) = item;
            if keep {
                let block = segment_info.blocks[block_idx].clone();

                result.push((
                    BlockMetaIndex {
                        segment_idx,
                        block_idx,
                        range,
                    },
                    block,
                ))
            }
        }

        // Perf
        {
            metrics_inc_pruning_before_block_nums(segment_info.blocks.len() as u64);
            metrics_inc_pruning_after_block_nums(result.len() as u64);
            metrics_inc_pruning_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(result)
    }

    fn block_pruning_sync(
        &self,
        segment_idx: usize,
        segment_info: &SegmentInfo,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let start = Instant::now();

        let mut result = Vec::with_capacity(segment_info.blocks.len());
        for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
            // check limit speculatively
            if limit_pruner.exceeded() {
                break;
            }
            let row_count = block_meta.row_count;
            if range_pruner.should_keep(&block_meta.col_stats)
                && limit_pruner.within_limit(row_count)
            {
                let (keep, range) = page_pruner.should_keep(&block_meta.cluster_stats);
                if keep {
                    result.push((
                        BlockMetaIndex {
                            segment_idx,
                            block_idx,
                            range,
                        },
                        block_meta.clone(),
                    ))
                }
            }
        }

        // Perf
        {
            metrics_inc_pruning_before_block_nums(segment_info.blocks.len() as u64);
            metrics_inc_pruning_after_block_nums(result.len() as u64);
            metrics_inc_pruning_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(result)
    }
}
