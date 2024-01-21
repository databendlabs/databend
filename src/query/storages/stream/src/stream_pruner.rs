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
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::*;
use databend_common_storages_fuse::pruning::BloomPruner;
use databend_common_storages_fuse::pruning::PruningContext;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::TopNPrunner;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures_util::future;
use log::warn;

pub struct StreamPruner {
    max_concurrency: usize,
    pub table_schema: TableSchemaRef,
    pub pruning_ctx: Arc<PruningContext>,
    pub push_down: Option<PushDownInfo>,
}

impl StreamPruner {
    pub fn create(
        ctx: &Arc<dyn TableContext>,
        table_schema: TableSchemaRef,
        push_down: Option<PushDownInfo>,
        fuse_table: &FuseTable,
    ) -> Result<Arc<Self>> {
        let max_concurrency = {
            let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
            // Prevent us from miss-configured max_storage_io_requests setting, e.g. 0
            let v = std::cmp::max(max_io_requests, 10);
            if v > max_io_requests {
                warn!(
                    "max_storage_io_requests setting is too low {}, increased to {}",
                    max_io_requests, v
                )
            }
            v
        };

        let (cluster_keys, cluster_key_meta) =
            if !fuse_table.is_native() || fuse_table.cluster_key_meta().is_none() {
                (vec![], None)
            } else {
                (
                    fuse_table.cluster_keys(ctx.clone()),
                    fuse_table.cluster_key_meta(),
                )
            };

        let pruning_ctx = PruningContext::try_create(
            ctx,
            fuse_table.get_operator(),
            table_schema.clone(),
            &push_down,
            cluster_key_meta,
            cluster_keys,
            fuse_table.bloom_index_cols(),
            max_concurrency,
        )?;

        Ok(Arc::new(StreamPruner {
            max_concurrency,
            table_schema,
            push_down,
            pruning_ctx,
        }))
    }

    #[async_backtrace::framed]
    pub async fn pruning(
        self: &Arc<Self>,
        block_metas: Vec<Arc<BlockMeta>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let mut remain = block_metas.len() % self.max_concurrency;
        let batch_size = block_metas.len() / self.max_concurrency;
        let mut works = Vec::with_capacity(self.max_concurrency);
        let mut block_metas = block_metas.into_iter().enumerate().collect::<Vec<_>>();

        while !block_metas.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = block_metas.drain(0..batch_size).collect::<Vec<_>>();
            works.push(
                self.pruning_ctx
                    .pruning_runtime
                    .spawn(self.pruning_ctx.ctx.get_id(), {
                        let self_clone = self.clone();

                        async move {
                            // Build pruning tasks.
                            let res =
                                if let Some(bloom_pruner) = &self_clone.pruning_ctx.bloom_pruner {
                                    self_clone.block_pruning(bloom_pruner, batch).await?
                                } else {
                                    // if no available filter pruners, just prune the blocks by
                                    // using zone map index, and do not spawn async tasks
                                    self_clone.block_pruning_sync(batch)?
                                };

                            Result::<_, ErrorCode>::Ok(res)
                        }
                    }),
            );
        }

        match futures::future::try_join_all(works).await {
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "segment pruning failure, {}",
                e
            ))),
            Ok(workers) => {
                let mut metas = vec![];
                for worker in workers {
                    let res = worker?;
                    metas.extend(res);
                }
                // Todo:: for now, all operation (contains other mutation other than delete, like select,update etc.)
                // will get here, we can prevent other mutations like update and so on.
                // TopN pruner.
                self.topn_pruning(metas)
            }
        }
    }

    // topn pruner:
    // if there are ordering + limit clause and no filters, use topn pruner
    fn topn_pruning(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let push_down = self.push_down.clone();
        if push_down
            .as_ref()
            .filter(|p| !p.order_by.is_empty() && p.limit.is_some() && p.filters.is_none())
            .is_some()
        {
            let schema = self.table_schema.clone();
            let push_down = push_down.as_ref().unwrap();
            let limit = push_down.limit.unwrap();
            let sort = push_down.order_by.clone();
            let topn_pruner = TopNPrunner::create(schema, sort, limit);
            return Ok(topn_pruner.prune(metas.clone()).unwrap_or(metas));
        }
        Ok(metas)
    }

    // Pruning stats.
    pub fn pruning_stats(&self) -> databend_common_catalog::plan::PruningStatistics {
        let stats = self.pruning_ctx.pruning_stats.clone();

        let segments_range_pruning_before = stats.get_segments_range_pruning_before() as usize;
        let segments_range_pruning_after = stats.get_segments_range_pruning_after() as usize;

        let blocks_range_pruning_before = stats.get_blocks_range_pruning_before() as usize;
        let blocks_range_pruning_after = stats.get_blocks_range_pruning_after() as usize;

        let blocks_bloom_pruning_before = stats.get_blocks_bloom_pruning_before() as usize;
        let blocks_bloom_pruning_after = stats.get_blocks_bloom_pruning_after() as usize;

        databend_common_catalog::plan::PruningStatistics {
            segments_range_pruning_before,
            segments_range_pruning_after,
            blocks_range_pruning_before,
            blocks_range_pruning_after,
            blocks_bloom_pruning_before,
            blocks_bloom_pruning_after,
        }
    }

    // async pruning with bloom index.
    #[async_backtrace::framed]
    pub async fn block_pruning(
        &self,
        bloom_pruner: &Arc<dyn BloomPruner + Send + Sync>,
        block_metas: Vec<(usize, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let mut blocks = block_metas.into_iter();
        let pruning_tasks = std::iter::from_fn(|| {
            // check limit speculatively
            if limit_pruner.exceeded() {
                return None;
            }

            type BlockPruningFutureReturn = Pin<
                Box<
                    dyn Future<Output = (usize, bool, Option<Range<usize>>, Arc<BlockMeta>)> + Send,
                >,
            >;
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
                                (block_idx, keep, range, block_meta)
                            } else {
                                (block_idx, keep, None, block_meta)
                            }
                        })
                    });
                    v
                } else {
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            (block_idx, false, None, block_meta)
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
        for item in joint {
            let (block_idx, keep, range, block) = item;
            if keep {
                result.push((
                    BlockMetaIndex {
                        segment_idx: 0,
                        block_idx,
                        range,
                        page_size: block.page_size() as usize,
                        block_id: 0,
                        block_location: block.location.0.clone(),
                        segment_location: "".to_string(),
                        snapshot_location: None,
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
        block_metas: Vec<(usize, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let start = Instant::now();

        let mut result = Vec::with_capacity(block_metas.len());
        for (block_idx, block_meta) in block_metas.into_iter() {
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
                            segment_idx: 0,
                            block_idx,
                            range,
                            page_size: block_meta.page_size() as usize,
                            block_id: 0,
                            block_location: block_meta.as_ref().location.0.clone(),
                            segment_location: "".to_string(),
                            snapshot_location: None,
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
