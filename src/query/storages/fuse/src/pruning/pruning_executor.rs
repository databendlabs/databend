//  Copyright 2021 Datafuse Labs.
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
use std::pin::Pin;
use std::sync::Arc;

use common_base::base::tokio::sync::OwnedSemaphorePermit;
use common_base::base::tokio::sync::Semaphore;
use common_base::base::tokio::task::JoinHandle;
use common_base::runtime::Runtime;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_pruner::LimiterPruner;
use common_storages_pruner::LimiterPrunerCreator;
use common_storages_pruner::RangePruner;
use common_storages_pruner::RangePrunerCreator;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use futures::future;
use opendal::Operator;
use tracing::warn;
use tracing::Instrument;

use super::pruner;
use crate::io::MetaReaders;
use crate::pruning::pruner::Pruner;
use crate::pruning::topn_pruner;

pub type BlockIndex = (usize, usize);
type SegmentPruningJoinHandles = Vec<JoinHandle<Result<Vec<(BlockIndex, Arc<BlockMeta>)>>>>;

struct PruningContext {
    limiter: LimiterPruner,
    range_pruner: Arc<dyn RangePruner + Send + Sync>,
    filter_pruner: Option<Arc<dyn Pruner + Send + Sync>>,
    rt: Arc<Runtime>,
    semaphore: Arc<Semaphore>,
}

pub struct BlockPruner;
impl BlockPruner {
    // prune blocks by utilizing min_max index and filter, according to the pushdowns
    #[tracing::instrument(level = "debug", skip(schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn prune(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        schema: DataSchemaRef,
        push_down: &Option<PushDownInfo>,
        segment_locs: Vec<Location>,
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
        if segment_locs.is_empty() {
            return Ok(vec![]);
        };

        let filter_expressions = push_down.as_ref().map(|extra| extra.filters.as_slice());

        // 1. prepare pruners

        // if there are ordering clause, ignore limit, even it has been pushed down
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty())
            .and_then(|p| p.limit);

        // prepare the limiter. in case that limit is none, an unlimited limiter will be returned
        let limiter = LimiterPrunerCreator::create(limit);

        // prepare the range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_pruner = RangePrunerCreator::try_create(ctx, filter_expressions, &schema)?;

        // prepare the filter.
        // None will be returned, if filter is not applicable (e.g. unsuitable filter expression, index not available, etc.)
        let filter_pruner =
            pruner::new_filter_pruner(ctx, filter_expressions, &schema, dal.clone())?;

        // 2. constraint the degree of parallelism
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
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

        let pruning_runtime = Arc::new(Runtime::with_worker_threads(
            max_threads,
            Some("pruning-worker".to_owned()),
        )?);

        let semaphore = Arc::new(Semaphore::new(max_concurrency));

        // 3. setup pruning context
        let pruning_ctx = Arc::new(PruningContext {
            limiter: limiter.clone(),
            range_pruner: range_pruner.clone(),
            filter_pruner,
            rt: pruning_runtime.clone(),
            semaphore: semaphore.clone(),
        });

        // 4. kick off
        // 4.1 generates the iterator of segment pruning tasks.
        let mut segments = segment_locs.into_iter().enumerate();
        let tasks = std::iter::from_fn(|| {
            // pruning tasks are executed concurrently, check if limit exceeded before proceeding
            if pruning_ctx.limiter.exceeded() {
                None
            } else {
                segments.next().map(|(segment_idx, segment_location)| {
                    let dal = dal.clone();
                    let pruning_ctx = pruning_ctx.clone();
                    move |permit| async move {
                        Self::prune_segment(permit, dal, pruning_ctx, segment_idx, segment_location)
                            .await
                    }
                })
            }
        });

        // 4.2 spawns the segment pruning tasks, with concurrency control
        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(semaphore.clone(), tasks)
            .await?;

        // 4.3 flatten the results
        let metas = Self::join_flatten_result(join_handlers).await?;

        // 5. if there are ordering + limit clause, use topn pruner
        if push_down
            .as_ref()
            .filter(|p| !p.order_by.is_empty() && p.limit.is_some())
            .is_some()
        {
            let push_down = push_down.as_ref().unwrap();
            let limit = push_down.limit.unwrap();
            let sort = push_down.order_by.clone();
            let tpruner = topn_pruner::TopNPrunner::new(schema, sort, limit);
            return tpruner.prune(metas);
        }

        Ok(metas)
    }

    #[inline]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn prune_segment(
        permit: OwnedSemaphorePermit,
        dal: Operator,
        pruning_ctx: Arc<PruningContext>,
        segment_idx: usize,
        segment_location: Location,
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
        let segment_reader = MetaReaders::segment_info_reader(dal.clone());

        let (path, ver) = segment_location;
        let segment_info = segment_reader.read(path, None, ver).await?;

        // IO job of reading segment done, release the permit, allows more concurrent pruners
        // Note that it is required to explicitly release this permit before pruning blocks, to avoid deadlock.
        drop(permit);

        let result = if pruning_ctx.range_pruner.should_keep(
            &segment_info.summary.col_stats,
            segment_info.summary.row_count,
        ) {
            if let Some(filter_pruner) = &pruning_ctx.filter_pruner {
                Self::prune_blocks(&pruning_ctx, filter_pruner, segment_idx, &segment_info).await?
            } else {
                // if no available filter pruners, just prune the blocks by
                // using zone map index, and do not spawn async tasks
                Self::prune_blocks_sync(&pruning_ctx, segment_idx, &segment_info)
            }
        } else {
            vec![]
        };
        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn prune_blocks(
        pruning_ctx: &Arc<PruningContext>,
        filter_pruner: &Arc<dyn Pruner + Send + Sync>,
        segment_idx: usize,
        segment_info: &SegmentInfo,
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
        let mut blocks = segment_info.blocks.iter().enumerate();
        let pruning_runtime = &pruning_ctx.rt;
        let semaphore = &pruning_ctx.semaphore;

        let tasks = std::iter::from_fn(|| {
            // check limit speculatively
            if pruning_ctx.limiter.exceeded() {
                return None;
            }
            type BlockPruningFutureReturn = Pin<Box<dyn Future<Output = (usize, bool)> + Send>>;
            type BlockPruningFuture =
                Box<dyn FnOnce(OwnedSemaphorePermit) -> BlockPruningFutureReturn + Send + 'static>;
            blocks.next().map(|(block_idx, block_meta)| {
                let row_count = block_meta.row_count;
                if pruning_ctx
                    .range_pruner
                    .should_keep(&block_meta.col_stats, row_count)
                {
                    // not pruned by block zone map index,
                    let ctx = pruning_ctx.clone();
                    let filter_pruner = filter_pruner.clone();
                    let index_location = block_meta.bloom_filter_index_location.clone();
                    let index_size = block_meta.bloom_filter_index_size;
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            let keep = filter_pruner.should_keep(&index_location, index_size).await
                                && ctx.limiter.within_limit(row_count);
                            (block_idx, keep)
                        })
                    });
                    v
                } else {
                    let v: BlockPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;
                            (block_idx, false)
                        })
                    });
                    v
                }
            })
        });

        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(semaphore.clone(), tasks)
            .await?;

        let joint = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        let mut result = Vec::with_capacity(segment_info.blocks.len());
        for item in joint {
            let (block_idx, keep) = item;
            if keep {
                let block = segment_info.blocks[block_idx].clone();
                result.push(((segment_idx, block_idx), block))
            }
        }
        Ok(result)
    }

    fn prune_blocks_sync(
        pruning_ctx: &Arc<PruningContext>,
        segment_idx: usize,
        segment_info: &SegmentInfo,
    ) -> Vec<(BlockIndex, Arc<BlockMeta>)> {
        let mut result = Vec::with_capacity(segment_info.blocks.len());
        for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
            // check limit speculatively
            if pruning_ctx.limiter.exceeded() {
                break;
            }
            let row_count = block_meta.row_count;
            if pruning_ctx
                .range_pruner
                .should_keep(&block_meta.col_stats, row_count)
                && pruning_ctx.limiter.within_limit(row_count)
            {
                result.push(((segment_idx, block_idx), block_meta.clone()))
            }
        }
        result
    }

    #[inline]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn join_flatten_result(
        join_handlers: SegmentPruningJoinHandles,
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
        let joint = future::try_join_all(join_handlers)
            .instrument(tracing::debug_span!("join_all_filter_segment"))
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        let metas: Result<Vec<(BlockIndex, Arc<BlockMeta>)>> =
            tracing::debug_span!("collect_result").in_scope(|| {
                // flatten the collected block metas
                let metas = joint
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten();
                Ok(metas.collect())
            });
        metas
    }
}
