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

use std::sync::Arc;

use common_base::base::tokio::sync::OwnedSemaphorePermit;
use common_base::base::tokio::sync::Semaphore;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::TableSnapshot;
use common_legacy_planners::Extras;
use futures::future;
use tracing::warn;
use tracing::Instrument;

use super::pruner;
use crate::io::MetaReaders;
use crate::pruning::limiter;
use crate::pruning::limiter::LimiterPruner;
use crate::pruning::pruner::Pruner;
use crate::pruning::range_pruner;
use crate::pruning::range_pruner::RangePruner;
use crate::pruning::topn_pruner;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

// TODO move this to somewhere
pub type SegmentIndex = usize;

// TODO
// investigate that if simple "select count() from t" calls table::read_partitions (can be avoided)

impl BlockPruner {
    pub fn new(table_snapshot: Arc<TableSnapshot>) -> Self {
        Self { table_snapshot }
    }

    // Sync version of method `prune`
    //
    // Please note that it will take a significant period of time to prune a large table, and
    // thread that calls this method will be blocked.
    #[tracing::instrument(level = "debug", skip(self, schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub fn sync_prune(
        &self,
        ctx: &Arc<dyn TableContext>,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
    ) -> Result<Vec<(SegmentIndex, BlockMeta)>> {
        futures::executor::block_on(self.prune(ctx, schema, push_down))
    }

    // prune blocks by utilizing min_max index and filter, according to the pushdowns
    #[tracing::instrument(level = "debug", skip(self, schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn prune(
        &self,
        ctx: &Arc<dyn TableContext>,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
    ) -> Result<Vec<(SegmentIndex, BlockMeta)>> {
        let segment_locs = self.table_snapshot.segments.clone();

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
        let limiter = limiter::new_limiter(limit);

        // prepare the range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_pruner = range_pruner::new_range_pruner(ctx, filter_expressions, &schema)?;

        // prepare the filter, if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let dal = ctx.get_storage_operator()?;
        let filter_pruner = pruner::new_filter_pruner(ctx, filter_expressions, &schema, dal)?;

        // 2. kick off
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrent_prune = {
            let max_concurrent_prune_setting =
                ctx.get_settings().get_max_concurrent_prune()? as usize;
            // Prevent us from miss-configured max_concurrent_prune setting, e.g. 0
            //
            // note that inside the segment pruning, the SAME Semaphore is used to
            // control the concurrency of block pruning, to prevent us from waiting for
            // a permit while hold the last permit, at least 2 permits should be
            // given to this semaphore.
            let v = std::cmp::max(max_concurrent_prune_setting, 10);
            if v > max_concurrent_prune_setting {
                warn!(
                    "max_concurrent_prune setting is too low {}, increased to {}",
                    max_concurrent_prune_setting, v
                )
            }
            v
        };

        let pruning_runtime = Arc::new(Runtime::with_worker_threads(
            max_threads,
            Some("pruning-worker".to_owned()),
        )?);
        let semaphore = Arc::new(Semaphore::new(max_concurrent_prune));

        let mut join_handlers = Vec::with_capacity(segment_locs.len());
        for (segment_idx, segment_location) in segment_locs.into_iter().enumerate() {
            let permit = Self::acquire_permit(semaphore.clone(), "prune segment").await?;
            let segment_pruning_fut = {
                // prepare the pruning context
                let pruning_ctx = PruningContext {
                    ctx: ctx.clone(),
                    segment_idx,
                    segment_location,
                    limiter: limiter.clone(),
                    range_filter_pruner: range_pruner.clone(),
                    filter_pruner: filter_pruner.clone(),
                    rt: pruning_runtime.clone(),
                    semaphore: semaphore.clone(),
                    permit, // move the permit to the pruning task
                };
                // build the segment pruning future
                Self::prune_segment(pruning_ctx)
            };

            join_handlers.push(pruning_runtime.try_spawn(segment_pruning_fut)?);
        }

        let metas = Self::join_flatten_result(join_handlers).await?;

        // if there are ordering + limit clause, use topn pruner

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
    async fn prune_segment(pruning_ctx: PruningContext) -> Result<Vec<(SegmentIndex, BlockMeta)>> {
        let _ = pruning_ctx.permit;
        let segment_reader = MetaReaders::segment_info_reader(pruning_ctx.ctx.as_ref());

        // segment pruning executed concurrently. before read segment info, check if limit already exceeded.
        if pruning_ctx.limiter.exceeded() {
            return Ok(vec![]);
        }

        let segment_idx = pruning_ctx.segment_idx;

        let (seg_loc, ver) = &pruning_ctx.segment_location;
        let segment_info = segment_reader.read(seg_loc, None, *ver).await?;
        let mut result = Vec::with_capacity(segment_info.blocks.len());
        if pruning_ctx.range_filter_pruner.should_keep(
            &segment_info.summary.col_stats,
            segment_info.summary.row_count,
        ) {
            result = Self::prune_blocks(&pruning_ctx, segment_idx, &segment_info).await?;
        }
        Ok::<_, ErrorCode>(result)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn prune_blocks(
        pruning_ctx: &PruningContext,
        segment_idx: SegmentIndex,
        segment_info: &SegmentInfo,
    ) -> Result<Vec<(SegmentIndex, BlockMeta)>> {
        let mut result = Vec::with_capacity(segment_info.blocks.len());
        let mut bloom_pruners = Vec::with_capacity(segment_info.blocks.len());
        for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
            // prune block using range filter
            if pruning_ctx.limiter.exceeded() {
                // before using filter to prune, check if limit already exceeded
                return Ok(result);
            }

            if pruning_ctx
                .range_filter_pruner
                .should_keep(&block_meta.col_stats, block_meta.row_count)
            {
                let filter_pruner = pruning_ctx.filter_pruner.clone();
                let limiter = pruning_ctx.limiter.clone();
                let row_count = block_meta.row_count;
                let index_location = block_meta.bloom_filter_index_location.clone();
                let index_size = block_meta.bloom_filter_index_size;

                let permit_prune_block =
                    Self::acquire_permit(pruning_ctx.semaphore.clone(), "prune block").await?;

                let prune_bock_fut = async move {
                    let _ = permit_prune_block;
                    if limiter.within_limit(row_count)
                        && filter_pruner.should_keep(&index_location, index_size).await
                    {
                        return Ok::<_, ErrorCode>((block_idx, true));
                    }
                    Ok::<_, ErrorCode>((block_idx, false))
                };
                let h = pruning_ctx.rt.spawn(
                    prune_bock_fut.instrument(tracing::debug_span!("filter_using_bloom_index")),
                );
                bloom_pruners.push(h);
            }
        }

        let joint = future::try_join_all(bloom_pruners)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;
        for item in joint {
            let (block_idx, keep) = item?;
            if keep {
                let block = &segment_info.blocks[block_idx];
                result.push((segment_idx, block.clone()))
            }
        }
        Ok(result)
    }

    #[inline]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn join_flatten_result(
        join_handlers: SegmentsJoint,
    ) -> Result<Vec<(SegmentIndex, BlockMeta)>> {
        let joint = future::try_join_all(join_handlers)
            .instrument(tracing::debug_span!("join_all_filter_segment"))
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        let metas: Result<Vec<(SegmentIndex, BlockMeta)>> = tracing::debug_span!("collect_result")
            .in_scope(|| {
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

    #[inline]
    async fn acquire_permit(semaphore: Arc<Semaphore>, msg: &str) -> Result<OwnedSemaphorePermit> {
        semaphore.acquire_owned().await.map_err(|e| {
            ErrorCode::UnexpectedError(format!(
                "semaphore closed, acquire ({}) permit failure. {}",
                msg, e
            ))
        })
    }
}

type SegmentsJoint = Vec<JoinHandle<Result<Vec<(SegmentIndex, BlockMeta)>>>>;

pub struct PruningContext {
    ctx: Arc<dyn TableContext>,
    segment_idx: SegmentIndex,
    segment_location: Location,
    limiter: LimiterPruner,
    range_filter_pruner: Arc<dyn RangePruner + Send + Sync>,
    filter_pruner: Arc<dyn Pruner + Send + Sync>,
    rt: Arc<Runtime>,
    semaphore: Arc<Semaphore>,
    permit: OwnedSemaphorePermit,
}
