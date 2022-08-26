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

use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::TableSnapshot;
use common_planners::Extras;
use futures::future;
use futures::StreamExt;
use futures::TryStreamExt;
use tracing::Instrument;

use super::bloom_pruner;
use crate::io::MetaReaders;
use crate::pruning::limiter;
use crate::pruning::range_pruner;
use crate::pruning::topn_pruner;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

const FUTURE_BUFFER_SIZE: usize = 10;

impl BlockPruner {
    pub fn new(table_snapshot: Arc<TableSnapshot>) -> Self {
        Self { table_snapshot }
    }

    // prune blocks by utilizing min_max index and bloom filter, according to the pushdowns
    #[tracing::instrument(level = "debug", skip(self, schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn prune(
        &self,
        ctx: &Arc<dyn TableContext>,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
    ) -> Result<Vec<(usize, BlockMeta)>> {
        let segment_locs = self.table_snapshot.segments.clone();

        if segment_locs.is_empty() {
            return Ok(vec![]);
        };

        // if there are ordering clause, ignore limit, even it has been pushed down
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty())
            .and_then(|p| p.limit);

        let filter_expressions = push_down.as_ref().map(|extra| extra.filters.as_slice());

        // shortcut, just returns all the blocks
        if limit.is_none() && filter_expressions.is_none() {
            return Self::all_the_blocks(segment_locs, ctx.as_ref()).await;
        }

        // 1. prepare pruners

        // prepare the limiter. in case that limit is none, an unlimited limiter will be returned
        let limiter = limiter::new_limiter(limit);

        // prepare the range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_filter_pruner =
            range_pruner::new_range_filter_pruner(ctx, filter_expressions, &schema)?;

        // prepare the bloom filter, if filter_expression is none, an dummy pruner will be returned
        let dal = ctx.get_storage_operator()?;
        let bloom_filter_pruner =
            bloom_pruner::new_bloom_filter_pruner(ctx, filter_expressions, &schema, dal)?;

        // 2. kick off
        //
        // As suggested by Winter, to make the pruning process more parallel (not just concurrent),
        // we create a dedicated runtime for pruning tasks.
        //
        // NOTE:
        // A. To simplify things, an optimistic way of error handling is taken: errors are handled
        // at the "collect" phase. e.g. if anything goes wrong, we do not break the whole
        // pruning task immediately, but only at the time that all tasks are done
        //
        // B. since limiter is working concurrently, we arrange some checks among the pruning,
        //    to avoid heavy io operation vainly,
        let pruning_runtime = Runtime::with_worker_threads(
            ctx.get_settings().get_max_threads()? as usize,
            Some("pruning-worker".to_owned()),
        )?;
        let mut join_handlers = Vec::with_capacity(segment_locs.len());
        for (idx, (seg_loc, ver)) in segment_locs.into_iter().enumerate() {
            let ctx = ctx.clone();
            let range_filter_pruner = range_filter_pruner.clone();
            let bloom_filter_pruner = bloom_filter_pruner.clone();
            let limiter = limiter.clone();
            let segment_pruning_fut = async move {
                let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
                if limiter.exceeded() {
                    // before read segment info, check if limit already exceeded
                    return Ok(vec![]);
                }
                let segment_info = segment_reader.read(seg_loc, None, ver).await?;
                let mut result = Vec::with_capacity(segment_info.blocks.len());
                if range_filter_pruner.should_keep(&segment_info.summary.col_stats) {
                    for block_meta in &segment_info.blocks {
                        // prune block using range filter
                        if limiter.exceeded() {
                            // before using bloom index to prune, check if limit already exceeded
                            return Ok(result);
                        }
                        if range_filter_pruner.should_keep(&block_meta.col_stats) {
                            // prune block using bloom filter
                            if bloom_filter_pruner
                                .should_keep(&block_meta.bloom_filter_index_location)
                                .await
                            {
                                if limiter.within_limit(block_meta.row_count) {
                                    result.push((idx, block_meta.clone()));
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok::<_, ErrorCode>(result)
            }
            .instrument(tracing::debug_span!("filter_segment_with_storage_rt"));
            join_handlers.push(pruning_runtime.try_spawn(segment_pruning_fut)?);
        }

        let joint = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

        // 3. collect the result
        let metas: Result<Vec<(usize, BlockMeta)>> = tracing::debug_span!("collect_result")
            .in_scope(|| {
                // flatten the collected block metas
                let metas = joint
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten();
                Ok(metas.collect())
            });
        let metas = metas?;

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

    async fn all_the_blocks(
        segment_locs: Vec<Location>,
        ctx: &dyn TableContext,
    ) -> Result<Vec<(usize, BlockMeta)>> {
        let segment_num = segment_locs.len();
        let block_metas = futures::stream::iter(segment_locs.into_iter().enumerate())
            .map(|(idx, (seg_loc, ver))| async move {
                let segment_reader = MetaReaders::segment_info_reader(ctx);
                let segment_info = segment_reader.read(seg_loc, None, ver).await?;
                Ok::<_, ErrorCode>(
                    segment_info
                        .blocks
                        .clone()
                        .into_iter()
                        .map(move |item| (idx, item)),
                )
            })
            .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
            .try_collect::<Vec<_>>()
            .await?;
        Ok(block_metas.into_iter().flatten().collect::<Vec<_>>())
    }
}
