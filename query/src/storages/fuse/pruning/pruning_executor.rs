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
//

use std::sync::Arc;

use common_base::base::TrySpawn;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::TableSnapshot;
use common_planners::Extras;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use futures::StreamExt;
use futures::TryStreamExt;

use super::bloom_pruner;
use crate::sessions::TableContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::pruning::bloom_pruner::NonPruner;
use crate::storages::fuse::pruning::limiter;
use crate::storages::fuse::pruning::range_pruner;
use crate::storages::fuse::pruning::range_pruner::RangeFilterPruner;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

const FUTURE_BUFFER_SIZE: usize = 40;

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

        let filter_expression = push_down.as_ref().and_then(|extra| extra.filters.get(0));

        // shortcut, just returns all the blocks
        if limit.is_none() && filter_expression.is_none() {
            return Self::all_the_blocks(segment_locs, ctx.as_ref()).await;
        }

        // prepare the limiter. in case that limit is none, an unlimited limiter will be returned
        use super::limiter::Limiter;
        let limiter: Arc<dyn Limiter + Send + Sync> = limiter::new_limiter(limit).into();

        // prepare the range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_filter_pruner =
            range_pruner::new_range_filter_pruner(ctx, filter_expression, &schema)?;

        let range_filter_pruner: Arc<dyn RangeFilterPruner + Send + Sync> =
            range_filter_pruner.into();

        let dal = ctx.get_storage_operator()?;
        let enable_bloom_index = ctx.get_settings().get_enable_bloom_filter_index()?;
        let bloom_filter_pruner = if enable_bloom_index != 0 {
            // prepare the bloom filter, if filter_expression is none, an dummy pruner will be returned
            bloom_pruner::new_bloom_filter_pruner(ctx, filter_expression, &schema, dal)?
        } else {
            // toggle off bloom filter according the session settings
            Box::new(NonPruner)
        };

        let bloom_filter_pruner = Arc::new(bloom_filter_pruner);

        let segments_with_idx = segment_locs.into_iter().enumerate();

        let rt = ctx.get_storage_runtime();
        let futs = segments_with_idx
            .map(|(idx, (seg_loc, ver))| {
                let ctx = ctx.clone();
                let range_filter_pruner = range_filter_pruner.clone();
                let bloom_filter_pruner = bloom_filter_pruner.clone();
                let limiter = limiter.clone();
                async move {
                    let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
                    let segment_info = segment_reader.read(seg_loc, None, ver).await?;
                    let mut result = Vec::with_capacity(segment_info.blocks.len());
                    // prune segment using range filter
                    if range_filter_pruner.should_keep(&segment_info.summary.col_stats) {
                        for block_meta in &segment_info.blocks {
                            // prune block using range filter, and bloom filter if necessary
                            if range_filter_pruner.should_keep(&block_meta.col_stats)
                                && bloom_filter_pruner
                                    .should_keep(block_meta.location.0.as_str())
                                    .await
                            {
                                result.push((idx, block_meta.clone()));
                                if !limiter.within_limit(block_meta.row_count as usize) {
                                    break;
                                }
                            }
                        }
                    }
                    Ok::<_, ErrorCode>(result)
                }
                .instrument(tracing::debug_span!("filter_segment_rt_storage"))
            })
            .map(|fut| rt.spawn(fut))
            .collect::<Vec<_>>();

        let block_metas = futures::future::try_join_all(futs).await.unwrap();

        Ok(block_metas
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>())

        // switching to storage runtime and explicit spawn
        //
        // let segment_num = segment_locs.len();
        // struct NonCopy<T>(T);
        //// convert u64 (which is Copy) into NonCopy( struct which is !Copy)
        //// so that "async move" can be avoided in the latter async block
        //// See https://github.com/rust-lang/rust/issues/81653
        // let segments_with_idx = segment_locs
        //    .into_iter()
        //    .enumerate()
        //    .map(|(idx, (loc, ver))| (NonCopy(idx), (loc, NonCopy(ver))));
        // let block_metas = futures::stream::iter(segments_with_idx)
        //    .map(|(idx, (seg_loc, ver))| async {
        //        let version = { ver }.0; // use block expression to force moving
        //        let idx = { idx }.0;
        //        let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
        //        let segment_info = segment_reader.read(seg_loc, None, version).await?;
        //        let mut result = Vec::with_capacity(segment_info.blocks.len());
        //        // prune segment using range filter
        //        if range_filter_pruner.should_keep(&segment_info.summary.col_stats) {
        //            for block_meta in &segment_info.blocks {
        //                // prune block using range filter, and bloom filter if necessary
        //                if range_filter_pruner.should_keep(&block_meta.col_stats)
        //                    && bloom_filter_pruner
        //                        .should_keep(block_meta.location.0.as_str())
        //                        .await
        //                {
        //                    result.push((idx, block_meta.clone()));
        //                    // doc here
        //                    if !limiter.within_limit(block_meta.row_count as usize) {
        //                        break;
        //                    }
        //                }
        //            }
        //        }
        //        Ok::<_, ErrorCode>(result)
        //    })
        //    .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
        //    .try_collect::<Vec<_>>()
        //    .await?;
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
