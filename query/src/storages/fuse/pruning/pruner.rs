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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::TableSnapshot;
use common_planners::Extras;
use common_tracing::tracing;
use futures::StreamExt;
use futures::TryStreamExt;

use super::bloom_filter_predicate;
use crate::sessions::TableContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::pruning::limiter;
use crate::storages::fuse::pruning::range_filter_predicate;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

const FUTURE_BUFFER_SIZE: usize = 10;

impl BlockPruner {
    pub fn new(table_snapshot: Arc<TableSnapshot>) -> Self {
        Self { table_snapshot }
    }

    #[tracing::instrument(level = "debug", name="block_pruner_apply", skip(self, schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn apply(
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

        let filter_expr = push_down.as_ref().and_then(|extra| extra.filters.get(0));

        // shortcut, just returns all the blocks
        if limit.is_none() && filter_expr.is_none() {
            return Self::all_the_blocks(segment_locs, ctx.as_ref()).await;
        }

        // prepare the limiter
        let limiter = limiter::new_limiter(limit);

        // prepare the min/max pruner
        let range_filter_pruner = range_filter_predicate::new(ctx, filter_expr, &schema)?;

        let dal = ctx.get_storage_operator()?;

        let bloom_filter_pruner = bloom_filter_predicate::new(ctx, filter_expr, &schema, &dal);

        let segment_num = segment_locs.len();

        struct NonCopy<T>(T);
        // convert u64 (which is Copy) into NonCopy( struct which is !Copy)
        // so that "async move" can be avoided in the latter async block
        // See https://github.com/rust-lang/rust/issues/81653
        let segments_with_idx = segment_locs
            .into_iter()
            .enumerate()
            .map(|(idx, (loc, ver))| (NonCopy(idx), (loc, NonCopy(ver))));

        let block_metas = futures::stream::iter(segments_with_idx)
            .map(|(idx, (seg_loc, ver))| async {
                let version = { ver }.0; // use block expression to force moving
                let idx = { idx }.0;
                let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
                let segment_info = segment_reader.read(seg_loc, None, version).await?;
                let mut result = Vec::with_capacity(segment_info.blocks.len());
                if range_filter_pruner(&segment_info.summary.col_stats)? {
                    for block_meta in &segment_info.blocks {
                        if range_filter_pruner(&block_meta.col_stats)?
                            && bloom_filter_pruner
                                .eval(block_meta.location.0.as_str())
                                .await?
                        {
                            result.push((idx, block_meta.clone()));
                            if !limiter.within_limit(block_meta.row_count as usize) {
                                break;
                            }
                        }
                    }
                }
                Ok::<_, ErrorCode>(result)
            })
            .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(block_metas.into_iter().flatten().collect::<Vec<_>>())
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

// TODO unit test cases:
// 0. normal case
// 1. col has no bloom index
// 1.1. error handling -> should not break the execution
// 1.2. (c1 = x and  c2_no_bloom_idx = b) should work, if bloom indicates that x not exists
