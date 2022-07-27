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

use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_arrow::parquet::FallibleStreamingIterator;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_fuse_meta::meta::TableSnapshot;
use common_planners::Extras;
use common_tracing::tracing;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use super::util;
use crate::sessions::TableContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::pruning::limiter::BloomFilterPruner;
use crate::storages::fuse::pruning::limiter::BloomPruner;
use crate::storages::fuse::pruning::limiter::Limiter;
use crate::storages::fuse::pruning::limiter::Unlimited;
use crate::storages::index::RangeFilter;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

const FUTURE_BUFFER_SIZE: usize = 10;

type Pred = Box<dyn Fn(&StatisticsOfColumns) -> Result<bool> + Send + Sync + Unpin>;

type BloomFilter = Box<dyn Fn(&str) -> dyn Future<Output = Result<bool>>>;

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
        let limiter: Box<dyn Limiter + Send + Sync> = if let Some(size) = limit {
            Box::new(AtomicUsize::new(size))
        } else {
            Box::new(Unlimited)
        };

        let range_filter = if let Some(expr) = filter_expr {
            RangeFilter::try_create(ctx.clone(), expr, schema.clone())?
        } else {
            todo!()
        };

        let dal = ctx.get_storage_operator()?;

        let bloom_filter: Box<dyn BloomPruner + Send + Sync> = if let Some(expr) = limit {
            Box::new(BloomFilterPruner::new())
        } else {
            todo!()
        };

        let block_pred: Pred = match push_down {
            Some(exprs) if !exprs.filters.is_empty() => {
                // for the time being, we only handle the first expr
                let range_filter = RangeFilter::try_create(ctx.clone(), &exprs.filters[0], schema)?;
                Box::new(move |v: &StatisticsOfColumns| range_filter.eval(v))
            }
            _ => Box::new(|_: &StatisticsOfColumns| Ok(true)),
        };

        let segment_num = segment_locs.len();

        // A !Copy Wrapper of u64
        struct NonCopy<T>(T);

        // convert u64 (which is Copy) into NonCopy( struct which is !Copy)
        // so that "async move" can be avoided in the latter async block
        // See https://github.com/rust-lang/rust/issues/81653
        let segs = segment_locs
            .into_iter()
            .enumerate()
            .map(|(idx, (loc, ver))| (NonCopy(idx), (loc, NonCopy(ver))));

        let block_metas = futures::stream::iter(segs.into_iter())
            .map(|(idx, (seg_loc, ver))| async {
                let version = { ver }.0; // use block expression to force moving
                let idx = { idx }.0;
                let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
                let segment_info = segment_reader.read(seg_loc, None, version).await?;
                let mut result = vec![];
                if range_filter.eval(&segment_info.summary.col_stats)? {
                    for block_meta in &segment_info.blocks {
                        if range_filter.eval(&block_meta.col_stats)? {
                            if bloom_filter.eval(block_meta.location.0.as_str()).await? {
                                if limiter.within_limit(block_meta.row_count as usize) {
                                    result.push((idx, block_meta.clone()))
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok::<_, ErrorCode>(result)
            })
            .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
            .try_collect::<Vec<_>>()
            .await?;

        return Ok(block_metas.into_iter().flatten().collect::<Vec<_>>());

        // let segment_stream = Self::stream_of_all_the_segments(segment_locs.clone(), ctx.as_ref());
        // let segment_num = segment_locs.len();
        // let filtered_blocks = segment_stream
        // .map(|item| async {
        // let (idx, seg) = item.await?;
        // if range_filter.eval(&seg.summary.col_stats)? {
        // let block_num = seg.blocks.len();
        // let mut acc = Vec::with_capacity(block_num);
        // for block_meta in &seg.blocks {
        // if range_filter.eval(&block_meta.col_stats)? {
        // if bloom_filter.eval(block_meta.location.0.as_str()).await? {
        // let num_rows = block_meta.row_count as usize;
        // if limiter.within_limit(num_rows) {
        // acc.push((idx, block_meta.clone()));
        // } else {
        // break;
        // }
        // }
        // }
        // }
        // Ok::<_, ErrorCode>(acc)
        // } else {
        // Ok(vec![])
        // }
        // })
        // .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
        // .try_collect::<Vec<_>>()
        // .await?;
        //
        // todo!()
        //
        // Ok(filtered_blocks.into_iter().flatten().collect::<Vec<_>>())
        //
    }

    #[inline]
    fn filter_segment(
        segment_info: &SegmentInfo,
        pred: &Pred,
        accumulated_rows: &AtomicUsize,
        limit: usize,
    ) -> Result<Vec<BlockMeta>> {
        if pred(&segment_info.summary.col_stats)? {
            let block_num = segment_info.blocks.len();
            let mut acc = Vec::with_capacity(block_num);
            for block_meta in &segment_info.blocks {
                if pred(&block_meta.col_stats)? {
                    let num_rows = block_meta.row_count as usize;
                    if accumulated_rows.fetch_add(num_rows, Ordering::Relaxed) < limit {
                        acc.push(block_meta.clone());
                    }
                }
            }
            Ok(acc)
        } else {
            Ok(vec![])
        }
    }

    fn stream_of_all_the_segments<'a>(
        segment_locs: Vec<Location>,
        ctx: &'a dyn TableContext,
    ) -> impl Stream<Item = impl Future<Output = Result<(usize, Arc<SegmentInfo>)>> + 'a> {
        futures::stream::iter(segment_locs.into_iter().enumerate()).map(
            move |(idx, (seg_loc, ver))| async move {
                let segment_reader = MetaReaders::segment_info_reader(ctx);
                let segment_info = segment_reader.read(seg_loc, None, ver).await?;
                Ok::<_, ErrorCode>((idx, segment_info))
            },
        )
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
                        .map(move |item| (idx, item.clone())),
                )
            })
            .buffered(std::cmp::min(FUTURE_BUFFER_SIZE, segment_num))
            .try_collect::<Vec<_>>()
            .await?;
        return Ok(block_metas.into_iter().flatten().collect::<Vec<_>>());
    }
}

// TODO unit test cases:
// 0. normal case
// 1. col has no bloom index
// 1.1. error handling -> should not break the execution
// 1.2. (c1 = x and  c2_no_bloom_idx = b) should work, if bloom indicates that x not exists
