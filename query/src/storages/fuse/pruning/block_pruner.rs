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

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Extras;
use common_streams::ParquetSourceBuilder;
use common_streams::Source;
use common_tracing::tracing;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::index::BloomFilterExprEvalResult;
use crate::storages::index::BloomFilterIndexer;
use crate::storages::index::RangeFilter;
use crate::storages::index::StatisticsOfColumns;

pub struct BlockPruner {
    table_snapshot: Arc<TableSnapshot>,
}

type Pred = Box<dyn Fn(&StatisticsOfColumns) -> Result<bool> + Send + Sync + Unpin>;
impl BlockPruner {
    pub fn new(table_snapshot: Arc<TableSnapshot>) -> Self {
        Self { table_snapshot }
    }

    #[tracing::instrument(level = "debug", name="block_pruner_apply", skip(self, schema, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn apply(
        &self,
        ctx: &Arc<QueryContext>,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
    ) -> Result<Vec<(usize, BlockMeta)>> {
        let block_pred: Pred = match push_down {
            Some(exprs) if !exprs.filters.is_empty() => {
                // TODO `exprs`.filters should be typed as Option<Expression>
                let range_filter =
                    RangeFilter::try_create(ctx.clone(), &exprs.filters[0], schema.clone())?;
                Box::new(move |v: &StatisticsOfColumns| range_filter.eval(v))
            }
            _ => {
                // TODO arrange a shortcut for this?
                Box::new(|_: &StatisticsOfColumns| Ok(true))
            }
        };

        let segment_locs = self.table_snapshot.segments.clone();
        let segment_num = segment_locs.len();

        if segment_locs.is_empty() {
            return Ok(vec![]);
        };

        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        // Segments and blocks are accumulated concurrently, thus an atomic counter is used
        // to **try** collecting as less blocks as possible. But concurrency is preferred to
        // "accuracy". In [FuseTable::do_read_partitions], the "limit" will be treated precisely.

        let accumulated_rows = AtomicUsize::new(0);

        // A !Copy Wrapper of u64
        struct NonCopy<T>(T);

        // convert u64 (which is Copy) into NonCopy( struct which is !Copy)
        // so that "async move" can be avoided in the latter async block
        // See https://github.com/rust-lang/rust/issues/81653
        let segment_locs = segment_locs
            .into_iter()
            .enumerate()
            .map(|(idx, (loc, ver))| (NonCopy(idx), (loc, NonCopy(ver))));

        let dal = ctx.get_storage_operator()?;
        let bloom_index_schema = BloomFilterIndexer::to_bloom_schema(schema.as_ref());

        // map from filed name to index
        let bloom_field_to_idx = bloom_index_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, filed)| (filed.name().clone(), idx))
            .collect::<HashMap<_, _>>();

        let stream = futures::stream::iter(segment_locs)
            .map(|(idx, (seg_loc, u))| async {
                let version = { u }.0; // use block expression to force moving
                let idx = { idx }.0;
                if accumulated_rows.load(Ordering::Acquire) < limit {
                    let segment_reader = MetaReaders::segment_info_reader(ctx);
                    let segment_info = segment_reader.read(seg_loc, None, version).await?;
                    let blocks = Self::filter_segment(
                        segment_info.as_ref(),
                        &block_pred,
                        &accumulated_rows,
                        limit,
                    )?
                    .into_iter()
                    .map(|v| (idx, v));

                    if let Some(Extras {
                        projection: Some(proj),
                        filters,
                        ..
                    }) = push_down
                    {
                        let mut res = vec![];
                        if !filters.is_empty() {
                            let filter_expr = &filters[0];

                            let mut parquet_source_builder =
                                ParquetSourceBuilder::create(Arc::new(bloom_index_schema.clone()));

                            // collects the column index that gonna be used
                            let mut projection = Vec::with_capacity(schema.fields().len());
                            for idx in proj {
                                let field = &schema.fields()[*idx];
                                let bloom_filter_col_name =
                                    BloomFilterIndexer::to_bloom_column_name(field.name());
                                if let Some(pos) = bloom_field_to_idx.get(&bloom_filter_col_name) {
                                    projection.push(*pos)
                                }
                            }
                            parquet_source_builder.projection(projection);

                            // load filters of columns
                            for (idx, meta) in blocks {
                                let bloom_idx_location =
                                    TableMetaLocationGenerator::block_bloom_index_location(
                                        &meta.location.0,
                                    );
                                let object = dal.object(bloom_idx_location.as_str());
                                let reader = object.seekable_reader(0..);
                                let mut source = parquet_source_builder.build(reader)?;
                                let block = source.read().await?.unwrap();
                                let ctx = ctx.clone();
                                let index = BloomFilterIndexer::from_bloom_block(
                                    schema.clone(),
                                    block,
                                    ctx,
                                )?;
                                if BloomFilterExprEvalResult::False != index.eval(filter_expr)? {
                                    res.push((idx, meta))
                                }
                            }
                            Ok::<_, ErrorCode>(res)
                        } else {
                            Ok::<_, ErrorCode>(blocks.collect::<Vec<_>>())
                        }
                    } else {
                        Ok::<_, ErrorCode>(blocks.collect::<Vec<_>>())
                    }
                } else {
                    Ok(vec![])
                }
            })
            // configuration of the max size of buffered futures
            .buffered(std::cmp::min(10, segment_num))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten();

        Ok(stream.collect::<Vec<_>>())
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
                    if accumulated_rows.fetch_add(num_rows, Ordering::Release) < limit {
                        acc.push(block_meta.clone());
                    }
                }
            }
            Ok(acc)
        } else {
            Ok(vec![])
        }
    }
}
