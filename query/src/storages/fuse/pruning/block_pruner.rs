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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_columns_many_async;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_fuse_meta::meta::TableSnapshot;
use common_planners::find_column_exprs;
use common_planners::Expression;
use common_planners::Extras;
use common_tracing::tracing;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;

use crate::sessions::TableContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::index::BloomFilterExprEvalResult;
use crate::storages::index::BloomFilterIndexer;
use crate::storages::index::RangeFilter;

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
        ctx: &Arc<dyn TableContext>,
        schema: DataSchemaRef,
        push_down: &Option<Extras>,
    ) -> Result<Vec<(usize, BlockMeta)>> {
        let block_pred: Pred = match push_down {
            Some(exprs) if !exprs.filters.is_empty() => {
                // for the time being, we only handle the first expr
                let range_filter =
                    RangeFilter::try_create(ctx.clone(), &exprs.filters[0], schema.clone())?;
                Box::new(move |v: &StatisticsOfColumns| range_filter.eval(v))
            }
            _ => Box::new(|_: &StatisticsOfColumns| Ok(true)),
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
        //        let bloom_index_schema = BloomFilterIndexer::to_bloom_schema(schema.as_ref());
        //
        //        // map from filed name to index
        //        let bloom_fields = bloom_index_schema
        //            .fields()
        //            .iter()
        //            .map(|filed| (field.name, filed.clone()))
        //            .collect::<HashMap<_, _>>();

        let stream = futures::stream::iter(segment_locs)
            .map(|(idx, (seg_loc, u))| async {
                // use block expression to force moving
                let version = { u }.0;
                let idx = { idx }.0;

                if accumulated_rows.load(Ordering::Acquire) < limit {
                    let segment_reader = MetaReaders::segment_info_reader(ctx.as_ref());
                    let segment_info = segment_reader.read(seg_loc, None, version).await?;
                    let blocks = Self::filter_segment(
                        segment_info.as_ref(),
                        &block_pred,
                        &accumulated_rows,
                        limit,
                    )?
                    .into_iter()
                    .map(|v| (idx, v));

                    if let Some(Extras { filters, .. }) = push_down {
                        let mut res = vec![];
                        if !filters.is_empty() {
                            // TODO init these once
                            let filter_expr = &filters[0];
                            let filter_block_project = filter_columns(filter_expr);

                            // load filters of columns
                            for (idx, meta) in blocks {
                                let bloom_idx_location =
                                    TableMetaLocationGenerator::block_bloom_index_location(
                                        &meta.location.0,
                                    );
                                let block = load_filter_columns(
                                    dal.clone(),
                                    &filter_block_project,
                                    &bloom_idx_location,
                                )
                                .await?;
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

fn filter_columns(filter_expr: &Expression) -> Vec<String> {
    // TODO avoid this clone!!!
    find_column_exprs(&[filter_expr.clone()])
        .iter()
        .map(|e| BloomFilterIndexer::to_bloom_column_name(&e.column_name()))
        .collect::<Vec<_>>()
}

async fn load_filter_columns(
    dal: Operator,
    projection: &[String],
    location: &str,
) -> Result<DataBlock> {
    use common_datavalues::ToDataType;
    let object = dal.object(location);
    let mut reader = object.seekable_reader(0..);
    let file_meta = read_metadata_async(&mut reader).await?;
    let row_groups = file_meta.row_groups;
    // TODO filter out columns that not in the bloom block
    let fields = projection
        .iter()
        .map(|name| DataField::new(name, Vu8::to_data_type()))
        .collect::<Vec<_>>();
    let row_group = &row_groups[0];
    let arrow_fields = fields.iter().map(|f| f.to_arrow()).collect::<Vec<_>>();
    let arrays = read_columns_many_async(
        || Box::pin(async { Ok(object.seekable_reader(0..)) }),
        row_group,
        arrow_fields,
        None,
    )
    .await?;
    // TODO error handling
    let chunk = RowGroupDeserializer::new(arrays, row_group.num_rows(), None)
        .next()
        .unwrap()?;
    let schema = Arc::new(DataSchema::new(fields));
    DataBlock::from_chunk(&schema, &chunk)
}
