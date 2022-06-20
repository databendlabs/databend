// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::parquet::metadata::FileMetaData;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use futures::stream::try_unfold;
use futures::stream::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;

use super::block_writer;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::ColumnMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::statistics::accumulator::BlockStatistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;
use crate::storages::index::ClusterKeyInfo;

pub type SegmentInfoStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<SegmentInfo>> + Send>>;

pub struct BlockStreamWriter {
    num_block_threshold: usize,
    data_accessor: Operator,
    number_of_blocks_accumulated: usize,
    statistics_accumulator: Option<StatisticsAccumulator>,
    meta_locations: TableMetaLocationGenerator,
    cluster_key_info: Option<ClusterKeyInfo>,
    ctx: Arc<QueryContext>,
}

impl BlockStreamWriter {
    pub async fn write_block_stream(
        ctx: Arc<QueryContext>,
        block_stream: SendableDataBlockStream,
        row_per_block: usize,
        block_per_segment: usize,
        meta_locations: TableMetaLocationGenerator,
        cluster_key_info: Option<ClusterKeyInfo>,
    ) -> SegmentInfoStream {
        // filter out empty blocks
        let block_stream =
            block_stream.try_filter(|block| std::future::ready(block.num_rows() > 0));

        // merge or split the blocks according to the settings `row_per_block`
        let block_stream_shaper = BlockCompactor::new(row_per_block);
        let block_stream = Self::transform(block_stream, block_stream_shaper);
        // flatten a TryStream of Vec<DataBlock> into a TryStream of DataBlock
        let block_stream = block_stream
            .map_ok(|vs| futures::stream::iter(vs.into_iter().map(Ok)))
            .try_flatten();

        // Write out the blocks.
        // And transform the stream of DataBlocks into Stream of SegmentInfo at the same time.
        let block_writer =
            BlockStreamWriter::new(block_per_segment, meta_locations, ctx, cluster_key_info);
        let segments = Self::transform(Box::pin(block_stream), block_writer);

        Box::pin(segments)
    }

    pub fn new(
        num_block_threshold: usize,
        meta_locations: TableMetaLocationGenerator,
        ctx: Arc<QueryContext>,
        cluster_key_info: Option<ClusterKeyInfo>,
    ) -> Self {
        let data_accessor = ctx.get_storage_operator().unwrap();
        Self {
            num_block_threshold,
            data_accessor,
            number_of_blocks_accumulated: 0,
            statistics_accumulator: None,
            meta_locations,
            cluster_key_info,
            ctx,
        }
    }

    /// Transforms a stream of S to a stream of T
    ///
    /// It's more like [Stream::filter_map] than [Stream::map] in the sense
    /// that m items of input stream may be mapped to n items, where m <> n (but
    /// for the convenience of impl, [TryStreamExt::try_unfold] is used).
    pub(crate) fn transform<R, A, S, T>(
        inputs: R,
        mapper: A,
    ) -> impl futures::stream::Stream<Item = Result<T>>
    where
        R: Stream<Item = Result<S>> + Unpin,
        A: Compactor<S, T>,
    {
        // For the convenience of passing mutable state back and forth, `unfold` is used
        let init_state = (Some(mapper), inputs);
        try_unfold(init_state, |(mapper, mut inputs)| async move {
            if let Some(mut acc) = mapper {
                while let Some(item) = inputs.next().await {
                    match acc.compact(item?).await? {
                        Some(item) => return Ok(Some((item, (Some(acc), inputs)))),
                        None => continue,
                    }
                }
                let remains = acc.finish()?;
                Ok(remains.map(|t| (t, (None, inputs))))
            } else {
                Ok::<_, ErrorCode>(None)
            }
        })
    }

    async fn write_block(&mut self, data_block: DataBlock) -> Result<Option<SegmentInfo>> {
        let mut cluster_stats = None;
        let mut block = data_block;
        if let Some(v) = self.cluster_key_info.as_mut() {
            let input_schema = block.schema().clone();
            let cluster_key_index = if v.cluster_key_index.is_empty() {
                let fields = input_schema.fields().clone();
                let index = v
                    .exprs
                    .iter()
                    .map(|e| {
                        let cname = e.column_name();
                        fields.iter().position(|f| f.name() == &cname).unwrap()
                    })
                    .collect::<Vec<_>>();
                v.cluster_key_index = index.clone();
                index
            } else {
                v.cluster_key_index.clone()
            };

            cluster_stats = BlockStatistics::clusters_statistics(
                v.cluster_key_id,
                cluster_key_index,
                block.clone(),
            )?;

            // Remove unused columns before serialize
            if v.data_schema != input_schema {
                let executor = if let Some(executor) = &v.expression_executor {
                    executor.clone()
                } else {
                    let exprs: Vec<Expression> = input_schema
                        .fields()
                        .iter()
                        .map(|f| Expression::Column(f.name().to_owned()))
                        .collect();

                    let executor = ExpressionExecutor::try_create(
                        self.ctx.clone(),
                        "remove unused columns",
                        input_schema,
                        v.data_schema.clone(),
                        exprs,
                        true,
                    )?;
                    executor.validate()?;
                    v.expression_executor = Some(executor.clone());
                    executor
                };

                block = executor.execute(&block)?;
            }
        }

        let mut acc = self.statistics_accumulator.take().unwrap_or_default();
        let partial_acc = acc.begin(&block, cluster_stats)?;
        let schema = block.schema().to_arrow();
        let location = self.meta_locations.gen_block_location();
        let (file_size, file_meta_data) =
            block_writer::write_block(&schema, block, self.data_accessor.clone(), &location)
                .await?;
        let col_metas = Self::column_metas(&file_meta_data)?;
        acc = partial_acc.end(file_size, location, col_metas);
        self.number_of_blocks_accumulated += 1;
        if self.number_of_blocks_accumulated >= self.num_block_threshold {
            let summary = acc.summary()?;
            let seg = SegmentInfo::new(acc.blocks_metas, Statistics {
                row_count: acc.summary_row_count,
                block_count: acc.summary_block_count,
                uncompressed_byte_size: acc.in_memory_size,
                compressed_byte_size: acc.file_size,
                col_stats: summary,
            });

            // Reset state
            self.number_of_blocks_accumulated = 0;
            self.statistics_accumulator = None;

            Ok(Some(seg))
        } else {
            // Stash the state
            self.statistics_accumulator = Some(acc);

            Ok(None)
        }
    }

    fn column_metas(file_meta: &FileMetaData) -> Result<HashMap<ColumnId, ColumnMeta>> {
        // currently we use one group only
        let num_row_groups = file_meta.row_groups.len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetError(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups[0];
        let mut col_metas = HashMap::with_capacity(row_group.columns().len());
        for (idx, col) in row_group.columns().iter().enumerate() {
            match &col.column_chunk().meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;
                    let res = ColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    col_metas.insert(idx as u32, res);
                }
                None => {
                    return Err(ErrorCode::ParquetError(format!(
                        "invalid parquet file, meta data of column idx {} is empty",
                        idx
                    )))
                }
            }
        }
        Ok(col_metas)
    }
}

/// Takes elements of type S in, and spills elements of type T.
#[async_trait::async_trait]
pub trait Compactor<S, T> {
    /// Takes an element s of type S, convert it into [Some<T>] if possible;
    /// otherwise, returns [None]
    ///
    /// for example. given a DataBlock s, a setting of `max_row_per_block`
    ///    - Some<Vec<DataBlock>> might be returned if s contains more rows than `max_row_per_block`
    ///       in this case, s will been split into vector of (smaller) blocks
    ///    - or [None] might be returned if s is too small
    ///       in this case, s will be accumulated
    async fn compact(&mut self, s: S) -> Result<Option<T>>;

    /// Indicate that no more elements remains.
    ///
    /// Spills [Some<T>] if there were, otherwise [None]
    fn finish(self) -> Result<Option<T>>;
}

#[async_trait::async_trait]
impl Compactor<DataBlock, SegmentInfo> for BlockStreamWriter {
    async fn compact(&mut self, s: DataBlock) -> Result<Option<SegmentInfo>> {
        self.write_block(s).await
    }

    fn finish(mut self) -> Result<Option<SegmentInfo>> {
        let acc = self.statistics_accumulator.take();
        match acc {
            None => Ok(None),
            Some(acc) => {
                let summary = acc.summary()?;
                let seg = SegmentInfo::new(acc.blocks_metas, Statistics {
                    row_count: acc.summary_row_count,
                    block_count: acc.summary_block_count,
                    uncompressed_byte_size: acc.in_memory_size,
                    compressed_byte_size: acc.file_size,
                    col_stats: summary,
                });
                Ok(Some(seg))
            }
        }
    }
}

pub struct BlockCompactor {
    // TODO threshold of block size
    /// Max number of rows per data block
    max_row_per_block: usize,
    /// Number of rows accumulate in `accumulated_blocks`.
    accumulated_rows: usize,
    /// Small data blocks accumulated
    ///
    /// Invariant: accumulated_blocks.iter().map(|item| item.num_rows()).sum() < max_row_per_block
    accumulated_blocks: Vec<DataBlock>,
}

impl BlockCompactor {
    pub fn new(max_row_per_block: usize) -> Self {
        Self {
            max_row_per_block,
            accumulated_rows: 0,
            accumulated_blocks: Vec::new(),
        }
    }
    fn reset(&mut self, remains: Vec<DataBlock>) {
        self.accumulated_rows = remains.iter().map(|item| item.num_rows()).sum();
        self.accumulated_blocks = remains;
    }

    /// split or merge the DataBlock according to the configuration
    pub fn compact(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        let num_rows = block.num_rows();

        // For cases like stmt `insert into .. select ... from ...`, the blocks that feeded
        // are likely to be properly sized, i.e. exeactly `max_row_per_block` rows per block,
        // In that cases, just return them.
        if num_rows == self.max_row_per_block {
            return Ok(Some(vec![block]));
        }

        if num_rows + self.accumulated_rows < self.max_row_per_block {
            self.accumulated_rows += num_rows;
            self.accumulated_blocks.push(block);
            Ok(None)
        } else {
            let mut blocks = std::mem::take(&mut self.accumulated_blocks);
            blocks.push(block);
            let merged = DataBlock::concat_blocks(&blocks)?;
            let blocks = DataBlock::split_block_by_size(&merged, self.max_row_per_block)?;

            let (result, remains) = blocks
                .into_iter()
                .partition(|item| item.num_rows() >= self.max_row_per_block);
            self.reset(remains);
            Ok(Some(result))
        }
    }

    /// Pack the remainders into a DataBlock
    pub fn finish(self) -> Result<Option<Vec<DataBlock>>> {
        let remains = self.accumulated_blocks;
        Ok(if remains.is_empty() {
            None
        } else {
            Some(vec![DataBlock::concat_blocks(&remains)?])
        })
    }
}

#[async_trait::async_trait]
impl Compactor<DataBlock, Vec<DataBlock>> for BlockCompactor {
    async fn compact(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        BlockCompactor::compact(self, block)
    }

    fn finish(self) -> Result<Option<Vec<DataBlock>>> {
        BlockCompactor::finish(self)
    }
}
