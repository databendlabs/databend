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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::stream::try_unfold;
use futures::stream::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;

use crate::storages::fuse::io::block_writer;
use crate::storages::fuse::io::locations::gen_block_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;

pub type SegmentInfoStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<SegmentInfo>> + Send>>;

pub struct BlockStreamWriter {
    num_block_threshold: usize,
    data_accessor: Operator,
    data_schema: Arc<DataSchema>,
    number_of_blocks_accumulated: usize,
    statistics_accumulator: Option<StatisticsAccumulator>,
}

impl BlockStreamWriter {
    pub async fn write_block_stream(
        data_accessor: Operator,
        block_stream: SendableDataBlockStream,
        data_schema: Arc<DataSchema>,
        row_per_block: usize,
        block_per_segment: usize,
    ) -> SegmentInfoStream {
        // filter out empty blocks
        let block_stream =
            block_stream.try_filter(|block| std::future::ready(block.num_rows() > 0));

        // merge or split the blocks according to the settings `row_per_block`
        let block_stream_shaper = BlockRegulator::new(row_per_block);
        let block_stream = Self::transform(block_stream, block_stream_shaper);
        // flatten a TryStream of Vec<DataBlock> into a TryStream of DataBlock
        let block_stream = block_stream
            .map_ok(|vs| futures::stream::iter(vs.into_iter().map(Ok)))
            .try_flatten();

        // Write out the blocks.
        // And transform the stream of DataBlocks into Stream of SegmentInfo at the same time.
        let block_writer = BlockStreamWriter::new(block_per_segment, data_accessor, data_schema);
        let segments = Self::transform(Box::pin(block_stream), block_writer);

        Box::pin(segments)
    }

    pub fn new(
        num_block_threshold: usize,
        data_accessor: Operator,
        data_schema: Arc<DataSchema>,
    ) -> Self {
        Self {
            num_block_threshold,
            data_accessor,
            data_schema,
            number_of_blocks_accumulated: 0,
            statistics_accumulator: None,
        }
    }

    /// Transforms a stream of S to a stream of T
    ///
    /// It's more like [Stream::filter_map] than [Stream::map] in the sense
    /// that m items of input stream may be mapped to n items, where m <> n (but
    /// for the convenience of impl, [TryStreamExt::try_unfold] is used).
    fn transform<R, A, S, T>(inputs: R, mapper: A) -> impl futures::stream::Stream<Item = Result<T>>
    where
        R: Stream<Item = Result<S>> + Unpin,
        A: Regulator<S, T>,
    {
        // For the convenience of passing mutable state back and forth, `unfold` is used
        let init_state = (Some(mapper), inputs);
        try_unfold(init_state, |(mapper, mut inputs)| async move {
            if let Some(mut acc) = mapper {
                while let Some(item) = inputs.next().await {
                    match acc.regulate(item?).await? {
                        Some(item) => return Ok(Some((item, (Some(acc), inputs)))),
                        None => continue,
                    }
                }
                let remains = acc.seal()?;
                Ok(remains.map(|t| (t, (None, inputs))))
            } else {
                Ok::<_, ErrorCode>(None)
            }
        })
    }

    pub async fn write_block(&mut self, block: DataBlock) -> Result<Option<SegmentInfo>> {
        let mut acc = self.statistics_accumulator.take().unwrap_or_default();
        let partial_acc = acc.begin(&block)?;
        let schema = block.schema().to_arrow();
        let location = gen_block_location();
        let file_size =
            block_writer::write_block(&schema, block, self.data_accessor.clone(), &location)
                .await?;
        acc = partial_acc.end(file_size, location);
        self.number_of_blocks_accumulated += 1;
        if self.number_of_blocks_accumulated >= self.num_block_threshold {
            let summary = acc.summary(self.data_schema.as_ref())?;
            let seg = SegmentInfo {
                blocks: acc.blocks_metas,
                summary: Statistics {
                    row_count: acc.summary_row_count,
                    block_count: acc.summary_block_count,
                    uncompressed_byte_size: acc.in_memory_size,
                    compressed_byte_size: acc.file_size,
                    col_stats: summary,
                },
            };

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
}

/// Takes elements of type S in, and spills elements of type T.
///
/// The "shape" of the sequence of S might be not be preserved.
#[async_trait::async_trait]
pub trait Regulator<S, T> {
    /// Takes an element s of type S, convert it into [Some<T>] if possible;
    /// otherwise, returns [None]
    ///
    /// for example. given a DataBlock s, a setting of `max_row_per_block`
    ///    - Some<Vec<DataBlock>> might be returned if s contains more rows than `max_row_per_block`
    ///       in this case, s will been split into vector of (smaller) blocks
    ///    - or [None] might be returned if s is too small
    ///       in this case, s will be accumulated
    async fn regulate(&mut self, s: S) -> Result<Option<T>>;

    /// Indicate that no more elements remains.
    ///
    /// Spills [Some<T>] if there were, otherwise [None]
    fn seal(self) -> Result<Option<T>>;
}

#[async_trait::async_trait]
impl Regulator<DataBlock, SegmentInfo> for BlockStreamWriter {
    async fn regulate(&mut self, s: DataBlock) -> Result<Option<SegmentInfo>> {
        self.write_block(s).await
    }

    fn seal(mut self) -> Result<Option<SegmentInfo>> {
        let acc = self.statistics_accumulator.take();
        let data_schema = self.data_schema.as_ref();
        match acc {
            None => Ok(None),
            Some(acc) => {
                let summary = acc.summary(data_schema)?;
                let seg = SegmentInfo {
                    blocks: acc.blocks_metas,
                    summary: Statistics {
                        row_count: acc.summary_row_count,
                        block_count: acc.summary_block_count,
                        uncompressed_byte_size: acc.in_memory_size,
                        compressed_byte_size: acc.file_size,
                        col_stats: summary,
                    },
                };
                Ok(Some(seg))
            }
        }
    }
}

pub struct BlockRegulator {
    /// Max number of rows per data block
    max_row_per_block: usize,
    /// Number of rows accumulate in `accumulated_blocks`.
    accumulated_rows: usize,
    /// Small data blocks accumulated
    ///
    /// Invariant: accumulated_blocks.iter().map(|item| item.num_rows()).sum() < max_row_per_block
    accumulated_blocks: Vec<DataBlock>,
}

impl BlockRegulator {
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
    pub fn regulate(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
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
    pub fn seal(self) -> Result<Option<Vec<DataBlock>>> {
        let remains = self.accumulated_blocks;
        Ok(if remains.is_empty() {
            None
        } else {
            Some(vec![DataBlock::concat_blocks(&remains)?])
        })
    }
}

// A delegation to keep trait [Regulator] private (to unit tests)
#[async_trait::async_trait]
impl Regulator<DataBlock, Vec<DataBlock>> for BlockRegulator {
    async fn regulate(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        BlockRegulator::regulate(self, block)
    }

    fn seal(self) -> Result<Option<Vec<DataBlock>>> {
        BlockRegulator::seal(self)
    }
}
