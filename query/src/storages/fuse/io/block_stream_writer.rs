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

use std::cmp::Reverse;
use std::sync::Arc;

use async_stream::stream;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::stream::TryChunksError;
use futures::StreamExt;
use futures::TryStreamExt;

use super::block_writer;
use crate::storages::fuse::io::locations::gen_block_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;

pub type SegmentInfoStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<SegmentInfo>> + Send>>;

pub struct BlockStreamWriter;

impl BlockStreamWriter {
    pub async fn write_block_stream(
        data_accessor: Arc<dyn DataAccessor>,
        stream: SendableDataBlockStream,
        data_schema: Arc<DataSchema>,
        chunk_block_num: usize,
        block_size_threshold: usize,
    ) -> SegmentInfoStream {
        let s = stream! {
            // filter out empty blocks
            let stream = stream.try_filter(|block| std::future::ready(block.num_rows() > 0));

            // chunks by chunk_block_num
            let mut stream = stream.try_chunks(chunk_block_num);

            // accumulate the stats and save the blocks
            while let Some(item) = stream.next().await {
                match item.map_err(|TryChunksError(_, e)| e) {
                    Err(e) => yield(Err(e)),
                    Ok(blocks) => {
                        let seg = Self::generate_segment(data_accessor.clone(), data_schema.clone(), blocks, block_size_threshold).await;
                        yield(seg);
                    }
                }
            }
        };
        Box::pin(s)
    }

    pub async fn generate_segment(
        data_accessor: Arc<dyn DataAccessor>,
        data_schema: Arc<DataSchema>,
        blocks: Vec<DataBlock>,
        block_size_threshold: usize,
    ) -> Result<SegmentInfo> {
        // re-shape the blocks
        let blocks = Self::reshape_blocks(blocks, block_size_threshold)?;
        let mut acc = StatisticsAccumulator::new();

        for block in blocks.into_iter() {
            let partial_acc = acc.begin(&block)?;
            let schema = block.schema().to_arrow();
            let location = gen_block_location();
            let file_size =
                block_writer::write_block(&schema, block, &data_accessor, &location).await?;
            acc = partial_acc.end(file_size, location);
        }

        // summary and generate a segment
        let summary = acc.summary(data_schema.as_ref())?;
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
        Ok(seg)
    }

    // A simple strategy of merging small blocks into larger ones:
    // for each n successive data blocks in `blocks`, if the sum of their `memory_size` exceeds
    //   `block_size_threshold`, they will be merged into one larger block.
    //  NOTE:
    //    - the max size of merge-block will be 2 * block_size_threshold
    //    - for block that is larger than `block_size_threshold`, they will NOT be split
    pub fn reshape_blocks(
        mut blocks: Vec<DataBlock>,
        block_size_threshold: usize,
    ) -> Result<Vec<DataBlock>> {
        // sort by memory_size DESC
        blocks.sort_unstable_by_key(|r| Reverse(r.memory_size()));

        let mut result = vec![];

        let mut block_size_acc = 0;
        let mut block_acc = vec![];

        for block in blocks {
            block_size_acc += block.memory_size();
            block_acc.push(block);
            if block_size_acc >= block_size_threshold {
                result.push(DataBlock::concat_blocks(&block_acc)?);
                block_acc.clear();
                block_size_acc = 0;
            }
        }

        if !block_acc.is_empty() {
            result.push(DataBlock::concat_blocks(&block_acc)?)
        }

        Ok(result)
    }
}
