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
//
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::stream::TryChunksError;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::storages::fuse::io;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Stats;
use crate::storages::fuse::statistics;
use crate::storages::fuse::statistics::BlockMetaAccumulator;
use crate::storages::fuse::statistics::StatisticsAccumulator;

pub struct BlockAppender;

impl BlockAppender {
    // A simple strategy of merging small blocks into larger ones:
    // for each n successive data blocks in `blocks`, if the sum of their `memory_size` exceeds
    //   `block_size_threshold`, they will be merged into one larger block.
    //  NOTE:
    //    - the max size of merge-block will be 2 * block_size_threshold
    //    - for block that is larger than `block_size_threshold`, they will NOT be split
    //        TODO handling split in table compact/optimize
    pub(crate) fn reshape_blocks(
        mut blocks: Vec<DataBlock>,
        block_size_threshold: usize,
    ) -> Result<Vec<DataBlock>> {
        // sort by memory_size DESC
        blocks.sort_unstable_by(|l, r| r.memory_size().cmp(&l.memory_size()));

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

    // TODO should return a stream of SegmentInfo (batch blocks into segments)
    pub async fn append_blocks(
        data_accessor: Arc<dyn DataAccessor>,
        stream: SendableDataBlockStream,
        data_schema: &DataSchema,
        chunk_block_num: usize,
        block_size_threshold: usize,
    ) -> Result<Vec<SegmentInfo>> {
        // filter out empty blocks
        let stream = stream.try_filter(|block| std::future::ready(block.num_rows() > 0));

        // chunks by chunk_block_num
        let mut stream = stream.try_chunks(chunk_block_num);

        let mut segments = vec![];
        // accumulate the stats and save the blocks
        while let Some(item) = stream.next().await {
            let blocks = item.map_err(|TryChunksError(_, e)| e)?;
            // re-shape the blocks
            let blocks = Self::reshape_blocks(blocks, block_size_threshold)?;
            let mut stats_acc = StatisticsAccumulator::new();
            let mut block_meta_acc = BlockMetaAccumulator::new();

            for block in blocks.into_iter() {
                stats_acc.acc(&block)?;
                let schema = block.schema().to_arrow();
                let location = io::gen_block_location();
                let file_size = Self::save_block(&schema, block, &data_accessor, &location).await?;
                block_meta_acc.acc(file_size, location, &mut stats_acc);
            }

            // summary and give back a segment_info
            // we need to send back a stream of segment latter
            let block_metas = block_meta_acc.blocks_metas;
            let summary = statistics::reduce_block_stats(&stats_acc.blocks_stats, data_schema)?;
            let seg = SegmentInfo {
                blocks: block_metas,
                summary: Stats {
                    row_count: stats_acc.summary_row_count,
                    block_count: stats_acc.summary_block_count,
                    uncompressed_byte_size: stats_acc.in_memory_size,
                    compressed_byte_size: stats_acc.file_size,
                    col_stats: summary,
                },
            };
            segments.push(seg)
        }
        Ok(segments)
    }

    pub(super) async fn save_block(
        arrow_schema: &ArrowSchema,
        block: DataBlock,
        data_accessor: impl AsRef<dyn DataAccessor>,
        location: &str,
    ) -> Result<u64> {
        let data_accessor = data_accessor.as_ref();
        let options = WriteOptions {
            write_statistics: true,
            compression: Compression::Lz4, // let's begin with lz4
            version: Version::V2,
        };
        let batch = RecordBatch::try_from(block)?;
        let encodings: Vec<_> = arrow_schema
            .fields()
            .iter()
            .map(|f| io::col_encoding(&f.data_type))
            .collect();

        let iter = vec![Ok(batch)];
        let row_groups =
            RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
        let parquet_schema = row_groups.parquet_schema().clone();

        // PutObject in S3 need to know the content-length in advance
        // multipart upload may intimidate this, but let's fit things together first
        // see issue #xxx

        use bytes::BufMut;
        // we need a configuration of block size threshold here
        let mut writer = Vec::with_capacity(100 * 1024 * 1024).writer();

        let len = common_arrow::parquet::write::write_file(
            &mut writer,
            row_groups,
            parquet_schema,
            options,
            None,
            None,
        )
        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

        let parquet = writer.into_inner();
        let stream_len = parquet.len();
        let stream = futures::stream::once(async move { Ok(bytes::Bytes::from(parquet)) });
        data_accessor
            .put_stream(location, Box::new(Box::pin(stream)), stream_len)
            .await?;

        Ok(len)
    }
}
