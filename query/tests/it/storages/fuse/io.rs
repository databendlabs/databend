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

use common_base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use databend_query::storages::fuse::io::BlockStreamWriter;
use databend_query::storages::fuse::DEFAULT_CHUNK_BLOCK_NUM;
use futures::StreamExt;
use tempfile::TempDir;

#[tokio::test]
async fn test_fuse_table_block_appender() {
    let tmp_dir = TempDir::new().unwrap();
    let local_fs = common_dal::Local::with_path(tmp_dir.path().to_owned());
    let local_fs = Arc::new(local_fs);
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);

    // single segments
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1, 2, 3])]);
    let block_stream = futures::stream::iter(vec![Ok(block)]);

    let segments = BlockStreamWriter::write_block_stream(
        local_fs.clone(),
        Box::pin(block_stream),
        schema.clone(),
        DEFAULT_CHUNK_BLOCK_NUM,
        0,
    )
    .await
    .collect::<Vec<_>>()
    .await;

    assert_eq!(segments.len(), 1);
    assert!(
        segments[0].is_ok(),
        "oops, unexpected result: {:?}",
        segments[0]
    );

    // multiple segments
    let number_of_blocks = 30;
    let chunk_size = 10;
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1, 2, 3])]);
    let blocks = std::iter::repeat(Ok(block)).take(number_of_blocks);
    let block_stream = futures::stream::iter(blocks);

    let segments = BlockStreamWriter::write_block_stream(
        local_fs.clone(),
        Box::pin(block_stream),
        schema.clone(),
        chunk_size,
        0,
    )
    .await
    .collect::<Vec<_>>()
    .await;

    let len = number_of_blocks / chunk_size;
    assert_eq!(segments.len(), len);
    for segment in segments.iter() {
        assert!(segment.is_ok(), "oops, unexpected result: {:?}", segment);
    }

    // empty blocks
    let block_stream = futures::stream::iter(vec![]);
    let segments = BlockStreamWriter::write_block_stream(
        local_fs,
        Box::pin(block_stream),
        schema,
        DEFAULT_CHUNK_BLOCK_NUM,
        0,
    )
    .await
    .collect::<Vec<_>>()
    .await;

    assert!(segments.is_empty())
}

#[test]
fn test_fuse_table_block_appender_reshape() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
    let sample_block = DataBlock::create_by_array(schema, vec![Series::new(vec![1, 2, 3])]);
    let sample_block_size = sample_block.memory_size();

    // 1 empty blocks
    // 1.1 empty block, zero block_size_threshold
    let blocks = vec![];
    let r = BlockStreamWriter::reshape_blocks(blocks, 0);
    assert!(r.is_ok(), "oops, unexpected result: {:?}", r);
    let r = r.unwrap();
    assert_eq!(r.len(), 0);

    // 1.2 empty block, arbitrary block_size_threshold
    let blocks = vec![];
    let r = BlockStreamWriter::reshape_blocks(blocks, 100);
    assert!(r.is_ok(), "oops, unexpected result: {:?}", r);
    let r = r.unwrap();
    assert_eq!(r.len(), 0);

    // 2. merge
    // 2.1 several blocks into exactly one block
    let block_num = 10;
    let (blocks, block_size_threshold) = gen_blocks(&sample_block, block_num);
    let r = BlockStreamWriter::reshape_blocks(blocks.collect(), block_size_threshold)?;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].memory_size(), block_size_threshold);

    // 2.1 with remainders
    // 2.1.1 reminders at tail
    let block_num = 10;
    let (blocks, block_size_threshold) = gen_blocks(&sample_block, block_num);
    // push back an extra block
    let blocks = blocks.chain(std::iter::once(sample_block.clone()));
    let r = BlockStreamWriter::reshape_blocks(blocks.collect(), block_size_threshold)?;
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].memory_size(), block_size_threshold);
    assert_eq!(r[1].memory_size(), sample_block_size);

    // 2.1.2 large blocks will not be split
    let block_num = 10;
    let (blocks, block_size_threshold) = gen_blocks(&sample_block, block_num);

    // generate a large block
    let (tmp_blocks, tmp_block_size_threshold) = gen_blocks(&sample_block, block_num * 2);
    assert!(tmp_block_size_threshold > block_size_threshold);
    let large_block = DataBlock::concat_blocks(&tmp_blocks.collect::<Vec<_>>())?;
    let large_block_size = large_block.memory_size();
    // push back the large block
    let blocks = blocks.chain(std::iter::once(large_block));

    let r = BlockStreamWriter::reshape_blocks(blocks.collect(), block_size_threshold)?;
    assert_eq!(r.len(), 2);
    // blocks are sorted (DESC by size) during reshape, thus we get the large_block at head
    assert_eq!(r[0].memory_size(), large_block_size);
    assert_eq!(r[1].memory_size(), block_size_threshold);

    Ok(())
}

fn gen_blocks(sample_block: &DataBlock, num: usize) -> (impl Iterator<Item = DataBlock>, usize) {
    let block_size = sample_block.memory_size();
    let block = sample_block.clone();
    let blocks = std::iter::repeat(block).take(num);
    let ideal_threshold = block_size * num;
    (blocks, ideal_threshold)
}
