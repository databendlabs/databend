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
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::storages::fuse::io::BlockRegulator;
use databend_query::storages::fuse::io::BlockStreamWriter;
use databend_query::storages::fuse::DEFAULT_CHUNK_BLOCK_NUM;
use futures::StreamExt;
use futures::TryStreamExt;
use num::Integer;
use opendal::ops::OpWrite;
use opendal::services::fs;
use opendal::Accessor;
use opendal::Operator;
use opendal::Reader;
use tempfile::TempDir;

#[tokio::test]
async fn test_fuse_table_block_appender() {
    let tmp_dir = TempDir::new().unwrap();
    let local_fs = Operator::new(
        fs::Backend::build()
            .root(tmp_dir.path().to_str().unwrap())
            .finish()
            .await
            .unwrap(),
    );
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);

    // single segment
    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1, 2, 3])]);
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
    let max_rows_per_block = 3;
    let max_blocks_per_segment = 1;
    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1, 2, 3])]);
    let blocks = std::iter::repeat(Ok(block)).take(number_of_blocks);
    let block_stream = futures::stream::iter(blocks);

    let segments = BlockStreamWriter::write_block_stream(
        local_fs.clone(),
        Box::pin(block_stream),
        schema.clone(),
        max_rows_per_block,
        max_blocks_per_segment,
    )
    .await
    .collect::<Vec<_>>()
    .await;

    let len = num::Integer::div_ceil(&number_of_blocks, &max_blocks_per_segment);
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
fn test_block_regulator() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);
    let test_case =
        |rows_per_sample_block, max_row_per_block, num_blocks, case_name| -> Result<()> {
            // One block, contains `rows_per_sample_block` rows
            let sample_block = gen_block(gen_rows(rows_per_sample_block));

            let mut regulator = BlockRegulator::new(max_row_per_block);
            let total_rows = rows_per_sample_block * num_blocks;

            let mut generated: Vec<DataBlock> = vec![];

            // feed blocks into shaper
            for _i in 0..num_blocks {
                let blks = regulator.regulate(sample_block.clone())?;
                generated.extend(blks.into_iter().flatten())
            }

            // indicates the shaper that we are done
            let sealed = regulator.seal()?;

            // Invariants of `generated` Blocks
            //
            //  1.) row numbers match
            //
            //      generated.iter().map(|b| b.num_rows()).sum() == total_rows
            //
            //  2.) well shaped
            //
            //      - if `max_row_per_block` divides `total_rows`
            //
            //        for all block B in `generated`, B.num_rows() == max_row_per_block,
            //
            //      - if `total_rows` % `max_row_per_block` = r and r > 0
            //
            //        - there should be exactly one block B in `generated` where B.num_rows() == r
            //
            //        - for all block B' in `generated`; B' =/= B implies that B'.row_count() == max_row_per_block,

            let remainder = total_rows % max_row_per_block;
            if remainder == 0 {
                assert!(
                    sealed.is_none(),
                    "[{}], expects no reminder, but got {}",
                    case_name,
                    remainder
                );
            } else {
                assert!(
                    sealed.is_some(),
                    "[{}], expects remainder, but got nothing",
                    case_name
                );
                let block = sealed.as_ref().unwrap();
                assert_eq!(
                    block.iter().map(|i| i.num_rows()).sum::<usize>(),
                    remainder,
                    "[{}], num_rows remains ",
                    case_name
                );
            }

            assert!(
                generated
                    .iter()
                    .all(|item| item.num_rows() == max_row_per_block),
                "[{}], for each block, the num_rows should be exactly max_row_per_block",
                case_name
            );

            generated.extend(sealed.into_iter().flatten());

            assert_eq!(
                total_rows,
                generated.iter().map(|item| item.num_rows()).sum::<usize>(),
                "{}, row numbers match",
                case_name
            );

            Ok(())
        };

    let case_name = "small blocks, with remainder";
    // 3 > 2; 2 * 10 % 3 = 2
    let max_row_per_block = 3;
    let rows_per_sample_block = 2;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "small blocks, no remainder";
    // 4 > 2; 2 * 10 % 4 = 0
    let max_row_per_block = 4;
    let rows_per_sample_block = 2;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "large blocks, no remainder";
    // 15 < 30; 30 * 10 % 15 = 0
    let max_row_per_block = 15;
    let rows_per_sample_block = 30;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "large blocks, with remainders";
    // 7 < 30; 30 * 10 % 7 = 6
    let max_row_per_block = 7;
    let rows_per_sample_block = 30;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    Ok(())
}

#[tokio::test]
async fn test_block_stream_writer() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);

    let test_case = |rows_per_sample_block,
                     max_rows_per_block,
                     max_blocks_per_segment,
                     num_blocks,
                     schema,
                     case_name: &'static str| async move {
        let sample_block = gen_block(gen_rows(rows_per_sample_block));
        let block_stream =
            futures::stream::iter(std::iter::repeat(Ok(sample_block)).take(num_blocks));

        let data_accessor = Arc::new(MockDataAccessor::new());
        let operator = Operator::new(data_accessor.clone());

        let stream = BlockStreamWriter::write_block_stream(
            operator,
            Box::pin(block_stream),
            schema,
            max_rows_per_block,
            max_blocks_per_segment,
        )
        .await;
        let segs = stream.try_collect::<Vec<_>>().await?;

        // verify the number of blocks
        let expected_blocks =
            Integer::div_ceil(&(rows_per_sample_block * num_blocks), &max_rows_per_block);
        assert_eq!(
            expected_blocks,
            data_accessor.blocks_written(),
            "case: {}",
            case_name
        );

        // verify the number of segment
        let expected_segments = Integer::div_ceil(&expected_blocks, &max_blocks_per_segment);
        assert_eq!(expected_segments, segs.len(), "case: {}", case_name);
        Ok::<_, ErrorCode>(())
    };

    let rows_perf_sample_block = 10;
    let rows_per_block = 10;
    let blocks_per_segment = 10;
    let number_of_blocks = 100;
    test_case(
        rows_perf_sample_block,
        rows_per_block,
        blocks_per_segment,
        number_of_blocks,
        schema.clone(),
        "simple regular data",
    )
    .await?;

    let rows_perf_sample_block = 10;
    let rows_per_block = 3;
    let blocks_per_segment = 3;
    let number_of_blocks = 100;
    test_case(
        rows_perf_sample_block,
        rows_per_block,
        blocks_per_segment,
        number_of_blocks,
        schema.clone(),
        "with remainder",
    )
    .await?;

    Ok(())
}

use common_infallible::Mutex;
struct MockDataAccessor {
    put_stream_called: Arc<Mutex<usize>>,
}

impl MockDataAccessor {
    fn new() -> Self {
        Self {
            put_stream_called: Arc::new(Mutex::new(0)),
        }
    }

    fn blocks_written(&self) -> usize {
        *self.put_stream_called.lock()
    }
}
#[async_trait::async_trait]
impl Accessor for MockDataAccessor {
    async fn write(&self, _r: Reader, args: &OpWrite) -> opendal::error::Result<usize> {
        let called = &mut *self.put_stream_called.lock();
        *called += 1;
        Ok(args.size as usize)
    }
}
