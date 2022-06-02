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

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::storages::fuse::io::BlockCompactor;
use databend_query::storages::fuse::io::BlockStreamWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::meta::TableSnapshot;
use databend_query::storages::fuse::meta::Versioned;
use databend_query::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use futures::StreamExt;
use futures::TryStreamExt;
use num::Integer;
use uuid::Uuid;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_fuse_table_block_appender() -> Result<()> {
    let ctx = create_query_context().await?;
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);

    // single segment
    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1, 2, 3])]);
    let block_stream = futures::stream::iter(vec![Ok(block)]);

    let locs = TableMetaLocationGenerator::with_prefix(".".to_owned());
    let segments = BlockStreamWriter::write_block_stream(
        ctx.clone(),
        Box::pin(block_stream),
        DEFAULT_BLOCK_PER_SEGMENT,
        0,
        locs.clone(),
        schema.clone(),
        vec![],
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
        ctx.clone(),
        Box::pin(block_stream),
        max_rows_per_block,
        max_blocks_per_segment,
        locs.clone(),
        schema.clone(),
        vec![],
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
        ctx,
        Box::pin(block_stream),
        DEFAULT_BLOCK_PER_SEGMENT,
        0,
        locs,
        schema,
        vec![],
    )
    .await
    .collect::<Vec<_>>()
    .await;

    assert!(segments.is_empty());
    Ok(())
}

#[test]
fn test_block_compactor() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);
    let test_case =
        |rows_per_sample_block, max_row_per_block, num_blocks, case_name| -> Result<()> {
            // One block, contains `rows_per_sample_block` rows
            let sample_block = gen_block(gen_rows(rows_per_sample_block));

            let mut compactor = BlockCompactor::new(max_row_per_block);
            let total_rows = rows_per_sample_block * num_blocks;

            let mut generated: Vec<DataBlock> = vec![];

            // feed blocks into shaper
            for _i in 0..num_blocks {
                let blks = compactor.compact(sample_block.clone())?;
                generated.extend(blks.into_iter().flatten())
            }

            // indicates the shaper that we are done
            let sealed = compactor.finish()?;

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
async fn test_block_stream_writer() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);

    let test_case = |rows_per_sample_block,
                     max_rows_per_block,
                     max_blocks_per_segment,
                     num_blocks,
                     case_name: &'static str| async move {
        let sample_block = gen_block(gen_rows(rows_per_sample_block));
        let block_stream =
            futures::stream::iter(std::iter::repeat(Ok(sample_block)).take(num_blocks));

        let ctx = create_query_context().await?;
        let locs = TableMetaLocationGenerator::with_prefix(".".to_owned());
        let stream = BlockStreamWriter::write_block_stream(
            ctx,
            Box::pin(block_stream),
            max_rows_per_block,
            max_blocks_per_segment,
            locs,
            DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]),
            vec![],
        )
        .await;
        let segs = stream.try_collect::<Vec<_>>().await?;

        // verify the number of blocks
        let expected_blocks =
            Integer::div_ceil(&(rows_per_sample_block * num_blocks), &max_rows_per_block);
        let actual_blocks = segs.iter().fold(0, |acc, x| acc + x.blocks.len());
        assert_eq!(expected_blocks, actual_blocks, "case: {}", case_name);

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
        "with remainder",
    )
    .await?;

    Ok(())
}

#[test]
fn test_meta_locations() -> Result<()> {
    let test_prefix = "test_pref";
    let locs = TableMetaLocationGenerator::with_prefix(test_prefix.to_owned());
    let block_loc = locs.gen_block_location();
    assert!(block_loc.starts_with(test_prefix));
    let seg_loc = locs.gen_segment_info_location();
    assert!(seg_loc.starts_with(test_prefix));
    let uuid = Uuid::new_v4();
    let snapshot_loc = locs.snapshot_location_from_uuid(&uuid, TableSnapshot::VERSION)?;
    assert!(snapshot_loc.starts_with(test_prefix));
    Ok(())
}
