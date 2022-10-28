//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::CompactTarget;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_storages_fuse::io::BlockWriter;
use common_storages_fuse::io::SegmentWriter;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::operations::SegmentAccumulator;
use common_storages_fuse::statistics::BlockStatistics;
use common_storages_fuse::statistics::StatisticsAccumulator;
use common_storages_fuse::FuseTable;
use common_streams::SendableDataBlockStream;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use futures_util::TryStreamExt;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_compact_segment_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let qry = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), qry).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), CompactTarget::Segments, None, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();
    mutator.try_commit(table.clone()).await?;

    // check segment count
    let qry = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), qry).await?;
    // after compact, in our case, there should be only 1 segment left
    assert_eq!(1, check_count(stream).await?);

    // check block count
    let qry = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);
    Ok(())
}

#[tokio::test]
async fn test_compact_segment_resolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), CompactTarget::Segments, None, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    // before commit compact segments, gives 9 append commits
    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    mutator.try_commit(table.clone()).await?;

    // check segment count
    let count_seg = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), count_seg).await?;
    // after compact, in our case, there should be only 1 + num_inserts segments left
    // during compact retry, newly appended segments will NOT be compacted again
    assert_eq!(1 + num_inserts as u64, check_count(stream).await?);

    // check block count
    let count_block = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = execute_query(fixture.ctx(), count_block).await?;
    assert_eq!(num_inserts as u64 * 2, check_count(stream).await?);

    // check table statistics

    let latest = table.refresh(ctx.as_ref()).await?;
    let latest_fuse_table = FuseTable::try_from_table(latest.as_ref())?;
    let table_statistics = latest_fuse_table.table_statistics()?.unwrap();

    assert_eq!(table_statistics.num_rows.unwrap() as usize, num_inserts * 2);

    Ok(())
}

#[tokio::test]
async fn test_compact_segment_unresolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);

    // try compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), CompactTarget::Segments, None, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    {
        // inject a unresolvable commit
        compact_segment(ctx.clone(), &table).await?;
    }

    // the compact operation committed latter should failed
    let r = mutator.try_commit(table.clone()).await;
    assert!(r.is_err());
    assert_eq!(r.err().unwrap().code(), ErrorCode::STORAGE_OTHER);

    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    let qry = "insert into t values(1)";
    for _ in 0..n {
        execute_command(ctx.clone(), qry).await?;
    }
    Ok(())
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    blocks[0].column(0).get_u64(0)
}

async fn compact_segment(ctx: Arc<QueryContext>, table: &Arc<dyn Table>) -> Result<()> {
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx, CompactTarget::Segments, None, &mut pipeline)
        .await?
        .unwrap();
    mutator.try_commit(table.clone()).await
}

#[tokio::test]
async fn test_segment_accumulator() -> Result<()> {
    // TODO refactor this suite

    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let data_accessor = ctx.get_data_operator()?;
    let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());

    let block_per_seg = 10;

    // empty segment will be dropped
    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);
        let mut accumulator = SegmentAccumulator::new(block_per_seg, segment_writer);

        let seg = SegmentInfo::new(vec![], Statistics::default());
        accumulator.add(&seg, ("test".to_owned(), 1)).await?;
        accumulator.finalize().await?;
        assert_eq!(accumulator.new_segments.len(), 0);
    }

    // all the segments feed in combined, is still a fragmented segment
    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);
        let mut accumulator = SegmentAccumulator::new(block_per_seg, segment_writer);

        let mut segs = vec![];
        for _ in 0..9 {
            let block_meta = BlockMeta::new(
                0,
                0,
                0,
                Default::default(),
                Default::default(),
                None,
                ("".to_string(), 0),
                None,
                0,
            );
            let statistics = Statistics {
                row_count: 0,
                block_count: 1,
                uncompressed_byte_size: 0,
                compressed_byte_size: 0,
                index_size: 0,
                col_stats: Default::default(),
            };

            let seg = SegmentInfo::new(vec![Arc::new(block_meta)], statistics);
            segs.push(seg);
        }

        for x in &segs {
            accumulator.add(x, ("test".to_owned(), 1)).await?;
        }
        accumulator.finalize().await?;
        assert_eq!(accumulator.new_segments.len(), 1);
        assert_eq!(accumulator.merged_statistics.block_count, 9);
    }

    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);

        let block_writer = BlockWriter::new(&data_accessor, &location_gen);
        let mut stats_acc = StatisticsAccumulator::new();

        // let mut rng = rand::thread_rng();
        let blocks = TestFixture::gen_sample_blocks(120, 1)
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        for block in blocks {
            let block_statistics = BlockStatistics::from(&block, "".to_owned(), None)?;
            let block_meta = block_writer.write(block, None).await?;
            stats_acc.add_with_block_meta(block_meta, block_statistics)?;
        }

        assert_eq!(stats_acc.summary_block_count, 120);

        let col_stats = stats_acc.summary()?;
        let segment_info = SegmentInfo::new(stats_acc.blocks_metas, Statistics {
            row_count: stats_acc.summary_row_count,
            block_count: stats_acc.summary_block_count,
            uncompressed_byte_size: stats_acc.in_memory_size,
            compressed_byte_size: stats_acc.file_size,
            index_size: stats_acc.index_size,
            col_stats,
        });
        seg_acc.add(&segment_info, ("".to_owned(), 1)).await?;
        seg_acc.finalize().await?;

        assert_eq!(seg_acc.new_segments.len(), 1);
        assert_eq!(seg_acc.merged_statistics.block_count, 120);
    }

    {
        eprintln!("=================");
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);

        let block_per_seg = 10;
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);
        let block_writer = BlockWriter::new(&data_accessor, &location_gen);

        let num_block_of_segments = vec![2, 3, 6, 5];

        // in this case,
        // - the first 3 segments should be merged
        // - the last segment will be kept and unchanged

        let segments = gen_segments(block_writer, &num_block_of_segments).await?;
        for seg in &segments {
            seg_acc.add(seg, ("".to_owned(), 1)).await?;
        }
        seg_acc.finalize().await?;

        assert_eq!(seg_acc.new_segments.len(), 1);
        assert_eq!(seg_acc.segment_locations.len(), 2);
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);

        let block_per_seg = 3;
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);
        let block_writer = BlockWriter::new(&data_accessor, &location_gen);

        // in this case,
        // - the first 3 segments should be merged
        // - the last segment will be kept and unchanged
        let num_block_of_segments = vec![1, 1, 1, 1];

        let segments = gen_segments(block_writer, &num_block_of_segments).await?;
        for seg in &segments {
            seg_acc.add(seg, ("".to_owned(), 1)).await?;
        }
        seg_acc.finalize().await?;

        assert_eq!(seg_acc.new_segments.len(), 1);
        assert_eq!(seg_acc.segment_locations.len(), 2);
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);

        let block_per_seg = 3;
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);
        let block_writer = BlockWriter::new(&data_accessor, &location_gen);

        // in this case,
        // - the first 3 segments should be merged
        // - the last segment will be kept and unchanged
        let num_block_of_segments = vec![2, 1, 2, 1];

        let segments = gen_segments(block_writer, &num_block_of_segments).await?;
        for seg in &segments {
            seg_acc.add(seg, ("".to_owned(), 1)).await?;
        }
        seg_acc.finalize().await?;

        assert_eq!(seg_acc.new_segments.len(), 2);
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);

        let block_per_seg = 10;
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);
        let block_writer = BlockWriter::new(&data_accessor, &location_gen);

        // in this case,
        // - the first 3 segments should be merged
        // - the last segment will be kept and unchanged
        let num_block_of_segments = vec![8, 3, 1];

        let segments = gen_segments(block_writer, &num_block_of_segments).await?;
        for seg in &segments {
            seg_acc.add(seg, ("".to_owned(), 1)).await?;
        }
        seg_acc.finalize().await?;

        assert_eq!(seg_acc.new_segments.len(), 1);
        assert_eq!(seg_acc.segment_locations.len(), 2);
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    Ok(())
}

async fn gen_segments(block_writer: BlockWriter<'_>, sizes: &[usize]) -> Result<Vec<SegmentInfo>> {
    let mut segments = vec![];
    for num_blocks in sizes {
        let blocks = TestFixture::gen_sample_blocks_ex(*num_blocks, 1, 1);
        let mut stats_acc = StatisticsAccumulator::new();
        for block in blocks {
            let block = block?;
            let block_statistics = BlockStatistics::from(&block, "".to_owned(), None)?;
            let block_meta = block_writer.write(block, None).await?;
            stats_acc.add_with_block_meta(block_meta, block_statistics)?;
        }
        let col_stats = stats_acc.summary()?;
        let segment_info = SegmentInfo::new(stats_acc.blocks_metas, Statistics {
            row_count: stats_acc.summary_row_count,
            block_count: stats_acc.summary_block_count,
            uncompressed_byte_size: stats_acc.in_memory_size,
            compressed_byte_size: stats_acc.file_size,
            index_size: stats_acc.index_size,
            col_stats,
        });
        segments.push(segment_info);
    }
    Ok(segments)
}
