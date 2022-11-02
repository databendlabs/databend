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
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::Versioned;
use common_storage::DataOperator;
use common_storages_fuse::io::BlockWriter;
use common_storages_fuse::io::MetaReaders;
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
    let location_gen = TableMetaLocationGenerator::with_prefix("./test/".to_owned());
    let block_per_seg = 10;

    let segment_reader = MetaReaders::segment_info_reader(ctx.get_data_operator()?.operator());

    {
        // case: highly fragmented segments

        let num_block_of_segments = vec![2, 2, 2, 2];
        // settings:
        // - block_per_seg is 10
        // - 4 segments, each of them have 2 blocks
        // expects:
        // - all the segments should be merged into one

        // setup & run
        let mut case = CompactCase::try_new(&ctx, block_per_seg)?;
        let seg_acc = case.run(&num_block_of_segments).await?;

        // verify that:
        // - one newly generated segment
        assert_eq!(seg_acc.new_segments.len(), 1);
        // - totally one segment collected
        assert_eq!(seg_acc.segment_locations.len(), 1);
        // - number of blocks should not change
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        // case: fragmented segments

        let num_block_of_segments = vec![2, 3, 6, 5];

        // settings:
        // - block_per_seg is 10
        // - 4 segments
        // expects:
        // - the first 3 segments should be merged
        //   segment of size [0, 2N] is allowed, to avoid the ripple effects
        // - the last segment will be kept and unchanged
        //   different from block compact:
        //   segment of size (2N..) is NOT allowed, where N is the threshold

        // setup & run
        let mut case = CompactCase::try_new(&ctx, block_per_seg)?;
        let seg_acc = case.run(&num_block_of_segments).await?;

        // verify that:
        // - one newly generated segment
        assert_eq!(seg_acc.new_segments.len(), 1);
        // - totally two segments collected
        assert_eq!(seg_acc.segment_locations.len(), 2);
        // number of blocks should not change
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        // case: fragmented segments, not so greedy

        let num_block_of_segments = vec![2, 8, 1, 8];

        // settings:
        // - block_per_seg is 10
        // - 4 segments
        // expects:
        // - the first 2 segments should be merged
        // - the last 2 segments should be merged

        let expected_block_of_segments = vec![10, 9];

        // setup & run
        let mut case = CompactCase::try_new(&ctx, block_per_seg)?;
        let seg_acc = case.run(&num_block_of_segments).await?;

        for (idx, x) in seg_acc.new_segments.iter().enumerate() {
            let seg = segment_reader.read(x, None, SegmentInfo::VERSION).await?;
            assert_eq!(seg.blocks.len(), expected_block_of_segments[idx]);
        }

        // verify that:
        // - one newly generated segment
        assert_eq!(seg_acc.new_segments.len(), 2);
        // - totally two segments collected
        assert_eq!(seg_acc.segment_locations.len(), 2);

        // number of blocks should not change
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    {
        // case: fragmented segments, greedy if ...

        let num_block_of_segments = vec![2, 10, 6, 8];

        // settings:
        // - block_per_seg is 10
        // - 4 segments
        // expects:
        // - the first 2 segments should be merged
        // - the last 2 segments should be merged
        // thus:
        let expected_num_block_of_new_segments = vec![12, 14];

        // setup & run
        let mut case = CompactCase::try_new(&ctx, block_per_seg)?;
        let seg_acc = case.run(&num_block_of_segments).await?;

        for (idx, x) in seg_acc.new_segments.iter().enumerate() {
            let seg = segment_reader.read(x, None, SegmentInfo::VERSION).await?;
            assert_eq!(seg.blocks.len(), expected_num_block_of_new_segments[idx]);
        }

        // verify that:
        // - 2 newly generated segment;
        assert_eq!(seg_acc.new_segments.len(), 2);
        // - totally two segments collected
        assert_eq!(seg_acc.segment_locations.len(), 2);

        // number of blocks should not change
        assert_eq!(
            seg_acc.merged_statistics.block_count as usize,
            num_block_of_segments.into_iter().sum::<usize>()
        );
    }

    // empty segment will be dropped
    {
        let segment_writer = SegmentWriter::new(&data_accessor, &location_gen, &None);
        let mut accumulator = SegmentAccumulator::new(block_per_seg, segment_writer);
        let seg = SegmentInfo::new(vec![], Statistics::default());
        accumulator.add(&seg, ("test".to_owned(), 1)).await?;
        accumulator.finalize().await?;
        assert_eq!(accumulator.new_segments.len(), 0);
    }

    Ok(())
}

async fn gen_segments(
    block_writer: &BlockWriter<'_>,
    sizes: &[usize],
) -> Result<Vec<(SegmentInfo, Location)>> {
    let mut segments = vec![];
    for (idx, num_blocks) in sizes.iter().enumerate() {
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
        segments.push((segment_info, (format!("seg_{}", idx), SegmentInfo::VERSION)));
    }

    Ok(segments)
}

struct CompactCase {
    block_per_seg: u64,
    data_accessor: DataOperator,
    location_gen: TableMetaLocationGenerator,
    segments: Vec<(SegmentInfo, Location)>,
}

impl CompactCase {
    fn try_new(ctx: &Arc<QueryContext>, block_per_seg: u64) -> Result<Self> {
        let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());
        let data_accessor = ctx.get_data_operator()?;
        Ok(Self {
            block_per_seg,
            data_accessor,
            location_gen,
            segments: vec![],
        })
    }
    async fn run(&mut self, num_block_of_segments: &[usize]) -> Result<SegmentAccumulator<'_>> {
        let block_per_seg = self.block_per_seg;
        let data_accessor = &self.data_accessor;
        let location_gen = &self.location_gen;
        let block_writer = BlockWriter::new(data_accessor, location_gen);
        let segment_writer = SegmentWriter::new(data_accessor, location_gen, &None);
        let mut seg_acc = SegmentAccumulator::new(block_per_seg, segment_writer);
        self.segments = gen_segments(&block_writer, num_block_of_segments).await?;
        for (seg, location) in &self.segments {
            seg_acc.add(seg, location.clone()).await?;
        }
        seg_acc.finalize().await?;
        Ok(seg_acc)
    }
}
