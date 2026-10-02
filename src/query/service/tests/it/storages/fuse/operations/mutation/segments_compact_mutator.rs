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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::Value;
use databend_common_expression::types::number::NumberColumn;
use databend_common_expression::types::number::NumberScalar;
use databend_common_pipeline::core::Pipeline;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::CompactSegmentInfoReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::read_segment_stats;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::operations::CompactOptions;
use databend_common_storages_fuse::operations::ConflictResolveContext;
use databend_common_storages_fuse::operations::SegmentCompactMutator;
use databend_common_storages_fuse::operations::SegmentCompactionState;
use databend_common_storages_fuse::operations::SegmentCompactor;
use databend_common_storages_fuse::statistics::RowOrientedSegmentBuilder;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;
use databend_query::sessions::TableContextTelemetry;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::ColumnTopNEntry;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::column_oriented_segment::SegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::VirtualBlockInput;
use databend_storages_common_table_meta::table::ClusterType;
use futures_util::TryStreamExt;
use rand::Rng;
use rand::thread_rng;

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_normal_case() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    // setup
    let qry = "create table t(c int)  block_per_segment=10";
    fixture.execute_command(qry).await?;

    let num_inserts = 9;
    fixture.append_rows(num_inserts).await?;

    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;

    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();
    mutator.try_commit_compact(fuse_table, ctx.clone()).await?;

    // check segment count
    let qry = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = fixture.execute_query(qry).await?;
    // after compact, in our case, there should be only 1 segment left
    assert_eq!(1, check_count(stream).await?);

    // check block count
    let qry = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = fixture.execute_query(qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_limit_selects_newest() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("create table t(c int) block_per_segment=4")
        .await?;
    fixture.append_rows(4).await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let base = fuse.read_table_snapshot().await?.unwrap();
    assert_eq!(base.segments.len(), 4);

    let mutator = build_mutator(fuse, ctx, Some(2)).await?.unwrap();
    let state = mutator.into_compaction_state();
    assert_eq!(state.new_segment_paths.len(), 1);
    assert_eq!(state.num_fragments_compacted, 2);
    assert!(state.replaced_segments.keys().all(|idx| *idx < 2));
    assert!(state.removed_segment_indexes.iter().all(|idx| *idx < 2));
    let output = output_locations(&base.segments, &state);
    assert_eq!(&output[1..], &base.segments[2..]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_limit_does_not_read_older_segments() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let thresholds = BlockThresholds {
        block_per_segment: 4,
        ..Default::default()
    };
    let (locations, _, _) = CompactSegmentTestFixture::gen_segments(
        ctx.clone(),
        vec![1, 1, 1],
        vec![1; 3],
        thresholds,
        None,
        false,
    )
    .await?;
    let mut snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
    let missing_older_location = ("test/nonexistent-segment".to_string(), SegmentInfo::VERSION);
    snapshot_segments[2] = missing_older_location.clone();

    let dal = ctx.get_application_level_data_operator()?.operator();
    let state = compact_segments(
        &dal,
        thresholds.block_per_segment,
        1,
        &snapshot_segments,
        Some(2),
    )
    .await?;
    assert_eq!(state.num_fragments_compacted, 2);
    let output = output_locations(&snapshot_segments, &state);
    assert_eq!(output.last(), Some(&missing_older_location));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_rejects_inconsistent_block_counts() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let dal = ctx.get_application_level_data_operator()?.operator();
    let thresholds = BlockThresholds {
        block_per_segment: 4,
        ..Default::default()
    };

    for incorrect_count in [0, 2] {
        let (locations, _, mut segments) = CompactSegmentTestFixture::gen_segments(
            ctx.clone(),
            vec![1, 1],
            vec![1, 1],
            thresholds,
            None,
            false,
        )
        .await?;
        segments[0].summary.block_count = incorrect_count;
        segments[0].write_meta(&dal, &locations[0].0).await?;

        let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
        let result = compact_segments(
            &dal,
            thresholds.block_per_segment,
            1,
            &snapshot_segments,
            None,
        )
        .await;
        assert!(result.is_err(), "inconsistent count: {incorrect_count}");
        assert!(
            result
                .err()
                .expect("compaction should reject inconsistent block counts")
                .to_string()
                .contains("blocks in its summary")
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_parallel_groups_preserve_hll_and_top_n_order() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let dal = ctx.get_application_level_data_operator()?.operator();
    let thresholds = BlockThresholds {
        block_per_segment: 10,
        ..Default::default()
    };
    // Five sources produce two independent merge groups. Give each block a
    // distinct HLL payload so the output proves that asynchronous completion
    // cannot reorder statistics within or between groups.
    let (locations, _, mut segments) = CompactSegmentTestFixture::gen_segments(
        ctx.clone(),
        vec![5, 5, 15, 5, 5],
        vec![1; 5],
        thresholds,
        None,
        false,
    )
    .await?;
    for (i, (location, segment)) in locations.iter().zip(&mut segments).enumerate() {
        let block_hlls = (0..segment.blocks.len())
            .map(|j| vec![i as u8, j as u8])
            .collect::<Vec<_>>();
        let block_top_ns = (0..segment.blocks.len())
            .map(|j| {
                HashMap::from([(0, ColumnTopN {
                    capacity: 1,
                    values: vec![ColumnTopNEntry {
                        scalar: Scalar::Number(NumberScalar::Int32(i as i32)),
                        count: j as u64 + 1,
                        error: 0,
                    }],
                    min_index: None,
                })])
            })
            .collect::<Vec<_>>();
        let stats = SegmentStatistics::new(block_hlls, block_top_ns);
        attach_segment_stats(&dal, &location.0, segment, stats).await?;
    }
    let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
    let state = compact_segments(
        &dal,
        thresholds.block_per_segment,
        4,
        &snapshot_segments,
        None,
    )
    .await?;
    assert_eq!(state.new_segment_paths.len(), 2);
    let expected = [(0, 1), (3, 4)];
    for (path, (first, second)) in state.new_segment_paths.iter().zip(expected) {
        let segment = SegmentsIO::read_compact_segment(
            dal.clone(),
            (path.clone(), SegmentInfo::VERSION),
            TestFixture::default_table_schema(),
            false,
        )
        .await?;
        let stats_loc = &segment
            .summary
            .additional_stats_meta
            .as_ref()
            .unwrap()
            .location;
        let stats = read_segment_stats(dal.clone(), stats_loc.clone()).await?;
        let expected_hlls = [first, second]
            .into_iter()
            .flat_map(|source| {
                (0..segments[source].blocks.len()).map(move |j| vec![source as u8, j as u8])
            })
            .collect::<Vec<_>>();
        assert_eq!(stats.block_hlls, expected_hlls);
        let expected_top_ns = [first, second]
            .into_iter()
            .flat_map(|source| {
                (0..segments[source].blocks.len()).map(move |j| {
                    HashMap::from([(0, ColumnTopN {
                        capacity: 1,
                        values: vec![ColumnTopNEntry {
                            scalar: Scalar::Number(NumberScalar::Int32(source as i32)),
                            count: j as u64 + 1,
                            error: 0,
                        }],
                        min_index: None,
                    })])
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(stats.block_top_ns, expected_top_ns);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_single_group_many_sources() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let dal = ctx.get_application_level_data_operator()?.operator();
    let thresholds = BlockThresholds {
        block_per_segment: 16,
        ..Default::default()
    };
    let (locations, _, segments) = CompactSegmentTestFixture::gen_segments(
        ctx,
        vec![1; 16],
        vec![1; 16],
        thresholds,
        None,
        false,
    )
    .await?;
    let expected = segments
        .iter()
        .flat_map(|s| s.blocks.iter().cloned())
        .collect::<Vec<_>>();
    let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
    for max_threads in [1, 4] {
        let state = compact_segments(
            &dal,
            thresholds.block_per_segment,
            max_threads,
            &snapshot_segments,
            None,
        )
        .await?;
        assert_eq!(state.new_segment_paths.len(), 1);
        let merged = SegmentsIO::read_compact_segment(
            dal.clone(),
            (state.new_segment_paths[0].clone(), SegmentInfo::VERSION),
            TestFixture::default_table_schema(),
            false,
        )
        .await?;
        assert_eq!(merged.block_metas()?, expected);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_skips_incomplete_stats() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let dal = ctx.get_application_level_data_operator()?.operator();
    let thresholds = BlockThresholds {
        block_per_segment: 10,
        ..Default::default()
    };
    let (locations, _, mut segments) = CompactSegmentTestFixture::gen_segments(
        ctx.clone(),
        vec![1, 1],
        vec![1, 1],
        thresholds,
        None,
        false,
    )
    .await?;
    let stats = SegmentStatistics::new(vec![vec![1]], vec![Default::default()]);
    attach_segment_stats(&dal, &locations[0].0, &mut segments[0], stats).await?;

    let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
    let state = compact_segments(
        &dal,
        thresholds.block_per_segment,
        4,
        &snapshot_segments,
        None,
    )
    .await?;
    assert_eq!(state.new_segment_paths.len(), 1);
    let merged = SegmentsIO::read_compact_segment(
        dal.clone(),
        (state.new_segment_paths[0].clone(), SegmentInfo::VERSION),
        TestFixture::default_table_schema(),
        false,
    )
    .await?;
    assert!(merged.summary.additional_stats_meta.is_none());
    let merged_stats_path =
        TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
            &state.new_segment_paths[0],
        );
    assert!(!dal.exists(&merged_stats_path).await?);
    Ok(())
}

// Reconstruct the snapshot exactly as the production commit path does, rather
// than maintaining a second full segment list during compaction selection.
fn output_locations(base: &[Location], state: &SegmentCompactionState) -> Vec<Location> {
    ConflictResolveContext::merge_segments(
        base.to_vec(),
        vec![],
        state.replaced_segments.clone(),
        state.removed_segment_indexes.clone(),
    )
}

// Compact `segments` (snapshot order, newest first) with the default test
// schema. Use four IO requests per merge worker in these focused tests.
async fn compact_segments(
    operator: &opendal::Operator,
    block_per_segment: usize,
    max_threads: usize,
    segments: &[Location],
    limit: Option<usize>,
) -> Result<SegmentCompactionState> {
    let location_gen = TableMetaLocationGenerator::new("test/".to_owned());
    SegmentCompactor::new(
        block_per_segment as u64,
        None,
        max_threads,
        max_threads * 4,
        TestFixture::default_table_schema(),
        operator,
        &location_gen,
        TestFixture::default_table_meta_timestamps(),
    )
    .compact(segments, limit, |_| {})
    .await
}

// Write `stats` as the segment's HLL/Top-N file and rewrite the segment so its
// summary points to it.
async fn attach_segment_stats(
    dal: &opendal::Operator,
    segment_path: &str,
    segment: &mut SegmentInfo,
    stats: SegmentStatistics,
) -> anyhow::Result<()> {
    let bytes = stats.to_bytes()?;
    let stats_path =
        TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(segment_path);
    segment.summary.additional_stats_meta = Some(AdditionalStatsMeta {
        size: bytes.len() as u64,
        location: (stats_path.clone(), SegmentStatistics::VERSION),
        ..Default::default()
    });
    dal.write(&stats_path, bytes).await?;
    segment.write_meta(dal, segment_path).await?;
    Ok(())
}

async fn list_paths(dal: &opendal::Operator, prefix: &str) -> anyhow::Result<Vec<String>> {
    let mut paths = dal
        .list(prefix)
        .await?
        .iter()
        .map(|entry| entry.path().to_string())
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_parallel_group_failure_cleans_outputs() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let dal = ctx.get_application_level_data_operator()?.operator();
    let thresholds = BlockThresholds {
        block_per_segment: 10,
        ..Default::default()
    };
    // The old end makes one merge group; the newest group contains a corrupt
    // summary, so the first group's output must be removed before returning.
    let (locations, _, mut segments) = CompactSegmentTestFixture::gen_segments(
        ctx.clone(),
        vec![1, 1, 10, 1, 1],
        vec![1; 5],
        thresholds,
        None,
        false,
    )
    .await?;
    segments.last_mut().unwrap().summary.block_count = 2;
    segments
        .last()
        .unwrap()
        .write_meta(&dal, &locations.last().unwrap().0)
        .await?;
    let before = list_paths(&dal, "test/_sg/").await?;
    // max_threads 4 allows four merge groups, so both groups are in flight
    // together and the failure of the second must clean up the first.
    let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
    let error = compact_segments(
        &dal,
        thresholds.block_per_segment,
        4,
        &snapshot_segments,
        None,
    )
    .await
    .err()
    .expect("the second group must reject inconsistent block counts");
    assert!(error.to_string().contains("blocks in its summary"));
    assert_eq!(
        before,
        list_paths(&dal, "test/_sg/").await?,
        "failed compaction left behind a segment file"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_resolvable_conflict() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    fixture.execute_command(create_tbl_command).await?;

    let num_inserts = 9;
    fixture.append_rows(num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;

    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    // before commit compact segments, gives 9 append commits
    let num_inserts = 9;
    fixture.append_rows(num_inserts).await?;

    mutator.try_commit_compact(fuse_table, ctx.clone()).await?;

    // check segment count
    let count_seg = "select segment_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = fixture.execute_query(count_seg).await?;
    // after compact, in our case, there should be only 1 + num_inserts segments left
    // during compact retry, newly appended segments will NOT be compacted again
    assert_eq!(1 + num_inserts as u64, check_count(stream).await?);

    // check block count
    let count_block = "select block_count as count from fuse_snapshot('default', 't') limit 1";
    let stream = fixture.execute_query(count_block).await?;
    assert_eq!(num_inserts as u64 * 2, check_count(stream).await?);

    // check table statistics

    let ctx = fixture.new_query_ctx().await?;
    let latest = table.refresh(ctx.as_ref()).await?;
    let latest_fuse_table = FuseTable::try_from_table(latest.as_ref())?;
    let table_statistics = latest_fuse_table
        .table_statistics(ctx.clone(), true, None)
        .await?
        .unwrap();

    assert_eq!(table_statistics.num_rows.unwrap() as usize, num_inserts * 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_unresolvable_conflict() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    // setup
    let create_tbl_command = "create table t(c int)  block_per_segment=10";
    fixture.execute_command(create_tbl_command).await?;

    let num_inserts = 9;
    fixture.append_rows(num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);

    // try compact segment
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();

    {
        // inject a unresolvable commit
        compact_segment(ctx.clone(), &table).await?;
    }

    // the compact operation committed latter should be failed.
    let r: Result<()> = mutator.try_commit_compact(fuse_table, ctx.clone()).await;
    assert!(r.is_err());
    assert_eq!(r.err().unwrap().code(), ErrorCode::UNRESOLVABLE_CONFLICT);

    Ok(())
}

#[async_trait::async_trait]
trait TryCommitCompact {
    async fn try_commit_compact(self, table: &FuseTable, ctx: Arc<QueryContext>) -> Result<()>;
}

#[async_trait::async_trait]
impl TryCommitCompact for SegmentCompactMutator {
    async fn try_commit_compact(self, table: &FuseTable, ctx: Arc<QueryContext>) -> Result<()> {
        let base_snapshot = self.base_snapshot().clone();
        let table_meta_timestamps =
            ctx.get_table_meta_timestamps(table, Some(base_snapshot.clone()))?;
        let compaction = self.into_compaction_state();
        if compaction.new_segment_paths.is_empty() {
            return Ok(());
        }
        let mut pipeline = Pipeline::create();
        table.build_compact_segment_pipeline(
            ctx.clone(),
            &mut pipeline,
            compaction,
            base_snapshot,
            table_meta_timestamps,
        )?;
        let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
        let executor = PipelineCompleteExecutor::from_pipelines(vec![pipeline], executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
trait AppendRow {
    async fn append_rows(&self, n: usize) -> Result<()>;
}

#[async_trait::async_trait]
impl AppendRow for TestFixture {
    async fn append_rows(&self, n: usize) -> Result<()> {
        let qry = "insert into t values(1)";
        for _ in 0..n {
            self.execute_command(qry).await?;
        }
        Ok(())
    }
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    match &blocks[0].get_by_offset(0).value() {
        Value::Scalar(Scalar::Number(NumberScalar::UInt64(s))) => Ok(*s),
        Value::Column(Column::Number(NumberColumn::UInt64(c))) => Ok(c[0]),
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Expected UInt64, but got {:?}",
            blocks[0].get_by_offset(0).value()
        ))),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_changes_preserve_position() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let threshold = BlockThresholds {
        block_per_segment: 3,
        ..Default::default()
    };
    let mut case_fixture = CompactSegmentTestFixture::try_new(&ctx, threshold)?;

    let (state, _, base_segments) = case_fixture
        .run(&[10, 10, 1, 2, 10, 10], None, None)
        .await?;

    assert_eq!(state.new_segment_paths.len(), 1);
    let new_segment = (state.new_segment_paths[0].clone(), SegmentInfo::VERSION);
    assert_eq!(state.replaced_segments.get(&2), Some(&new_segment));
    assert_eq!(state.removed_segment_indexes, vec![3]);
    let output = output_locations(&base_segments, &state);
    assert_eq!(output[2], new_segment);
    assert_eq!(output.len(), 5);

    Ok(())
}

pub async fn compact_segment(ctx: Arc<QueryContext>, table: &Arc<dyn Table>) -> Result<()> {
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mutator = build_mutator(fuse_table, ctx.clone(), None).await?.unwrap();
    mutator.try_commit_compact(fuse_table, ctx).await
}

async fn build_mutator(
    tbl: &FuseTable,
    ctx: Arc<dyn TableContext>,
    limit: Option<usize>,
) -> Result<Option<SegmentCompactMutator>> {
    let snapshot_opt = tbl.read_table_snapshot().await?;
    let base_snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot, no compaction.
        return Ok(None);
    };

    if base_snapshot.summary.block_count <= 1 {
        return Ok(None);
    }

    let table_meta_timestamps = ctx.get_table_meta_timestamps(tbl, Some(base_snapshot.clone()))?;

    let block_per_seg = tbl.get_option("block_per_segment", 1000);

    let compact_params = CompactOptions {
        base_snapshot,
        block_per_seg,
        num_segment_limit: limit,
        num_block_limit: None,
    };

    let mut segment_mutator = SegmentCompactMutator::try_create(
        ctx.clone(),
        compact_params,
        tbl.meta_location_generator().clone(),
        tbl.get_operator(),
        tbl.cluster_key_info(),
        table_meta_timestamps,
    )?;

    if segment_mutator.target_select().await? {
        Ok(Some(segment_mutator))
    } else {
        Ok(None)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_compactor() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let threshold_10 = BlockThresholds {
        block_per_segment: 10,
        ..Default::default()
    };
    let threshold_3 = BlockThresholds {
        block_per_segment: 3,
        ..Default::default()
    };
    let threshold_5 = BlockThresholds {
        block_per_segment: 5,
        ..Default::default()
    };

    {
        let case_name = "highly fragmented segments";
        let case = CompactCase {
            // 3 fragmented segments
            // - each of them have number blocks lesser than `threshold`
            blocks_number_of_input_segments: vec![1, 2, 3],
            // - these segments should be compacted into one
            expected_number_of_output_segments: 1,
            // - which contains 6 blocks
            expected_block_number_of_new_segments: vec![1 + 2 + 3],
            case_name,
        };

        // run, and verify that
        // - numbers are as expected
        //   - number of the newly created segments (which are compacted)
        //   - number of segments unchanged
        // - other general invariants
        //   - unchanged segments still be there
        //   - blocks and the order of them are not changed
        //   - statistics are as expected
        //   - the output segments could not be compacted further
        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // Several independent groups finish concurrently. Output locations,
        // block order and statistics must still follow the snapshot traversal.
        let case = CompactCase {
            blocks_number_of_input_segments: vec![5, 5, 15, 5, 5, 15, 5, 5, 15, 5, 5],
            expected_number_of_output_segments: 7,
            expected_block_number_of_new_segments: vec![10, 10, 10, 10],
            case_name: "multiple parallel merge groups",
        };
        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        let case_name = "greedy compact, but not too greedy(1), right assoc";
        let case = CompactCase {
            // - 4 segments
            blocks_number_of_input_segments: vec![1, 8, 2, 8],
            // - these segments should be compacted into 2 new segments
            expected_number_of_output_segments: 2,
            // compaction is right-assoc
            // -  (2 + 8) meets threshold 10
            //    they should be compacted into one new segment.
            //    although the next segment contains only 1 segment, it should NOT
            //    be compacted (the not too greedy rule)
            // - (1 + 8) should be compacted into another new segment
            //
            // To let the case more readable, we specify the block numbers of new
            // segments in the order of input segments
            expected_block_number_of_new_segments: vec![1 + 8, 2 + 8],
            case_name,
        };
        // run & verify
        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        let case_name = "greedy compact, but not too greedy (2), right-assoc";
        let case = CompactCase {
            // 4 segments
            blocks_number_of_input_segments: vec![5, 2, 3, 6],
            // these segments should be compacted into 2 segments
            expected_number_of_output_segments: 2,
            // (2 + 3 + 6) exceeds the threshold 10
            //  - but not too much, lesser than 2 * threshold, which is 20;
            //  - they are allowed to be compacted into one, to avoid the ripple effects:
            //      since the order of blocks should be preserved, they might be cases, that
            //      to compact one fragment, a large amount of non-fragmented segments have to be
            //      split into pieces and re-compacted.
            // - but the last segment of 5 blocks should be kept alone
            //   thus, only one new segment will be generated (the not too greedy rule)
            //   if it is the last segment of the snapshot, we just tolerant this situation.
            //   if a limited
            expected_block_number_of_new_segments: vec![2 + 3 + 6],
            case_name,
        };

        // run & verify
        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(1), right-assoc";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![5, 6, 11, 2, 10],
            // these segments should be compacted into 2 new segments, 1 segment unchanged
            // unchanged: (10)
            // new segments: (11 + 2), ( 5 + 6)
            expected_number_of_output_segments: 2 + 1,
            expected_block_number_of_new_segments: vec![5 + 6, 11 + 2],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(2)";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![10, 10, 1, 2, 10],
            // these segments should be compacted into
            // (10), (10), (1 + 2 + 10)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![1 + 2 + 10],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // case: fragmented segments, with barrier

        let case_name = "barrier(3)";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 19, 5, 6],
            // these segments should be compacted into
            // (1), (19), (5, 6)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![5 + 6],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // edge case: empty segments should be dropped

        let case_name = "empty segments should be dropped";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![0, 1, 0, 19, 0, 5, 0, 6, 0],
            // these segments should be compacted into
            // (1), (19), (5, 6)
            expected_number_of_output_segments: 3,
            expected_block_number_of_new_segments: vec![5 + 6],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        // edge case: single jumbo block

        let case_name = "single jumbo block";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![10],
            expected_number_of_output_segments: 1,
            expected_block_number_of_new_segments: vec![],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_3, None).await?;
    }

    {
        let case_name = "jumbo block with single fragment";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![7, 2],
            // inputs will not be compacted ( 7 > 2 * 3)
            expected_number_of_output_segments: 2,
            // no new segment should be generated
            expected_block_number_of_new_segments: vec![],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_3, None).await?;
    }

    {
        let case_name = "right assoc";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![8, 5, 7],
            expected_number_of_output_segments: 2,
            // one new segment should be generated, since
            // - (5 + 7) < 2 * 10
            // - but (8 + 5 + 7) = 20 >= 20, which exceed the upper limit
            expected_block_number_of_new_segments: vec![5 + 7],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_10, None).await?;
    }

    {
        let case_name = "limit (normal case)";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 2, 3, 2, 3],
            expected_number_of_output_segments: 4,
            expected_block_number_of_new_segments: vec![1 + 2],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_5, Some(2)).await?;
    }

    {
        let case_name = "limit (auto adjust limit)";
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 2, 3, 2, 3],
            expected_number_of_output_segments: 4,
            expected_block_number_of_new_segments: vec![1 + 2],
            case_name,
        };

        // if limit is specified as 1, it will be adjusted to 2 during execution
        // since at least two fragmented segments are needed for compaction
        let limit = Some(1);
        case.run_and_verify(&ctx, threshold_5, limit).await?;
    }

    {
        let case_name = "limit (abundant limit)";
        let limit = Some(5);
        let case = CompactCase {
            // input segments
            blocks_number_of_input_segments: vec![1, 1, 3, 2, 3],
            expected_number_of_output_segments: 2,
            expected_block_number_of_new_segments: vec![1 + 1 + 3, 2 + 3],
            case_name,
        };

        case.run_and_verify(&ctx, threshold_5, limit).await?;
    }

    {
        // case: rand test

        let case_name = "rand";
        let threshold = 3;
        let mut rng = thread_rng();

        // let rounds = 200; // use this setting at home
        let rounds = 20;
        for _ in 0..rounds {
            let num_segments: usize = rng.gen_range(0..10);
            let mut blocks_number_of_input_segments = Vec::with_capacity(num_segments);

            // simulate the compaction process, verifies that the test target works as expected
            let mut num_accumulated_blocks = 0;
            let mut fragmented_segments = 0;

            // - number of segment expected in the output of compaction, includes both the compacted
            //   new segments and the unchanged non-fragmented segments
            let mut expected_number_of_output_segments = 0;
            // - number of new segments created during the compaction
            let mut expected_block_number_of_new_segments = vec![];
            for _ in 0..num_segments {
                let block_num: usize = rng.gen_range(0..20);
                blocks_number_of_input_segments.push(block_num);
            }

            // traverse the input segments in reversed order (to let the compaction "right-assoc")
            for item in blocks_number_of_input_segments.iter().rev() {
                let block_num = *item;
                if block_num != 0 {
                    let s = block_num + num_accumulated_blocks;
                    if s < threshold {
                        // input segment is fragmented, but fragments collected so far
                        // are not enough yet.
                        num_accumulated_blocks = s;
                        // only in this branch, the number of fragmented_segments will increase
                        fragmented_segments += 1;
                    } else if s >= threshold && s < 2 * threshold {
                        // input segment is fragmented, and fragments collected are
                        // large enough to be compacted
                        num_accumulated_blocks = 0;
                        // mark that a segment will be included in the output
                        expected_number_of_output_segments += 1;
                        if fragmented_segments > 0 {
                            // mark that a NEW segment will be generated, which
                            // "contains" all the fragmented segments collected so far.
                            expected_block_number_of_new_segments.push(s);
                        }
                        // reset state
                        fragmented_segments = 0;
                    } else {
                        // input segment is larger than threshold
                        // - fragmented segments collected so far should be compacted first
                        if fragmented_segments > 0 {
                            // some fragments left there, check them out
                            if fragmented_segments > 1 {
                                // if there are more than one fragments, a new segment is expected
                                // to be generated
                                expected_block_number_of_new_segments.push(num_accumulated_blocks);
                            }
                            // mark that another segment will be include in the output
                            expected_number_of_output_segments += 1;
                        }
                        // - after compacting the fragments, count this large segment in
                        expected_number_of_output_segments += 1;

                        // no fragments left currently, reset the counters
                        fragmented_segments = 0;
                        num_accumulated_blocks = 0;
                    }
                }
            }

            // finalize, compact left fragments if any
            if fragmented_segments > 0 {
                if fragmented_segments > 1 {
                    // if there are more than one fragments left there, a new segment should be created
                    expected_block_number_of_new_segments.push(num_accumulated_blocks);
                }
                // mark that another segment will be include in the output
                expected_number_of_output_segments += 1;
            }

            // To make the non-random test cases more readable, paths of newly created segments are
            // specified in the order of original(Input) order.
            // But during this simulated compaction, the paths are kept in reversed order,
            // so here we reverse the paths, to make the test verifications followed pass.
            expected_block_number_of_new_segments.reverse();
            let case = CompactCase {
                blocks_number_of_input_segments,
                expected_number_of_output_segments,
                expected_block_number_of_new_segments,
                case_name,
            };

            case.run_and_verify(&ctx, threshold_3, None).await?;
        }
    }

    Ok(())
}

pub struct CompactSegmentTestFixture {
    threshold: BlockThresholds,
    ctx: Arc<dyn TableContext>,
    data_accessor: DataOperator,
    location_gen: TableMetaLocationGenerator,
    // blocks of input_segments, order by segment
    input_blocks: Vec<BlockMeta>,
}

impl CompactSegmentTestFixture {
    fn try_new(ctx: &Arc<QueryContext>, threshold: BlockThresholds) -> Result<Self> {
        let location_gen = TableMetaLocationGenerator::new("test/".to_owned());
        let data_accessor = ctx.get_application_level_data_operator()?;
        Ok(Self {
            ctx: ctx.clone(),
            threshold,
            data_accessor,
            location_gen,
            input_blocks: vec![],
        })
    }

    async fn run<'a>(
        &'a mut self,
        num_block_of_segments: &'a [usize],
        limit: Option<usize>,
        cluster_key_id: Option<u32>,
    ) -> Result<(SegmentCompactionState, Statistics, Vec<Location>)> {
        let data_accessor = &self.data_accessor.operator();
        let location_gen = &self.location_gen;

        let schema = TestFixture::default_table_schema();
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = self.ctx.get_settings().get_max_storage_io_requests()? as usize;

        let cluster_key_info = cluster_key_id
            .map(|id| ClusterKeyInfo::new((id, "(id)".to_string()), ClusterType::Linear));
        let seg_acc = SegmentCompactor::new(
            self.threshold.block_per_segment as u64,
            cluster_key_info.clone(),
            max_threads,
            max_io_requests,
            schema,
            data_accessor,
            location_gen,
            TestFixture::default_table_meta_timestamps(),
        );

        let rows_per_block = vec![1; num_block_of_segments.len()];
        let (locations, blocks, segments) = Self::gen_segments(
            self.ctx.clone(),
            num_block_of_segments.to_owned(),
            rows_per_block,
            self.threshold,
            cluster_key_id,
            false,
        )
        .await?;
        let mut summary = Statistics::default();
        for segment in segments {
            merge_statistics_mut(&mut summary, &segment.summary, cluster_key_info.as_ref());
        }
        self.input_blocks = blocks;
        // The input is in snapshot order (newest first); gen_segments writes
        // locations oldest first so reverse them before selecting the window.
        let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
        let state = seg_acc
            .compact(&snapshot_segments, limit, |status| {
                self.ctx.set_status_info(&status);
            })
            .await?;
        Ok((state, summary, snapshot_segments))
    }

    pub async fn gen_segments(
        ctx: Arc<dyn TableContext>,
        block_num_of_segments: Vec<usize>,
        rows_per_blocks: Vec<usize>,
        thresholds: BlockThresholds,
        cluster_key_id: Option<u32>,
        unclustered: bool,
    ) -> Result<(Vec<Location>, Vec<BlockMeta>, Vec<SegmentInfo>)> {
        let location_gen = TableMetaLocationGenerator::new("test/".to_owned());
        let data_accessor = ctx.get_application_level_data_operator()?.operator();
        let threads_nums = ctx.get_settings().get_max_threads()? as usize;

        let mut tasks = vec![];
        for (num_blocks, rows_per_block) in
            block_num_of_segments.into_iter().zip(rows_per_blocks).rev()
        {
            let location_gen = location_gen.clone();
            let data_accessor = data_accessor.clone();
            tasks.push(async move {
                let (schema, blocks) =
                    TestFixture::gen_sample_blocks_ex(num_blocks, rows_per_block, 1);
                let mut stats_acc = RowOrientedSegmentBuilder::default();

                let mut collected_blocks = vec![];
                for block in blocks {
                    let block = block?;

                    let col_stats = gen_columns_statistics(
                        &block,
                        None,
                        &schema,
                        &BTreeMap::new(),
                        HashMap::new(),
                    )?;

                    let cluster_stats = if unclustered && num_blocks % 4 == 0 {
                        None
                    } else {
                        cluster_key_id.map(|v| {
                            let val = block.get_by_offset(0);
                            let left_value = unsafe { val.index_unchecked(0) }.to_owned();
                            let right_value =
                                unsafe { val.index_unchecked(val.value().len() - 1) }.to_owned();
                            let left = vec![left_value];
                            let right = vec![right_value];
                            let level = if left.eq(&right)
                                && block.num_rows() >= thresholds.block_per_segment
                            {
                                -1
                            } else {
                                0
                            };
                            ClusterStatistics::new(v, left, right, level)
                        })
                    };

                    let (location, _) = location_gen
                        .gen_block_location(TestFixture::default_table_meta_timestamps());
                    let row_count = block.num_rows() as u64;
                    let block_size = block.memory_size() as u64;

                    let write_settings = WriteSettings {
                        storage_format: FuseStorageFormat::Parquet,
                        ..Default::default()
                    };

                    let (col_metas, buf) = serialize_block(&write_settings, &schema, block)?;
                    let file_size = buf.len() as u64;

                    data_accessor.write(&location.0, buf).await?;

                    let block_meta = BlockMeta::new(
                        row_count,
                        block_size,
                        file_size,
                        col_stats,
                        col_metas,
                        cluster_stats,
                        location,
                        None,
                        0,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Compression::Lz4Raw,
                        Some(Utc::now()),
                    );

                    collected_blocks.push(block_meta.clone());
                    stats_acc
                        .add_block(block_meta, VirtualBlockInput::None)
                        .unwrap();
                }
                let cluster_key_info = cluster_key_id
                    .map(|id| ClusterKeyInfo::new((id, "(id)".to_string()), ClusterType::Linear));
                let segment_info = stats_acc.build(thresholds, cluster_key_info.as_ref(), None)?;
                let path = location_gen
                    .gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
                segment_info.write_meta(&data_accessor, &path).await?;
                Ok::<_, ErrorCode>(((path, SegmentInfo::VERSION), collected_blocks, segment_info))
            });
        }

        let res = execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let mut locations = vec![];
        let mut collected_blocks = vec![];
        let mut segment_infos = vec![];
        for (location, blocks, info) in res.into_iter() {
            locations.push(location);
            collected_blocks.extend(blocks);
            segment_infos.push(info);
        }
        Ok((locations, collected_blocks, segment_infos))
    }

    // verify that newly generated segments contain the proper number of blocks
    pub async fn verify_new_segments(
        case_name: &str,
        new_segment_paths: &[String],
        expected_num_blocks: &[usize],
        compact_segment_reader: &CompactSegmentInfoReader,
    ) -> Result<()> {
        // traverse the paths of new segments  in reversed order
        for (idx, x) in new_segment_paths.iter().rev().enumerate() {
            let load_params = LoadParams {
                location: x.to_string(),
                len_hint: None,
                ver: SegmentInfo::VERSION,
                put_cache: false,
            };

            let compact_segment = compact_segment_reader.read(&load_params).await?;
            let segment = SegmentInfo::try_from(compact_segment)?;
            assert_eq!(
                segment.blocks.len(),
                expected_num_blocks[idx],
                "case name :{}, verify_block_number_of_new_segments",
                case_name
            );
        }
        Ok(())
    }
}

struct CompactCase {
    blocks_number_of_input_segments: Vec<usize>,
    expected_block_number_of_new_segments: Vec<usize>,
    // number of output segments, newly created and unchanged
    expected_number_of_output_segments: usize,
    case_name: &'static str,
}

impl CompactCase {
    async fn run_and_verify(
        &self,
        ctx: &Arc<QueryContext>,
        threshold: BlockThresholds,
        limit: Option<usize>,
    ) -> Result<()> {
        // setup & run
        let compact_segment_reader = MetaReaders::segment_info_reader(
            ctx.get_application_level_data_operator()?.operator(),
            TestFixture::default_table_schema(),
        );
        let mut case_fixture = CompactSegmentTestFixture::try_new(ctx, threshold)?;
        let (r, summary, base_segments) = case_fixture
            .run(&self.blocks_number_of_input_segments, limit, None)
            .await?;
        let output = output_locations(&base_segments, &r);

        // verify that:

        // 1. number of newly generated segment is as expected
        let expected_num_of_new_segments = self.expected_block_number_of_new_segments.len();
        assert_eq!(
            r.new_segment_paths.len(),
            expected_num_of_new_segments,
            "case: {}, step: verify number of new segments generated, segment block size {:?}",
            self.case_name,
            self.blocks_number_of_input_segments,
        );

        // 2. number of segments is as expected (including both of the segments that not changed and newly generated segments)
        assert_eq!(
            output.len(),
            self.expected_number_of_output_segments,
            "case: {}, step: verify number of output segments (new segments and unchanged segments)",
            self.case_name,
        );

        // 3. each new segment contains expected number of blocks
        CompactSegmentTestFixture::verify_new_segments(
            self.case_name,
            &r.new_segment_paths,
            &self.expected_block_number_of_new_segments,
            &compact_segment_reader,
        )
        .await?;

        // invariants 4 - 6 are general rules, for all the cases.
        let mut idx = 0;
        let mut statistics_of_input_segments = Statistics::default();
        let mut block_num_of_output_segments = vec![];

        // 4. input blocks should be there and in the original order
        for location in output.iter().rev() {
            let load_params = LoadParams {
                location: location.0.clone(),
                len_hint: None,
                ver: location.1,
                put_cache: false,
            };

            let compact_segment = compact_segment_reader.read(&load_params).await?;
            let segment = SegmentInfo::try_from(compact_segment)?;
            merge_statistics_mut(&mut statistics_of_input_segments, &segment.summary, None);
            block_num_of_output_segments.push(segment.blocks.len());

            for x in &segment.blocks {
                let original_block_meta = &case_fixture.input_blocks[idx];
                assert_eq!(
                    original_block_meta,
                    x.as_ref(),
                    "case : {}, verify block order",
                    self.case_name
                );
                idx += 1;
            }
        }
        block_num_of_output_segments.reverse();

        // 5. statistics should be the same
        assert_eq!(
            statistics_of_input_segments, summary,
            "case : {}",
            self.case_name
        );

        // 6. the output segments can not be compacted further, if (no limit)
        if limit.is_none() {
            let mut case_fixture = CompactSegmentTestFixture::try_new(ctx, threshold)?;
            let (r, _, base_segments) = case_fixture
                .run(&block_num_of_output_segments, None, None)
                .await?;
            assert_eq!(
                r.new_segment_paths.len(),
                0,
                "case: {}, verify number of new segment",
                self.case_name
            );
            let num_of_output_segments = block_num_of_output_segments.len();
            assert_eq!(
                output_locations(&base_segments, &r).len(),
                num_of_output_segments,
                "case: {}, verify number of segments",
                self.case_name
            );
        }

        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_segment_with_cluster() -> anyhow::Result<()> {
    let cluster_key_id = 0;
    let threshold = BlockThresholds {
        block_per_segment: 5,
        ..Default::default()
    };

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let location_gen = TableMetaLocationGenerator::new("test/".to_owned());
    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let schema = TestFixture::default_table_schema();

    let settings = ctx.get_settings();
    settings.set_max_threads(2)?;
    settings.set_max_storage_io_requests(4)?;

    let compact_segment_reader =
        MetaReaders::segment_info_reader(data_accessor.clone(), schema.clone());

    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);

        let limit: usize = rand.gen_range(1..10);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..6));
        }

        let number_of_blocks: usize = block_number_of_segments.iter().sum();
        if number_of_blocks < 2 {
            eprintln!("number_of_blocks must large than 1");
            continue;
        }
        eprintln!(
            "generating segments number of segments {},  number of blocks {}",
            number_of_segments, number_of_blocks,
        );

        // setup & run
        let rows_per_block = vec![1; block_number_of_segments.len()];
        let (locations, _, segments) = CompactSegmentTestFixture::gen_segments(
            ctx.clone(),
            block_number_of_segments,
            rows_per_block,
            threshold,
            Some(cluster_key_id),
            false,
        )
        .await?;
        let cluster_key_info = Some(ClusterKeyInfo::new(
            (cluster_key_id, "(id)".to_string()),
            ClusterType::Linear,
        ));
        let mut summary = Statistics::default();
        for segment in &segments {
            merge_statistics_mut(&mut summary, &segment.summary, cluster_key_info.as_ref());
        }

        eprintln!("running compact, limit {}", limit);
        let seg_acc = SegmentCompactor::new(
            threshold.block_per_segment as u64,
            cluster_key_info.clone(),
            settings.get_max_threads()? as usize,
            settings.get_max_storage_io_requests()? as usize,
            schema.clone(),
            &data_accessor,
            &location_gen,
            TestFixture::default_table_meta_timestamps(),
        );
        let snapshot_segments = locations.into_iter().rev().collect::<Vec<_>>();
        let state = seg_acc
            .compact(&snapshot_segments, Some(limit), |status| {
                ctx.set_status_info(&status);
            })
            .await?;

        // Chunk-local cluster sorting may reorder blocks. Compaction must
        // still preserve every original block exactly once and all statistics.
        let mut input_block_id = Vec::with_capacity(number_of_blocks);
        for segment in &segments {
            input_block_id.extend(segment.blocks.iter().map(|b| b.location.clone()));
        }

        let output = output_locations(&snapshot_segments, &state);
        let mut statistics_of_segments: Statistics = Statistics::default();
        let mut output_block_id = Vec::with_capacity(number_of_blocks);
        for location in output.iter().rev() {
            let load_params = LoadParams {
                location: location.0.clone(),
                len_hint: None,
                ver: location.1,
                put_cache: false,
            };

            let compact_segment = compact_segment_reader.read(&load_params).await?;
            let segment = SegmentInfo::try_from(compact_segment)?;
            merge_statistics_mut(
                &mut statistics_of_segments,
                &segment.summary,
                cluster_key_info.as_ref(),
            );

            output_block_id.extend(segment.blocks.iter().map(|b| b.location.clone()));
        }

        input_block_id.sort();
        output_block_id.sort();
        assert_eq!(input_block_id, output_block_id);
        assert_eq!(summary, statistics_of_segments);
    }

    Ok(())
}
