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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ReclusterMode;
use databend_common_storages_fuse::operations::ReclusterMutator;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use rand::Rng;
use rand::thread_rng;
use uuid::Uuid;

use crate::storages::fuse::operations::mutation::CompactSegmentTestFixture;

fn test_cluster_key_expr() -> Expr<usize> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        data_type: DataType::Number(NumberDataType::Int32),
        id: 0,
        display_name: "c0".to_string(),
    })
}

async fn gen_recluster_segments(
    data_accessor: &opendal::Operator,
    location_generator: &TableMetaLocationGenerator,
    num_segments: usize,
    blocks_per_segment: usize,
    row_count: u64,
    block_size: u64,
    file_size: u64,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
) -> anyhow::Result<Vec<meta::Location>> {
    let mut segment_locations = Vec::with_capacity(num_segments);
    for _ in 0..num_segments {
        let mut blocks = Vec::with_capacity(blocks_per_segment);
        for _ in 0..blocks_per_segment {
            let block_id = Uuid::new_v4().simple().to_string();
            let location = (block_id, DataBlock::VERSION);
            blocks.push(Arc::new(BlockMeta::new(
                row_count,
                block_size,
                file_size,
                HashMap::default(),
                HashMap::default(),
                Some(ClusterStatistics::new(
                    cluster_key_id,
                    vec![Scalar::from(1i32)],
                    vec![Scalar::from(100i32)],
                    0,
                    None,
                )),
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
                meta::Compression::Lz4Raw,
                Some(Utc::now()),
            )));
        }

        let block_refs = blocks
            .iter()
            .map(|block| block.as_ref())
            .collect::<Vec<_>>();
        let statistics = reduce_block_metas(&block_refs, thresholds, Some(cluster_key_id));
        let segment = SegmentInfo::new(blocks, statistics);
        let segment_location = location_generator
            .gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
        segment.write_meta(data_accessor, &segment_location).await?;
        segment_locations.push((segment_location, SegmentInfo::VERSION));
    }
    Ok(segment_locations)
}

async fn gen_recluster_segments_by_level(
    data_accessor: &opendal::Operator,
    location_generator: &TableMetaLocationGenerator,
    level_counts: &[(i32, usize)],
    row_count: u64,
    block_size: u64,
    file_size: u64,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
) -> anyhow::Result<Vec<meta::Location>> {
    let total_segments = level_counts.iter().map(|(_, count)| *count).sum();
    let mut segment_locations = Vec::with_capacity(total_segments);
    for &(level, count) in level_counts {
        for _ in 0..count {
            let block_id = Uuid::new_v4().simple().to_string();
            let location = (block_id, DataBlock::VERSION);
            let block = Arc::new(BlockMeta::new(
                row_count,
                block_size,
                file_size,
                HashMap::default(),
                HashMap::default(),
                Some(ClusterStatistics::new(
                    cluster_key_id,
                    vec![Scalar::from(1i32)],
                    vec![Scalar::from(100i32)],
                    level,
                    None,
                )),
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
                meta::Compression::Lz4Raw,
                Some(Utc::now()),
            ));
            let statistics =
                reduce_block_metas(&[block.as_ref()], thresholds, Some(cluster_key_id));
            let segment = SegmentInfo::new(vec![block], statistics);
            let segment_location = location_generator
                .gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
            segment.write_meta(data_accessor, &segment_location).await?;
            segment_locations.push((segment_location, SegmentInfo::VERSION));
        }
    }
    Ok(segment_locations)
}

async fn gen_recluster_segments_by_ranges(
    data_accessor: &opendal::Operator,
    location_generator: &TableMetaLocationGenerator,
    ranges_by_segment: &[Vec<(i32, i32)>],
    row_count: u64,
    block_size: u64,
    file_size: u64,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
) -> anyhow::Result<Vec<meta::Location>> {
    let mut segment_locations = Vec::with_capacity(ranges_by_segment.len());
    for ranges in ranges_by_segment {
        let mut blocks = Vec::with_capacity(ranges.len());
        for &(min, max) in ranges {
            let block_id = Uuid::new_v4().simple().to_string();
            let location = (block_id, DataBlock::VERSION);
            blocks.push(Arc::new(BlockMeta::new(
                row_count,
                block_size,
                file_size,
                HashMap::default(),
                HashMap::default(),
                Some(ClusterStatistics::new(
                    cluster_key_id,
                    vec![Scalar::from(min)],
                    vec![Scalar::from(max)],
                    0,
                    None,
                )),
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
                meta::Compression::Lz4Raw,
                Some(Utc::now()),
            )));
        }

        let block_refs = blocks
            .iter()
            .map(|block| block.as_ref())
            .collect::<Vec<_>>();
        let statistics = reduce_block_metas(&block_refs, thresholds, Some(cluster_key_id));
        let segment = SegmentInfo::new(blocks, statistics);
        let segment_location = location_generator
            .gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
        segment.write_meta(data_accessor, &segment_location).await?;
        segment_locations.push((segment_location, SegmentInfo::VERSION));
    }
    Ok(segment_locations)
}

async fn target_select_segments_by_level_with_mode(
    level_counts: &[(i32, usize)],
    thresholds: BlockThresholds,
    max_tasks: usize,
    mode: ReclusterMode,
) -> anyhow::Result<(u64, ReclusterParts)> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let segment_locations = gen_recluster_segments_by_level(
        &data_accessor,
        &location_generator,
        level_counts,
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        max_tasks,
        1000,
        mode,
    )
    .await?;
    Ok((block_num, parts))
}

async fn target_select_segment_locations_with_mode(
    ctx: Arc<dyn TableContext>,
    data_accessor: opendal::Operator,
    segment_locations: Vec<meta::Location>,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
    max_tasks: usize,
    max_segments: usize,
    mode: ReclusterMode,
) -> anyhow::Result<(usize, u64, ReclusterParts)> {
    let schema = TestFixture::default_table_schema();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = FuseTable::segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        &None,
        segment_locations,
    )
    .await?;

    let mutator = ReclusterMutator::new(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        vec![test_cluster_key_expr()],
        1.0,
        thresholds,
        cluster_key_id,
        max_tasks,
    );

    let segment_windows = mutator.select_segments(&compact_segments, max_segments)?;
    let mut selected_segments = 0;
    let mut block_num = 0;
    let mut parts = ReclusterParts::default();
    for selected_segs in segment_windows {
        selected_segments = selected_segs.len();
        let (candidate_block_num, candidate_parts) =
            mutator.target_select(selected_segs, mode).await?;
        if !candidate_parts.is_empty() {
            block_num = candidate_block_num;
            parts = candidate_parts;
            break;
        }
    }
    Ok((selected_segments, block_num, parts))
}

fn task_part_counts(parts: &ReclusterParts) -> Vec<usize> {
    parts
        .tasks
        .iter()
        .map(|task| task.parts.len())
        .collect::<Vec<_>>()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_limits_removed_segments_to_selected_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);

    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 10), (2, 9), (12, 13)],
            vec![(20, 21), (22, 23)],
            vec![(30, 31), (32, 33)],
        ],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (selected_segments, block_num, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        1000,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(selected_segments, 3);
    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![2]);
    assert_eq!(parts.removed_segment_indexes, vec![0]);
    assert_eq!(parts.remained_blocks.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_block_select() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());

    let data_accessor = ctx.get_application_level_data_operator()?.operator();

    let cluster_key_id = 0;
    let thresholds = BlockThresholds::default();
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(1, 3)], vec![(2, 4)], vec![(4, 5)]],
        1,
        1,
        1,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, _, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        8,
        ReclusterMode::Normal,
    )
    .await?;
    let need_recluster = !parts.is_empty();
    assert!(need_recluster);
    let tasks = parts.tasks;
    assert_eq!(tasks.len(), 1);
    let total_block_nums = tasks.iter().map(|t| t.parts.len()).sum::<usize>();
    assert_eq!(total_block_nums, 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_selects_multiple_segment_windows() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 2);

    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 10)],
            vec![(2, 9)],
            vec![(20, 30)],
            vec![(21, 29)],
            vec![(40, 41)],
            vec![(50, 51)],
            vec![(60, 61)],
        ],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = TestFixture::default_table_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = FuseTable::segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        &None,
        segment_locations,
    )
    .await?;

    let mutator = ReclusterMutator::new(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        vec![test_cluster_key_expr()],
        1.0,
        thresholds,
        cluster_key_id,
        1,
    );

    let segment_windows = mutator.select_segments(&compact_segments, 8)?;
    let window_index_sets = segment_windows
        .iter()
        .map(|window| {
            window
                .iter()
                .map(|segment| segment.loc.segment_idx)
                .collect::<HashSet<_>>()
        })
        .collect::<Vec<_>>();
    assert!(window_index_sets.contains(&HashSet::from([0, 1])));
    assert!(window_index_sets.contains(&HashSet::from([2, 3])));
    let selected_segments = window_index_sets
        .iter()
        .flat_map(|window| window.iter().copied())
        .collect::<HashSet<_>>();
    assert_eq!(selected_segments.len(), 7);

    let thresholds = BlockThresholds::new(1000, 100, 100, 1);
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 10)],
            vec![(2, 9)],
            vec![(3, 8)],
            vec![(4, 7)],
            vec![(5, 6)],
        ],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = FuseTable::segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        &None,
        segment_locations,
    )
    .await?;
    let mutator = ReclusterMutator::new(
        ctx,
        data_accessor,
        schema,
        vec![test_cluster_key_expr()],
        1.0,
        thresholds,
        cluster_key_id,
        1,
    );
    let segment_windows = mutator.select_segments(&compact_segments, 3)?;
    let window_sizes = segment_windows
        .iter()
        .map(|window| window.len())
        .collect::<Vec<_>>();
    assert_eq!(window_sizes, vec![3, 3]);
    let first_window = segment_windows[0]
        .iter()
        .map(|segment| segment.loc.segment_idx)
        .collect::<HashSet<_>>();
    let second_window = segment_windows[1]
        .iter()
        .map(|segment| segment.loc.segment_idx)
        .collect::<HashSet<_>>();
    let union = first_window
        .union(&second_window)
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(union.len(), 5);
    assert_eq!(first_window.intersection(&second_window).count(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_split_tasks_by_parallel_budget() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 10, 10, 1000);

    // 300 small blocks with four workers should be selected and split by the
    // parallel budget while keeping each task above the minimum fill ratio.
    let segment_locations = gen_recluster_segments(
        &data_accessor,
        &location_generator,
        30,
        10,
        1000,
        10,
        10,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        4,
        1000,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(block_num, 300);
    assert_eq!(parts.tasks.len(), 4);
    assert_eq!(task_part_counts(&parts), vec![75, 75, 75, 75]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_skips_when_minimal_task_exceeds_memory() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(150)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);

    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(1, 10)], vec![(2, 9)], vec![(3, 8)]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(block_num, 0);
    assert!(parts.tasks.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_skips_singleton_over_memory_boundary() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(100)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);

    // The selected order is [40, 40, 90, 20]. The first two blocks form a
    // valid task, then 90 + 20 crosses the memory boundary. Since the pending
    // 90-byte singleton cannot form a normal recluster task, it is dropped and
    // no over-budget tail task is emitted.
    let mut segment_locations = Vec::new();
    for (range, block_size) in [((1, 10), 40), ((2, 9), 40), ((3, 8), 90), ((4, 7), 20)] {
        segment_locations.extend(
            gen_recluster_segments_by_ranges(
                &data_accessor,
                &location_generator,
                &[vec![range]],
                1000,
                block_size,
                100,
                thresholds,
                cluster_key_id,
            )
            .await?,
        );
    }

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = target_select_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![2]);
    assert_eq!(parts.tasks[0].total_bytes, 80);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_only_lowest_small_batch_yields() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = target_select_segments_by_level_with_mode(
        &[(0, 3), (1, 3), (2, 10)],
        thresholds,
        1,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 1);
    assert_eq!(task_part_counts(&parts), vec![3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_backfills_deferred_lowest_batch() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = target_select_segments_by_level_with_mode(
        &[(1, 3), (2, 4)],
        thresholds,
        2,
        ReclusterMode::Normal,
    )
    .await?;

    assert_eq!(block_num, 7);
    assert_eq!(parts.tasks.len(), 2);
    assert_eq!(
        parts
            .tasks
            .iter()
            .map(|task| task.level)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(task_part_counts(&parts), vec![4, 3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_final_recluster_groups_mature_levels_in_two_level_bands() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = target_select_segments_by_level_with_mode(
        &[(0, 1), (1, 1), (2, 1)],
        thresholds,
        1,
        ReclusterMode::Final,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 1);
    assert_eq!(task_part_counts(&parts), vec![2]);

    let (block_num, parts) = target_select_segments_by_level_with_mode(
        &[(3, 2), (4, 1)],
        thresholds,
        1,
        ReclusterMode::Final,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 3);
    assert_eq!(task_part_counts(&parts), vec![3]);

    // The mature bands are fixed and non-overlapping. A 2-3 overlap crosses
    // the 1-2 / 3-4 boundary by design, so it is not one rewrite task.
    let (block_num, parts) = target_select_segments_by_level_with_mode(
        &[(2, 1), (3, 1)],
        thresholds,
        1,
        ReclusterMode::Final,
    )
    .await?;

    assert_eq!(block_num, 0);
    assert!(parts.tasks.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_final_recluster_skips_high_level_two_block_batch() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) =
        target_select_segments_by_level_with_mode(&[(2, 2)], thresholds, 1, ReclusterMode::Final)
            .await?;

    assert_eq!(block_num, 0);
    assert!(parts.tasks.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_skips_max_level_blocks() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) =
        target_select_segments_by_level_with_mode(&[(32, 3)], thresholds, 1, ReclusterMode::Final)
            .await?;

    assert_eq!(block_num, 0);
    assert!(parts.tasks.is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_safety_for_recluster() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let operator = ctx.get_application_level_data_operator()?.operator();

    let recluster_block_size = 300;
    ctx.get_settings()
        .set_recluster_block_size(recluster_block_size as u64)?;

    let cluster_key_id = 0;
    let threshold = BlockThresholds::new(5, 1024, 100, 5);

    let data_accessor = operator.clone();
    let schema = TestFixture::default_table_schema();
    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);
        let max_tasks: usize = rand.gen_range(1..5);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);
        let mut rows_per_blocks = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..8));
            rows_per_blocks.push(rand.gen_range(1..8));
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

        let unclustered: bool = rand.r#gen();
        let (locations, _, segment_infos) = CompactSegmentTestFixture::gen_segments(
            ctx.clone(),
            block_number_of_segments,
            rows_per_blocks,
            threshold,
            Some(cluster_key_id),
            unclustered,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in &segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary, Some(cluster_key_id));
        }

        let mut block_ids = HashSet::new();
        for seg in &segment_infos {
            for b in &seg.blocks {
                block_ids.insert(b.location.clone());
            }
        }

        let ctx: Arc<dyn TableContext> = ctx.clone();
        let segment_locations = create_segment_location_vector(locations.clone(), None);
        let compact_segments = FuseTable::segment_pruning(
            &ctx,
            schema.clone(),
            data_accessor.clone(),
            &None,
            segment_locations,
        )
        .await?;

        let mut parts = ReclusterParts::default();
        let mutator = Arc::new(ReclusterMutator::new(
            ctx.clone(),
            data_accessor.clone(),
            schema.clone(),
            vec![test_cluster_key_expr()],
            1.0,
            threshold,
            cluster_key_id,
            max_tasks,
        ));
        let segment_windows = mutator.select_segments(&compact_segments, 8)?;
        // select the blocks with the highest depth.
        for selected_segs in segment_windows {
            let (_, candidate_parts) = mutator
                .target_select(selected_segs, ReclusterMode::Normal)
                .await?;
            if !candidate_parts.is_empty() {
                parts = candidate_parts;
                break;
            }
        }

        if !parts.is_empty() {
            eprintln!("need_recluster");
            let ReclusterParts {
                tasks,
                remained_blocks,
                removed_segment_indexes,
                ..
            } = parts;
            assert!(tasks.len() <= max_tasks);
            eprintln!("tasks_num: {}, max_tasks: {}", tasks.len(), max_tasks);
            let mut blocks = Vec::new();
            for task in tasks.into_iter() {
                let parts = task.parts.partitions;
                assert!(task.total_bytes <= recluster_block_size);
                for part in parts.into_iter() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    blocks.push(fuse_part.location.clone());
                }
            }

            eprintln!(
                "selected segments number {}, selected blocks number {}, remained blocks number {}",
                removed_segment_indexes.len(),
                blocks.len(),
                remained_blocks.len()
            );
            for remain in remained_blocks {
                blocks.push(remain.0.location.0.clone());
            }

            let block_ids_after_target = HashSet::from_iter(blocks.into_iter());

            let mut origin_blocks_ids = HashSet::new();
            for idx in &removed_segment_indexes {
                for b in &segment_infos[*idx].blocks {
                    origin_blocks_ids.insert(b.location.0.clone());
                }
            }
            assert_eq!(block_ids_after_target, origin_blocks_ids);
        }
    }

    Ok(())
}
