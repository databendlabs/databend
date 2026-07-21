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
use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VectorDataType;
use databend_common_sql::parse_to_filters;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ReclusterFinalCarry;
use databend_common_storages_fuse::operations::ReclusterMode;
use databend_common_storages_fuse::operations::ReclusterMutator;
use databend_common_storages_fuse::operations::SelectedReclusterSegment;
use databend_common_storages_fuse::pruning::SegmentLocation;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::statistics::VectorClusterInfo;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use databend_storages_common_table_meta::meta::Versioned;
use futures::TryStreamExt;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::Semaphore;
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

fn test_cluster_key_exprs() -> Vec<Expr<usize>> {
    vec![test_cluster_key_expr()]
}

fn test_cluster_schema() -> TableSchemaRef {
    TestFixture::default_table_schema()
}

fn new_test_mutator(
    ctx: Arc<dyn TableContext>,
    data_accessor: opendal::Operator,
    schema: TableSchemaRef,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
    max_tasks: usize,
    mode: ReclusterMode,
) -> ReclusterMutator {
    ReclusterMutator::new(
        ctx,
        data_accessor,
        schema,
        test_cluster_key_exprs(),
        1.0,
        thresholds,
        cluster_key_id,
        max_tasks,
        mode,
        None,
    )
}

async fn segment_pruning(
    ctx: &Arc<dyn TableContext>,
    schema: TableSchemaRef,
    data_accessor: opendal::Operator,
    segment_locations: Vec<SegmentLocation>,
) -> anyhow::Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
    let (pruning_ctx, segment_pruner, max_concurrency) =
        FuseTable::create_recluster_segment_pruner(ctx, schema, data_accessor, &None)?;
    Ok(FuseTable::segment_pruning(
        pruning_ctx,
        segment_pruner,
        max_concurrency,
        segment_locations,
    )
    .await?)
}

fn make_recluster_block(
    cluster_key_id: u32,
    min: i32,
    max: i32,
    level: i32,
    row_count: u64,
    block_size: u64,
    file_size: u64,
) -> Arc<BlockMeta> {
    let block_id = Uuid::new_v4().simple().to_string();
    Arc::new(BlockMeta::new(
        row_count,
        block_size,
        file_size,
        HashMap::default(),
        HashMap::default(),
        Some(ClusterStatistics::new(
            cluster_key_id,
            vec![Scalar::from(min)],
            vec![Scalar::from(max)],
            level,
        )),
        (block_id, DataBlock::VERSION),
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
    ))
}

async fn write_recluster_segment(
    data_accessor: &opendal::Operator,
    location_generator: &TableMetaLocationGenerator,
    blocks: Vec<Arc<BlockMeta>>,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
) -> anyhow::Result<meta::Location> {
    let block_refs = blocks
        .iter()
        .map(|block| block.as_ref())
        .collect::<Vec<_>>();
    let statistics = reduce_block_metas(&block_refs, thresholds, Some(cluster_key_id));
    let segment = SegmentInfo::new(blocks, statistics);
    let segment_location = location_generator
        .gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
    segment.write_meta(data_accessor, &segment_location).await?;
    Ok((segment_location, SegmentInfo::VERSION))
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
        let blocks = (0..blocks_per_segment)
            .map(|_| {
                make_recluster_block(cluster_key_id, 1, 100, 0, row_count, block_size, file_size)
            })
            .collect::<Vec<_>>();
        segment_locations.push(
            write_recluster_segment(
                data_accessor,
                location_generator,
                blocks,
                thresholds,
                cluster_key_id,
            )
            .await?,
        );
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
            let block = make_recluster_block(
                cluster_key_id,
                1,
                100,
                level,
                row_count,
                block_size,
                file_size,
            );
            segment_locations.push(
                write_recluster_segment(
                    data_accessor,
                    location_generator,
                    vec![block],
                    thresholds,
                    cluster_key_id,
                )
                .await?,
            );
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
        let blocks = ranges
            .iter()
            .map(|&(min, max)| {
                make_recluster_block(
                    cluster_key_id,
                    min,
                    max,
                    0,
                    row_count,
                    block_size,
                    file_size,
                )
            })
            .collect::<Vec<_>>();
        segment_locations.push(
            write_recluster_segment(
                data_accessor,
                location_generator,
                blocks,
                thresholds,
                cluster_key_id,
            )
            .await?,
        );
    }
    Ok(segment_locations)
}

fn test_vector_cluster_key_expr() -> Expr<usize> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        data_type: DataType::Vector(VectorDataType::Float32(2)),
        id: 1,
        display_name: "embedding".to_string(),
    })
}

fn vector_column_stats(centroid: [f32; 2], radius: f32) -> VectorColumnStatistics {
    VectorColumnStatistics {
        centroid: centroid.into_iter().map(F32::from).collect(),
        radius: F32::from(radius),
        row_count: 1,
    }
}

fn vector_recluster_schema() -> TableSchemaRef {
    TableSchemaRef::new(TableSchema::new_from_column_ids(
        vec![
            TableField::new_from_column_id(
                "tenant",
                TableDataType::Number(NumberDataType::Int32),
                0,
            ),
            TableField::new_from_column_id(
                "embedding",
                TableDataType::Vector(VectorDataType::Float32(2)),
                1,
            ),
        ],
        Default::default(),
        2,
    ))
}

type VectorBlockStatsSpec = (i32, i32, [f32; 2], f32);
type VectorSegmentStatsSpec = Vec<VectorBlockStatsSpec>;

async fn gen_recluster_segments_by_vector_stats(
    data_accessor: &opendal::Operator,
    location_generator: &TableMetaLocationGenerator,
    blocks_by_segment: &[VectorSegmentStatsSpec],
    scalar_cluster_stats: bool,
    row_count: u64,
    block_size: u64,
    file_size: u64,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
) -> anyhow::Result<Vec<meta::Location>> {
    let mut segment_locations = Vec::with_capacity(blocks_by_segment.len());
    for blocks_spec in blocks_by_segment {
        let mut blocks = Vec::with_capacity(blocks_spec.len());
        for &(tenant_min, tenant_max, centroid, radius) in blocks_spec {
            let block_id = Uuid::new_v4().simple().to_string();
            let location = (block_id, DataBlock::VERSION);
            let mut col_stats = HashMap::new();
            col_stats.insert(
                0,
                ColumnStatistics::new(
                    Scalar::from(tenant_min),
                    Scalar::from(tenant_max),
                    0,
                    4,
                    Some(1),
                ),
            );

            let mut vector_stats = HashMap::new();
            vector_stats.insert(
                (1, VectorDistanceType::L2),
                vector_column_stats(centroid, radius),
            );

            let mut block = BlockMeta::new(
                row_count,
                block_size,
                file_size,
                col_stats,
                HashMap::default(),
                Some(ClusterStatistics::new(
                    cluster_key_id,
                    if scalar_cluster_stats {
                        vec![Scalar::from(tenant_min)]
                    } else {
                        vec![]
                    },
                    if scalar_cluster_stats {
                        vec![Scalar::from(tenant_max)]
                    } else {
                        vec![]
                    },
                    0,
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
            );
            block.vector_stats = Some(vector_stats);
            blocks.push(Arc::new(block));
        }

        segment_locations.push(
            write_recluster_segment(
                data_accessor,
                location_generator,
                blocks,
                thresholds,
                cluster_key_id,
            )
            .await?,
        );
    }
    Ok(segment_locations)
}

async fn materialize_segments_by_level_with_mode(
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
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
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

async fn materialize_segment_locations_with_mode(
    ctx: Arc<dyn TableContext>,
    data_accessor: opendal::Operator,
    segment_locations: Vec<meta::Location>,
    thresholds: BlockThresholds,
    cluster_key_id: u32,
    max_tasks: usize,
    max_segments: usize,
    mode: ReclusterMode,
) -> anyhow::Result<(usize, u64, ReclusterParts)> {
    let schema = test_cluster_schema();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;

    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        max_tasks,
        mode,
    );

    let segment_windows = mutator.select_segments(&compact_segments, max_segments)?;
    let mut selected_segments = 0;
    let mut block_num = 0;
    let mut parts = ReclusterParts::default();
    for selected_segs in segment_windows {
        selected_segments = selected_segs.len();
        let (candidate_block_num, candidate_parts) =
            materialize_candidate_window(&mutator, selected_segs, max_tasks).await?;
        if !candidate_parts.is_empty() {
            block_num = candidate_block_num;
            parts = candidate_parts;
            break;
        }
    }
    Ok((selected_segments, block_num, parts))
}

async fn materialize_candidate_window(
    mutator: &ReclusterMutator,
    selected_segs: Vec<SelectedReclusterSegment>,
    task_budget: usize,
) -> anyhow::Result<(u64, ReclusterParts)> {
    let segment_indexes = selected_segs
        .iter()
        .map(|segment| (segment.loc.location.clone(), segment.loc.segment_idx))
        .collect::<HashMap<_, _>>();
    let decode_runtime = Arc::new(Runtime::with_worker_threads(
        2,
        Some("recluster-block-meta-test-worker".to_owned()),
    )?);
    let decode_semaphore = Arc::new(Semaphore::new(4));
    let window = mutator
        .probe_candidate_window(selected_segs, task_budget, decode_runtime, decode_semaphore)
        .await?;
    if window.task_count() == 0 {
        return Ok((0, ReclusterParts::default()));
    }

    let live_segments = segment_indexes
        .iter()
        .map(|(location, segment_idx)| (location, *segment_idx))
        .collect::<HashMap<_, _>>();
    let mut selected = (0..window.task_count())
        .map(|task_idx| (task_idx, window.task_score(task_idx)))
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| right.1.cmp_desc(&left.1).then_with(|| left.0.cmp(&right.0)));
    let selected = selected
        .into_iter()
        .take(task_budget)
        .map(|(task_idx, _)| task_idx)
        .collect::<Vec<_>>();

    Ok(mutator
        .materialize_task_candidates(&live_segments, vec![(window, selected)])
        .await?)
}

fn task_part_counts(parts: &ReclusterParts) -> Vec<usize> {
    parts
        .tasks
        .iter()
        .map(|task| task.parts.len())
        .collect::<Vec<_>>()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_limit_skips_empty_range() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(1)?;

    let mut create_table_plan = fixture.default_create_table_plan();
    create_table_plan
        .options
        .insert(FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(), "2".to_owned());
    create_table_plan
        .options
        .insert(FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned());
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // Newer append segments are prepended to the snapshot. Write matching data
    // first, then write 32 pruned segments so LIMIT scans an empty range first.
    let table = fixture.latest_default_table().await?;
    let blocks = TestFixture::gen_sample_blocks_stream_ex(4, 2, 100)
        .try_collect()
        .await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let table = fixture.latest_default_table().await?;
    let blocks = TestFixture::gen_sample_blocks_stream_ex(32, 2, 0)
        .try_collect()
        .await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    assert_eq!(
        fuse_table
            .read_table_snapshot()
            .await?
            .unwrap()
            .segments
            .len(),
        36
    );

    let push_downs = PushDownInfo {
        filters: Some(parse_to_filters(ctx.clone(), table.clone(), "id > 90")?),
        ..Default::default()
    };
    let mut carry = ReclusterFinalCarry::default();
    let (parts, _) = fuse_table
        .do_recluster(
            ctx.clone(),
            Some(push_downs),
            Some(2),
            ReclusterMode::Conservative,
            &mut carry,
        )
        .await?
        .expect("recluster should read the later matching scan range");

    assert!(!parts.is_empty());
    assert!(!parts.tasks.is_empty());
    assert!(
        parts
            .removed_segment_indexes
            .iter()
            .all(|segment_idx| *segment_idx >= 32)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_removed_segments_match_selected_blocks() -> anyhow::Result<()> {
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
    let (selected_segments, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        1000,
        ReclusterMode::Conservative,
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
async fn test_compacts_small_overlaps() -> anyhow::Result<()> {
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
    let (_, _, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        8,
        ReclusterMode::Conservative,
    )
    .await?;
    assert!(!parts.is_empty());
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repack_window_without_rewrite() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());

    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(1, 2)], vec![(3, 4)], vec![(5, 6)]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        create_segment_location_vector(segment_locations.clone(), None),
    )
    .await?;
    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Conservative,
    );
    let selected_segs = mutator
        .select_segments(&compact_segments, 8)?
        .into_iter()
        .next()
        .expect("repack-only window should be selected");
    let decode_runtime = Arc::new(Runtime::with_worker_threads(
        2,
        Some("recluster-block-meta-test-worker".to_owned()),
    )?);
    let decode_semaphore = Arc::new(Semaphore::new(4));
    let window = mutator
        .probe_candidate_window(selected_segs, 1, decode_runtime, decode_semaphore)
        .await?;
    assert_eq!(window.task_count(), 1);
    let score = window.task_score(0);
    assert_eq!(score.selected_total_bytes, 0);
    assert_eq!(score.max_depth, 0);
    assert_eq!(score.average_depth, 0.0);

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        8,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert!(parts.tasks.is_empty());
    assert_eq!(parts.remained_blocks.len(), 3);
    assert_eq!(parts.removed_segment_indexes.len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_conservative_repack_unordered_window_without_rewrite() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 2);
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(3, 4), (1, 2)]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        create_segment_location_vector(segment_locations.clone(), None),
    )
    .await?;

    let aggressive_mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Aggressive,
    );
    let conservative_mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Conservative,
    );

    let selected_segs = aggressive_mutator
        .select_segments(&compact_segments, 2)?
        .into_iter()
        .next()
        .expect("unordered single-segment window should be selected");
    let decode_runtime = Arc::new(Runtime::with_worker_threads(
        2,
        Some("recluster-block-meta-test-worker".to_owned()),
    )?);
    let decode_semaphore = Arc::new(Semaphore::new(4));
    let aggressive_window = aggressive_mutator
        .probe_candidate_window(
            selected_segs.clone(),
            1,
            decode_runtime.clone(),
            decode_semaphore.clone(),
        )
        .await?;
    assert_eq!(aggressive_window.task_count(), 0);

    let conservative_window = conservative_mutator
        .probe_candidate_window(selected_segs, 1, decode_runtime, decode_semaphore)
        .await?;
    assert_eq!(conservative_window.task_count(), 1);
    let score = conservative_window.task_score(0);
    assert_eq!(score.selected_total_bytes, 0);
    assert_eq!(score.max_depth, 0);
    assert_eq!(score.average_depth, 0.0);

    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        1,
        2,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert!(parts.tasks.is_empty());
    assert_eq!(parts.remained_blocks.len(), 2);
    assert_eq!(parts.removed_segment_indexes.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_segments_covers_candidates() -> anyhow::Result<()> {
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

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;

    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Aggressive,
    );

    let segment_windows = mutator.select_segments(&compact_segments, 3)?;
    let window_index_sets = segment_windows
        .iter()
        .map(|window| {
            window
                .iter()
                .map(|segment| segment.loc.segment_idx)
                .collect::<HashSet<_>>()
        })
        .collect::<Vec<_>>();
    // Windows are segment-disjoint and together cover every candidate segment.
    // The trailing window is folded into the previous one, so a window may hold
    // more than the cap.
    assert_eq!(window_index_sets.len(), 2);
    let mut covered = HashSet::new();
    let mut total = 0;
    for window in &window_index_sets {
        total += window.len();
        covered.extend(window.iter().copied());
    }
    assert_eq!(total, covered.len());
    assert_eq!(covered.len(), 7);

    // `RECLUSTER LIMIT 1`: each window holds at most one segment (segments on
    // distinct cluster-key points are never grouped), so the limit is honored.
    let limit_one_windows = mutator.select_segments(&compact_segments, 1)?;
    for window in &limit_one_windows {
        assert_eq!(window.len(), 1);
    }

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
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;
    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Aggressive,
    );
    let segment_windows = mutator.select_segments(&compact_segments, 3)?;
    let window_sizes = segment_windows
        .iter()
        .map(|window| window.len())
        .collect::<Vec<_>>();
    // The trailing window is folded into the previous one, so all five nested
    // segments end up in a single window.
    assert_eq!(window_sizes, vec![5]);
    let selected = segment_windows
        .iter()
        .flat_map(|window| window.iter().map(|segment| segment.loc.segment_idx))
        .collect::<HashSet<_>>();
    assert_eq!(selected.len(), 5);

    // Four segments sharing the identical range [1, 2] start at the same sweep
    // point, so they are never split and stay in one window even with a cap of 2.
    let thresholds = BlockThresholds::new(1000, 100, 100, 2);
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(1, 2)], vec![(1, 2)], vec![(1, 2)], vec![(1, 2)]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;
    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Aggressive,
    );
    let segment_windows = mutator.select_segments(&compact_segments, 2)?;
    assert_eq!(segment_windows.len(), 1);
    assert_eq!(segment_windows[0].len(), 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_segments_normal_conservative() -> anyhow::Result<()> {
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
            vec![(1, 2)],
            vec![(1, 2)],
            vec![(1, 2)],
            vec![(1, 2)],
            vec![(10, 11)],
            vec![(10, 11)],
            vec![(10, 11)],
            vec![(20, 21)],
            vec![(20, 21)],
            vec![(30, 31)],
        ],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        create_segment_location_vector(segment_locations, None),
    )
    .await?;

    let aggressive_mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Aggressive,
    );
    let aggressive_windows = aggressive_mutator.select_segments(&compact_segments, 2)?;
    assert_eq!(
        aggressive_windows
            .iter()
            .map(|window| window.len())
            .collect::<Vec<_>>(),
        vec![4, 3, 3]
    );

    let conservative_mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Conservative,
    );
    let conservative_windows = conservative_mutator.select_segments(&compact_segments, 2)?;
    assert_eq!(
        conservative_windows
            .iter()
            .map(|window| window.len())
            .collect::<Vec<_>>(),
        vec![4]
    );

    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &TableMetaLocationGenerator::new("_prefix_disjoint".to_owned()),
        &[vec![(1, 2)], vec![(3, 4)], vec![(5, 6)]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let compact_segments = segment_pruning(
        &ctx,
        schema,
        data_accessor,
        create_segment_location_vector(segment_locations, None),
    )
    .await?;
    let conservative_windows = conservative_mutator.select_segments(&compact_segments, 2)?;
    assert_eq!(conservative_windows.len(), 1);
    assert_eq!(conservative_windows[0].len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_vector_mixed_key_overlap_selection() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 1);

    let segment_locations = gen_recluster_segments_by_vector_stats(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 1, [0.0, 0.0], 1.0)],
            vec![(1, 1, [0.5, 0.0], 1.0)],
            // Vector sphere overlaps with segment 0/1, but scalar range does not.
            vec![(2, 2, [0.5, 0.0], 1.0)],
            // Scalar range overlaps with segment 0/1, but vector sphere does not.
            vec![(1, 1, [10.0, 0.0], 1.0)],
        ],
        true,
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = vector_recluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        create_segment_location_vector(segment_locations, None),
    )
    .await?;

    let vector_cluster_info = VectorClusterInfo {
        key_index: 1,
        column_id: 1,
        column_name: "embedding".to_string(),
        dimension: 2,
        distance_type: VectorDistanceType::L2,
    };
    let mutator = ReclusterMutator::new(
        ctx,
        data_accessor,
        schema,
        vec![test_cluster_key_expr(), test_vector_cluster_key_expr()],
        1.0,
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Conservative,
        Some(vector_cluster_info),
    );

    let segment_windows = mutator.select_segments(&compact_segments, 8)?;
    let vector_window = segment_windows
        .into_iter()
        .find(|window| {
            window
                .iter()
                .map(|segment| segment.loc.segment_idx)
                .collect::<HashSet<_>>()
                == HashSet::from([0, 1])
        })
        .unwrap();
    let (block_num, parts) = materialize_candidate_window(&mutator, vector_window, 1).await?;
    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![2]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_vector_only_overlap_selection() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 1);

    let segment_locations = gen_recluster_segments_by_vector_stats(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 1, [0.0, 0.0], 1.0)],
            vec![(2, 2, [0.5, 0.0], 1.0)],
            vec![(3, 3, [10.0, 0.0], 1.0)],
        ],
        false,
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = vector_recluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        create_segment_location_vector(segment_locations, None),
    )
    .await?;

    let vector_cluster_info = VectorClusterInfo {
        key_index: 0,
        column_id: 1,
        column_name: "embedding".to_string(),
        dimension: 2,
        distance_type: VectorDistanceType::L2,
    };
    let mutator = ReclusterMutator::new(
        ctx,
        data_accessor,
        schema,
        vec![test_vector_cluster_key_expr()],
        1.0,
        thresholds,
        cluster_key_id,
        1,
        ReclusterMode::Conservative,
        Some(vector_cluster_info),
    );

    let segment_windows = mutator.select_segments(&compact_segments, 8)?;
    let vector_window = segment_windows
        .into_iter()
        .find(|window| {
            window
                .iter()
                .map(|segment| segment.loc.segment_idx)
                .collect::<HashSet<_>>()
                == HashSet::from([0, 1])
        })
        .unwrap();
    let (block_num, parts) = materialize_candidate_window(&mutator, vector_window, 1).await?;
    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![2]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_accumulates_tasks_across_windows() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 2);

    // Two overlapping clusters that are far apart in key space. With a window cap
    // of 2 they fall into two separate, segment-disjoint windows, so reaching
    // the budget of 2 requires accumulating tasks across both windows.
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[vec![(1, 10)], vec![(2, 9)], vec![(100, 110)], vec![(
            101, 109,
        )]],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;

    let max_tasks = 2;
    let select_mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        max_tasks,
        ReclusterMode::Aggressive,
    );
    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor.clone(),
        schema.clone(),
        thresholds,
        cluster_key_id,
        max_tasks,
        ReclusterMode::Conservative,
    );

    let segment_windows = select_mutator.select_segments(&compact_segments, 2)?;
    // The two far-apart clusters produce two disjoint windows.
    assert_eq!(segment_windows.len(), 2);

    // Disjoint windows can each materialize work, so a full budget may be filled
    // from multiple windows.
    let mut parts = ReclusterParts::default();
    for selected_segs in segment_windows {
        let task_budget = max_tasks.saturating_sub(parts.tasks.len());
        if task_budget == 0 {
            break;
        }
        let (_, candidate_parts) =
            materialize_candidate_window(&mutator, selected_segs, task_budget).await?;
        parts.tasks.extend(candidate_parts.tasks);
        parts
            .removed_segment_indexes
            .extend(candidate_parts.removed_segment_indexes);
    }

    // One task per window, accumulated to the full budget of 2.
    assert_eq!(parts.tasks.len(), 2);
    // The four overlapping segments are all scheduled for removal, with no
    // duplicates across windows.
    let removed = parts
        .removed_segment_indexes
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(removed.len(), 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_builds_multiple_peaks_in_one_window() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 100, 100, 1);

    // Peak A is the deepest. Its side expansion may absorb Peak B's remaining
    // block, but Peak C is independent and should still fill the second slot.
    let segment_locations = gen_recluster_segments_by_ranges(
        &data_accessor,
        &location_generator,
        &[
            vec![(1, 39)],
            vec![(2, 10)],
            vec![(3, 9)],
            vec![(20, 30)],
            vec![(50, 60)],
            vec![(51, 59)],
        ],
        1000,
        100,
        100,
        thresholds,
        cluster_key_id,
    )
    .await?;

    let schema = test_cluster_schema();
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let compact_segments = segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        segment_locations,
    )
    .await?;
    let mutator = new_test_mutator(
        ctx.clone(),
        data_accessor,
        schema,
        thresholds,
        cluster_key_id,
        2,
        ReclusterMode::Conservative,
    );

    let mut segment_windows = mutator.select_segments(&compact_segments, 1000)?;
    assert_eq!(segment_windows.len(), 1);
    let (_, parts) = materialize_candidate_window(&mutator, segment_windows.remove(0), 2).await?;

    assert_eq!(parts.tasks.len(), 2);

    let mut selected_locations = HashSet::new();
    let mut selected_count = 0;
    for task in &parts.tasks {
        for part in &task.parts.partitions {
            let fuse_part = FuseBlockPartInfo::from_part(part)?;
            selected_count += 1;
            assert!(selected_locations.insert(fuse_part.location.clone()));
        }
    }
    assert_eq!(selected_count, 6);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_splits_hotspot_by_memory() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(1000)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 10, 10, 1000);

    // 300 small blocks are 3000 bytes total. With a 1000-byte task memory
    // threshold they should be packed into three full hotspot tasks, even when
    // four workers are available.
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
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        4,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 300);
    assert_eq!(parts.tasks.len(), 3);
    assert_eq!(task_part_counts(&parts), vec![100, 100, 100]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repacks_when_min_task_over_memory() -> anyhow::Result<()> {
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
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert!(parts.tasks.is_empty());
    assert_eq!(parts.remained_blocks.len(), 3);
    assert_eq!(parts.removed_segment_indexes.len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_skips_singleton_over_memory() -> anyhow::Result<()> {
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
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![2]);
    assert_eq!(parts.tasks[0].total_bytes, 80);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keeps_full_hotspot_task() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(100)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 10, 100, 1000);

    let mut segment_locations = Vec::new();
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &[vec![(1, 39)], vec![(2, 39)]],
            1000,
            15,
            15,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );
    let hotspot_ranges = vec![vec![(40, 60)]; 4];
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &hotspot_ranges,
            1000,
            20,
            20,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &[vec![(61, 90)], vec![(61, 89)]],
            1000,
            15,
            15,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 5);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![5]);
    assert_eq!(parts.tasks[0].total_bytes, 95);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keeps_hotspot_plateau() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(100)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 10, 100, 1000);

    // Points 20, 30, and 40 all have the same max depth. The last block enters
    // the plateau after the first max point, so selecting only max_point would
    // leave it out once the first max-point task is already 80% full.
    let mut segment_locations = Vec::new();
    for (range, block_size) in [
        ((10, 20), 30),
        ((10, 40), 30),
        ((20, 40), 20),
        ((30, 50), 10),
    ] {
        segment_locations.extend(
            gen_recluster_segments_by_ranges(
                &data_accessor,
                &location_generator,
                &[vec![range]],
                1000,
                block_size,
                block_size,
                thresholds,
                cluster_key_id,
            )
            .await?,
        );
    }

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        2,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 4);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(task_part_counts(&parts), vec![4]);
    assert_eq!(parts.tasks[0].total_bytes, 90);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fills_hotspot_tail_from_sides() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_recluster_block_size(100)?;

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let location_generator = TableMetaLocationGenerator::new("_prefix".to_owned());
    let cluster_key_id = 0;
    let thresholds = BlockThresholds::new(1000, 10, 100, 1000);

    let mut segment_locations = Vec::new();
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &[vec![(1, 39)], vec![(2, 39)]],
            1000,
            15,
            15,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );
    let hotspot_ranges = vec![vec![(40, 60)]; 11];
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &hotspot_ranges,
            1000,
            20,
            20,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );
    segment_locations.extend(
        gen_recluster_segments_by_ranges(
            &data_accessor,
            &location_generator,
            &[vec![(61, 90)], vec![(61, 89)]],
            1000,
            15,
            15,
            thresholds,
            cluster_key_id,
        )
        .await?,
    );

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let (_, block_num, parts) = materialize_segment_locations_with_mode(
        ctx,
        data_accessor,
        segment_locations,
        thresholds,
        cluster_key_id,
        3,
        1000,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 15);
    assert_eq!(parts.tasks.len(), 3);
    assert_eq!(task_part_counts(&parts), vec![5, 5, 5]);
    assert_eq!(
        parts
            .tasks
            .iter()
            .map(|task| task.total_bytes)
            .collect::<Vec<_>>(),
        vec![100, 100, 80]
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_defers_after_empty_group() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(0, 1), (1, 3), (2, 10)],
        thresholds,
        1,
        ReclusterMode::Conservative,
    )
    .await?;

    // Empty groups must not consume the single defer slot. The first real small
    // group is deferred, so a later larger group can use the only task slot.
    assert_eq!(block_num, 10);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 2);
    assert_eq!(task_part_counts(&parts), vec![10]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keeps_low_level_small_task() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(0, 3)],
        thresholds,
        1,
        ReclusterMode::Conservative,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 0);
    assert_eq!(task_part_counts(&parts), vec![3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fills_budget_with_next_task() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(1, 3), (2, 4)],
        thresholds,
        2,
        ReclusterMode::Conservative,
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
async fn test_final_groups_mature_level_bands() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    // Levels 1 and 2 share the {1-3} bin; level 0 stays isolated, so only the
    // two mature blocks overlap and form one rewrite task.
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(0, 1), (1, 1), (2, 1)],
        thresholds,
        1,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert_eq!(parts.tasks.len(), 1);
    // {1-3} bin: majority tie between level 1 and 2 picks the lower level.
    assert_eq!(parts.tasks[0].level, 1);
    assert_eq!(task_part_counts(&parts), vec![2]);

    // Two high-level blocks alone do not produce rewrite tasks (both are at or
    // above MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS), but they can still be repacked
    // when that reduces segment count.
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(2, 2)],
        thresholds,
        1,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 2);
    assert!(parts.tasks.is_empty());
    assert_eq!(parts.remained_blocks.len(), 2);
    assert_eq!(parts.removed_segment_indexes.len(), 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_final_wide_bin_merges_mature_levels() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    // Levels 4 and 8 sit at the edges of the {4-8} bin; the fixed wide bin packs
    // them into a single task even though they are four levels apart.
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(4, 1), (8, 1), (8, 1)],
        thresholds,
        1,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert_eq!(parts.tasks.len(), 1);
    // Majority level in {4-8} is 8 (two blocks vs one at level 4).
    assert_eq!(parts.tasks[0].level, 8);
    assert_eq!(task_part_counts(&parts), vec![3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_final_high_maturity_bin_majority_level() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    // Levels 9 and above all fall in the {9+} bin and merge into one task; the
    // output level follows the majority, picking the smaller level on a tie.
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(9, 1), (10, 2), (12, 1)],
        thresholds,
        1,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 4);
    assert_eq!(parts.tasks.len(), 1);
    assert_eq!(parts.tasks[0].level, 10);
    assert_eq!(task_part_counts(&parts), vec![4]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_final_bin_boundary_is_a_hard_split() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    // Levels 3 and 4 fall in different bins ({1-3} vs {4-8}). The fixed bins do
    // not merge across that boundary, so the two level bands form two separate
    // tasks rather than one merged overlap.
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(3, 3), (4, 3)],
        thresholds,
        2,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 6);
    assert_eq!(parts.tasks.len(), 2);
    let mut levels = parts
        .tasks
        .iter()
        .map(|task| task.level)
        .collect::<Vec<_>>();
    levels.sort_unstable();
    assert_eq!(levels, vec![3, 4]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repacks_max_level_without_rewrite() -> anyhow::Result<()> {
    let thresholds = BlockThresholds::new(1000, 100, 100, 10);
    let (block_num, parts) = materialize_segments_by_level_with_mode(
        &[(32, 3)],
        thresholds,
        1,
        ReclusterMode::Aggressive,
    )
    .await?;

    assert_eq!(block_num, 3);
    assert!(parts.tasks.is_empty());
    assert_eq!(parts.remained_blocks.len(), 3);
    assert_eq!(parts.removed_segment_indexes.len(), 3);

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
    let schema = test_cluster_schema();
    let mut rand = StdRng::seed_from_u64(0x5eed);

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

        let ctx: Arc<dyn TableContext> = ctx.clone();
        let segment_locations = create_segment_location_vector(locations.clone(), None);
        let compact_segments = segment_pruning(
            &ctx,
            schema.clone(),
            data_accessor.clone(),
            segment_locations,
        )
        .await?;

        let mut parts = ReclusterParts::default();
        let select_mutator = new_test_mutator(
            ctx.clone(),
            data_accessor.clone(),
            schema.clone(),
            threshold,
            cluster_key_id,
            max_tasks,
            ReclusterMode::Aggressive,
        );
        let mutator = new_test_mutator(
            ctx.clone(),
            data_accessor.clone(),
            schema.clone(),
            threshold,
            cluster_key_id,
            max_tasks,
            ReclusterMode::Conservative,
        );
        let segment_windows = select_mutator.select_segments(&compact_segments, 8)?;
        // `select_segments` only skips large unclustered segments (level < 0 and
        // block_count >= block_per_segment). Every other candidate segment must
        // land in exactly one window, with no duplicates across windows.
        let expected_segments = compact_segments
            .iter()
            .filter(|(_, info)| {
                let level = info
                    .summary
                    .cluster_stats
                    .as_ref()
                    .filter(|stats| stats.cluster_key_id == cluster_key_id)
                    .map_or(0, |stats| stats.level);
                !(level < 0 && info.summary.block_count as usize >= threshold.block_per_segment)
            })
            .count();
        let total_windowed: usize = segment_windows.iter().map(|window| window.len()).sum();
        assert_eq!(expected_segments, total_windowed);

        // select the blocks with the highest depth.
        for selected_segs in segment_windows {
            let (_, candidate_parts) =
                materialize_candidate_window(&mutator, selected_segs, max_tasks).await?;
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

            let block_ids_after_recluster = HashSet::from_iter(blocks.into_iter());

            let mut origin_blocks_ids = HashSet::new();
            for idx in &removed_segment_indexes {
                for b in &segment_infos[*idx].blocks {
                    origin_blocks_ids.insert(b.location.0.clone());
                }
            }
            assert_eq!(block_ids_after_recluster, origin_blocks_ids);
        }
    }

    Ok(())
}
