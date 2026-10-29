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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Engine;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReadPartitionsPruningMode;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeLimitFilter;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_meta_app::schema::CreateOption;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_sql::parse_to_filters;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::pruning::FusePruner;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::QueryPipelineExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextPartitionStats;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextRuntimeFilter;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_query::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use opendal::Operator;

async fn apply_snapshot_pruning(
    table_snapshot: Arc<TableSnapshot>,
    schema: TableSchemaRef,
    push_down: &Option<PushDownInfo>,
    ctx: Arc<QueryContext>,
    op: Operator,
    bloom_index_cols: BloomIndexColumns,
    fuse_table: &FuseTable,
    cache_key: Option<String>,
    plan_id: u32,
) -> Result<(Vec<PartInfoPtr>, PartStatistics)> {
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locs = table_snapshot.segments.clone();
    let segment_locs = create_segment_location_vector(segment_locs, None);
    let fuse_pruner = Arc::new(FusePruner::create(
        &table_ctx,
        op,
        schema,
        push_down,
        None,
        bloom_index_cols,
        vec![],
        HashSet::new(),
        None,
    )?);

    let mut prune_pipeline = Pipeline::create();
    let (segment_tx, segment_rx) = async_channel::bounded(8);
    let (res_tx, res_rx) = async_channel::unbounded();
    fuse_table.prune_segments_with_pipeline(
        fuse_pruner.clone(),
        &mut prune_pipeline,
        ctx.clone(),
        0,
        segment_rx,
        res_tx,
        cache_key,
        segment_locs.len(),
        plan_id,
    )?;
    prune_pipeline.set_max_threads(1);
    prune_pipeline.set_on_init(move || {
        // We cannot use the runtime associated with the query to avoid increasing its lifetime.
        GlobalIORuntime::instance().spawn(async move {
            // avoid block global io runtime
            let runtime = Runtime::with_worker_threads(2, None)?;
            let join_handler = runtime.spawn(async move {
                for segment in segment_locs {
                    let _ = segment_tx.send(segment).await;
                }
                Ok::<_, ErrorCode>(())
            });
            join_handler
                .await
                .unwrap()
                .expect("Join error while in prune pipeline");
            Ok::<_, ErrorCode>(())
        });
        Ok(())
    });

    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_queries_executor: false,
        max_threads: 8,
        executor_node_id: "".to_string(),
        perf_event_groups: vec![],
    };
    let executor = QueryPipelineExecutor::create(prune_pipeline, settings)?;

    executor.execute()?;
    let mut got = Vec::new();

    while let Ok(Ok(segment)) = res_rx.recv().await {
        got.push(segment);
    }

    let stats = ctx
        .get_pruned_partitions_stats()
        .get(&plan_id)
        .cloned()
        .ok_or_else(|| ErrorCode::Internal("missing pruning statistics for test"))?;

    Ok((got, stats))
}

/// Creates `test_tbl_name` with columns `a`, `b` and appends `num_blocks`
/// blocks of `row_per_block` rows, one block per segment. Column `a` is
/// always 1; column `b` equals the block index.
async fn create_test_table(
    fixture: &TestFixture,
    ctx: Arc<QueryContext>,
    test_tbl_name: &str,
    num_blocks: usize,
    row_per_block: usize,
) -> Result<Arc<dyn Table>> {
    let test_schema = TableSchemaRefExt::create(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", TableDataType::Number(NumberDataType::UInt64)),
    ]);

    let num_blocks_opt = row_per_block.to_string();

    // create test table
    let create_table_plan = CreateTablePlan {
        catalog: "default".to_owned(),
        create_option: CreateOption::Create,
        tenant: fixture.default_tenant(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        schema: test_schema.clone(),
        engine: Engine::Fuse,
        engine_options: Default::default(),
        storage_params: None,
        options: [
            (FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
            (FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_comments: vec![],
        field_stats_truncate_len: vec![],
        as_select: None,
        cluster_key: None,
        ttl: None,
        table_indexes: None,
        table_constraints: None,
        clone: None,
        attached_columns: None,
        table_partition: None,
        table_properties: None,
        index_user_dictionaries: None,
    };

    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // get table
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let gen_col =
        |value, rows| UInt64Type::from_data(std::iter::repeat_n(value, rows).collect::<Vec<u64>>());

    // prepare test blocks
    // - there will be `num_blocks` blocks, for each block, it comprises of `row_per_block` rows,
    //    in our case, there will be 10 blocks, and 10 rows for each block
    let blocks = (0..num_blocks)
        .map(|idx| {
            DataBlock::new_from_columns(vec![
                // value of column a always equals  1
                gen_col(1, row_per_block),
                // for column b
                // - for all block `B` in blocks, whose index is `i`
                // - for all row in `B`, value of column b  equals `i`
                gen_col(idx as u64, row_per_block),
            ])
        })
        .collect::<Vec<_>>();

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // get the latest tbl
    catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_pruner() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;

    let num_blocks = 10;
    let row_per_block = 10;
    let table = create_test_table(
        &fixture,
        ctx.clone(),
        "test_index_helper",
        num_blocks,
        row_per_block,
    )
    .await?;

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();

    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());

    let load_params = LoadParams {
        location: snapshot_loc.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
        put_cache: false,
    };

    let snapshot = reader.read(&load_params).await?;

    // nothing is pruned
    let e1 = PushDownInfo {
        filters: Some(parse_to_filters(ctx.clone(), table.clone(), "a > 3")?),
        ..Default::default()
    };

    // some blocks pruned
    let mut e2 = PushDownInfo::default();
    let max_val_of_b = 6u64;

    e2.filters = Some(parse_to_filters(
        ctx.clone(),
        table.clone(),
        "a > 0 and b > 6",
    )?);
    let deterministic_filter = PushDownInfo {
        is_deterministic: true,
        ..e2.clone()
    };
    let b2 = num_blocks - max_val_of_b as usize - 1;

    // Sort asc Limit: TopN-pruner.
    let e3 = PushDownInfo {
        order_by: vec![(
            RemoteExpr::ColumnRef {
                span: None,
                id: "b".to_string(),
                data_type: Int64Type::data_type(),
                display_name: "b".to_string(),
            },
            true,
            false,
        )],
        limit: Some(3),
        ..Default::default()
    };

    // Sort desc Limit: TopN-pruner.
    let e4 = PushDownInfo {
        order_by: vec![(
            RemoteExpr::ColumnRef {
                span: None,
                id: "b".to_string(),
                data_type: Int64Type::data_type(),
                display_name: "b".to_string(),
            },
            false,
            false,
        )],
        limit: Some(4),
        ..Default::default()
    };

    // Limit push-down, Limit-pruner.
    let e5 = PushDownInfo {
        order_by: vec![],
        limit: Some(11),
        ..Default::default()
    };

    let extras = vec![
        (None, num_blocks, num_blocks * row_per_block),
        (Some(e1), 0, 0),
        (Some(e2), b2, b2 * row_per_block),
        // ORDER BY + LIMIT without an inverted-index filter is not pruned
        // statically; the runtime TopN filter prunes blocks during the scan.
        (Some(e3), num_blocks, num_blocks * row_per_block),
        // Desc variant follows the same rule.
        (Some(e4), num_blocks, num_blocks * row_per_block),
        (Some(e5), 2, 2 * row_per_block),
    ];

    let stats_res = vec![
        (10, 10, 10, 10),
        (10, 0, 0, 0),
        (10, 3, 3, 3),
        // No static TopN pruning: range-pruning counters stay at 10.
        (10, 10, 10, 10),
        (10, 10, 10, 10),
        (10, 10, 10, 2),
    ];

    for (id, (extra, expected_blocks, expected_rows)) in extras.into_iter().enumerate() {
        let cache_key = Some(format!("test_block_pruner_{}", id));
        let (parts, stats) = apply_snapshot_pruning(
            snapshot.clone(),
            table.get_table_info().schema(),
            &extra,
            ctx.clone(),
            fuse_table.get_operator(),
            fuse_table.bloom_index_cols(),
            fuse_table,
            cache_key.clone(),
            id as u32,
        )
        .await?;
        let rows = parts
            .iter()
            .map(|b| {
                b.as_any()
                    .downcast_ref::<FuseBlockPartInfo>()
                    .unwrap()
                    .nums_rows
            })
            .sum::<usize>();

        assert_eq!(expected_rows, rows);
        assert_eq!(expected_blocks, parts.len());

        check_stats(stats, &stats_res, id)?;
        let (stats, partitions) = FuseTable::check_prune_cache(&cache_key).unwrap();
        assert_eq!(stats.pruning_stats, Default::default());
        assert_eq!(expected_blocks, partitions.partitions.len());
    }

    let segment_locs = create_segment_location_vector(snapshot.segments.clone(), None);
    let normal_stats = fuse_table
        .prune_snapshot_blocks(
            ctx.clone(),
            Some(deterministic_filter.clone()),
            table.get_table_info().schema(),
            segment_locs.clone(),
            snapshot.summary.block_count as usize,
            ReadPartitionsPruningMode::Normal,
            None,
        )
        .await?
        .0;
    assert_eq!(0, normal_stats.pruning_stats.segments_read_cost);
    assert_eq!(0, normal_stats.pruning_stats.segments_decompress_cost);

    let explain_ctx = fixture.new_query_ctx().await?;
    explain_ctx.attach_query_str(QueryKind::Explain, "explain".to_string());
    let explain_stats = fuse_table
        .prune_snapshot_blocks(
            explain_ctx,
            Some(deterministic_filter),
            table.get_table_info().schema(),
            segment_locs,
            snapshot.summary.block_count as usize,
            ReadPartitionsPruningMode::Normal,
            None,
        )
        .await?
        .0;
    assert!(explain_stats.pruning_stats.segments_read_cost > 0);
    assert!(explain_stats.pruning_stats.segments_decompress_cost > 0);

    Ok(())
}

/// Runs a lazy prune pipeline built by `build_prune_pipeline` and returns the
/// parts it produced.
async fn execute_lazy_prune_pipeline(
    ctx: Arc<QueryContext>,
    fuse_table: &FuseTable,
    prune_pipeline: Pipeline,
) -> Result<Vec<PartInfoPtr>> {
    let rx = fuse_table.pruned_result_receiver.lock().clone().unwrap();
    let executor =
        QueryPipelineExecutor::create(prune_pipeline, ExecutorSettings::try_create(ctx)?)?;
    executor.execute()?;
    let mut parts = Vec::new();
    while let Ok(Ok(part)) = rx.recv().await {
        parts.push(part);
    }
    Ok(parts)
}

fn block_locations(parts: &[PartInfoPtr]) -> Vec<String> {
    let mut locations = parts
        .iter()
        .map(|part| FuseBlockPartInfo::from_part(part).unwrap().location.clone())
        .collect::<Vec<_>>();
    locations.sort();
    locations
}

/// Lazy (distributed) pruning runs the prune pipeline inside the query
/// executor. It must produce the same partitions as plan-time pruning, and its
/// parallelism is bounded by the number of segments to prune, not by
/// `max_threads` / `max_storage_io_requests`.
#[tokio::test(flavor = "multi_thread")]
async fn test_lazy_prune_pipeline_over_segments() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;

    let settings = ctx.get_settings();
    settings.set_max_threads(8)?;
    settings.set_max_storage_io_requests(8)?;
    // Both plans below prune the same segments with the same filter; disable
    // the prune cache so the second one does not just replay the first.
    settings.set_setting("enable_prune_cache".to_string(), "0".to_string())?;

    // 3 segments with a single block each: strictly fewer than `max_threads`.
    let num_blocks = 3;
    let row_per_block = 10;
    let table = create_test_table(
        &fixture,
        ctx.clone(),
        "test_lazy_prune_pipeline",
        num_blocks,
        row_per_block,
    )
    .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    // prunes the block whose `b` is 0
    let push_downs = Some(PushDownInfo {
        filters: Some(parse_to_filters(ctx.clone(), table.clone(), "b > 0")?),
        ..Default::default()
    });

    // Lazy plan: the partitions are the segments, pruned at execution time.
    settings.set_setting("enable_distributed_pruning".to_string(), "1".to_string())?;
    let lazy_plan = table
        .read_plan(ctx.clone(), push_downs.clone(), None, false, false)
        .await?;
    assert!(matches!(
        lazy_plan.parts.partitions_type(),
        PartInfoType::LazyLevel
    ));
    assert_eq!(lazy_plan.parts.len(), num_blocks);

    let mut source_pipeline = Pipeline::create();
    let prune_pipeline = table
        .build_prune_pipeline(ctx.clone(), &lazy_plan, &mut source_pipeline, 0)?
        .expect("lazy partitions must build a prune pipeline");
    assert!(
        prune_pipeline.get_max_threads() <= num_blocks,
        "prune pipeline width {} must not exceed the {} segments to prune",
        prune_pipeline.get_max_threads(),
        num_blocks
    );
    let lazy_parts = execute_lazy_prune_pipeline(ctx.clone(), fuse_table, prune_pipeline).await?;

    // Plan-time pruning of the same table with the same filter.
    settings.set_setting("enable_distributed_pruning".to_string(), "0".to_string())?;
    let eager_plan = table
        .read_plan(ctx.clone(), push_downs, None, false, false)
        .await?;
    assert!(matches!(
        eager_plan.parts.partitions_type(),
        PartInfoType::BlockLevel
    ));

    assert_eq!(lazy_parts.len(), num_blocks - 1);
    assert_eq!(
        block_locations(&lazy_parts),
        block_locations(&eager_plan.parts.partitions)
    );

    Ok(())
}

/// Builds the lazy prune pipeline of `plan`. `None` means the pruning result
/// is replayed from the prune cache instead of being pruned again.
fn build_lazy_prune_pipeline(
    ctx: Arc<QueryContext>,
    table: &Arc<dyn Table>,
    plan: &DataSourcePlan,
    plan_id: u32,
) -> Result<Option<Pipeline>> {
    let mut source_pipeline = Pipeline::create();
    table.build_prune_pipeline(ctx, plan, &mut source_pipeline, plan_id)
}

/// A runtime `LIMIT` scan filter that never fires must not disable the prune
/// result cache: `SELECT ... WHERE <selective> LIMIT n` should replay the
/// cached pruning result on re-execution exactly like the query without
/// `LIMIT`. When the limit does fire and cuts pruning short, the partial
/// result must not be cached.
#[tokio::test(flavor = "multi_thread")]
async fn test_lazy_prune_cache_with_runtime_limit_filter() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;

    let enable_lazy_prune_cache = |ctx: &Arc<QueryContext>| -> Result<()> {
        let settings = ctx.get_settings();
        settings.set_max_threads(8)?;
        settings.set_max_storage_io_requests(8)?;
        settings.set_setting("enable_prune_cache".to_string(), "1".to_string())?;
        settings.set_setting("enable_distributed_pruning".to_string(), "1".to_string())
    };
    enable_lazy_prune_cache(&ctx)?;

    let num_blocks = 4;
    let row_per_block = 10;
    let table = create_test_table(
        &fixture,
        ctx.clone(),
        "test_lazy_prune_cache_runtime_limit",
        num_blocks,
        row_per_block,
    )
    .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    // Case 1: a registered, never-fired LIMIT filter (the limit is far above
    // the rows that survive pruning). The result is complete and must be cached.
    // `b = 2` keeps exactly one block; `is_deterministic` makes it cacheable.
    let push_downs = Some(PushDownInfo {
        filters: Some(parse_to_filters(ctx.clone(), table.clone(), "b = 2")?),
        limit: Some(100),
        is_deterministic: true,
        ..Default::default()
    });
    let plan = table
        .read_plan(ctx.clone(), push_downs, None, false, false)
        .await?;
    assert!(matches!(
        plan.parts.partitions_type(),
        PartInfoType::LazyLevel
    ));
    ctx.register_runtime_scan_filter(plan.scan_id, Arc::new(RuntimeLimitFilter::new()));

    let prune_pipeline = build_lazy_prune_pipeline(ctx.clone(), &table, &plan, 0)?
        .expect("nothing is cached yet, the first run must prune");
    let parts = execute_lazy_prune_pipeline(ctx.clone(), fuse_table, prune_pipeline).await?;
    assert_eq!(parts.len(), 1);

    assert!(
        build_lazy_prune_pipeline(ctx.clone(), &table, &plan, 1)?.is_none(),
        "complete pruning result under an idle LIMIT filter must be cached"
    );
    let cached_stats = ctx
        .get_pruned_partitions_stats()
        .get(&1)
        .cloned()
        .expect("a cache hit publishes the cached statistics");
    assert_eq!(cached_stats.partitions_scanned, 1);
    assert_eq!(cached_stats.read_rows, row_per_block);

    // Case 2: a LIMIT filter that has already fired: pruning stops early, the
    // result is partial and must not be cached.
    let ctx2 = fixture.new_query_ctx().await?;
    enable_lazy_prune_cache(&ctx2)?;
    let push_downs = Some(PushDownInfo {
        filters: Some(parse_to_filters(ctx2.clone(), table.clone(), "b >= 0")?),
        limit: Some(1),
        is_deterministic: true,
        ..Default::default()
    });
    let plan2 = table
        .read_plan(ctx2.clone(), push_downs, None, false, false)
        .await?;
    assert!(matches!(
        plan2.parts.partitions_type(),
        PartInfoType::LazyLevel
    ));
    let fired = Arc::new(RuntimeLimitFilter::new());
    fired.finish();
    ctx2.register_runtime_scan_filter(plan2.scan_id, fired);

    let prune_pipeline = build_lazy_prune_pipeline(ctx2.clone(), &table, &plan2, 0)?
        .expect("nothing is cached yet, the first run must prune");
    let parts = execute_lazy_prune_pipeline(ctx2.clone(), fuse_table, prune_pipeline).await?;
    assert!(parts.is_empty(), "a fired LIMIT filter drops every block");
    assert!(
        build_lazy_prune_pipeline(ctx2.clone(), &table, &plan2, 1)?.is_some(),
        "a pruning result cut short by a runtime LIMIT filter must not be cached"
    );

    Ok(())
}

fn check_stats(
    stats: PartStatistics,
    stats_res: &[(usize, usize, usize, usize)],
    id: usize,
) -> Result<()> {
    let (segments_before, segment_after, block_before, block_after) = stats_res[id];
    let prune_stats = stats.pruning_stats;
    assert_eq!(prune_stats.segments_range_pruning_before, segments_before);
    assert_eq!(prune_stats.segments_range_pruning_after, segment_after);
    assert_eq!(prune_stats.blocks_range_pruning_before, block_before);
    assert_eq!(prune_stats.blocks_range_pruning_after, block_after);
    Ok(())
}
