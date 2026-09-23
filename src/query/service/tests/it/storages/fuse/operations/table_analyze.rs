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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::number::NumberScalar;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_pipeline::core::Pipeline;
use databend_common_statistics::Datum;
use databend_common_storage::MetaHLL12;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::AnalyzeHistogramInfo;
use databend_common_storages_fuse::operations::AnalyzeOptions;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;
use databend_query::sql::Planner;
use databend_query::sql::plans::Plan;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::testing::TableSnapshotStatisticsV3;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_table_modify_column_ndv_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // setup
    let create_tbl_command = "create table t(c int)";
    fixture.execute_command(create_tbl_command).await?;

    let catalog = ctx.get_catalog("default").await?;

    let num_inserts = 3;
    append_rows(ctx.clone(), num_inserts).await?;
    ctx.evict_table_from_cache("default", "default", "t")?;
    let statistics_sql = "analyze table default.t";
    fixture.execute_command(statistics_sql).await?;

    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;

    // check count
    ctx.evict_table_from_cache("default", "default", "t")?;
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(num_inserts, query_count(stream).await? as usize);

    let expected = HashMap::from([(0, num_inserts as u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    // append the same values again, and ndv does changed.
    append_rows(ctx.clone(), num_inserts).await?;
    ctx.evict_table_from_cache("default", "default", "t")?;
    fixture.execute_command(statistics_sql).await?;

    // check count
    ctx.evict_table_from_cache("default", "default", "t")?;
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(num_inserts * 2, query_count(stream).await? as usize);

    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    // delete
    ctx.evict_table_from_cache("default", "default", "t")?;
    let query = "delete from default.t where c=1";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    if let Plan::DataMutation { s_expr, schema, .. } = plan {
        do_mutation(ctx.clone(), *s_expr.clone(), schema.clone()).await?;
    }
    ctx.evict_table_from_cache("default", "default", "t")?;
    fixture.execute_command(statistics_sql).await?;

    // check count: delete not affect counts
    check_column_ndv_statistics(ctx, table.clone(), expected).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_update_analyze_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let db_name = fixture.default_db_name();
    let tb_name = fixture.default_table_name();

    // insert
    for i in 0..3 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tb_name, i);
        fixture.execute_command(&qry).await?;
    }

    // update
    let query = format!("update {}.{} set id = 3 where id = 0", db_name, tb_name);
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    if let Plan::DataMutation { s_expr, schema, .. } = plan {
        do_mutation(ctx.clone(), *s_expr.clone(), schema.clone()).await?;
    }

    // check summary after update
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_update = fuse_table.read_table_snapshot().await?.unwrap();
    let base_summary = after_update.summary.clone();
    let id_stats = base_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(0)));
    assert!(base_summary.additional_stats_meta.is_some());
    assert_eq!(base_summary.additional_stats_meta.unwrap().row_count, 4);

    // get segments summary
    let mut segment_summary = Statistics::default();
    let segment_reader = MetaReaders::segment_info_reader(
        ctx.get_application_level_data_operator()?.operator(),
        TestFixture::default_table_schema(),
    );
    for segment in after_update.segments.iter() {
        let param = LoadParams {
            location: segment.0.clone(),
            len_hint: None,
            ver: segment.1,
            put_cache: false,
        };
        let compact_segment = segment_reader.read(&param).await?;
        let segment_info = SegmentInfo::try_from(compact_segment)?;
        merge_statistics_mut(
            &mut segment_summary,
            &segment_info.summary,
            fuse_table.cluster_key_info().as_ref(),
        );
    }

    // analyze
    fixture.analyze_table().await?;

    // check summary after analyze
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_analyze = fuse_table.read_table_snapshot().await?.unwrap();
    let last_summary = after_analyze.summary.clone();
    let id_stats = last_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));
    assert!(last_summary.additional_stats_meta.is_some());
    assert_eq!(
        last_summary
            .additional_stats_meta
            .as_ref()
            .map(|v| v.row_count),
        Some(3)
    );

    segment_summary.additional_stats_meta = last_summary.additional_stats_meta.clone();
    assert_eq!(segment_summary, last_summary);

    Ok(())
}

async fn check_column_ndv_statistics(
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    expected: HashMap<u32, u64>,
) -> Result<()> {
    let provider = table.column_statistics_provider(ctx).await?;

    for (i, num) in expected.iter() {
        let stat = provider.column_statistics(*i);
        assert!(stat.is_some());
        // Safe to unwrap: FuseTable's ndv is not None.
        assert_eq!(stat.unwrap().ndv.unwrap(), *num);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_analyze_count_min_sketch_preserves_hll_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let table_name = "t_cms_preserve_hll";

    fixture
        .execute_command(&format!(
            "create table {table_name}(c int) approx_distinct_columns = 'c'"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table_name} select number::int from numbers(10)"
        ))
        .await?;
    fixture
        .execute_command(&format!("analyze table default.{table_name}"))
        .await?;

    ctx.evict_table_from_cache("default", "default", table_name)?;
    let table = catalog
        .get_table(&ctx.get_tenant(), "default", table_name)
        .await?;
    let expected = HashMap::from([(0, 10_u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    fixture
        .execute_command(&format!(
            "alter table default.{table_name} set options(\
             analyze_frequency_columns = 'c', \
             analyze_top_n_size = 0, \
             analyze_count_min_sketch_error_rate = '0.001')"
        ))
        .await?;
    fixture
        .execute_command(&format!("analyze table default.{table_name}"))
        .await?;
    fixture
        .execute_command(&format!("analyze table default.{table_name}"))
        .await?;

    ctx.evict_table_from_cache("default", "default", table_name)?;
    let table = catalog
        .get_table(&ctx.get_tenant(), "default", table_name)
        .await?;
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected).await?;
    let provider = table.column_statistics_provider(ctx.clone()).await?;
    assert!(provider.count_min_sketch(0).is_some());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_analyze_without_prev_table_seq() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // setup
    let create_tbl_command = "create table t(c int)";
    fixture.execute_command(create_tbl_command).await?;

    append_rows(ctx.clone(), 3).await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    // genenrate snapshot without prev_table_seq
    let snapshot_0 = fuse_table.read_table_snapshot().await?.unwrap();
    let snapshot_1 = TableSnapshot::try_from_previous(
        snapshot_0.clone(),
        fuse_table.cluster_key_info(),
        None,
        TestFixture::default_table_meta_timestamps(),
    )?;
    let snapshot_loc_1 =
        location_gen.gen_snapshot_location(&snapshot_1.snapshot_id, TableSnapshot::VERSION)?;
    snapshot_1.write_meta(&operator, &snapshot_loc_1).await?;

    // generate table statistics.
    let col: Vec<u8> = vec![1, 3, 0, 0, 0, 118, 5, 1, 21, 6, 3, 229, 13, 3];
    let hll: HashMap<ColumnId, MetaHLL12> =
        HashMap::from([(0, borsh_deserialize_from_slice(&col)?)]);
    let table_statistics_v3 =
        TableSnapshotStatisticsV3::new(hll, HashMap::new(), snapshot_1.snapshot_id);
    let table_statistics = TableSnapshotStatistics::from(table_statistics_v3);
    let table_statistics_location = location_gen.snapshot_statistics_location_from_uuid(
        &table_statistics.snapshot_id,
        table_statistics.format_version(),
    )?;
    // genenrate snapshot without prev_table_seq
    let mut snapshot_2 = TableSnapshot::try_from_previous(
        Arc::new(snapshot_1.clone()),
        fuse_table.cluster_key_info(),
        None,
        TestFixture::default_table_meta_timestamps(),
    )?;
    snapshot_2.table_statistics_location = Some(table_statistics_location);
    fuse_table
        .commit_to_meta_server(
            fixture.new_query_ctx().await?.as_ref(),
            fuse_table.get_table_info(),
            location_gen,
            snapshot_2,
            Some(table_statistics),
            &None,
            &operator,
        )
        .await?;

    // check statistics.
    let table = table.refresh(ctx.as_ref()).await?;
    let expected = HashMap::from([(0, 3_u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    let qry = "insert into t values(4)";
    execute_command(ctx.clone(), qry).await?;

    ctx.evict_table_from_cache("default", "default", "t")?;
    let statistics_sql = "analyze table default.t";
    fixture.execute_command(statistics_sql).await?;

    let table = table.refresh(ctx.as_ref()).await?;
    let expected = HashMap::from([(0, 4_u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;
    Ok(())
}

fn no_scan_options() -> AnalyzeOptions {
    AnalyzeOptions::from_table_options(&BTreeMap::new())
        .unwrap()
        .no_scan()
}

/// Run ANALYZE with `snapshot` as the collection baseline, whatever the table's current
/// snapshot is. This is how a stale baseline is reproduced deterministically.
async fn execute_analyze_from_snapshot(
    ctx: Arc<QueryContext>,
    table: &FuseTable,
    snapshot: Arc<TableSnapshot>,
    options: AnalyzeOptions,
) -> Result<()> {
    let mut pipeline = Pipeline::create();
    table.do_analyze(ctx.clone(), snapshot, &mut pipeline, options)?;
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(ctx.clone())?;
    let executor = PipelineCompleteExecutor::from_pipelines(vec![pipeline], settings)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute().await
}

async fn latest_fuse_table(ctx: &Arc<QueryContext>, name: &str) -> Result<FuseTable> {
    ctx.evict_table_from_cache("default", "default", name)?;
    let table = ctx
        .get_catalog("default")
        .await?
        .get_table(&ctx.get_tenant(), "default", name)
        .await?;
    Ok(FuseTable::try_from_table(table.as_ref())?.clone())
}

/// The table's current snapshot and the statistics file it points to.
async fn latest_statistics(
    ctx: &Arc<QueryContext>,
    name: &str,
) -> Result<(FuseTable, Arc<TableSnapshot>, TableSnapshotStatistics)> {
    let table = latest_fuse_table(ctx, name).await?;
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let location = snapshot.table_statistics_location.as_ref().unwrap();
    let statistics = MetaReaders::table_snapshot_statistics_reader(table.get_operator())
        .read(&LoadParams {
            location: location.clone(),
            len_hint: None,
            ver: TableMetaLocationGenerator::table_statistics_version(location),
            put_cache: false,
        })
        .await?;
    Ok((table, snapshot, statistics.as_ref().clone()))
}

/// Final content of the table built by `setup_stale_baseline`.
const STALE_BASELINE_ROWS: u64 = 28;
const STALE_BASELINE_NDV: u64 = 20;

/// True frequency of a value in the table built by `setup_stale_baseline`.
fn stale_baseline_frequency(value: i32) -> u64 {
    match value {
        0 => 5,
        1 | 2 => 3,
        _ => 1,
    }
}

/// Build a table whose statistics snapshot is `base`, then append rows so that the table
/// moves ahead of `base` before ANALYZE commits. Returns the stale table handle and baseline.
///
/// Final content: 10 + 5 + 10 + 3 = 28 rows, values 0..20, with 0 -> 5 copies and 1, 2 -> 3
/// copies each so Top-N has a clear order.
async fn setup_stale_baseline(
    fixture: &TestFixture,
    ctx: &Arc<QueryContext>,
    name: &str,
) -> Result<(FuseTable, Arc<TableSnapshot>)> {
    fixture
        .execute_command(&format!(
            "create table {name}(c int) approx_distinct_columns = 'c' \
             analyze_frequency_columns = 'c' analyze_top_n_size = 3 \
             analyze_count_min_sketch_error_rate = '0.01'"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {name} select number::int from numbers(10)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {name} values (0), (0), (0), (1), (1)"
        ))
        .await?;

    let table = latest_fuse_table(ctx, name).await?;
    let base = table.read_table_snapshot().await?.unwrap();

    // The table moves on before the statistics are committed.
    fixture
        .execute_command(&format!(
            "insert into {name} select number::int + 10 from numbers(10)"
        ))
        .await?;
    fixture
        .execute_command(&format!("insert into {name} values (0), (2), (2)"))
        .await?;
    Ok((table, base))
}

/// Frequency statistics as configured on the table, plus the given histogram.
fn table_options_with_histogram(
    table: &FuseTable,
    histogram: AnalyzeHistogramInfo,
) -> Result<AnalyzeOptions> {
    Ok(
        AnalyzeOptions::from_table_options(table.get_table_info().options())?
            .with_histogram(histogram),
    )
}

/// The hook path: NOSCAN, HLL and column statistics only.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rebases_append_only_snapshot() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_enable_table_snapshot_stats(1)?;
    let (stale_table, base) = setup_stale_baseline(&fixture, &ctx, "t_rebase").await?;

    execute_analyze_from_snapshot(ctx.clone(), &stale_table, base, no_scan_options()).await?;

    let (table, snapshot, statistics) = latest_statistics(&ctx, "t_rebase").await?;
    assert_eq!(snapshot.summary.row_count, STALE_BASELINE_ROWS);
    let col_stats = snapshot.summary.col_stats.get(&0).unwrap();
    assert_eq!(col_stats.min(), &Scalar::Number(NumberScalar::Int32(0)));
    assert_eq!(col_stats.max(), &Scalar::Number(NumberScalar::Int32(19)));
    assert_eq!(
        snapshot
            .summary
            .additional_stats_meta
            .as_ref()
            .map(|meta| meta.row_count),
        Some(STALE_BASELINE_ROWS)
    );
    assert!(statistics.is_fresh_for(&snapshot));
    check_column_ndv_statistics(
        ctx,
        Arc::new(table),
        HashMap::from([(0, STALE_BASELINE_NDV)]),
    )
    .await?;
    Ok(())
}

/// The manual path: Top-N, count-min sketch and KLL sketches all follow the appended
/// segments.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rebases_frequency_and_kll_fast_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let (stale_table, base) = setup_stale_baseline(&fixture, &ctx, "t_rebase_freq").await?;
    let options = table_options_with_histogram(&stale_table, AnalyzeHistogramInfo::KllFast {
        relative_error: 0.01,
    })?;

    execute_analyze_from_snapshot(ctx.clone(), &stale_table, base, options).await?;

    let (_, snapshot, statistics) = latest_statistics(&ctx, "t_rebase_freq").await?;
    assert!(statistics.is_fresh_for(&snapshot));
    assert_eq!(statistics.row_count, STALE_BASELINE_ROWS);
    // HLL is an estimate; 20 distinct values merged from four block sketches.
    let ndv = statistics.hll.get(&0).unwrap().count() as u64;
    assert!(
        (STALE_BASELINE_NDV - 1..=STALE_BASELINE_NDV + 2).contains(&ndv),
        "unexpected NDV estimate {ndv}"
    );

    // Top-N is a space-saving style summary: counts are upper bounds whose exact values
    // depend on the block merge order, so check the guarantees rather than exact counts.
    let top_n = statistics.top_n.get(&0).unwrap();
    assert_eq!(top_n.capacity, 3);
    assert_eq!(
        top_n.values.first().map(|entry| &entry.scalar),
        Some(&Scalar::Number(NumberScalar::Int32(0)))
    );
    for entry in &top_n.values {
        let value = *entry.scalar.as_number().unwrap().as_int32().unwrap();
        let truth = stale_baseline_frequency(value);
        assert!(
            entry.count.saturating_sub(entry.error) <= truth && truth <= entry.count,
            "top-n entry {entry:?} does not bound the true frequency {truth}"
        );
    }

    // Count-min sketch merges exactly under the same parameters; with 20 distinct values
    // and a 1% error rate these estimates are exact.
    let cms = statistics.count_min_sketch.get(&0).unwrap();
    for value in [0, 1, 2, 15, 19] {
        assert_eq!(
            cms.estimate(&Scalar::Number(NumberScalar::Int32(value))),
            Some(stale_baseline_frequency(value))
        );
    }

    // KLL fast: buckets are derived from the merged sketch and cover every row.
    assert_eq!(
        statistics.histograms.get(&0).unwrap().num_values(),
        STALE_BASELINE_ROWS as f64
    );
    Ok(())
}

/// KLL full keeps the bucket boundaries fixed from the baseline but counts appended rows
/// into them, so the histogram still covers the whole table.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rebases_kll_full_histogram() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let (stale_table, base) = setup_stale_baseline(&fixture, &ctx, "t_rebase_full").await?;
    let options = table_options_with_histogram(&stale_table, AnalyzeHistogramInfo::KllFull {
        relative_error: 0.01,
    })?;

    execute_analyze_from_snapshot(ctx.clone(), &stale_table, base, options).await?;

    let (_, snapshot, statistics) = latest_statistics(&ctx, "t_rebase_full").await?;
    assert!(statistics.is_fresh_for(&snapshot));
    assert_eq!(statistics.row_count, STALE_BASELINE_ROWS);
    assert_eq!(
        statistics.histograms.get(&0).unwrap().num_values(),
        STALE_BASELINE_ROWS as f64
    );
    Ok(())
}

/// A column that is all NULL in the baseline has no KLL sketch there, so its bucket
/// boundaries come from the appended rows instead.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rebases_kll_full_histogram_over_null_baseline() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    fixture
        .execute_command("create table t_rebase_full_null(a int null, b int)")
        .await?;
    fixture
        .execute_command("insert into t_rebase_full_null values (null, 1), (null, 2), (null, 3)")
        .await?;
    let table = latest_fuse_table(&ctx, "t_rebase_full_null").await?;
    let base = table.read_table_snapshot().await?.unwrap();

    fixture
        .execute_command(
            "insert into t_rebase_full_null select number::int, number::int from numbers(10)",
        )
        .await?;

    let options = table_options_with_histogram(&table, AnalyzeHistogramInfo::KllFull {
        relative_error: 0.01,
    })?;
    execute_analyze_from_snapshot(ctx.clone(), &table, base, options).await?;

    let (_, snapshot, statistics) = latest_statistics(&ctx, "t_rebase_full_null").await?;
    assert!(statistics.is_fresh_for(&snapshot));
    assert_eq!(statistics.row_count, 13);
    // `a`: collector created during the rebase, covering only the non-NULL appended rows.
    assert_eq!(statistics.histograms.get(&0).unwrap().num_values(), 10.0);
    // `b`: collector from the baseline, extended with the appended rows.
    assert_eq!(statistics.histograms.get(&1).unwrap().num_values(), 13.0);
    Ok(())
}

/// Every value is counted into the first bucket whose routing upper bound is not below it,
/// so the observed `[lower, upper]` ranges of the buckets are disjoint and each bucket holds
/// exactly the column values inside its range, whatever order the blocks were counted in.
async fn assert_kll_full_buckets_match_table(
    fixture: &TestFixture,
    ctx: &Arc<QueryContext>,
    name: &str,
) -> anyhow::Result<()> {
    let (_, snapshot, statistics) = latest_statistics(ctx, name).await?;
    assert!(statistics.is_fresh_for(&snapshot));
    let histogram = statistics.histograms.get(&0).unwrap();
    assert!(histogram.num_buckets() > 1);

    let int = |datum: Datum| match datum {
        Datum::Int(value) => value,
        other => panic!("unexpected bound {other:?}"),
    };
    let buckets: Vec<(i64, i64, f64)> = histogram
        .bucket_iter()
        .map(|bucket| {
            (
                int(bucket.lower_bound()),
                int(bucket.upper_bound()),
                bucket.num_values(),
            )
        })
        .collect();
    let mut select = vec!["count(a)".to_string()];
    select.extend(
        buckets
            .iter()
            .map(|(lower, upper, _)| format!("count_if(a between {lower} and {upper})")),
    );
    let blocks = fixture
        .execute_query(&format!("select {} from {name}", select.join(", ")))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let row = &blocks[0];
    let count = |offset: usize| -> f64 {
        row.get_by_offset(offset)
            .index(0)
            .unwrap()
            .to_string()
            .parse()
            .unwrap()
    };

    assert_eq!(histogram.num_values(), count(0));
    for (offset, (lower, upper, num_values)) in buckets.iter().enumerate() {
        assert_eq!(
            *num_values,
            count(offset + 1),
            "bucket [{lower}, {upper}] of {name}"
        );
    }
    Ok(())
}

/// The KLL full bucket scan counts blocks in parallel; the buckets must still hold
/// exactly the table's values, with and without a rebase over appended blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_kll_full_histogram_over_many_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let name = "t_kll_full_blocks";
    fixture
        .execute_command(&format!(
            "create table {name}(a int null) row_per_block = 37 block_per_segment = 3"
        ))
        .await?;
    // Repeated values and NULLs, spread over many blocks and segments.
    let insert = format!(
        "insert into {name} select if(number % 10 = 0, null, (number * 7919 % 1000)::int) \
         from numbers(3000)"
    );
    fixture.execute_command(&insert).await?;
    let table = latest_fuse_table(&ctx, name).await?;
    let base = table.read_table_snapshot().await?.unwrap();
    fixture.execute_command(&insert).await?;
    let snapshot = latest_fuse_table(&ctx, name)
        .await?
        .read_table_snapshot()
        .await?
        .unwrap();
    // Far more blocks than the scan keeps in flight (2 * max_threads), in both scans below.
    assert!(base.summary.block_count > 64);
    assert!(snapshot.summary.block_count - base.summary.block_count > 64);

    let kll_full = AnalyzeHistogramInfo::KllFull {
        relative_error: 0.01,
    };
    for max_threads in [1, 4] {
        ctx.get_settings().set_max_threads(max_threads)?;
        // Rebase: buckets from `base`, then the appended blocks are counted into them; and
        // without rebase: the whole table is counted by the bucket scan.
        for from in [&base, &snapshot] {
            let options = table_options_with_histogram(&table, kll_full.clone())?;
            execute_analyze_from_snapshot(ctx.clone(), &table, from.clone(), options).await?;
            assert_kll_full_buckets_match_table(&fixture, &ctx, name).await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rejects_non_append_snapshot_change() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    fixture
        .execute_command("create table t_rebase_delete(c int) approx_distinct_columns = 'c'")
        .await?;
    fixture
        .execute_command("insert into t_rebase_delete select number::int from numbers(10)")
        .await?;
    let table = latest_fuse_table(&ctx, "t_rebase_delete").await?;
    let base = table.read_table_snapshot().await?.unwrap();

    fixture
        .execute_command("delete from t_rebase_delete where c < 5")
        .await?;

    let err = execute_analyze_from_snapshot(ctx, &table, base, no_scan_options())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::UNRESOLVABLE_CONFLICT);
    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    for i in 0..n {
        let qry = format!("insert into t values({})", i);
        execute_command(ctx.clone(), &qry).await?;
    }
    Ok(())
}
