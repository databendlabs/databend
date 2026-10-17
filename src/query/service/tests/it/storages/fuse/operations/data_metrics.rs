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

//! `QueryContext::get_io_stats` backs `system.processes.data_read_bytes /
//! data_write_bytes` and `system_history.query_history.scan_io_bytes /
//! written_io_bytes`.
//! These tests check that it reports the block IO of `SELECT`,
//! `INSERT ... SELECT`, `REPLACE INTO` and `COPY INTO` regardless of how the
//! interpreter was entered, and that the partition counters still come from
//! the attached table.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::runtime::IoStatsSnapshot;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::plan::PartStatistics;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextPartitionStats;
use databend_query::sessions::TableContextProgress;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextSettings;
use databend_query::sql::Planner;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

struct Observed {
    io_stats: IoStatsSnapshot,
    partitions_scanned: u64,
    partitions_total: u64,
    /// `pruned_partitions_stats` as `explain analyze` reads it.
    per_plan: HashMap<u32, PartStatistics>,
    scan_bytes: usize,
    write_bytes: usize,
}

impl Observed {
    /// Every pruning path publishes under the physical id of its scan node,
    /// and each value for that id is published by exactly one party. For a
    /// single-scan statement that means: one entry in the per-plan map, and
    /// the query-level sum equals it.
    fn assert_single_scan_published_once(&self) {
        assert_eq!(
            self.per_plan.len(),
            1,
            "a single-scan statement must publish under exactly one plan id, got {:?}",
            self.per_plan.keys().collect::<Vec<_>>()
        );
        let (_, stat) = self.per_plan.iter().next().unwrap();
        assert_eq!(
            (self.partitions_total, self.partitions_scanned),
            (stat.partitions_total as u64, stat.partitions_scanned as u64),
            "query-level sum must equal the single plan entry"
        );
    }
}

/// Run `sql` on a fresh `QueryContext` created and executed *without* any
/// tracking payload carrying `io_stats`, the way `FlightSQL` and other
/// handlers that do not install one call the interpreter. The interpreter
/// itself must bind the query's counter so the IO is attributed.
async fn run_and_observe(fixture: &TestFixture, sql: &str) -> Result<Observed> {
    run_and_observe_with(fixture, sql, |_| Ok(())).await
}

async fn run_and_observe_with(
    fixture: &TestFixture,
    sql: &str,
    configure: impl FnOnce(&Arc<QueryContext>) -> Result<()>,
) -> Result<Observed> {
    let ctx = fixture.new_query_ctx().await?;
    configure(&ctx)?;
    assert!(
        ThreadTracker::io_stats().is_none(),
        "test must run outside any io_stats tracking scope to exercise the unbound path"
    );

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    stream.try_collect::<Vec<DataBlock>>().await?;

    let partition_stats = ctx.get_partition_stats();
    Ok(Observed {
        io_stats: ctx.get_io_stats(),
        partitions_scanned: partition_stats.scanned,
        partitions_total: partition_stats.total,
        per_plan: ctx.get_pruned_partitions_stats(),
        scan_bytes: ctx.get_scan_progress_value().bytes,
        write_bytes: ctx.get_write_progress_value().bytes,
    })
}

/// Upper bound for "only snapshot / segment metadata was read or written"
/// in this fixture. The block files are an order of magnitude larger, so any
/// value above this proves block IO was counted.
const META_ONLY_BYTES: u64 = 4096;

/// `progress` is the scan / write progress of the query, an independent
/// measure that block data flowed. `io_bytes` is what the query's `IoStats`
/// saw on the operator layer; it must reflect that block IO.
fn assert_block_io_counted(what: &str, io_bytes: u64, progress: usize) {
    assert!(
        progress as u64 > META_ONLY_BYTES,
        "{what}: progress {progress} indicates no block IO happened; test setup is broken"
    );
    assert!(
        io_bytes > META_ONLY_BYTES,
        "{what}: IoStats reports {io_bytes} bytes, i.e. metadata only or nothing; \
         block IO (progress {progress}) was not attributed to the query"
    );
}

async fn setup() -> Result<(TestFixture, String, String)> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let src = format!("{db}.src");
    let dst = format!("{db}.dst");

    fixture
        .execute_command(&format!("create table {src} (a int, b string)"))
        .await?;
    fixture
        .execute_command(&format!("create table {dst} (a int, b string)"))
        .await?;
    // 20k rows so that block files are unambiguously larger than snapshot /
    // segment metadata.
    fixture
        .execute_command(&format!(
            "insert into {src} select number, to_string(number) from numbers(20000)"
        ))
        .await?;

    Ok((fixture, src, dst))
}

#[tokio::test(flavor = "multi_thread")]
async fn select_counts_block_reads() -> Result<()> {
    let (fixture, src, _) = setup().await?;

    // The predicate cannot be constant folded or range pruned, so the block is
    // actually read.
    let observed = run_and_observe(
        &fixture,
        &format!("select count() from {src} where b like '%99%'"),
    )
    .await?;

    assert_block_io_counted(
        "select read",
        observed.io_stats.read_bytes,
        observed.scan_bytes,
    );
    assert_eq!(observed.io_stats.write_bytes, 0);
    assert_eq!(observed.partitions_total, 1);
    assert_eq!(observed.partitions_scanned, 1);
    Ok(())
}

/// With more segments than nodes the scan is *lazy*: block pruning happens in
/// a prune pipeline on a table instance built from the plan, not on the one
/// attached to the context. Partition counters must still reach the query.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_scan_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.lazy_scan");

    // Several blocks per segment so that "segments" and "blocks" differ: both
    // counters must be in blocks.
    fixture
        .execute_command(&format!(
            "create table {table} (a int, b string) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    // Two inserts give two segments of three blocks each; on the single-node
    // test context `segments > nodes` makes `read_partitions` return lazy parts.
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number, to_string(number) from numbers(300)"
            ))
            .await?;
    }

    let observed = run_and_observe(&fixture, &format!("select a from {table}")).await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(observed.partitions_scanned, 6);

    let observed =
        run_and_observe(&fixture, &format!("select a from {table} where a >= 200")).await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(
        observed.partitions_scanned, 2,
        "min/max pruning keeps the last block of each segment"
    );

    // A third segment with a disjoint range is dropped by segment-level
    // pruning before any of its blocks is looked at; it still counts towards
    // the total, in blocks.
    fixture
        .execute_command(&format!(
            "insert into {table} select number + 1000, to_string(number) from numbers(300)"
        ))
        .await?;
    let observed =
        run_and_observe(&fixture, &format!("select a from {table} where a < 100")).await?;
    assert_eq!(observed.partitions_total, 9);
    assert_eq!(
        observed.partitions_scanned, 2,
        "first block of the two low segments; the high segment is pruned whole"
    );

    // `_segment_name` skips the other segments before their metadata is
    // read; the total is the snapshot's block count regardless.
    let segment = fixture
        .execute_query(&format!(
            "select _segment_name from {table} where a = 0 limit 1"
        ))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?
        .remove(0)
        .get_by_offset(0)
        .index(0)
        .and_then(|s| s.as_string().map(|s| s.to_string()))
        .expect("segment name");
    let observed = run_and_observe(
        &fixture,
        &format!("select a from {table} where _segment_name = '{segment}'"),
    )
    .await?;
    assert_eq!(observed.partitions_total, 9);
    assert_eq!(observed.partitions_scanned, 3);
    Ok(())
}

/// `_segment_name` is applied before any segment is read. On an eager scan
/// that can reject every segment, so the planner has partition counters
/// but no pruning statistics; they must still reach the query.
#[tokio::test(flavor = "multi_thread")]
async fn eager_segment_name_filter_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.segment_name_eager");

    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select number from numbers(300)"
        ))
        .await?;

    let observed = run_and_observe(
        &fixture,
        &format!("select a from {table} where _segment_name = 'no-such-segment'"),
    )
    .await?;
    assert_eq!(observed.partitions_total, 3);
    assert_eq!(observed.partitions_scanned, 0);
    Ok(())
}

/// Lazy scans that do not go through the row-oriented prune pipeline must
/// still publish what they scanned: `enable_prune_pipeline = 0` falls back to
/// an eager `prune_snapshot_blocks` inside `read_data`, and column-oriented
/// segments have their own prune pipeline. The planner publishes the total.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_scan_without_prune_pipeline_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.lazy_no_prune_pipeline");

    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number from numbers(300)"
            ))
            .await?;
    }

    let observed = run_and_observe_with(&fixture, &format!("select a from {table}"), |ctx| {
        ctx.get_settings()
            .set_setting("enable_prune_pipeline".into(), "0".into())
    })
    .await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(observed.partitions_scanned, 6);

    let observed = run_and_observe_with(
        &fixture,
        &format!("select a from {table} where a >= 200"),
        |ctx| {
            ctx.get_settings()
                .set_setting("enable_prune_pipeline".into(), "0".into())
        },
    )
    .await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(observed.partitions_scanned, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lazy_column_oriented_scan_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.lazy_column_oriented");

    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10 \
             segment_format='column_oriented'"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number from numbers(300)"
            ))
            .await?;
    }

    let observed = run_and_observe(&fixture, &format!("select a from {table}")).await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(observed.partitions_scanned, 6);

    let observed =
        run_and_observe(&fixture, &format!("select a from {table} where a >= 200")).await?;
    assert_eq!(observed.partitions_total, 6);
    assert_eq!(observed.partitions_scanned, 2);
    Ok(())
}

/// `ANALYZE TABLE` with window histograms builds one physical plan per
/// eligible column on the same context. Each `PhysicalPlanBuilder::build`
/// clears the per-plan statistics map; the query-level counters must still
/// cover every plan.
#[tokio::test(flavor = "multi_thread")]
async fn analyze_table_counts_every_histogram_scan() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.analyze_histograms");

    // Two histogram-eligible columns, one block.
    fixture
        .execute_command(&format!("create table {table} (a int, b int)"))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select number, number * 2 from numbers(100)"
        ))
        .await?;

    let observed = run_and_observe_with(&fixture, &format!("analyze table {table}"), |ctx| {
        let settings = ctx.get_settings();
        settings.set_setting("enable_analyze_histogram".into(), "1".into())?;
        settings.set_setting("analyze_histogram_algorithm".into(), "window".into())
    })
    .await?;
    // One scan per histogram column (a, b) over the single block.
    assert_eq!(observed.partitions_total, 2);
    assert_eq!(observed.partitions_scanned, 2);
    Ok(())
}

/// Every pruning mode must publish its statistics under the scan node's
/// physical plan id, exactly once per value. The query here has an aggregate
/// above the scan so that the logical `scan_id` (0) and the physical plan id
/// differ; a publisher keyed on the wrong one shows up as a second entry in
/// the per-plan map, and a duplicate publisher as a sum larger than the entry.
#[tokio::test(flavor = "multi_thread")]
async fn every_pruning_mode_publishes_under_the_scan_plan_id() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    // (table, lazy?) — lazy needs segments > nodes (1 here).
    let row = format!("{db}.inv_row");
    let col = format!("{db}.inv_col");
    for (table, extra) in [(&row, ""), (&col, " segment_format='column_oriented'")] {
        fixture
            .execute_command(&format!(
                "create table {table} (a int) row_per_block=100 block_per_segment=10{extra}"
            ))
            .await?;
        for _ in 0..2 {
            fixture
                .execute_command(&format!(
                    "insert into {table} select number from numbers(300)"
                ))
                .await?;
        }
    }
    // Eager: a single segment.
    let eager = format!("{db}.inv_eager");
    fixture
        .execute_command(&format!("create table {eager} (a int) row_per_block=100"))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {eager} select number from numbers(300)"
        ))
        .await?;

    let aggregate =
        |table: &str| format!("select a % 3, count() from {table} where a >= 200 group by a % 3");

    struct Case {
        name: &'static str,
        sql: String,
        prune_pipeline: bool,
        expect: (u64, u64),
    }
    let cases = [
        Case {
            name: "eager scan",
            sql: aggregate(&eager),
            prune_pipeline: true,
            expect: (3, 1),
        },
        Case {
            name: "lazy row-oriented, prune pipeline",
            sql: aggregate(&row),
            prune_pipeline: true,
            expect: (6, 2),
        },
        Case {
            name: "lazy row-oriented, synchronous",
            sql: aggregate(&row),
            prune_pipeline: false,
            expect: (6, 2),
        },
        Case {
            name: "lazy column-oriented, prune pipeline",
            sql: aggregate(&col),
            prune_pipeline: true,
            expect: (6, 2),
        },
        // No "lazy column-oriented, synchronous" case: `prune_snapshot_blocks`
        // reads segments through `RowOrientedSegmentReader` and fails on a
        // column-oriented table with `unsupported encoding`, on `main` too.
        Case {
            name: "eager update",
            sql: format!("update {eager} set a = a where a >= 200"),
            prune_pipeline: true,
            expect: (3, 1),
        },
        Case {
            name: "eager delete",
            sql: format!("delete from {eager} where a >= 200"),
            prune_pipeline: true,
            expect: (3, 1),
        },
    ];

    for case in cases {
        let observed = run_and_observe_with(&fixture, &case.sql, |ctx| {
            ctx.get_settings().set_setting(
                "enable_prune_pipeline".into(),
                if case.prune_pipeline { "1" } else { "0" }.into(),
            )
        })
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", case.name));
        assert_eq!(
            (observed.partitions_total, observed.partitions_scanned),
            case.expect,
            "{}: (total, scanned)",
            case.name
        );
        observed.assert_single_scan_published_once();
    }
    Ok(())
}

/// `UPDATE` / `DELETE` read through `MutationSource`, not `TableScan`. Their
/// pruning happens in the planner and the statistics must still reach the
/// query's partition counters.
#[tokio::test(flavor = "multi_thread")]
async fn eager_mutation_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.eager_mutation");

    fixture
        .execute_command(&format!(
            "create table {table} (a int, b string) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select number, to_string(number) from numbers(300)"
        ))
        .await?;

    let observed = run_and_observe(
        &fixture,
        &format!("update {table} set b = 'x' where a >= 200"),
    )
    .await?;
    assert_eq!(observed.partitions_total, 3);
    assert_eq!(observed.partitions_scanned, 1);

    let observed = run_and_observe(&fixture, &format!("delete from {table} where a < 100")).await?;
    assert_eq!(observed.partitions_total, 3);
    assert_eq!(observed.partitions_scanned, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_select_counts_block_reads_and_writes() -> Result<()> {
    let (fixture, src, dst) = setup().await?;

    let observed =
        run_and_observe(&fixture, &format!("insert into {dst} select * from {src}")).await?;

    assert_block_io_counted(
        "insert-select read",
        observed.io_stats.read_bytes,
        observed.scan_bytes,
    );
    assert_block_io_counted(
        "insert-select write",
        observed.io_stats.write_bytes,
        observed.write_bytes,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn replace_into_counts_block_writes() -> Result<()> {
    let (fixture, _, dst) = setup().await?;

    let observed = run_and_observe(
        &fixture,
        &format!("replace into {dst} on (a) select number, to_string(number) from numbers(20000)"),
    )
    .await?;

    assert_block_io_counted(
        "replace-into write",
        observed.io_stats.write_bytes,
        observed.write_bytes,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_into_table_counts_block_writes() -> Result<()> {
    let (fixture, src, dst) = setup().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!("create stage {db}_stage"))
        .await?;
    fixture
        .execute_command(&format!(
            "copy into @{db}_stage from (select * from {src}) file_format = (type = parquet)"
        ))
        .await?;

    let observed = run_and_observe(
        &fixture,
        &format!("copy into {dst} from @{db}_stage file_format = (type = parquet)"),
    )
    .await?;

    assert_block_io_counted(
        "copy-into write",
        observed.io_stats.write_bytes,
        observed.write_bytes,
    );
    // Reading the staged parquet file goes through the same operator layer.
    assert!(observed.io_stats.read_bytes > META_ONLY_BYTES);
    Ok(())
}

/// Handlers such as HTTP and MySQL install the query's `IoStats` in their
/// tracking payload before calling the interpreter. The interpreter must reuse
/// that counter rather than start a second one, so both views agree.
#[tokio::test(flavor = "multi_thread")]
async fn handler_installed_io_stats_is_reused() -> Result<()> {
    let (fixture, src, _) = setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let mut payload = ThreadTracker::new_tracking_payload();
    payload.query_id = Some(ctx.get_id());
    payload.io_stats = Some(ctx.get_local_io_stats());
    let handler_counter = payload.io_stats.clone().unwrap();

    let sql = format!("select count() from {src} where b like '%99%'");
    let exec_ctx = ctx.clone();
    payload
        .tracking(async move {
            let mut planner = Planner::new(exec_ctx.clone());
            let (plan, _) = planner.plan_sql(&sql).await?;
            let interpreter = InterpreterFactory::get(exec_ctx.clone(), &plan).await?;
            let stream = interpreter.execute(exec_ctx).await?;
            stream.try_collect::<Vec<DataBlock>>().await?;
            Result::Ok(())
        })
        .await?;

    let handler_view = handler_counter.snapshot();
    assert!(handler_view.read_bytes > META_ONLY_BYTES);
    assert_eq!(ctx.get_io_stats(), handler_view);
    Ok(())
}

/// On a cluster worker, pipeline *preparation* (`INIT_QUERY_FRAGMENTS`) runs in
/// its own RPC scope, before `START_PREPARED_QUERY`. For a lazy `DELETE` that
/// phase already reads segment and index data (`do_mutation_block_pruning`).
/// This exercises `init_query_fragments` the way the RPC handler does, outside
/// any `io_stats` scope, and checks the preparation IO lands on the query's
/// counter before any pipeline has executed, so it cannot be hidden behind
/// execution-time reads.
#[tokio::test(flavor = "multi_thread")]
async fn init_query_fragments_counts_preparation_reads() -> Result<()> {
    use databend_query::interpreters::MutationInterpreter;
    use databend_query::schedulers::Fragmenter;
    use databend_query::schedulers::QueryFragmentsActions;
    use databend_query::servers::flight::v1::actions::init_query_fragments;
    use databend_query::servers::flight::v1::exchange::DataExchangeManager;
    use databend_query::sql::plans::Plan;

    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.lazy_delete");

    fixture
        .execute_command(&format!("create table {table} (a int, b string)"))
        .await?;
    // Two inserts give two segments; with a two-node cluster below the DELETE
    // source becomes lazy (`segments >= nodes`), deferring block pruning to
    // pipeline preparation.
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number, to_string(number) from numbers(20000)"
            ))
            .await?;
    }

    let local_id = "node1";
    let ctx = fixture
        .new_query_ctx_with_cluster(
            ClusterDescriptor::new()
                .with_node(local_id, "")
                .with_node("node2", ""),
        )
        .await?;
    assert!(ThreadTracker::io_stats().is_none());

    let sql = format!("delete from {table} where b like '%99%'");
    let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
    let Plan::DataMutation { s_expr, schema, .. } = plan else {
        unreachable!("expected a DataMutation plan")
    };
    let mutation = s_expr.mutation()?.clone();
    let interpreter = MutationInterpreter::try_create(
        ctx.clone(),
        s_expr.planned()?.clone(),
        schema,
        mutation.metadata.clone(),
    )?;
    let physical_plan = interpreter.build_physical_plan(&mutation, false).await?;
    assert_eq!(
        ctx.get_io_stats().read_bytes,
        0,
        "planning must not read through the query counter in this setup"
    );

    // Split the plan into fragments exactly as the coordinator does, and take
    // the ones that `INIT_QUERY_FRAGMENTS` would hand to this node.
    let mut actions = QueryFragmentsActions::create(ctx.clone());
    for fragment in Fragmenter::try_create(ctx.clone())?.build_fragment(&physical_plan)? {
        fragment.get_actions(ctx.clone(), &mut actions)?;
    }
    let fragments = actions
        .get_query_fragments()?
        .remove(local_id)
        .expect("local node must receive fragments");
    assert!(
        fragments
            .fragments
            .iter()
            .any(|fragment| { format!("{:?}", fragment.physical_plan).contains("MutationSource") }),
        "the lazy mutation source must be prepared on this node"
    );

    // Register the context like `INIT_QUERY_ENV` does on a worker, then run
    // the real `INIT_QUERY_FRAGMENTS` handler.
    let query_id = ctx.get_id();
    let exchange_manager = DataExchangeManager::instance();
    exchange_manager.handle_statistics_exchange(query_id.clone(), "peer".to_string())?;
    exchange_manager.set_ctx(&query_id, ctx.clone())?;

    let prepared = init_query_fragments(fragments).await;
    // Undo the registration above. In production this happens when the query
    // finishes executing, which this test never does; without it the
    // `QueryCoordinator` (and the `QueryContext` it holds) would stay in the
    // global `DataExchangeManager` for the rest of the test binary. Done before
    // `?` so that a failed preparation is cleaned up too.
    exchange_manager.on_finished_query(&query_id, None);
    prepared?;

    let io_stats = ctx.get_io_stats();
    assert!(
        io_stats.read_bytes > 0,
        "lazy DELETE pruning during pipeline preparation read nothing through the query counter"
    );
    assert_eq!(io_stats.write_bytes, 0, "nothing executed yet");

    // `partitions_total` is the whole-table block count, published once by
    // `build_physical_plan` above on the coordinator. The deferred pruning
    // publishes only what this node scanned under the same plan id: the two
    // segments are reshuffled across the two nodes, so one segment of one
    // block; `b like '%99%'` cannot be range-pruned. A whole-table total
    // from every node would show up here as 4.
    let partition_stats = ctx.get_partition_stats();
    assert_eq!(partition_stats.total, 2);
    assert_eq!(partition_stats.scanned, 1);
    Ok(())
}
