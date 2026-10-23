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
//! the attached table. They also cover the segment pruning of a lazy
//! distributed `DELETE`, at pipeline preparation or at query runtime.

use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::runtime::IoStatsSnapshot;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::EmptySink;
use databend_query::interpreters::InterpreterFactory;
use databend_query::pipelines::PipelineBuilder;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::servers::flight::v1::packets::QueryFragments;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextProgress;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextSettings;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

struct Observed {
    io_stats: IoStatsSnapshot,
    partitions_scanned: u64,
    partitions_total: u64,
    scan_bytes: usize,
    write_bytes: usize,
}

/// Run `sql` on a fresh `QueryContext` created and executed *without* any
/// tracking payload carrying `io_stats`, the way `FlightSQL` and other
/// handlers that do not install one call the interpreter. The interpreter
/// itself must bind the query's counter so the IO is attributed.
async fn run_and_observe(fixture: &TestFixture, sql: &str) -> Result<Observed> {
    let ctx = fixture.new_query_ctx().await?;
    assert!(
        ThreadTracker::io_stats().is_none(),
        "test must run outside any io_stats tracking scope to exercise the unbound path"
    );

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    stream.try_collect::<Vec<DataBlock>>().await?;

    let data_metrics = ctx.get_data_metrics();
    Ok(Observed {
        io_stats: ctx.get_io_stats(),
        partitions_scanned: data_metrics.get_partitions_scanned(),
        partitions_total: data_metrics.get_partitions_total(),
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
    // Partition counters still come from pruning on the attached table.
    assert_eq!(observed.partitions_total, 1);
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

/// Plans a lazy `DELETE` on a two-node cluster and runs the real
/// `INIT_QUERY_FRAGMENTS` handler for this node, the way the RPC handler does,
/// outside any `io_stats` scope. Returns the context and the fragments this
/// node received.
///
/// `enable_prune_pipeline` decides when the lazy segments are pruned: while the
/// pipeline is being prepared (`false`, `do_mutation_block_pruning`), or in a
/// pruning pipeline once the query runs (`true`).
async fn init_lazy_delete_fragments(
    fixture: &TestFixture,
    enable_prune_pipeline: bool,
) -> Result<(Arc<QueryContext>, QueryFragments)> {
    use databend_query::interpreters::MutationInterpreter;
    use databend_query::schedulers::Fragmenter;
    use databend_query::schedulers::QueryFragmentsActions;
    use databend_query::servers::flight::v1::actions::init_query_fragments;
    use databend_query::servers::flight::v1::exchange::DataExchangeManager;
    use databend_query::sql::plans::Plan;
    use databend_query::test_kits::ClusterDescriptor;

    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.lazy_delete");

    fixture
        .execute_command(&format!("create table {table} (a int, b string)"))
        .await?;
    // Two inserts give two segments; with a two-node cluster below the DELETE
    // source becomes lazy (`segments >= nodes`), deferring block pruning to
    // the node that runs the mutation source.
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
    ctx.get_settings().set_setting(
        "enable_prune_pipeline".to_string(),
        u64::from(enable_prune_pipeline).to_string(),
    )?;
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

    let prepared = init_query_fragments(fragments.clone()).await;
    // Undo the registration above. In production this happens when the query
    // finishes executing, which this test never does through the exchange
    // manager; without it the `QueryCoordinator` (and the `QueryContext` it
    // holds) would stay in the global `DataExchangeManager` for the rest of the
    // test binary. Done before `?` so that a failed preparation is cleaned up
    // too.
    exchange_manager.on_finished_query(&query_id, None);
    prepared?;

    Ok((ctx, fragments))
}

/// Executes `pipelines` the way `START_PREPARED_QUERY` does on a worker: the
/// executor is created while the query's counter is bound to the tracking
/// payload, which every processor of the pipelines then inherits.
async fn execute_like_prepared_query(
    ctx: &Arc<QueryContext>,
    pipelines: Vec<Pipeline>,
) -> Result<()> {
    let executor = {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(ctx.get_id());
        tracking_payload.io_stats = Some(ctx.get_local_io_stats());
        let _guard = ThreadTracker::tracking(tracking_payload);

        let mut settings = ExecutorSettings::try_create(ctx.clone())?;
        settings.enable_queries_executor = false;
        PipelineCompleteExecutor::from_pipelines(pipelines, settings)?
    };
    ctx.set_executor(executor.get_inner())?;
    executor.execute().await
}

/// With the pruning pipeline disabled, a lazy `DELETE` still prunes its
/// segments while `INIT_QUERY_FRAGMENTS` prepares the pipeline. That phase runs
/// in its own RPC scope, before `START_PREPARED_QUERY`; check its IO lands on
/// the query's counter before any pipeline has executed, so it cannot be
/// hidden behind execution-time reads.
#[tokio::test(flavor = "multi_thread")]
async fn init_query_fragments_counts_preparation_reads() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let (ctx, _) = init_lazy_delete_fragments(&fixture, false).await?;

    let io_stats = ctx.get_io_stats();
    assert!(
        io_stats.read_bytes > 0,
        "lazy DELETE pruning during pipeline preparation read nothing through the query counter"
    );
    assert_eq!(io_stats.write_bytes, 0, "nothing executed yet");
    Ok(())
}

/// With the pruning pipeline enabled, `INIT_QUERY_FRAGMENTS` only builds the
/// pruning pipeline of a lazy `DELETE`, so preparing it reads nothing. The
/// segments are pruned once the query runs; check that IO lands on the query's
/// counter. The pruning pipeline runs first and alone, so the bytes it reads
/// are not mixed with the block reads of the mutation source.
#[tokio::test(flavor = "multi_thread")]
async fn prune_pipeline_counts_runtime_pruning_reads() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let (ctx, fragments) = init_lazy_delete_fragments(&fixture, true).await?;
    assert_eq!(
        ctx.get_io_stats().read_bytes,
        0,
        "lazy DELETE pruning must not run while the pipeline is prepared"
    );

    // `INIT_QUERY_FRAGMENTS` built the pipelines inside the exchange manager,
    // whose execution needs the flight channels of a real cluster. Build the
    // mutation source fragment again and run it here instead.
    let fragment = fragments
        .fragments
        .iter()
        .find(|fragment| format!("{:?}", fragment.physical_plan).contains("MutationSource"))
        .expect("the lazy mutation source must be prepared on this node");
    let mut build_res =
        PipelineBuilder::create(ctx.get_function_context()?, ctx.get_settings(), ctx.clone())
            .finalize(&fragment.physical_plan)?;
    build_res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    assert!(
        !build_res.sources_pipelines.is_empty(),
        "the lazy mutation source must come with a pruning pipeline"
    );

    // The pruned parts are buffered in the channel feeding the mutation
    // sources, which stay alive (not yet executed) in the main pipeline.
    let prune_pipelines = std::mem::take(&mut build_res.sources_pipelines);
    execute_like_prepared_query(&ctx, prune_pipelines).await?;
    let pruned = ctx.get_io_stats();
    assert!(
        pruned.read_bytes > 0,
        "lazy DELETE pruning at query runtime read nothing through the query counter"
    );
    assert_eq!(pruned.write_bytes, 0, "only the pruning pipeline executed");

    // Then let the mutation sources consume the pruned parts.
    let mut main_pipeline = build_res.main_pipeline;
    if !main_pipeline.is_complete_pipeline()? {
        main_pipeline.add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))))?;
    }
    execute_like_prepared_query(&ctx, vec![main_pipeline]).await?;
    assert!(
        ctx.get_io_stats().read_bytes > pruned.read_bytes,
        "the mutation sources read nothing of the parts streamed by the pruning pipeline"
    );
    Ok(())
}
