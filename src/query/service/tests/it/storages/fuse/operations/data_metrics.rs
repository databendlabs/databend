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
//! interpreter was entered. The partition counters are covered in
//! `partition_stats.rs`, which shares the helpers below.

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
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

pub(super) struct Observed {
    pub(super) io_stats: IoStatsSnapshot,
    pub(super) partitions_scanned: u64,
    pub(super) partitions_total: u64,
    /// `pruned_partitions_stats` as `explain analyze` reads it.
    pub(super) per_plan: HashMap<u32, PartStatistics>,
    pub(super) scan_bytes: usize,
    pub(super) write_bytes: usize,
}

impl Observed {
    /// Every pruning path publishes under the physical id of its scan node,
    /// and each value for that id is published by exactly one party. For a
    /// single-scan statement that means: one entry in the per-plan map, and
    /// the query-level sum equals it.
    pub(super) fn assert_single_scan_published_once(&self) {
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

pub(super) async fn run_and_observe(fixture: &TestFixture, sql: &str) -> Result<Observed> {
    run_and_observe_with(fixture, sql, |_| Ok(())).await
}

pub(super) async fn run_and_observe_with(
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

pub(super) async fn setup() -> Result<(TestFixture, String, String)> {
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

// No single-node test for `explain analyze delete` on a lazy `MutationSource`
// (`MutationSource::set_pruning_stats`): a `DELETE` is lazy only on a cluster
// with `segments >= nodes`, or on a column-oriented table — and the latter
// fails in `do_mutation_block_pruning` with `unsupported encoding: 21` on
// `main` too (row-oriented segment reader). The cluster sqllogictest
// `mode/cluster/lazy_pruning.test` covers it.

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
