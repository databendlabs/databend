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

//! `QueryContext::get_data_metrics` backs `system.processes.data_read_bytes /
//! data_write_bytes` and `system_history.query_history.scan_io_bytes /
//! written_io_bytes`.
//! These tests check that it reports the block IO of `SELECT`,
//! `INSERT ... SELECT`, `REPLACE INTO` and `COPY INTO`, not just the snapshot
//! and segment metadata touched through the attached table instance.

use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::runtime::IoStatsSnapshot;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storage::StorageMetrics;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextProgress;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

struct Observed {
    data_metrics: StorageMetrics,
    io_stats: IoStatsSnapshot,
    scan_bytes: usize,
    write_bytes: usize,
}

/// Run `sql` on a fresh `QueryContext` inside a tracking scope that shares the
/// context's per-query `IoStats`, mirroring what the HTTP / MySQL handlers
/// install, and return everything a `system.processes` row is built from.
async fn run_and_observe(fixture: &TestFixture, sql: &str) -> Result<Observed> {
    let ctx = fixture.new_query_ctx().await?;

    let mut payload = ThreadTracker::new_tracking_payload();
    payload.query_id = Some(ctx.get_id());
    payload.io_stats = Some(ctx.get_local_io_stats());

    let ctx_for_exec: Arc<QueryContext> = ctx.clone();
    let sql_owned = sql.to_string();
    payload
        .tracking(async move {
            let mut planner = Planner::new(ctx_for_exec.clone());
            let (plan, _) = planner.plan_sql(&sql_owned).await?;
            let interpreter = InterpreterFactory::get(ctx_for_exec.clone(), &plan).await?;
            let stream = interpreter.execute(ctx_for_exec).await?;
            stream.try_collect::<Vec<DataBlock>>().await?;
            Result::Ok(())
        })
        .await?;

    let observed = Observed {
        data_metrics: ctx.get_data_metrics(),
        io_stats: ctx.get_io_stats(),
        scan_bytes: ctx.get_scan_progress_value().bytes,
        write_bytes: ctx.get_write_progress_value().bytes,
    };
    eprintln!(
        "[{sql}]\n  data_metrics: read={} write={}\n  io_stats: {:?}\n  scan_progress.bytes={} write_progress.bytes={}",
        observed.data_metrics.get_read_bytes(),
        observed.data_metrics.get_write_bytes(),
        observed.io_stats,
        observed.scan_bytes,
        observed.write_bytes
    );
    Ok(observed)
}

/// Upper bound for "only snapshot / segment metadata was read or written"
/// in this fixture. The block files are an order of magnitude larger, so any
/// value above this proves block IO was counted.
const META_ONLY_BYTES: usize = 4096;

fn assert_block_io_counted(what: &str, data_metrics: usize, io_stats: u64, progress: usize) {
    assert!(
        progress > META_ONLY_BYTES,
        "{what}: progress {progress} indicates no block IO happened; test setup is broken"
    );
    assert!(
        data_metrics > META_ONLY_BYTES,
        "{what}: data_metrics reports {data_metrics} bytes, i.e. metadata only; \
         block IO (progress {progress}) is missing"
    );
    assert_eq!(
        data_metrics as u64, io_stats,
        "{what}: data_metrics must be sourced from the per-query IoStats"
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
        observed.data_metrics.get_read_bytes(),
        observed.io_stats.read_bytes,
        observed.scan_bytes,
    );
    assert_eq!(observed.data_metrics.get_write_bytes(), 0);
    // Partition counters still come from pruning on the attached table.
    assert_eq!(observed.data_metrics.get_partitions_total(), 1);
    assert_eq!(observed.data_metrics.get_partitions_scanned(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_select_counts_block_reads_and_writes() -> Result<()> {
    let (fixture, src, dst) = setup().await?;

    let observed =
        run_and_observe(&fixture, &format!("insert into {dst} select * from {src}")).await?;

    assert_block_io_counted(
        "insert-select read",
        observed.data_metrics.get_read_bytes(),
        observed.io_stats.read_bytes,
        observed.scan_bytes,
    );
    assert_block_io_counted(
        "insert-select write",
        observed.data_metrics.get_write_bytes(),
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
        observed.data_metrics.get_write_bytes(),
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
        observed.data_metrics.get_write_bytes(),
        observed.io_stats.write_bytes,
        observed.write_bytes,
    );
    // Reading the staged parquet file goes through the same operator layer.
    assert!(observed.data_metrics.get_read_bytes() > META_ONLY_BYTES);
    Ok(())
}
