// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::ProgressValues;
use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

const NUM_ROWS: usize = 1000;

/// Runs `sql` in a fresh query context and returns the scan progress the
/// query accumulated, i.e. what `system.processes.scan_progress_read_rows` /
/// `scan_progress_read_bytes` expose while the query is running.
async fn scan_progress_of(fixture: &TestFixture, sql: &str) -> Result<ProgressValues> {
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    stream.try_collect::<Vec<DataBlock>>().await?;
    Ok(ctx.get_scan_progress_value())
}

/// Scan progress must report the rows and bytes the scan read and decoded,
/// not the rows left after prewhere / runtime filters were applied inside
/// the scan. A single block of `NUM_ROWS` rows is created so that no block
/// is pruned by the filters below.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_progress_counts_rows_before_prewhere() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!(
            "create table {db}.t(a int, b string) block_size_threshold = 1073741824"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.t select number, repeat('x', 10) from numbers({NUM_ROWS})"
        ))
        .await?;

    // Scan bytes of the whole block when nothing is filtered inside the scan.
    let unfiltered = scan_progress_of(&fixture, &format!("select a, b from {db}.t")).await?;
    assert_eq!(unfiltered.rows, NUM_ROWS);
    assert!(unfiltered.bytes > 0);

    // All projected columns are prewhere columns: `count()` only needs `a`.
    let all_prewhere =
        scan_progress_of(&fixture, &format!("select count() from {db}.t where a = 5")).await?;
    assert_eq!(all_prewhere.rows, NUM_ROWS);
    assert!(all_prewhere.bytes > 0);

    // Prewhere on `a` with `b` as a remaining column. One row survives, but
    // the scan still accounts for the whole block: `b` is decoded for the
    // selected row only and its size is estimated for all rows, so the bytes
    // are not exact but must cover more than the prewhere column alone.
    let with_remaining =
        scan_progress_of(&fixture, &format!("select b from {db}.t where a = 5")).await?;
    assert_eq!(with_remaining.rows, NUM_ROWS);
    assert!(
        with_remaining.bytes > all_prewhere.bytes,
        "remaining column bytes must be counted: {} <= {}",
        with_remaining.bytes,
        all_prewhere.bytes
    );

    // Prewhere drops every row; the block is still read, so progress is not 0.
    // The predicate is opaque to constant folding and range pruning so that
    // the block is not skipped before the scan.
    let all_filtered = scan_progress_of(
        &fixture,
        &format!("select b from {db}.t where to_string(a) = 'nope'"),
    )
    .await?;
    assert_eq!(all_filtered.rows, NUM_ROWS);
    assert!(all_filtered.bytes > 0);

    Ok(())
}

/// Virtual columns are materialized after the prewhere pass and must be
/// included in the scan bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_progress_includes_virtual_columns() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!(
            "create table {db}.vt(id int, val json) storage_format = 'parquet' \
             enable_virtual_column = true block_size_threshold = 1073741824"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.vt select number, \
             parse_json('{{\"a\":' || number || ',\"c\":\"' || repeat('y', 20) || '\"}}') \
             from numbers({NUM_ROWS})"
        ))
        .await?;

    let prewhere_only =
        scan_progress_of(&fixture, &format!("select id from {db}.vt where id = 5")).await?;
    let with_virtual = scan_progress_of(
        &fixture,
        &format!("select val['c'] from {db}.vt where id = 5"),
    )
    .await?;

    assert_eq!(prewhere_only.rows, NUM_ROWS);
    assert_eq!(with_virtual.rows, NUM_ROWS);
    assert!(
        with_virtual.bytes > prewhere_only.bytes,
        "virtual column bytes must be counted: {} <= {}",
        with_virtual.bytes,
        prewhere_only.bytes
    );

    Ok(())
}
