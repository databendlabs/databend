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
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

const NUM_ROWS: usize = 1000;

/// Runs `sql` and returns the scan progress it accumulated. This is the
/// value `system.processes.scan_progress_read_rows` / `_bytes` expose.
async fn scan_progress_of(fixture: &TestFixture, sql: &str) -> Result<ProgressValues> {
    let ctx = fixture.new_query_ctx().await?;
    let stream = TestFixture::execute_query_with_ctx(ctx.clone(), sql).await?;
    stream.try_collect::<Vec<DataBlock>>().await?;
    Ok(ctx.get_scan_progress_value())
}

/// Scan progress reports the rows and bytes the scan read and decoded, not
/// the rows left after prewhere / runtime filters. `NUM_ROWS` rows inserted
/// at once form a single block, and the predicates below cannot prune it.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_progress_counts_rows_before_prewhere() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    // t(a int, b string): a = 0..NUM_ROWS, b = 'xxxxxxxxxx' (10 chars)
    let create = format!("create table {db}.t(a int, b string)");
    let insert =
        format!("insert into {db}.t select number, repeat('x', 10) from numbers({NUM_ROWS})");
    fixture.execute_command(&create).await?;
    fixture.execute_command(&insert).await?;

    // Only `a` is read, and it is the prewhere column: there are no remaining
    // columns to decode after the filter.
    let count =
        scan_progress_of(&fixture, &format!("select count() from {db}.t where a = 5")).await?;
    // `b` is a remaining column, decoded only for the surviving row.
    let remaining =
        scan_progress_of(&fixture, &format!("select b from {db}.t where a = 5")).await?;
    // Every row is filtered out; `to_string` keeps the planner from folding
    // the predicate to `false` and skipping the block entirely.
    let empty = scan_progress_of(
        &fixture,
        &format!("select b from {db}.t where to_string(a) = 'nope'"),
    )
    .await?;

    assert_eq!(count.rows, NUM_ROWS);
    assert_eq!(remaining.rows, NUM_ROWS);
    assert_eq!(empty.rows, NUM_ROWS);

    assert!(count.bytes > 0);
    assert!(remaining.bytes > count.bytes);
    assert!(empty.bytes > 0);

    Ok(())
}

/// Virtual columns read from their sidecar are decoded after the prewhere
/// pass and must be included in the scan bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_scan_progress_includes_virtual_columns() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    // vt(id int, val json): id = 0..NUM_ROWS, val = {"a": id, "c": "yyyy..." (20 chars)}
    // Parquet with virtual columns enabled, so `val['c']` is served from the
    // virtual column sidecar once it is refreshed.
    let create = format!(
        "create table {db}.vt(id int, val json) \
         storage_format = 'parquet' enable_virtual_column = true"
    );
    let insert = format!(
        "insert into {db}.vt select number, \
         parse_json('{{\"a\":' || number || ',\"c\":\"' || repeat('y', 20) || '\"}}') \
         from numbers({NUM_ROWS})"
    );
    // INSERT alone does not write the virtual column sidecar; it is produced
    // by `refresh virtual column`. Without the sidecar, `val['c']` is served
    // by decoding the whole `val` column as a remaining column, which is
    // accounted for elsewhere and would make this test pass regardless of
    // whether sidecar bytes are counted.
    let refresh = format!("refresh virtual column for {db}.vt");
    fixture.execute_command(&create).await?;
    fixture.execute_command(&insert).await?;
    fixture.execute_command(&refresh).await?;

    let prewhere_only =
        scan_progress_of(&fixture, &format!("select id from {db}.vt where id = 5")).await?;
    let with_virtual = scan_progress_of(
        &fixture,
        &format!("select val['c'] from {db}.vt where id = 5"),
    )
    .await?;

    assert_eq!(prewhere_only.rows, NUM_ROWS);
    assert_eq!(with_virtual.rows, NUM_ROWS);
    assert!(with_virtual.bytes > prewhere_only.bytes);

    Ok(())
}
