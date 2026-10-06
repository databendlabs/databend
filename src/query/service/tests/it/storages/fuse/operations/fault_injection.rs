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

//! Storage failures on the fuse write and read paths, driven by
//! `databend_common_storage::FaultInjection`.
//!
//! The rule registry is process-global: rules are scoped to the test table's storage prefix
//! so that other tests running in the same process are not affected, and these tests still
//! run one at a time to keep the hit counters unambiguous.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_storage::FaultInjection;
use databend_common_storage::FaultKind;
use databend_common_storage::FaultOp;
use databend_common_storage::FaultRule;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::*;
use futures::TryStreamExt;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Row count computed from the data itself (`count(*)` alone is answered from statistics
/// and would not touch a single block).
async fn count(fixture: &TestFixture) -> Result<u64> {
    let sql = format!(
        "SELECT count(id) FROM {}.{} WHERE id > 0",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let blocks: Vec<DataBlock> = fixture.execute_query(&sql).await?.try_collect().await?;
    match blocks[0].get_by_offset(0).value() {
        Value::Scalar(Scalar::Number(NumberScalar::UInt64(s))) => Ok(s),
        Value::Column(Column::Number(NumberColumn::UInt64(c))) => Ok(c[0]),
        other => Err(ErrorCode::BadDataValueType(format!(
            "expected UInt64, got {other:?}"
        ))),
    }
}

/// Fixture with a two-row default table; the returned string is the table's storage prefix,
/// to scope fault rules to this table only.
async fn setup() -> Result<(TestFixture, String)> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {}.{} VALUES (1, (1, 1)), (2, (2, 2))",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;
    let table = fixture.latest_default_table().await?;
    let prefix = FuseTable::try_from_table(table.as_ref())?
        .meta_location_generator()
        .prefix()
        .to_string();
    Ok((fixture, prefix))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_snapshot_write_fails_the_statement_and_leaves_the_table_intact() -> Result<()>
{
    let _serial = SERIAL.lock().await;
    let (fixture, prefix) = setup().await?;
    assert_eq!(count(&fixture).await?, 2);

    // Every snapshot write fails: the commit cannot succeed, the statement must report it,
    // and the table must still be exactly what it was before.
    let fault = FaultInjection::install(FaultRule::new(
        FaultOp::Write,
        format!("{prefix}/_ss/"),
        FaultKind::Permanent,
    ));
    let insert = format!(
        "INSERT INTO {}.{} VALUES (3, (3, 3))",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let err = fixture.execute_command(&insert).await.unwrap_err();
    assert_ne!(
        err.code(),
        ErrorCode::UNWIND_ERROR,
        "storage failure must not panic: {err}"
    );
    assert!(fault.hits() >= 1, "the fault was never exercised");
    fault.remove();

    assert_eq!(
        count(&fixture).await?,
        2,
        "a failed commit must not change the table"
    );

    // The table is usable afterwards.
    fixture.execute_command(&insert).await?;
    assert_eq!(count(&fixture).await?, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_block_is_reported_as_an_error() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let (fixture, prefix) = setup().await?;

    let fault = FaultInjection::install(FaultRule::new(
        FaultOp::Read,
        format!("{prefix}/_b/"),
        FaultKind::NotFound,
    ));
    let err = count(&fixture).await.unwrap_err();
    assert_ne!(
        err.code(),
        ErrorCode::UNWIND_ERROR,
        "storage failure must not panic: {err}"
    );
    assert!(fault.hits() >= 1, "the fault was never exercised");
    fault.remove();

    assert_eq!(count(&fixture).await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_temporary_read_failure_is_retried() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let (fixture, prefix) = setup().await?;

    // One transient failure on a block read: the retry layer must absorb it and the query
    // must succeed with the right result.
    let fault = FaultInjection::install(
        FaultRule::new(FaultOp::Read, format!("{prefix}/_b/"), FaultKind::Temporary).times(1),
    );
    assert_eq!(count(&fixture).await?, 2);
    assert_eq!(fault.hits(), 1, "the fault was never exercised");
    fault.remove();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_segment_write_fails_the_statement_and_leaves_the_table_intact() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let (fixture, prefix) = setup().await?;

    let fault = FaultInjection::install(FaultRule::new(
        FaultOp::Write,
        format!("{prefix}/_sg/"),
        FaultKind::Permanent,
    ));
    let insert = format!(
        "INSERT INTO {}.{} VALUES (3, (3, 3))",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let err = fixture.execute_command(&insert).await.unwrap_err();
    assert_ne!(
        err.code(),
        ErrorCode::UNWIND_ERROR,
        "storage failure must not panic: {err}"
    );
    assert!(fault.hits() >= 1, "the fault was never exercised");
    fault.remove();

    assert_eq!(
        count(&fixture).await?,
        2,
        "a failed commit must not change the table"
    );
    Ok(())
}
