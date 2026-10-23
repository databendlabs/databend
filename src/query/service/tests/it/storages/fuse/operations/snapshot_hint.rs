// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::load_last_snapshot_hint;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;

// Single-session behavior is covered by txn_snapshot_hint.test. These tests
// control concurrent writers to exercise successful and failed commit retries.
async fn execute(session: &Arc<Session>, sql: &str) -> anyhow::Result<()> {
    let ctx = session.create_query_context(&BUILD_INFO).await?;
    TestFixture::execute_query_with_ctx(ctx, sql)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn table(fixture: &TestFixture, name: &str) -> anyhow::Result<FuseTable> {
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx
        .get_table("default", &fixture.default_db_name(), name)
        .await?;
    Ok(FuseTable::try_from_table(table.as_ref())?.clone())
}

fn snapshot_path(table: &FuseTable) -> String {
    format!(
        "{}{}",
        table.get_operator().info().root(),
        table.snapshot_loc().unwrap()
    )
}

async fn hint(table: &FuseTable) -> anyhow::Result<Option<String>> {
    Ok(load_last_snapshot_hint(
        table.meta_location_generator().prefix(),
        &table.get_operator(),
    )
    .await?
    .map(|hint| hint.snapshot_full_path))
}

async fn setup() -> anyhow::Result<TestFixture> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture
        .execute_command("SET auto_compaction_imperfect_blocks_threshold=0")
        .await?;
    fixture.execute_command("SET enable_auto_analyze=0").await?;
    Ok(fixture)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_snapshot_hints_after_retry() -> anyhow::Result<()> {
    let fixture = setup().await?;
    let db = fixture.default_db_name();
    let writer = fixture.new_session_with_type(SessionType::Dummy).await?;
    let names = ["hint_retry_a", "hint_retry_b"];
    for name in names {
        fixture
            .execute_command(&format!("CREATE TABLE {db}.{name}(id INT, v INT)"))
            .await?;
        fixture
            .execute_command(&format!("INSERT INTO {db}.{name} VALUES (1,10)"))
            .await?;
    }

    fixture.execute_command("BEGIN").await?;
    fixture
        .execute_command(&format!(
            "INSERT ALL INTO {db}.{} INTO {db}.{} SELECT 2,20",
            names[0], names[1]
        ))
        .await?;
    let mut buffered = Vec::new();
    for name in names {
        fixture
            .execute_command(&format!("INSERT INTO {db}.{name} VALUES (3,30)"))
            .await?;
        buffered.push(table(&fixture, name).await?.snapshot_loc());
        // Advance both tables from another session to force snapshot rebuilds.
        execute(&writer, &format!("INSERT INTO {db}.{name} VALUES (4,40)")).await?;
    }
    fixture.execute_command("COMMIT").await?;
    for (index, name) in names.into_iter().enumerate() {
        let table = table(&fixture, name).await?;
        assert_ne!(table.snapshot_loc(), buffered[index]);
        assert_eq!(hint(&table).await?, Some(snapshot_path(&table)));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_transaction_commit_preserves_snapshot_hint() -> anyhow::Result<()> {
    let fixture = setup().await?;
    let target = format!("{}.hint_conflict", fixture.default_db_name());
    fixture
        .execute_command(&format!("CREATE TABLE {target}(id INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {target} VALUES (1),(2)"))
        .await?;
    let writer = fixture.new_session_with_type(SessionType::Dummy).await?;
    fixture.execute_command("BEGIN").await?;
    fixture
        .execute_command(&format!("DELETE FROM {target} WHERE id=1"))
        .await?;
    // Both deletes rewrite the same segment, making the conflict unresolvable.
    execute(&writer, &format!("DELETE FROM {target} WHERE id=2")).await?;
    let ctx = writer.create_query_context(&BUILD_INFO).await?;
    let latest = ctx
        .get_table("default", &fixture.default_db_name(), "hint_conflict")
        .await?;
    let latest = FuseTable::try_from_table(latest.as_ref())?;
    let expected = Some(snapshot_path(latest));
    assert_eq!(hint(latest).await?, expected);

    let err = fixture.execute_command("COMMIT").await.unwrap_err();
    assert_eq!(
        err.code(),
        databend_common_exception::ErrorCode::UNRESOLVABLE_CONFLICT
    );
    assert_eq!(hint(latest).await?, expected);
    assert_eq!(
        table(&fixture, "hint_conflict").await?.snapshot_loc(),
        latest.snapshot_loc()
    );
    Ok(())
}
