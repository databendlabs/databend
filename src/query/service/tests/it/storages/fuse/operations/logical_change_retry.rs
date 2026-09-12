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
use databend_common_catalog::table::Table;
use databend_common_expression::DataBlock;
use databend_common_sql::Planner;
use databend_common_storages_fuse::FuseTable;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures::TryStreamExt;

async fn execute(session: &Arc<Session>, sql: &str) -> anyhow::Result<()> {
    let ctx = session.create_query_context(&BUILD_INFO).await?;
    let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
    InterpreterFactory::get(ctx.clone(), &plan)
        .await?
        .execute(ctx)
        .await?
        .try_collect::<Vec<DataBlock>>()
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

async fn totals(fixture: &TestFixture, name: &str) -> anyhow::Result<(u64, u64)> {
    let snapshot = table(fixture, name)
        .await?
        .read_table_snapshot()
        .await?
        .unwrap();
    let value = serde_json::to_value(snapshot.as_ref())?;
    let counters = &value["logical_change_counters"];
    // This is the raw cumulative interpretation used by pre-epoch readers.
    Ok((
        counters["updated_rows_total"].as_u64().unwrap(),
        counters["deleted_rows_total"].as_u64().unwrap(),
    ))
}

async fn make_epochless(fixture: &TestFixture, name: &str) -> anyhow::Result<()> {
    let table = table(fixture, name).await?;
    let location = table.snapshot_loc().unwrap();
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let mut value = serde_json::to_value(snapshot.as_ref())?;
    value["logical_change_counters"]
        .as_object_mut()
        .unwrap()
        .remove("epoch");
    let snapshot: TableSnapshot = serde_json::from_value(value)?;
    // Test-only rewrite of the checkpoint: preserve data and totals, remove epoch.
    table
        .get_operator()
        .write(&location, snapshot.to_bytes()?)
        .await?;
    if let Some(cache) = CacheManager::instance().get_table_snapshot_cache() {
        cache.evict(&location);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_epochless_transaction_conflict_preserves_complete_delta() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let name = "counter_retry";
    let target = format!("{}.{}", fixture.default_db_name(), name);
    fixture
        .execute_command(&format!("CREATE TABLE {target}(id INT, v INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {target} VALUES (1,10),(2,20),(3,30)"))
        .await?;
    make_epochless(&fixture, name).await?;
    let writer = fixture.new_session_with_type(SessionType::Dummy).await?;
    let id = table(&fixture, name).await?.get_id();

    // The other session commits after this transaction buffers its mutations,
    // forcing a metadata conflict at COMMIT.
    {
        let before = totals(&fixture, name).await?;
        fixture.execute_command("BEGIN").await?;
        fixture
            .execute_command(&format!("UPDATE {target} SET v=v+1 WHERE id=1"))
            .await?;
        fixture
            .execute_command(&format!("UPDATE {target} SET v=v+1 WHERE id=3"))
            .await?;
        fixture
            .execute_command(&format!("DELETE FROM {target} WHERE id=2"))
            .await?;
        assert_eq!(
            fixture
                .default_session()
                .txn_mgr()
                .lock()
                .logical_change_deltas()
                .get(&id),
            Some(&Some((2, 1)))
        );
        execute(&writer, &format!("INSERT INTO {target} VALUES (100,0)")).await?;
        fixture.execute_command("COMMIT").await?;
        assert_eq!(totals(&fixture, name).await?, (before.0 + 2, before.1 + 1));
        assert_eq!(
            table(&fixture, name)
                .await?
                .read_table_snapshot()
                .await?
                .unwrap()
                .summary
                .row_count,
            3
        );
        assert!(
            fixture
                .default_session()
                .txn_mgr()
                .lock()
                .logical_change_deltas()
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_and_multi_table_retry_preserve_counters() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let writer = fixture.new_session_with_type(SessionType::Dummy).await?;
    let db = fixture.default_db_name();
    let names = ["counter_a", "counter_b"];
    let mut ids = Vec::new();
    for session in [fixture.default_session(), writer.clone()] {
        execute(&session, "SET auto_compaction_imperfect_blocks_threshold=0").await?;
        execute(&session, "SET enable_auto_analyze=0").await?;
    }
    for name in names {
        let target = format!("{db}.{name}");
        fixture
            .execute_command(&format!("CREATE TABLE {target}(id INT, v INT)"))
            .await?;
        fixture
            .execute_command(&format!("INSERT INTO {target} VALUES (1,10),(2,20),(3,30)"))
            .await?;
        fixture
            .execute_command(&format!("UPDATE {target} SET v=11 WHERE id=1"))
            .await?;
        fixture
            .execute_command(&format!("DELETE FROM {target} WHERE id=2"))
            .await?;
        assert_eq!(totals(&fixture, name).await?, (1, 1));
        ids.push(table(&fixture, name).await?.get_id());
    }

    let a = format!("{db}.{}", names[0]);
    let b = format!("{db}.{}", names[1]);
    // Start with two rows per table. ANALYZE + concurrent append leaves three;
    // INSERT ALL + concurrent append leaves five, which OVERWRITE removes.
    for (statements, delta, expected_rows) in [
        (
            vec![format!("ANALYZE TABLE {a}"), format!("ANALYZE TABLE {b}")],
            (0, 0),
            3,
        ),
        (
            vec![format!("INSERT ALL INTO {a} INTO {b} SELECT 9,90")],
            (0, 0),
            5,
        ),
        (
            vec![format!(
                "INSERT OVERWRITE ALL INTO {a} INTO {b} SELECT 9,90"
            )],
            (0, 5),
            2,
        ),
    ] {
        let mut before = Vec::new();
        for name in names {
            // Exercise each write path from an old, nonzero counter baseline.
            make_epochless(&fixture, name).await?;
            before.push(totals(&fixture, name).await?);
        }
        fixture.execute_command("BEGIN").await?;
        for sql in &statements {
            fixture.execute_command(sql).await?;
        }
        let deltas = fixture
            .default_session()
            .txn_mgr()
            .lock()
            .logical_change_deltas();
        for id in &ids {
            assert_eq!(deltas.get(id), Some(&Some(delta)), "{statements:?}");
        }
        for name in names {
            execute(&writer, &format!("INSERT INTO {db}.{name} VALUES (100,0)")).await?;
        }
        fixture.execute_command("COMMIT").await?;
        for (index, name) in names.into_iter().enumerate() {
            assert_eq!(
                totals(&fixture, name).await?,
                (before[index].0 + delta.0, before[index].1 + delta.1),
                "persisted counters after retry: {statements:?}, {name}"
            );
            assert_eq!(
                table(&fixture, name)
                    .await?
                    .read_table_snapshot()
                    .await?
                    .unwrap()
                    .summary
                    .row_count,
                expected_rows
            );
        }
        assert!(
            fixture
                .default_session()
                .txn_mgr()
                .lock()
                .logical_change_deltas()
                .is_empty()
        );
    }
    Ok(())
}
