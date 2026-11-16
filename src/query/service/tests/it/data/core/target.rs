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

use std::time::Duration;

use databend_common_catalog::session_type::SessionType;
use databend_query::data::core::TableRef;
use databend_query::data::core::TableTarget;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::OSSSetup;
use databend_query::test_kits::TestFixture;

fn set_ttl(fixture: &TestFixture, ms: u64) -> anyhow::Result<()> {
    fixture
        .default_session()
        .get_settings()
        .set_setting("data_api_table_cache_ttl_ms".to_string(), ms.to_string())?;
    Ok(())
}

/// The number of blocks of `db.t` a new request sees.
async fn blocks(fixture: &TestFixture, db: &str) -> anyhow::Result<usize> {
    let table = TableRef {
        catalog: None,
        database: Some(db),
        table: "t",
    };
    let target = TableTarget::resolve(fixture.new_query_ctx().await?, table).await?;
    Ok(target.blocks().await?.len())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_target_reuses_tables_within_ttl() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!("CREATE TABLE {db}.t (a INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.t VALUES (1)"))
        .await?;

    set_ttl(&fixture, 60_000)?;
    assert_eq!(blocks(&fixture, &db).await?, 1);
    fixture
        .execute_command(&format!("INSERT INTO {db}.t VALUES (2)"))
        .await?;
    // Within the TTL: the table resolved before the insert, at its snapshot.
    assert_eq!(blocks(&fixture, &db).await?, 1);

    // Without the cache, every request resolves the latest table.
    set_ttl(&fixture, 0)?;
    assert_eq!(blocks(&fixture, &db).await?, 2);

    // An entry older than the TTL of the request is resolved again.
    set_ttl(&fixture, 1)?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(blocks(&fixture, &db).await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_target_never_shares_temporary_tables() -> anyhow::Result<()> {
    // Temporary tables require a client session rather than SessionType::Dummy.
    let fixture = TestFixture::setup_with_custom_and_session_type(
        OSSSetup {
            config: ConfigBuilder::create().config(),
        },
        SessionType::MySQL,
    )
    .await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!("CREATE TABLE {db}.t (a INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.t VALUES (1)"))
        .await?;
    set_ttl(&fixture, 60_000)?;
    assert_eq!(blocks(&fixture, &db).await?, 1);

    // The temporary table of the session shadows the cached table of the same name.
    fixture
        .execute_command(&format!("CREATE TEMP TABLE {db}.t (a INT)"))
        .await?;
    assert_eq!(blocks(&fixture, &db).await?, 0);
    for row in [1, 2] {
        fixture
            .execute_command(&format!("INSERT INTO {db}.t VALUES ({row})"))
            .await?;
    }
    // Two blocks, unlike the cached table of one.
    assert_eq!(blocks(&fixture, &db).await?, 2);
    Ok(())
}
