// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_catalog::table::Table;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::attach_table::RealAttachTableHandler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;
use futures::TryStreamExt;

async fn query_text(fixture: &TestFixture, sql: &str) -> anyhow::Result<String> {
    let blocks = fixture
        .execute_query(sql)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut text = String::new();
    for block in blocks {
        for row in 0..block.num_rows() {
            for entry in block.columns() {
                text.push_str(&entry.value().index(row).unwrap().to_string());
                text.push('\n');
            }
        }
    }
    Ok(text)
}

async fn assert_attached_statistics(
    fixture: &TestFixture,
    target: &str,
    can_rewrite: bool,
    expected_max: &str,
) -> anyhow::Result<()> {
    let plan = query_text(
        fixture,
        &format!("EXPLAIN SELECT MIN(wide), MAX(wide) FROM {target}"),
    )
    .await?;
    assert_eq!(plan.contains("DummyTableScan"), can_rewrite, "{plan}");
    let result = query_text(fixture, &format!("SELECT MIN(s), MAX(s) FROM {target}")).await?;
    assert_eq!(result, format!("'abcdefghijklmnop'\n'{expected_max}'\n"));
    Ok(())
}

// Normal ATTACH, projection, defaults and snapshot refresh are covered by
// 05_0024_attach_string_statistics.test. Only raw historical hint files need Rust.
#[tokio::test(flavor = "multi_thread")]
async fn test_attach_string_statistics_with_historical_hints() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    RealAttachTableHandler::init()?;
    fixture.create_default_database().await?;
    fixture
        .execute_command("SET auto_compaction_imperfect_blocks_threshold=0")
        .await?;
    fixture.execute_command("SET enable_auto_analyze=0").await?;
    let storage = tempfile::tempdir()?;
    let db = fixture.default_db_name();
    fixture.execute_command(&format!(
        "CREATE TABLE {db}.source(s STRING STATS_TRUNCATE_LEN 4, wide STRING STATS_TRUNCATE_LEN 40) 'fs://{}/'",
        storage.path().display()
    )).await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.source VALUES ('abcdefghijklmnop', 'abcdefghijklmnopqrst')"
        ))
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let source = ctx.get_table("default", &db, "source").await?;
    let source = FuseTable::try_from_table(source.as_ref())?;
    let operator = source.get_operator();
    let prefix = source.get_storage_prefix();
    let v2_path = format!("{prefix}/{FUSE_TBL_LAST_SNAPSHOT_HINT_V2}");
    let mut old_hint: serde_json::Value =
        serde_json::from_slice(&operator.read(&v2_path).await?.to_vec())?;
    old_hint
        .as_object_mut()
        .unwrap()
        .remove("field_stats_truncate_len");
    operator
        .write(&v2_path, serde_json::to_vec(&old_hint)?)
        .await?;
    let uri = format!(
        "fs://{}/{prefix}",
        operator.info().root().trim_end_matches('/')
    );
    for format in ["old_v2", "legacy"] {
        if format == "legacy" {
            operator.delete(&v2_path).await?;
            operator
                .write(
                    &format!("{prefix}/{FUSE_TBL_LAST_SNAPSHOT_HINT}"),
                    old_hint["snapshot_full_path"].as_str().unwrap().to_string(),
                )
                .await?;
        }
        let target = format!("{db}.attached_{format}");
        fixture
            .execute_command(&format!("ATTACH TABLE {target} '{uri}'"))
            .await?;
        assert_attached_statistics(&fixture, &target, false, "abcdefghijklmnop").await?;
    }

    // A normal commit publishes settings without rewriting historical blocks.
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.source VALUES ('zzzzzzzzzzzzzzzz', 'abcdefghijklmnopqrst')"
        ))
        .await?;
    for format in ["old_v2", "legacy"] {
        assert_attached_statistics(
            &fixture,
            &format!("{db}.attached_{format}"),
            true,
            "zzzzzzzzzzzzzzzz",
        )
        .await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let attached = ctx.get_table("default", &db, "attached_old_v2").await?;
    let mut known_info = attached.get_table_info().clone();

    // An older writer can publish a hint after an upgrade. Existing attachments
    // must forget the previously known settings and return to the safe fallback.
    let mut downgraded_hint: serde_json::Value =
        serde_json::from_slice(&operator.read(&v2_path).await?.to_vec())?;
    downgraded_hint
        .as_object_mut()
        .unwrap()
        .remove("field_stats_truncate_len");
    operator
        .write(&v2_path, serde_json::to_vec(&downgraded_hint)?)
        .await?;
    for format in ["old_v2", "legacy"] {
        assert_attached_statistics(
            &fixture,
            &format!("{db}.attached_{format}"),
            false,
            "zzzzzzzzzzzzzzzz",
        )
        .await?;
    }
    // Refresh must clear known settings carried by a cached TableInfo too,
    // not just handle the empty metadata of attachments created from old hints.
    known_info
        .meta
        .options
        .remove(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG);
    let refreshed = FuseTable::try_create(known_info, None, false)?;
    assert!(
        refreshed
            .get_table_info()
            .meta
            .field_stats_truncate_len
            .is_empty()
    );
    let ranges = refreshed
        .accurate_columns_ranges(fixture.new_query_ctx().await?, &[0, 1])
        .await?
        .unwrap();
    assert!(
        ranges
            .values()
            .all(|range| range.min.may_be_truncated && range.max.may_be_truncated)
    );
    Ok(())
}
