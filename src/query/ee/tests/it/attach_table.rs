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

#[tokio::test(flavor = "multi_thread")]
async fn test_attach_string_statistics_with_historical_hints() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    // EESetup initializes only the handlers its existing tests require.
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
    let bytes = operator.read(&v2_path).await?.to_vec();
    let mut old_hint: serde_json::Value = serde_json::from_slice(&bytes)?;
    old_hint
        .as_object_mut()
        .unwrap()
        .remove("field_stats_truncate_len");
    let uri = format!(
        "fs://{}/{prefix}",
        operator.info().root().trim_end_matches('/')
    );
    for format in ["new", "old_v2", "legacy"] {
        if format == "old_v2" {
            operator
                .write(&v2_path, serde_json::to_vec(&old_hint)?)
                .await?;
        } else if format == "legacy" {
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
            .execute_command(&format!("ATTACH TABLE {target}(wide, s) '{uri}'"))
            .await?;
        // SHOW CREATE for an attached table intentionally renders ATTACH syntax,
        // not column definitions. Inspect its actual metadata instead.
        let ctx = fixture.new_query_ctx().await?;
        let attached = ctx
            .get_table("default", &db, &format!("attached_{format}"))
            .await?;
        let settings = &attached.get_table_info().meta.field_stats_truncate_len;
        if format == "new" {
            assert_eq!(
                settings,
                &source.get_table_info().meta.field_stats_truncate_len
            );
        } else {
            assert!(settings.is_empty());
        }
        let plan = query_text(
            &fixture,
            &format!("EXPLAIN SELECT MIN(wide), MAX(wide) FROM {target}"),
        )
        .await?;
        assert_eq!(plan.contains("DummyTableScan"), format == "new", "{plan}");
        let result = query_text(&fixture, &format!("SELECT MIN(s), MAX(s) FROM {target}")).await?;
        assert_eq!(result.matches("abcdefghijklmnop").count(), 2, "{result}");
    }

    // A new writer publishes the missing settings without rewriting old blocks.
    // Existing attachments, including those created from old hints, upgrade on refresh.
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.source VALUES ('zzzzzzzzzzzzzzzz', 'abcdefghijklmnopqrst')"
        ))
        .await?;
    for format in ["new", "old_v2", "legacy"] {
        let target = format!("{db}.attached_{format}");
        let ctx = fixture.new_query_ctx().await?;
        let attached = ctx
            .get_table("default", &db, &format!("attached_{format}"))
            .await?;
        assert_eq!(
            attached.get_table_info().meta.field_stats_truncate_len,
            source.get_table_info().meta.field_stats_truncate_len
        );
        let plan = query_text(
            &fixture,
            &format!("EXPLAIN SELECT MIN(wide), MAX(wide) FROM {target}"),
        )
        .await?;
        assert!(plan.contains("DummyTableScan"), "{plan}");
        let result = query_text(&fixture, &format!("SELECT MIN(s), MAX(s) FROM {target}")).await?;
        assert!(result.contains("abcdefghijklmnop"), "{result}");
        assert!(result.contains("zzzzzzzzzzzzzzzz"), "{result}");
    }
    Ok(())
}
