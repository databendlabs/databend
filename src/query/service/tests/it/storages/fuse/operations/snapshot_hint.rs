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
use databend_common_config::GlobalConfig;
use databend_common_storages_fuse::FUSE_OPT_KEY_ATTACH_COLUMN_IDS;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::load_last_snapshot_hint;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::OSSSetup;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use futures::TryStreamExt;

// Single-session transaction behavior is covered by txn_snapshot_hint.test.
// These tests also exercise concurrent commits and attached metadata refresh.
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

async fn ids(fixture: &TestFixture, name: &str) -> anyhow::Result<Vec<String>> {
    let blocks = fixture
        .execute_query(&format!(
            "SELECT id FROM {}.{name} ORDER BY id",
            fixture.default_db_name()
        ))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut result = Vec::new();
    for block in blocks {
        for row in 0..block.num_rows() {
            result.push(
                block
                    .get_by_offset(0)
                    .value()
                    .index(row)
                    .unwrap()
                    .to_string(),
            );
        }
    }
    Ok(result)
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
    // Temporary tables require a client session rather than SessionType::Dummy.
    let fixture = TestFixture::setup_with_custom_and_session_type(
        OSSSetup {
            config: ConfigBuilder::create().config(),
        },
        SessionType::MySQL,
    )
    .await?;
    fixture.create_default_database().await?;
    fixture
        .execute_command("SET auto_compaction_imperfect_blocks_threshold=0")
        .await?;
    fixture.execute_command("SET enable_auto_analyze=0").await?;
    Ok(fixture)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_attached_snapshot_hint_truncation_settings() -> anyhow::Result<()> {
    let fixture = setup().await?;
    let db = fixture.default_db_name();
    // Test both explicit settings and an explicitly empty map (defaults).
    for (name, column_option, exact_option, exact_value) in [
        (
            "hint_explicit",
            "STATS_TRUNCATE_LEN 4",
            "STATS_TRUNCATE_LEN 40",
            "abcdefghijklmnopqrst",
        ),
        ("hint_defaults", "", "", "abc"),
    ] {
        fixture
            .execute_command(&format!(
                "CREATE TABLE {db}.{name}(id INT, s STRING {column_option}, exact STRING {exact_option}, plain STRING)"
            ))
            .await?;
        fixture
            .execute_command(&format!(
                "INSERT INTO {db}.{name} VALUES (1, 'abcdefghijklmnop', '{exact_value}', 'abc')"
            ))
            .await?;
        let source = table(&fixture, name).await?;
        let operator = source.get_operator();
        let prefix = source.get_storage_prefix();
        let v2_path = format!("{prefix}/{FUSE_TBL_LAST_SNAPSHOT_HINT_V2}");
        let legacy_path = format!("{prefix}/{FUSE_TBL_LAST_SNAPSHOT_HINT}");
        let bytes = operator.read(&v2_path).await?.to_vec();
        let published = load_last_snapshot_hint(prefix, &operator).await?.unwrap();
        assert_eq!(
            published.field_stats_truncate_len.as_ref(),
            Some(&source.get_table_info().meta.field_stats_truncate_len)
        );

        // Simulate an already-existing attached table with no source settings.
        let mut info = source.get_table_info().clone();
        info.meta.storage_params = Some(GlobalConfig::instance().storage.params.clone());
        info.meta.options.insert(
            OPT_KEY_TABLE_ATTACHED_DATA_URI.to_string(),
            "attached".to_string(),
        );
        info.meta.field_stats_truncate_len.clear();
        // Reorder and project by stable ID: exact is column 2, s is column 1.
        let mut schema = info.meta.schema.as_ref().clone();
        schema.metadata.insert(
            FUSE_OPT_KEY_ATTACH_COLUMN_IDS.to_string(),
            "2,1".to_string(),
        );
        info.meta.schema = Arc::new(schema);
        let attached = FuseTable::try_create(info, None, false)?;
        let meta = &attached.get_table_info().meta;
        assert!(
            meta.options
                .contains_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN)
        );
        assert_eq!(meta.schema.fields()[0].column_id(), 2);
        assert_eq!(meta.schema.fields()[1].column_id(), 1);
        assert_eq!(
            meta.field_stats_truncate_len.get(&2),
            (!exact_option.is_empty()).then_some(&40)
        );
        assert_eq!(
            meta.field_stats_truncate_len.get(&1),
            column_option.starts_with("STATS").then_some(&4)
        );
        let ctx = fixture.new_query_ctx().await?;
        let ranges = attached
            .accurate_columns_ranges(ctx.clone(), &[1, 2])
            .await?
            .unwrap();
        assert!(!ranges[&2].min.may_be_truncated);
        assert!(!ranges[&2].max.may_be_truncated);
        assert!(ranges[&1].min.may_be_truncated);
        assert!(ranges[&1].max.may_be_truncated);

        // A distributed worker must see the same settings without rereading hints.
        let fixed = attached.get_table_info().clone();
        let worker = FuseTable::try_create(
            serde_json::from_slice(&serde_json::to_vec(&fixed)?)?,
            None,
            false,
        )?;
        assert_eq!(worker.get_table_info().meta, fixed.meta);

        // Downgrade to an old v2 hint, then to the legacy plain-text format.
        let mut old_v2: serde_json::Value = serde_json::from_slice(&bytes)?;
        old_v2
            .as_object_mut()
            .unwrap()
            .remove("field_stats_truncate_len");
        operator
            .write(&v2_path, serde_json::to_vec(&old_v2)?)
            .await?;
        for legacy in [false, true] {
            if legacy {
                operator.delete(&v2_path).await?;
                operator
                    .write(&legacy_path, published.snapshot_full_path.clone())
                    .await?;
            }
            let mut stale = fixed.clone();
            stale
                .meta
                .options
                .remove(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG);
            let refreshed = FuseTable::try_create(stale, None, false)?;
            let meta = &refreshed.get_table_info().meta;
            assert!(meta.field_stats_truncate_len.is_empty());
            assert!(
                !meta
                    .options
                    .contains_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN)
            );
            let ranges = refreshed
                .accurate_columns_ranges(ctx.clone(), &[1, 2])
                .await?
                .unwrap();
            // Even bounds shorter than the default 16 must stay conservative.
            for range in ranges.values() {
                assert!(range.min.may_be_truncated);
                assert!(range.max.may_be_truncated);
            }
        }
        operator.delete(&legacy_path).await?;
        let mut stale = fixed;
        stale
            .meta
            .options
            .remove(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG);
        let empty = FuseTable::try_create(stale, None, false)?;
        assert!(empty.snapshot_loc().is_none());
        assert!(
            empty
                .get_table_info()
                .meta
                .field_stats_truncate_len
                .is_empty()
        );
        assert!(
            !empty
                .get_table_info()
                .options()
                .contains_key(OPT_KEY_ATTACH_STATS_TRUNCATE_LEN_KNOWN)
        );
    }
    Ok(())
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
    let temp = "hint_retry_temp";
    fixture
        .execute_command(&format!("CREATE TEMP TABLE {db}.{temp}(id INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.{temp} VALUES (0)"))
        .await?;
    let before = table(&fixture, temp).await?;
    assert_eq!(hint(&before).await?, Some(snapshot_path(&before)));

    fixture.execute_command("BEGIN").await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.{temp} VALUES (1)"))
        .await?;
    let temp_buffered = table(&fixture, temp).await?;
    assert_ne!(temp_buffered.snapshot_loc(), before.snapshot_loc());
    assert_eq!(hint(&before).await?, Some(snapshot_path(&before)));
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
        assert_eq!(ids(&fixture, name).await?, ["1", "2", "3", "4"]);
    }
    let committed = table(&fixture, temp).await?;
    assert_eq!(
        committed.get_table_info().meta,
        temp_buffered.get_table_info().meta
    );
    assert_eq!(hint(&committed).await?, Some(snapshot_path(&committed)));
    assert_eq!(ids(&fixture, temp).await?, ["0", "1"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_transaction_commit_preserves_snapshot_hint() -> anyhow::Result<()> {
    let fixture = setup().await?;
    let db = fixture.default_db_name();
    let writer = fixture.new_session_with_type(SessionType::Dummy).await?;
    // Cover both a missing first hint and an existing committed hint.
    for (temp, initialized) in [
        ("hint_conflict_empty", false),
        ("hint_conflict_existing", true),
    ] {
        let target = format!("{db}.{temp}_persistent");
        fixture
            .execute_command(&format!("CREATE TABLE {target}(id INT)"))
            .await?;
        fixture
            .execute_command(&format!("INSERT INTO {target} VALUES (1),(2)"))
            .await?;
        fixture
            .execute_command(&format!("CREATE TEMP TABLE {db}.{temp}(id INT)"))
            .await?;
        if initialized {
            fixture
                .execute_command(&format!("INSERT INTO {db}.{temp} VALUES (0)"))
                .await?;
        }
        let before = table(&fixture, temp).await?;
        let expected_temp_hint = hint(&before).await?;
        assert_eq!(
            expected_temp_hint,
            initialized.then(|| snapshot_path(&before))
        );
        fixture.execute_command("BEGIN").await?;
        fixture
            .execute_command(&format!("INSERT INTO {db}.{temp} VALUES (1)"))
            .await?;
        fixture
            .execute_command(&format!("DELETE FROM {target} WHERE id=1"))
            .await?;
        // Both deletes rewrite the same segment, making the conflict unresolvable.
        execute(&writer, &format!("DELETE FROM {target} WHERE id=2")).await?;
        let ctx = writer.create_query_context(&BUILD_INFO).await?;
        let latest = ctx
            .get_table("default", &db, &format!("{temp}_persistent"))
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
            table(&fixture, &format!("{temp}_persistent"))
                .await?
                .snapshot_loc(),
            latest.snapshot_loc()
        );
        assert_eq!(ids(&fixture, &format!("{temp}_persistent")).await?, ["1"]);
        let after = table(&fixture, temp).await?;
        assert_eq!(after.get_table_info().meta, before.get_table_info().meta);
        assert_eq!(hint(&after).await?, expected_temp_hint);
        let expected_ids = if initialized { vec!["0"] } else { vec![] };
        assert_eq!(ids(&fixture, temp).await?, expected_ids);
    }
    Ok(())
}
