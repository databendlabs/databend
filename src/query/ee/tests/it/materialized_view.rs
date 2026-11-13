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

use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_store::LocalMetaService;
use databend_common_storages_fuse::FuseTable;
use databend_common_users::UserApiProvider;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;

#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_view_revoke_all_legacy_grants() -> anyhow::Result<()> {
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("mv-legacy-grants").await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    for sql in [
        "CREATE DATABASE mv_legacy_grants",
        "CREATE TABLE mv_legacy_grants.source(c INT) CHANGE_TRACKING = true",
        "CREATE MATERIALIZED VIEW mv_legacy_grants.mv AS SELECT c FROM mv_legacy_grants.source",
        "CREATE ROLE mv_legacy_reader",
    ] {
        fixture.execute_command(sql).await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let catalog = ctx.get_default_catalog()?;
    let db_id = catalog
        .get_database(&tenant, "mv_legacy_grants")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let table_id = catalog
        .get_table(&tenant, "mv_legacy_grants", "mv")
        .await?
        .get_id();
    let users = UserApiProvider::instance();
    // Old GRANT ALL accepted table privileges on MVs. Seed persisted grants through
    // the same API, since the explicit MV syntax intentionally cannot create them.
    for object in [
        GrantObject::TableById("default".into(), db_id, table_id),
        GrantObject::Table("default".into(), "mv_legacy_grants".into(), "mv".into()),
    ] {
        let privileges = object.available_privileges(false);
        users
            .grant_privileges_to_role(&tenant, "mv_legacy_reader", object, privileges)
            .await?;
    }
    let before = users.get_role(&tenant, "mv_legacy_reader".into()).await?;
    assert_eq!(before.grants.entries().len(), 2);
    fixture
        .execute_command(
            "REVOKE ALL ON MATERIALIZED VIEW mv_legacy_grants.mv FROM ROLE mv_legacy_reader",
        )
        .await?;
    let after = users.get_role(&tenant, "mv_legacy_reader".into()).await?;
    assert!(after.grants.entries().is_empty(), "{:?}", after.grants);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_view_compacts_before_return_and_releases_refresh_lock()
-> anyhow::Result<()> {
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("mv-best-effort-hooks").await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    fixture
        .default_session()
        .get_settings()
        .set_setting("acquire_lock_timeout".into(), "2".into())?;
    for sql in [
        "CREATE DATABASE mv_hooks",
        "CREATE TABLE mv_hooks.source(id INT) auto_compaction_imperfect_blocks_threshold=0",
        "CREATE MATERIALIZED VIEW mv_hooks.mv AS SELECT id FROM mv_hooks.source WHERE 1=1",
        "ALTER MATERIALIZED VIEW mv_hooks.mv SET OPTIONS (row_per_block=3, auto_compaction_imperfect_blocks_threshold=3)",
        "ALTER MATERIALIZED VIEW mv_hooks.mv CLUSTER BY (id)",
    ] {
        fixture.execute_command(sql).await?;
    }
    for id in [3, 1, 2] {
        fixture
            .execute_command(&format!("INSERT INTO mv_hooks.source VALUES ({id})"))
            .await?;
        fixture
            .execute_command("REFRESH MATERIALIZED VIEW mv_hooks.mv")
            .await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "mv_hooks", "mv").await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(snapshot.summary.row_count, 3);
    assert_eq!(
        snapshot.summary.block_count, 1,
        "Compact must finish before REFRESH returns"
    );

    // A delayed Recluster demand may still be queued, but it must not retain the MV's
    // checkpoint/lifecycle lock or capture a subsequent refresh's transaction context.
    fixture
        .execute_command("INSERT INTO mv_hooks.source VALUES (4)")
        .await?;
    fixture
        .execute_command("REFRESH MATERIALIZED VIEW mv_hooks.mv")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "mv_hooks", "mv").await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(snapshot.summary.row_count, 4);
    let checkpoint = table
        .get_table_info()
        .meta
        .options
        .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION)
        .cloned();
    assert!(
        checkpoint.is_some(),
        "refresh must persist its logical source checkpoint"
    );
    fixture
        .execute_command("REFRESH MATERIALIZED VIEW mv_hooks.mv")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "mv_hooks", "mv").await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(
        snapshot.summary.row_count, 4,
        "repeated refresh must not replay source rows"
    );
    assert_eq!(
        table
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION)
            .cloned(),
        checkpoint,
        "physical maintenance must not advance the logical source checkpoint"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_view_rejects_source_moved_across_databases() -> anyhow::Result<()> {
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("mv-moved-source").await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    for sql in [
        "CREATE DATABASE mv_moved_old",
        "CREATE DATABASE mv_moved_new",
        "CREATE TABLE mv_moved_old.t(id INT) CHANGE_TRACKING = true",
    ] {
        fixture.execute_command(sql).await?;
    }
    // The binder has rejected cross-database RENAME TABLE since #14098; older versions moved the
    // table without updating its recorded database id. Seed that state through the catalog.
    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    ctx.get_default_catalog()?
        .rename_table(RenameTableReq {
            if_exists: false,
            name_ident: TableNameIdent {
                tenant: tenant.clone(),
                db_name: "mv_moved_old".to_string(),
                table_name: "t".to_string(),
            },
            new_db_name: "mv_moved_new".to_string(),
            new_table_name: "t".to_string(),
        })
        .await?;

    let error = fixture
        .execute_command(
            "CREATE MATERIALIZED VIEW mv_moved_new.mv AS SELECT id FROM mv_moved_new.t WHERE 1 = 1",
        )
        .await
        .unwrap_err();
    assert_eq!(
        ErrorCode::INVALID_MATERIALIZED_VIEW,
        error.code(),
        "{error}"
    );
    Ok(())
}
