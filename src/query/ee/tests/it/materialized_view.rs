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

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_store::LocalMetaService;
use databend_common_users::UserApiProvider;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_client::types::MatchSeq;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON;

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

/// Clear the invalid reason of MV `mv_id`, as if the write after source DDL was lost.
async fn clear_mv_invalid_reason(mv_id: u64) -> anyhow::Result<()> {
    UserApiProvider::instance()
        .get_meta_store_client()
        .upsert_table_option(UpsertTableOptionReq {
            table_id: mv_id,
            seq: MatchSeq::Any,
            options: HashMap::from([(OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(), None)]),
        })
        .await?;
    Ok(())
}

async fn mv_invalid_reason(fixture: &TestFixture, mv_id: u64) -> anyhow::Result<Option<String>> {
    let ctx = fixture.new_query_ctx().await?;
    let table_meta = ctx
        .get_default_catalog()?
        .get_table_meta_by_id(mv_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("materialized view {mv_id} must exist"))?;
    Ok(table_meta
        .data
        .materialized_view_invalid_reason()
        .map(str::to_string))
}

/// A column change whose invalid reason was lost: REFRESH records it while the definition still
/// binds; once it no longer binds, reads and REFRESH fail without recording it.
#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_view_lost_invalid_reason() -> anyhow::Result<()> {
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("mv-lost-reason").await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    for sql in [
        "CREATE DATABASE mv_reason",
        // Empty, so a type change does not rewrite the data, which CHANGE_TRACKING rejects.
        "CREATE TABLE mv_reason.source(a INT, b INT) CHANGE_TRACKING = true",
        "CREATE MATERIALIZED VIEW mv_reason.mv AS SELECT a, b FROM mv_reason.source WHERE a > 0",
        "REFRESH MATERIALIZED VIEW mv_reason.mv",
    ] {
        fixture.execute_command(sql).await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let mv_id = ctx
        .get_default_catalog()?
        .get_table(&ctx.get_tenant(), "mv_reason", "mv")
        .await?
        .get_id();
    let select = "SELECT * FROM mv_reason.mv";
    let refresh = "REFRESH MATERIALIZED VIEW mv_reason.mv";

    fixture
        .execute_command("ALTER TABLE mv_reason.source MODIFY COLUMN b BIGINT")
        .await?;
    clear_mv_invalid_reason(mv_id).await?;
    let error = fixture.execute_command(refresh).await.unwrap_err();
    assert_eq!(
        ErrorCode::INVALID_MATERIALIZED_VIEW,
        error.code(),
        "{error}"
    );
    assert_eq!(
        mv_invalid_reason(&fixture, mv_id).await?.as_deref(),
        Some(MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON)
    );

    fixture
        .execute_command("ALTER TABLE mv_reason.source RENAME COLUMN b TO b2")
        .await?;
    clear_mv_invalid_reason(mv_id).await?;
    for sql in [select, refresh] {
        let error = fixture.execute_command(sql).await.unwrap_err();
        assert_eq!(
            ErrorCode::INVALID_MATERIALIZED_VIEW,
            error.code(),
            "{error}"
        );
    }
    assert_eq!(mv_invalid_reason(&fixture, mv_id).await?, None);
    Ok(())
}
