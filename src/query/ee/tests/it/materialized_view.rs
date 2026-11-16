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
use databend_common_users::UserApiProvider;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;

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
