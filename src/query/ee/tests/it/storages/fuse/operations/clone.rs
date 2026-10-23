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

use databend_common_config::MetaConfig;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::query_count;

async fn assert_secured_rows(
    fixture: &TestFixture,
    table: &str,
    column: &str,
    expected: u64,
) -> anyhow::Result<()> {
    for sql in [
        format!("SELECT count(*) FROM {table}"),
        format!("SELECT count(*) FROM {table} WHERE id < 3 AND {column} = '***'"),
    ] {
        assert_eq!(
            query_count(fixture.execute_query(&sql).await?).await?,
            expected,
            "{sql}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_inherited_policies_for_restricted_owner() -> anyhow::Result<()> {
    // Catalog metadata and policy APIs must share one Meta service, as they do in production.
    let meta = MetaStoreProvider::new(MetaConfig::default().to_meta_grpc_client_conf())
        .create_meta_store::<DatabendRuntime>()
        .await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.inner().endpoints.clone();
    let fixture = TestFixture::setup_with_custom(setup).await?;
    fixture.enable_experimental_clone_table()?;
    let session = fixture.default_session();
    let mut admin = session.get_current_user()?;
    admin.grants.grant_role("account_admin".to_string());
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    // Keep source objects and policies out of PUBLIC ownership. Merely changing an admin's
    // current role would not prove the authorization of an actual unprivileged clone owner.
    for sql in [
        "SET enable_experimental_row_access_policy = 1",
        "CREATE DATABASE policy_source",
        "CREATE DATABASE policy_target",
        "CREATE TABLE policy_source.t(id INT, secret STRING)",
        "INSERT INTO policy_source.t VALUES (1, 'raw-one'), (3, 'raw-three')",
        "CREATE MASKING POLICY clone_mask AS (val STRING) RETURNS STRING -> '***'",
        "CREATE ROW ACCESS POLICY clone_rows AS (id INT) RETURNS BOOLEAN -> id < 3",
        "ALTER TABLE policy_source.t MODIFY COLUMN secret SET MASKING POLICY clone_mask",
        "ALTER TABLE policy_source.t ADD ROW ACCESS POLICY clone_rows ON (id)",
    ] {
        fixture.execute_command(sql).await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_default_catalog()?;
    let tenant = ctx.get_tenant();
    let source = catalog.get_table(&tenant, "policy_source", "t").await?;
    let snapshot = FuseTable::try_from_table(source.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    fixture
        .execute_command("INSERT INTO policy_source.t VALUES (2, 'raw-two')")
        .await?;
    let source_db = catalog
        .get_database(&tenant, "policy_source")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let target_db = catalog
        .get_database(&tenant, "policy_target")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let mut creator = UserInfo::new_no_auth("clone_policy_owner", "%");
    creator.grants.grant_privileges(
        &GrantObject::TableById("default".into(), source_db, source.get_id()),
        UserPrivilegeType::Select.into(),
    );
    creator.grants.grant_privileges(
        &GrantObject::DatabaseById("default".into(), target_db),
        UserPrivilegeType::Create.into(),
    );
    session.set_authed_user(creator.clone(), None).await?;
    session.set_current_role_checked("public").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;

    for (name, point, expected) in [
        ("latest", String::new(), 2),
        (
            "historical",
            format!(" AT (SNAPSHOT => '{}')", snapshot.snapshot_id.simple()),
            1,
        ),
    ] {
        let target = format!("policy_target.{name}");
        fixture
            .execute_command(&format!(
                "CREATE TABLE {target} CLONE policy_source.t{point}"
            ))
            .await?;
        assert_secured_rows(&fixture, &target, "secret", expected).await?;
        // Successful schema DDL demonstrates table ownership, rather than a denial caused by
        // missing ALTER privileges. Policies must keep following their stable column IDs.
        fixture
            .execute_command(&format!(
                "ALTER TABLE {target} RENAME COLUMN secret TO redacted"
            ))
            .await?;
        for sql in [
            format!("ALTER TABLE {target} MODIFY COLUMN redacted UNSET MASKING POLICY"),
            format!("ALTER TABLE {target} DROP ROW ACCESS POLICY clone_rows"),
            format!("ALTER TABLE {target} DROP ALL ROW ACCESS POLICIES"),
        ] {
            let error = fixture.execute_command(&sql).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{sql}: {error}");
        }
        assert_secured_rows(&fixture, &target, "redacted", expected).await?;
    }

    // Removing the source's policies must not change the policies enforced on its clones.
    session.set_authed_user(admin, None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("ALTER TABLE policy_source.t MODIFY COLUMN secret UNSET MASKING POLICY")
        .await?;
    fixture
        .execute_command("ALTER TABLE policy_source.t DROP ROW ACCESS POLICY clone_rows")
        .await?;
    session.set_authed_user(creator, None).await?;
    session.set_current_role_checked("public").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    assert_secured_rows(&fixture, "policy_target.latest", "redacted", 2).await?;
    assert_secured_rows(&fixture, "policy_target.historical", "redacted", 1).await?;
    Ok(())
}
