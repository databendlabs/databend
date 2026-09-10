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

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_version::BUILD_INFO;
use databend_enterprise_fail_safe::FailSafeHandler;
use databend_enterprise_query::fail_safe::RealFailSafeHandler;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_query;
use futures::TryStreamExt;

use super::shared_stream::command;
use super::shared_stream::rows;
use super::shared_stream::setup;

async fn reader(fixture: &TestFixture, user: UserInfo) -> anyhow::Result<Arc<Session>> {
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    session
        .get_settings()
        .set_setting("sandbox_tenant".to_string(), "stream_consumer".to_string())?;
    session
        .get_settings()
        .set_setting("enable_experimental_table_ref".to_string(), "1".to_string())?;
    session.set_authed_user(user, None).await?;
    Ok(session)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_functions_require_select() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t CLUSTER BY (a)")
        .await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (73)")
        .await?;

    let functions = [
        "fuse_block('shared', 't')",
        "fuse_block_statistics('shared', 't')",
        "fuse_segment('shared', 't')",
        "fuse_column('shared', 't')",
        "fuse_page('shared', 't')",
        "fuse_virtual_column('shared', 't')",
        "clustering_statistics('shared', 't')",
        "fuse_snapshot('shared', 't')",
        "fuse_tag('shared', 't')",
        "fuse_dump_snapshots('shared', 't')",
        "fuse_statistic('shared', 't')",
        "fuse_statistic('default', 'shared', 't')",
        "clustering_information('shared', 't')",
        "fuse_encoding('shared', 't')",
        "table_statistics('shared', 't')",
    ];

    for privilege in [None, Some(UserPrivilegeType::Alter)] {
        let mut user = UserInfo::new_no_auth("diagnostics_reader", "%");
        if let Some(privilege) = privilege {
            user.grants.grant_privileges(
                &GrantObject::Database("default".into(), "shared".into()),
                privilege.into(),
            );
        }
        let session = reader(&fixture, user).await?;
        let error = rows(&session, "SELECT a FROM shared.t").await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");
        for function in functions {
            let sql = format!("SELECT * FROM {function}");
            let error = rows(&session, &sql).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{sql}: {error}");
        }
        for function in ["fuse_encoding('shared')", "table_statistics('shared')"] {
            assert_eq!(
                rows(&session, &format!("SELECT * FROM {function}")).await?,
                0
            );
        }
    }

    // SELECT on one table permits its diagnostics while database-wide functions
    // must filter other shared tables, including ones with nonempty snapshots.
    for sql in [
        "CREATE TABLE provider.hidden(a INT)",
        "INSERT INTO provider.hidden VALUES (99)",
        "GRANT SELECT ON TABLE provider.hidden TO SHARE s",
    ] {
        fixture.execute_command(sql).await?;
    }
    command(&consumer, "CREATE ROLE diagnostics_reader").await?;
    command(
        &consumer,
        "GRANT SELECT ON shared.t TO ROLE diagnostics_reader",
    )
    .await?;
    let mut user = UserInfo::new_no_auth("diagnostics_reader", "%");
    user.grants.grant_role("diagnostics_reader".to_string());
    let session = reader(&fixture, user).await?;
    assert_eq!(rows(&session, "SELECT a FROM shared.t").await?, 1);
    for function in functions {
        rows(&session, &format!("SELECT * FROM {function}")).await?;
    }
    for sql in [
        "SELECT DISTINCT table_name FROM fuse_encoding('shared')",
        "SELECT `table` FROM table_statistics('shared')",
    ] {
        let blocks: Vec<DataBlock> =
            execute_query(session.create_query_context(&BUILD_INFO).await?, sql)
                .await?
                .try_collect()
                .await?;
        let block = DataBlock::concat(&blocks)?;
        assert_eq!(block.num_rows(), 1, "{sql}");
        assert_eq!(
            block.get_by_offset(0).index(0),
            Some(ScalarRef::String("t"))
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_functions_grants_and_ownership() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (73)")
        .await?;
    command(
        &consumer,
        &format!(
            "CREATE DATABASE shared_alias FROM SHARE {}.s",
            fixture.default_tenant().tenant_name()
        ),
    )
    .await?;
    let ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let catalog = ctx.get_catalog("default").await?;
    let database_id = catalog
        .get_database(&ctx.get_tenant(), "shared")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let alias_id = catalog
        .get_database(&ctx.get_tenant(), "shared_alias")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let provider_id = catalog
        .get_database(&fixture.default_tenant(), "provider")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let table_id = ctx.get_table("default", "shared", "t").await?.get_id();
    let sql = "SELECT statistics FROM fuse_block_statistics('shared', 't')";

    for (object, allowed) in [
        (
            GrantObject::Table("default".into(), "shared".into(), "t".into()),
            true,
        ),
        (
            GrantObject::Database("default".into(), "shared".into()),
            true,
        ),
        (
            GrantObject::TableById("default".into(), database_id, table_id),
            true,
        ),
        (
            GrantObject::DatabaseById("default".into(), database_id),
            true,
        ),
        (
            GrantObject::TableById("default".into(), alias_id, table_id),
            false,
        ),
        (
            GrantObject::TableById("default".into(), provider_id, table_id),
            false,
        ),
    ] {
        let mut user = UserInfo::new_no_auth("diagnostics_reader", "%");
        user.grants
            .grant_privileges(&object, UserPrivilegeType::Select.into());
        let session = reader(&fixture, user).await?;
        let result = rows(&session, sql).await;
        if allowed {
            assert_eq!(result?, 1, "{object}");
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.code(),
                ErrorCode::PERMISSION_DENIED,
                "{object}: {error}"
            );
        }
    }

    command(&consumer, "CREATE ROLE diagnostics_owner").await?;
    command(&consumer, "CREATE ROLE diagnostics_table_owner").await?;
    // The fixture's root user has global privileges but no roles. Ownership
    // transfer requires both the existing and target owner roles to be available.
    let mut admin = consumer.get_current_user()?;
    admin.grants.grant_role("account_admin".to_string());
    admin.grants.grant_role("diagnostics_owner".to_string());
    admin
        .grants
        .grant_role("diagnostics_table_owner".to_string());
    consumer.set_authed_user(admin, None).await?;
    command(
        &consumer,
        "GRANT OWNERSHIP ON shared.* TO ROLE diagnostics_owner",
    )
    .await?;
    let mut owner = UserInfo::new_no_auth("diagnostics_owner", "%");
    owner.grants.grant_role("diagnostics_owner".to_string());
    let session = reader(&fixture, owner).await?;
    assert_eq!(rows(&session, "SELECT a FROM shared.t").await?, 1);
    assert_eq!(rows(&session, sql).await?, 1);
    let error = rows(
        &session,
        "SELECT * FROM fuse_block_statistics('shared_alias', 't')",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");

    command(
        &consumer,
        "GRANT OWNERSHIP ON shared.t TO ROLE diagnostics_table_owner",
    )
    .await?;
    let mut owner = UserInfo::new_no_auth("diagnostics_table_owner", "%");
    owner
        .grants
        .grant_role("diagnostics_table_owner".to_string());
    let session = reader(&fixture, owner).await?;
    assert_eq!(rows(&session, "SELECT a FROM shared.t").await?, 1);
    assert_eq!(rows(&session, sql).await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_functions_reject_arbitrary_block_paths() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    for sql in [
        "CREATE TABLE provider.public_json(v VARIANT)",
        "CREATE TABLE provider.private_json(v VARIANT)",
        "INSERT INTO provider.public_json SELECT parse_json('{\"public\": 1}')",
        "INSERT INTO provider.private_json SELECT parse_json('{\"private_only_field\": 123}')",
        "GRANT SELECT ON TABLE provider.public_json TO SHARE s",
        "GRANT SELECT ON TABLE provider.private_json TO SHARE s",
    ] {
        fixture.execute_command(sql).await?;
    }
    let blocks: Vec<DataBlock> = execute_query(
        consumer.create_query_context(&BUILD_INFO).await?,
        "SELECT block_location FROM fuse_block('shared', 'private_json')",
    )
    .await?
    .try_collect()
    .await?;
    let block = DataBlock::concat(&blocks)?;
    let Some(ScalarRef::String(location)) = block.get_by_offset(0).index(0) else {
        panic!("expected block location")
    };
    fixture
        .execute_command("REVOKE SELECT ON TABLE provider.private_json FROM SHARE s")
        .await?;
    assert!(
        rows(&consumer, "SELECT * FROM shared.private_json")
            .await
            .is_err()
    );
    // Even a consumer administrator cannot use an authorized table's operator
    // to inspect a retained block path from a revoked provider table.
    let sql =
        format!("SELECT * FROM fuse_virtual_column_build('shared', 'public_json', '{location}')");
    let error = rows(&consumer, &sql).await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::INVALID_OPERATION, "{error}");
    assert!(
        error.message().contains("not supported on shared tables"),
        "{error}"
    );

    let blocks: Vec<DataBlock> = fixture
        .execute_query(&format!(
            "SELECT * FROM fuse_virtual_column_build('provider', 'private_json', '{location}')"
        ))
        .await?
        .try_collect()
        .await?;
    assert!(blocks.iter().map(DataBlock::num_rows).sum::<usize>() > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_functions_cannot_amend() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    RealFailSafeHandler::init()?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (73)")
        .await?;
    let error = rows(&consumer, "SELECT * FROM fuse_amend('shared', 't')")
        .await
        .unwrap_err();
    assert!(error.message().contains("READ ONLY"), "{error}");

    // The recovery handler also enforces read-only status for non-SQL callers.
    let ctx: Arc<dyn TableContext> = consumer.create_query_context(&BUILD_INFO).await?;
    let table = ctx.get_table("default", "shared", "t").await?;
    let error = RealFailSafeHandler {}
        .recover_table_data(&ctx, table.get_table_info().clone())
        .await
        .unwrap_err();
    assert!(error.message().contains("READ ONLY"), "{error}");

    let blocks: Vec<DataBlock> = fixture
        .execute_query("SELECT * FROM fuse_amend('provider', 't')")
        .await?
        .try_collect()
        .await?;
    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 1);
    assert_eq!(rows(&consumer, "SELECT a FROM shared.t").await?, 1);
    Ok(())
}
