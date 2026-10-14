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

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_sql::Planner;
use databend_common_sql::binder::ddl::table::validate_attach_table_privileges;
use databend_common_sql::plans::Plan;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;

fn user_with_roles(name: &str, roles: &[&str]) -> UserInfo {
    let mut user = UserInfo::new_no_auth(name, "%");
    for role in roles {
        user.grants.grant_role((*role).to_string());
    }
    user
}

async fn user_context(
    fixture: &TestFixture,
    user: UserInfo,
    current_role: &str,
) -> anyhow::Result<Arc<QueryContext>> {
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    session.set_authed_user(user, None).await?;
    session.set_current_role_checked(current_role).await?;
    Ok(session.create_query_context(&BUILD_INFO).await?)
}

async fn setup_roles(fixture: &TestFixture) -> anyhow::Result<()> {
    let session = fixture.default_session();
    session
        .set_authed_user(
            user_with_roles("root", &["account_admin"]),
            Some("account_admin".to_string()),
        )
        .await?;
    session.set_current_role_checked("account_admin").await?;
    for sql in [
        "CREATE DATABASE attach_target",
        "CREATE DATABASE attach_other",
        "CREATE ROLE attach_creator",
        "CREATE ROLE attach_operator",
        "CREATE ROLE attach_parent",
        "CREATE ROLE attach_owner",
        "GRANT CREATE ON attach_target.* TO ROLE attach_creator",
        "GRANT CREATE ON attach_other.* TO ROLE attach_creator",
        "GRANT SUPER ON *.* TO ROLE attach_creator",
        "GRANT ATTACH TABLE ON attach_target.* TO ROLE attach_operator",
        "GRANT ROLE attach_operator TO ROLE attach_parent",
    ] {
        fixture.execute_command(sql).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_attach_table_privileges() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup_roles(&fixture).await?;

    // Current ID-based grants, including inherited role grants.
    for attach_role in ["attach_operator", "attach_parent"] {
        let ctx = user_context(
            &fixture,
            user_with_roles("operator", &["attach_creator", attach_role]),
            "attach_creator",
        )
        .await?;
        validate_attach_table_privileges(ctx.as_ref(), "default", "attach_target").await?;
        let err = validate_attach_table_privileges(ctx.as_ref(), "default", "attach_other")
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
        assert!(err.message().contains("[ATTACH TABLE]"));
    }

    // ATTACH does not replace CREATE, even when CREATE is on a secondary role.
    let ctx = user_context(
        &fixture,
        user_with_roles("secondary_creator", &["attach_creator", "attach_operator"]),
        "attach_operator",
    )
    .await?;
    let err = validate_attach_table_privileges(ctx.as_ref(), "default", "attach_target")
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
    assert!(err.message().contains("[CREATE]"));

    // Direct USER grants are allowed for ATTACH; CREATE still comes from the role.
    // Also exercise the legacy name-based grants and global grants.
    for object in [
        GrantObject::Database("default".to_string(), "attach_target".to_string()),
        GrantObject::Global,
    ] {
        let mut user = user_with_roles("direct_attach", &["attach_creator"]);
        user.grants
            .grant_privileges(&object, UserPrivilegeType::AttachTable.into());
        let ctx = user_context(&fixture, user, "attach_creator").await?;
        validate_attach_table_privileges(ctx.as_ref(), "default", "attach_target").await?;
    }

    let ctx = user_context(
        &fixture,
        user_with_roles("admin", &["account_admin"]),
        "account_admin",
    )
    .await?;
    validate_attach_table_privileges(ctx.as_ref(), "default", "attach_target").await?;

    // Authorized attachment still probes storage and binds to CREATE TABLE.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let sql = format!(
        "ATTACH TABLE attach_target.t 's3://bucket/data/1/2' CONNECTION = (access_key_id = 'key' secret_access_key = 'secret' region = 'us-east-1' endpoint_url = '{}')",
        server.uri()
    );
    let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
    assert!(
        matches!(&plan, Plan::CreateTable(create) if create.options.contains_key(OPT_KEY_STORAGE_PREFIX))
    );
    InterpreterFactory::get(ctx, &plan).await?;
    assert!(!server.received_requests().await.unwrap().is_empty());

    fixture
        .execute_command("REVOKE ATTACH TABLE ON attach_target.* FROM ROLE attach_operator")
        .await?;
    let ctx = user_context(
        &fixture,
        user_with_roles("revoked", &["attach_creator", "attach_parent"]),
        "attach_creator",
    )
    .await?;
    let err = validate_attach_table_privileges(ctx.as_ref(), "default", "attach_target")
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
    assert!(err.message().contains("[ATTACH TABLE]"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_attach_table_denied_before_storage_access() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup_roles(&fixture).await?;
    fixture
        .execute_command("GRANT OWNERSHIP ON attach_target.* TO ROLE attach_owner")
        .await?;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&server)
        .await;
    let connection = format!(
        "CONNECTION = (access_key_id = 'key' secret_access_key = 'secret' endpoint_url = '{}')",
        server.uri()
    );
    for (role, missing) in [
        ("public", "CREATE"),
        ("attach_creator", "ATTACH TABLE"),
        ("attach_operator", "CREATE"),
        ("attach_owner", "ATTACH TABLE"),
    ] {
        let ctx = user_context(&fixture, user_with_roles("denied", &[role]), role).await?;
        for sql in [
            format!("ATTACH TABLE attach_target.t 's3://bucket/data/1/2' {connection}"),
            // A missing connection must not mask the missing target privilege.
            "ATTACH TABLE attach_target.t 's3://bucket/data/1/2' CONNECTION = (connection_name = 'missing')".to_string(),
            // Ordinary CREATE cannot bypass ATTACH by supplying internal options.
            format!("CREATE TABLE attach_target.t (a INT) 's3://bucket/data/' {connection} STORAGE_PREFIX = '1/2'"),
        ] {
            let err = Planner::new(ctx.clone()).plan_sql(&sql).await.unwrap_err();
            assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED, "{sql}: {err}");
            assert!(err.message().contains(&format!("[{missing}]")), "{err}");
        }
    }
    assert!(server.received_requests().await.unwrap().is_empty());

    // Keep the plan-level guard for plans not coming through the ATTACH binder.
    let ctx = user_context(
        &fixture,
        user_with_roles("creator", &["attach_creator"]),
        "attach_creator",
    )
    .await?;
    let (mut plan, _) = Planner::new(ctx.clone())
        .plan_sql("CREATE TABLE attach_target.ordinary (a INT)")
        .await?;
    InterpreterFactory::get(ctx.clone(), &plan).await?;
    let Plan::CreateTable(create) = &mut plan else {
        panic!("expected CREATE TABLE plan");
    };
    create
        .options
        .insert(OPT_KEY_STORAGE_PREFIX.to_string(), "1/2".to_string());
    // A fake temporary-table option must not skip the attachment guard.
    create
        .options
        .insert(OPT_KEY_TEMP_PREFIX.to_string(), "temp".to_string());
    let err = InterpreterFactory::get(ctx, &plan)
        .await
        .err()
        .expect("attachment plan must require ATTACH TABLE");
    assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
    assert!(err.message().contains("[ATTACH TABLE]"));
    Ok(())
}
