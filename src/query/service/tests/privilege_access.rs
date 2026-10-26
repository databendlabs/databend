// Copyright 2026 Datafuse Labs
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

// Keep this integration target separate from `it`: the path-count test installs a process-wide
// fastrace reporter, which must not replace the reporter used by unrelated service tests.

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleIdent;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::UpsertKV;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::collector::TestReporter;
use fastrace::future::FutureExt;

async fn user_context(
    fixture: &TestFixture,
    user: UserInfo,
    auth_role: Option<String>,
) -> Result<Arc<QueryContext>> {
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    session.set_authed_user(user, auth_role).await?;
    session.create_query_context(&BUILD_INFO).await
}

async fn check_access(ctx: Arc<QueryContext>, sql: &str) -> Result<()> {
    let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
    InterpreterFactory::get(ctx, &plan).await.map(|_| ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_has_privilege_and_validate_privilege() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    let object = GrantObject::TableById("default".into(), db_id, table_id);
    let mut reader = RoleInfo::new("reader", None);
    reader
        .grants
        .grant_privileges(&object, UserPrivilegeType::Select.into());
    api.create_role(&tenant, reader, &CreateOption::Create)
        .await?;
    api.create_role(
        &tenant,
        RoleInfo::new("limited", None),
        &CreateOption::Create,
    )
    .await?;
    RoleCacheManager::instance().invalidate_cache(&tenant);

    let mut user = UserInfo::new_no_auth("grant_user", "%");
    user.grants.grant_role("reader".into());
    user.grants.grant_role("limited".into());
    user.grants
        .grant_privileges(&object, UserPrivilegeType::Insert.into());
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    session.set_authed_user(user, None).await?;
    session.set_current_role_checked("limited").await?;

    for (privilege, current_role_only, expected) in [
        (UserPrivilegeType::Select, false, true),
        (UserPrivilegeType::Select, true, false),
        (UserPrivilegeType::Insert, true, true),
        (UserPrivilegeType::Update, false, false),
    ] {
        assert_eq!(
            session
                .has_privilege(&object, privilege, current_role_only)
                .await?,
            expected,
        );
        let result = session
            .validate_privilege(&object, privilege, current_role_only)
            .await;
        if expected {
            result?;
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
            assert_eq!(error.message(), "Permission denied");
        }
    }

    // Local sessions preserve their authorization bypass even for users without grants.
    let local = fixture.new_session_with_type(SessionType::Local).await?;
    local
        .set_authed_user(UserInfo::new_no_auth("local_user", "%"), None)
        .await?;
    assert!(
        local
            .has_privilege(&object, UserPrivilegeType::Select, false)
            .await?
    );
    local
        .validate_privilege(&object, UserPrivilegeType::Select, false)
        .await?;

    // Neither API may turn a role-loading failure into an ordinary grant miss.
    let key = RoleIdent::new(tenant.clone(), "reader").to_string_key();
    api.get_meta_store_client()
        .upsert_kv(UpsertKV::update(&key, b"invalid role protobuf"))
        .await?;
    RoleCacheManager::instance().invalidate_cache(&tenant);
    let error = session
        .has_privilege(&object, UserPrivilegeType::Select, false)
        .await
        .unwrap_err();
    assert_ne!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");
    assert_eq!(
        session
            .validate_privilege(&object, UserPrivilegeType::Select, false)
            .await
            .unwrap_err()
            .code(),
        error.code(),
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_access_path_counts() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    let (reporter, spans) = TestReporter::new();
    fastrace::set_reporter(reporter, fastrace::collector::Config::default());

    let mut cases = Vec::new();
    let mut legacy = UserInfo::new_no_auth("legacy", "%");
    legacy.grants.grant_privileges(
        &GrantObject::Table("default".into(), "select_access".into(), "t".into()),
        UserPrivilegeType::Select.into(),
    );
    cases.push((legacy, [1, 0, 0, 0, 0], true));
    let mut by_id = UserInfo::new_no_auth("by_id", "%");
    by_id.grants.grant_privileges(
        &GrantObject::TableById("default".into(), db_id, table_id),
        UserPrivilegeType::Select.into(),
    );
    cases.push((by_id, [1, 1, 1, 0, 0], true));
    for (role, object, expected) in [
        (
            "table_owner",
            OwnershipObject::Table {
                catalog_name: "default".into(),
                db_id,
                table_id,
            },
            [1, 1, 1, 1, 0],
        ),
        (
            "database_owner",
            OwnershipObject::Database {
                catalog_name: "default".into(),
                db_id,
            },
            [1, 1, 1, 1, 1],
        ),
    ] {
        api.create_role(&tenant, RoleInfo::new(role, None), &CreateOption::Create)
            .await?;
        api.grant_ownership_to_role(&tenant, &object, role).await?;
        let mut user = UserInfo::new_no_auth(role, "%");
        user.grants.grant_role(role.to_string());
        cases.push((user, expected, true));
    }
    RoleCacheManager::instance().invalidate_cache(&tenant);
    cases.push((UserInfo::new_no_auth("denied", "%"), [1, 1, 1, 1, 1], false));

    for (user, expected, allowed) in cases {
        let ctx = user_context(&fixture, user, None).await?;
        // Trace only authorization, not initial binding. ID conversion must reuse the bound
        // table and perform exactly one get_database; ownership must never resolve names again.
        let (plan, _) = Planner::new(ctx.clone())
            .plan_sql("SELECT * FROM select_access.t")
            .await?;
        let trace = SpanContext::random();
        let result = InterpreterFactory::get(ctx, &plan)
            .in_span(Span::root("select_access_test", trace))
            .await;
        assert_eq!(result.is_ok(), allowed);
        if let Err(error) = result {
            assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
        }
        fastrace::flush();
        let spans = spans.lock();
        for (name, expected) in [
            "Access.NameGrant",
            "Access.ResolveIdentity",
            "Access.IdGrant",
            "Access.TableOwnership",
            "Access.DatabaseOwnership",
        ]
        .into_iter()
        .zip(expected)
        {
            let count = spans
                .iter()
                .filter(|span| span.trace_id == trace.trace_id && span.name == name)
                .count();
            assert_eq!(count, expected, "{name}");
        }
        // Span name comes from `#[fastrace::trace]` on `SchemaApi::get_database` in meta-api.
        let database_reads = spans
            .iter()
            .filter(|span| {
                span.trace_id == trace.trace_id
                    && span.name.split("::").any(|part| part == "get_database")
            })
            .count();
        assert_eq!(database_reads, expected[1]);
    }

    // A role-loading failure is not an unmatched grant. It must stop at NameGrant, even
    // for an owner, rather than trying identity resolution or masking it as PermissionDenied.
    let mut user = UserInfo::new_no_auth("owner", "%");
    user.grants.grant_role("table_owner".into());
    let ctx = user_context(&fixture, user, None).await?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("SELECT * FROM select_access.t")
        .await?;
    let key = RoleIdent::new(tenant.clone(), "table_owner").to_string_key();
    api.get_meta_store_client()
        .upsert_kv(UpsertKV::update(&key, b"invalid role protobuf"))
        .await?;
    RoleCacheManager::instance().invalidate_cache(&tenant);
    let trace = SpanContext::random();
    let error = InterpreterFactory::get(ctx, &plan)
        .in_span(Span::root("select_access_error_test", trace))
        .await
        .err()
        .expect("invalid role metadata must fail authorization");
    assert_ne!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");
    fastrace::flush();
    let spans = spans.lock();
    let paths: Vec<_> = spans
        .iter()
        .filter(|span| span.trace_id == trace.trace_id && span.name.starts_with("Access."))
        .map(|span| span.name.as_ref())
        .collect();
    assert_eq!(paths.len(), 2, "{paths:?}");
    assert!(paths.contains(&"Access.SelectTable"));
    assert!(paths.contains(&"Access.NameGrant"));
    Ok(())
}

async fn setup_table(fixture: &TestFixture) -> Result<(u64, u64)> {
    fixture
        .execute_command("CREATE DATABASE select_access")
        .await?;
    fixture
        .execute_command("CREATE TABLE select_access.t(a INT)")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let db_id = ctx
        .get_default_catalog()?
        .get_database(&fixture.default_tenant(), "select_access")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let table_id = ctx
        .get_table("default", "select_access", "t")
        .await?
        .get_id();
    // Keep PUBLIC from satisfying the owner fallback regardless of fixture defaults.
    let api = UserApiProvider::instance();
    for object in [
        OwnershipObject::Database {
            catalog_name: "default".into(),
            db_id,
        },
        OwnershipObject::Table {
            catalog_name: "default".into(),
            db_id,
            table_id,
        },
    ] {
        api.grant_ownership_to_role(
            &fixture.default_tenant(),
            &object,
            BUILTIN_ROLE_ACCOUNT_ADMIN,
        )
        .await?;
    }
    Ok((db_id, table_id))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_grants_skip_ownership() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let api = UserApiProvider::instance();
    let tenant = fixture.default_tenant();

    // An unreadable ownership record makes any accidental ownership lookup fail. This checks
    // short-circuiting, not just the final authorization result: ownership-first cannot pass.
    let meta = api.get_meta_store_client();
    for object in [
        OwnershipObject::Table {
            catalog_name: "default".into(),
            db_id,
            table_id,
        },
        OwnershipObject::Database {
            catalog_name: "default".into(),
            db_id,
        },
    ] {
        let key = TenantOwnershipObjectIdent::new(tenant.clone(), object).to_string_key();
        meta.upsert_kv(UpsertKV::update(&key, b"invalid ownership protobuf"))
            .await?;
    }

    for object in [
        GrantObject::Table("default".into(), "select_access".into(), "t".into()),
        GrantObject::Database("default".into(), "select_access".into()),
        GrantObject::TableById("default".into(), db_id, table_id),
        GrantObject::DatabaseById("default".into(), db_id),
        GrantObject::Global,
    ] {
        let mut user = UserInfo::new_no_auth("select_user", "%");
        user.grants
            .grant_privileges(&object, UserPrivilegeType::Select.into());
        check_access(
            user_context(&fixture, user, None).await?,
            "SELECT * FROM select_access.t",
        )
        .await?;
    }

    // Effective-role grants must take the same fast path, including inherited legacy grants.
    for (index, object) in [
        GrantObject::Table("default".into(), "select_access".into(), "t".into()),
        GrantObject::TableById("default".into(), db_id, table_id),
    ]
    .into_iter()
    .enumerate()
    {
        let child_name = format!("select_child_{index}");
        let parent_name = format!("select_parent_{index}");
        let mut child = RoleInfo::new(&child_name, None);
        child
            .grants
            .grant_privileges(&object, UserPrivilegeType::Select.into());
        api.create_role(&tenant, child, &CreateOption::Create)
            .await?;
        let mut parent = RoleInfo::new(&parent_name, None);
        parent.grants.grant_role(child_name);
        api.create_role(&tenant, parent, &CreateOption::Create)
            .await?;
        RoleCacheManager::instance().invalidate_cache(&tenant);
        let mut user = UserInfo::new_no_auth("select_user", "%");
        user.grants.grant_role(parent_name);
        check_access(
            user_context(&fixture, user, None).await?,
            "SELECT left_t.a FROM select_access.t left_t JOIN select_access.t right_t ON left_t.a = right_t.a",
        )
        .await?;
    }

    // Without a matching grant, the same unreadable record must be reported, not converted
    // into a permission denial (and certainly not accepted).
    let error = check_access(
        user_context(&fixture, UserInfo::new_no_auth("denied", "%"), None).await?,
        "SELECT * FROM select_access.t",
    )
    .await
    .unwrap_err();
    assert_ne!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");

    // Non-query operations still use their ownership-first path even with an explicit grant.
    let mut user = UserInfo::new_no_auth("insert_user", "%");
    user.grants.grant_privileges(
        &GrantObject::TableById("default".into(), db_id, table_id),
        UserPrivilegeType::Insert.into(),
    );
    let error = check_access(
        user_context(&fixture, user, None).await?,
        "INSERT INTO select_access.t VALUES (1)",
    )
    .await
    .unwrap_err();
    assert_ne!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_ownership_fallback_and_denial() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    for (role, object) in [
        ("table_owner", OwnershipObject::Table {
            catalog_name: "default".into(),
            db_id,
            table_id,
        }),
        ("database_owner", OwnershipObject::Database {
            catalog_name: "default".into(),
            db_id,
        }),
    ] {
        api.create_role(&tenant, RoleInfo::new(role, None), &CreateOption::Create)
            .await?;
        api.grant_ownership_to_role(&tenant, &object, role).await?;
    }
    RoleCacheManager::instance().invalidate_cache(&tenant);
    for role in ["table_owner", "database_owner"] {
        let mut user = UserInfo::new_no_auth("owner_user", "%");
        user.grants.grant_role(role.to_string());
        check_access(
            user_context(&fixture, user, None).await?,
            "SELECT * FROM select_access.t",
        )
        .await?;
    }

    let mut user = UserInfo::new_no_auth("denied", "%");
    user.grants.grant_privileges(
        &GrantObject::TableById("default".into(), db_id, table_id),
        UserPrivilegeType::Insert.into(),
    );
    let error = check_access(
        user_context(&fixture, user, None).await?,
        "SELECT * FROM select_access.t",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
    assert!(error.message().contains("privilege [Select]"), "{error}");
    assert!(
        error.message().contains("'default'.'select_access'.'t'"),
        "{error}"
    );

    // A grant on the first table must not stop authorization of the remaining tables.
    fixture
        .execute_command("CREATE TABLE select_access.allowed(a INT)")
        .await?;
    let mut user = UserInfo::new_no_auth("partial", "%");
    user.grants.grant_privileges(
        &GrantObject::Table("default".into(), "select_access".into(), "allowed".into()),
        UserPrivilegeType::Select.into(),
    );
    let error = check_access(
        user_context(&fixture, user, None).await?,
        "SELECT allowed.a FROM select_access.allowed JOIN select_access.t ON allowed.a = t.a",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
    assert!(
        error.message().contains("'default'.'select_access'.'t'"),
        "{error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_grants_respect_auth_role_and_secondary_roles() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    let mut reader = RoleInfo::new("reader", None);
    reader.grants.grant_privileges(
        &GrantObject::TableById("default".into(), db_id, table_id),
        UserPrivilegeType::Select.into(),
    );
    api.create_role(&tenant, reader, &CreateOption::Create)
        .await?;
    api.create_role(
        &tenant,
        RoleInfo::new("limited", None),
        &CreateOption::Create,
    )
    .await?;
    RoleCacheManager::instance().invalidate_cache(&tenant);
    let mut user = UserInfo::new_no_auth("role_user", "%");
    user.grants.grant_role("reader".into());
    user.grants.grant_role("limited".into());

    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    session.set_authed_user(user.clone(), None).await?;
    session.set_current_role_checked("limited").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    let error = check_access(
        session.create_query_context(&BUILD_INFO).await?,
        "SELECT * FROM select_access.t",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
    session.set_secondary_roles_checked(None).await?;
    check_access(
        session.create_query_context(&BUILD_INFO).await?,
        "SELECT * FROM select_access.t",
    )
    .await?;
    session.set_current_role_checked("reader").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    check_access(
        session.create_query_context(&BUILD_INFO).await?,
        "SELECT * FROM select_access.t",
    )
    .await?;

    // A restricted AUTH ROLE must not acquire grants from the user's other available roles.
    let error = check_access(
        user_context(&fixture, user.clone(), Some("limited".into())).await?,
        "SELECT * FROM select_access.t",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
    check_access(
        user_context(&fixture, user, Some("reader".into())).await?,
        "SELECT * FROM select_access.t",
    )
    .await?;
    Ok(())
}
