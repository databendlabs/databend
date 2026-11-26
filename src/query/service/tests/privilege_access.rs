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

// Keep this integration target separate from `it`: the path-count tests install a process-wide
// fastrace reporter, which must not replace the reporter used by unrelated service tests.

use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_ast::ast::SetType;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleIdent;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::Object;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::UpsertKV;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextAuthorization;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sql::Planner;
use databend_query::sql::plans::Plan;
use databend_query::sql::plans::RewriteKind;
use databend_query::sql::plans::SetPlan;
use databend_query::sql::plans::SetScalarsOrQuery;
use databend_query::test_kits::TestFixture;
use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::collector::SpanRecord;
use fastrace::collector::TestReporter;
use fastrace::future::FutureExt;
use parking_lot::Mutex;

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

async fn assert_plan_denied(ctx: Arc<QueryContext>, plan: &Plan) {
    let err = InterpreterFactory::get(ctx, plan)
        .await
        .err()
        .expect("expected permission denial before execution");
    assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED, "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_checks_query_source_privileges() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let object = GrantObject::TableById("default".into(), db_id, table_id);

    for (has_select, has_super) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut user = UserInfo::new_no_auth("set_reader", "%");
        if has_select {
            user.grants
                .grant_privileges(&object, UserPrivilegeType::Select.into());
        }
        if has_super {
            user.grants
                .grant_privileges(&GrantObject::Global, UserPrivilegeType::Super.into());
        }
        let ctx = user_context(&fixture, user, None).await?;
        let (query, _) = Planner::new(ctx.clone())
            .plan_sql("SELECT a FROM select_access.t")
            .await?;
        // Exercise the SET authorization boundary without running the embedded query.
        for (set_type, name) in [
            (SetType::Variable, "value"),
            (SetType::SettingsSession, "max_threads"),
            (SetType::SettingsGlobal, "max_threads"),
        ] {
            let global = matches!(set_type, SetType::SettingsGlobal);
            let plan = Plan::Set(Box::new(SetPlan {
                set_type,
                idents: vec![name.into()],
                values: SetScalarsOrQuery::Query(Box::new(query.clone())),
            }));
            if has_select && (!global || has_super) {
                InterpreterFactory::get(ctx.clone(), &plan).await?;
            } else {
                assert_plan_denied(ctx.clone(), &plan).await;
            }
        }
        check_access(ctx, "SET VARIABLE value = 1").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_checks_user_references() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, table_id) = setup_table(&fixture).await?;
    let object = GrantObject::TableById("default".into(), db_id, table_id);
    fixture
        .execute_command("CREATE TABLE select_access.show_target(a INT)")
        .await?;
    let show_target = GrantObject::Table(
        "default".into(),
        "select_access".into(),
        "show_target".into(),
    );
    for (sql, needs_target_grant) in [
        ("SHOW DATABASES", false),
        ("SHOW ENGINES", false),
        ("SHOW TABLE_FUNCTIONS", false),
        ("SHOW TABLES FROM select_access", true),
        ("SHOW COLUMNS FROM select_access.show_target", true),
    ] {
        for has_select in [false, true] {
            let mut user = UserInfo::new_no_auth("show_reader", "%");
            if needs_target_grant {
                user.grants
                    .grant_privileges(&show_target, UserPrivilegeType::Select.into());
            }
            if has_select {
                user.grants
                    .grant_privileges(&object, UserPrivilegeType::Select.into());
            }
            let ctx = user_context(&fixture, user, None).await?;
            let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
            InterpreterFactory::get(ctx.clone(), &plan).await?;
            let Plan::Query { metadata, .. } = &plan else {
                panic!("expected a rewritten query for {sql}");
            };
            // Register a user reference in the authorization metadata without changing
            // the executable query. Permissions must apply to both kinds of SHOW plans.
            let table = ctx.get_table("default", "select_access", "t").await?;
            metadata.write().add_table(
                "default".into(),
                "select_access".into(),
                table,
                None,
                false,
                false,
                None,
            );
            if has_select {
                InterpreterFactory::get(ctx.clone(), &plan).await?;
            } else {
                assert_plan_denied(ctx.clone(), &plan).await;
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_checks_additional_reference_to_generated_system_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = user_context(&fixture, UserInfo::new_no_auth("show_only", "%"), None).await?;
    let (plan, _) = Planner::new(ctx.clone()).plan_sql("SHOW ENGINES").await?;
    InterpreterFactory::get(ctx.clone(), &plan).await?;
    let Plan::Query { metadata, .. } = &plan else {
        panic!("expected a rewritten SHOW query");
    };
    {
        let mut metadata = metadata.write();
        let generated = metadata
            .tables()
            .iter()
            .find(|table| table.is_show_system_table())
            .expect("the generated FROM must be exempt");
        let table = generated.table();
        metadata.add_table(
            "default".into(),
            "system".into(),
            table,
            None,
            false,
            false,
            None,
        );
    }
    assert_plan_denied(ctx, &plan).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_checks_udf_privileges() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE FUNCTION show_test_predicate AS (p) -> (p > 0)")
        .await?;
    // Fixture DDL may run with PUBLIC as the current role; the readers must not own the UDF.
    UserApiProvider::instance()
        .grant_ownership_to_role(
            &fixture.default_tenant(),
            &OwnershipObject::UDF {
                name: "show_test_predicate".into(),
            },
            BUILTIN_ROLE_ACCOUNT_ADMIN,
        )
        .await?;
    for has_usage in [false, true] {
        let mut user = UserInfo::new_no_auth("show_udf_reader", "%");
        if has_usage {
            user.grants.grant_privileges(
                &GrantObject::UDF("show_test_predicate".into()),
                UserPrivilegeType::Usage.into(),
            );
        }
        let ctx = user_context(&fixture, user, None).await?;
        // Project the boolean UDF call so it stays in the plan inspected by the access checker.
        let (mut plan, _) = Planner::new(ctx.clone())
            .plan_sql("SELECT show_test_predicate(number) FROM numbers(2)")
            .await?;
        let Plan::Query {
            rewrite_kind,
            s_expr,
            ..
        } = &mut plan
        else {
            panic!("expected a query plan");
        };
        assert!(
            s_expr
                .get_udfs()?
                .iter()
                .any(|name| *name == "show_test_predicate")
        );
        // Test authorization of a SHOW plan containing a UDF, not the UDF's execution.
        *rewrite_kind = Some(RewriteKind::ShowDatabases);
        if has_usage {
            InterpreterFactory::get(ctx, &plan).await?;
        } else {
            assert_plan_denied(ctx, &plan).await;
        }
    }
    Ok(())
}

/// Spans of every test in this target. The reporter is process-wide, so it is installed once;
/// tests tell their spans apart by trace id.
fn reported_spans() -> Arc<Mutex<Vec<SpanRecord>>> {
    static SPANS: OnceLock<Arc<Mutex<Vec<SpanRecord>>>> = OnceLock::new();
    SPANS
        .get_or_init(|| {
            let (reporter, spans) = TestReporter::new();
            fastrace::set_reporter(reporter, fastrace::collector::Config::default());
            spans
        })
        .clone()
}

/// Counts spans of `trace` whose name is `name` or whose `::`-separated path ends with it.
fn count_spans(spans: &[SpanRecord], trace: SpanContext, name: &str) -> usize {
    spans
        .iter()
        .filter(|span| {
            span.trace_id == trace.trace_id
                && (span.name == name || span.name.split("::").any(|part| part == name))
        })
        .count()
}

/// DROP ... IF EXISTS that drops nothing keeps the name's ownership, which stands in for
/// an object created concurrently under the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_if_exists_keeps_ownership_when_nothing_is_dropped() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    let name = "drop_if_exists_absent".to_string();
    for (object, sql) in [
        (
            OwnershipObject::Stage { name: name.clone() },
            "DROP STAGE IF EXISTS drop_if_exists_absent",
        ),
        (
            OwnershipObject::UDF { name: name.clone() },
            "DROP FUNCTION IF EXISTS drop_if_exists_absent",
        ),
        (
            OwnershipObject::Connection { name: name.clone() },
            "DROP CONNECTION IF EXISTS drop_if_exists_absent",
        ),
        (
            OwnershipObject::Sequence { name: name.clone() },
            "DROP SEQUENCE IF EXISTS drop_if_exists_absent",
        ),
    ] {
        api.grant_ownership_to_role(&tenant, &object, BUILTIN_ROLE_ACCOUNT_ADMIN)
            .await?;
        fixture.execute_command(sql).await?;
        assert!(
            api.get_ownership(&tenant, &object).await?.is_some(),
            "{sql}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sequence_visibility_checker_is_cached_per_query() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let user = UserInfo::new_no_auth("sequence_checker_cache", "%");
    // `nextval` asks for this checker for every VALUES row and every block; it lists sequence
    // ownerships for non-admin users, so a query must build it only once.
    let ctx = user_context(&fixture, user.clone(), None).await?;
    let first = ctx.get_visibility_checker(false, Object::Sequence).await?;
    let second = ctx.get_visibility_checker(false, Object::Sequence).await?;
    assert!(Arc::ptr_eq(&first, &second));

    // The cache belongs to the query: a new query sees ownership changes made since.
    let next_query = user_context(&fixture, user, None).await?;
    let third = next_query
        .get_visibility_checker(false, Object::Sequence)
        .await?;
    assert!(!Arc::ptr_eq(&first, &third));
    Ok(())
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
    let spans = reported_spans();

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
        // table and the database id read with it; nothing resolves names again.
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
        assert_eq!(database_reads, 0);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table_checks_destination_database_by_name() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, _) = setup_table(&fixture).await?;
    let mut user = UserInfo::new_no_auth("creator", "%");
    user.grants.grant_privileges(
        &GrantObject::DatabaseById("default".into(), db_id),
        UserPrivilegeType::Create.into(),
    );
    let ctx = user_context(&fixture, user, None).await?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("CREATE TABLE select_access.dst LIKE select_access.t")
        .await?;
    assert_eq!(
        ctx.get_resolved_database_id("default", "select_access"),
        Some(db_id)
    );

    // Binding found the source database. Authorizing the destination still needs a name lookup.
    let spans = reported_spans();
    let trace = SpanContext::random();
    InterpreterFactory::get(ctx, &plan)
        .in_span(Span::root("create_access_test", trace))
        .await?;
    fastrace::flush();
    assert_eq!(count_spans(&spans.lock(), trace, "get_database"), 1);
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

/// A multi-table SELECT resolves each database id once, checks a table bound several times
/// once, and reads the ownership fallback of all tables without a matching grant in one MGet.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_multi_table_select_access_counts() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let (db_id, _) = setup_table(&fixture).await?;
    for sql in [
        "CREATE TABLE select_access.t2(a INT)",
        "CREATE TABLE select_access.t3(a INT)",
    ] {
        fixture.execute_command(sql).await?;
    }
    let tenant = fixture.default_tenant();
    let api = UserApiProvider::instance();
    let ctx = fixture.new_query_ctx().await?;
    let t2_id = ctx
        .get_table("default", "select_access", "t2")
        .await?
        .get_id();
    let t3_id = ctx
        .get_table("default", "select_access", "t3")
        .await?
        .get_id();
    // Like `setup_table`, keep the fixture's creating role from owning `t2` for every user.
    api.grant_ownership_to_role(
        &tenant,
        &OwnershipObject::Table {
            catalog_name: "default".into(),
            db_id,
            table_id: t2_id,
        },
        BUILTIN_ROLE_ACCOUNT_ADMIN,
    )
    .await?;
    for (role, object) in [
        ("database_owner", OwnershipObject::Database {
            catalog_name: "default".into(),
            db_id,
        }),
        ("t3_owner", OwnershipObject::Table {
            catalog_name: "default".into(),
            db_id,
            table_id: t3_id,
        }),
    ] {
        api.create_role(&tenant, RoleInfo::new(role, None), &CreateOption::Create)
            .await?;
        api.grant_ownership_to_role(&tenant, &object, role).await?;
    }
    RoleCacheManager::instance().invalidate_cache(&tenant);
    let spans = reported_spans();

    let name_grant =
        |table: &str| GrantObject::Table("default".into(), "select_access".into(), table.into());
    let mut database_owner = UserInfo::new_no_auth("database_owner", "%");
    database_owner.grants.grant_role("database_owner".into());
    let mut grants_only = UserInfo::new_no_auth("grants_only", "%");
    for object in [
        name_grant("t"),
        name_grant("t2"),
        GrantObject::TableById("default".into(), db_id, t3_id),
    ] {
        grants_only
            .grants
            .grant_privileges(&object, UserPrivilegeType::Select.into());
    }
    let mut mixed = UserInfo::new_no_auth("mixed", "%");
    mixed
        .grants
        .grant_privileges(&name_grant("t2"), UserPrivilegeType::Select.into());
    mixed.grants.grant_role("t3_owner".into());
    let mut partial = UserInfo::new_no_auth("partial", "%");
    partial
        .grants
        .grant_privileges(&name_grant("t3"), UserPrivilegeType::Select.into());

    let all_tables = "SELECT count(*) FROM select_access.t a, select_access.t b, \
        select_access.t2 c, select_access.t3 d";
    let t2_and_t3 =
        "SELECT count(*) FROM select_access.t2 c, select_access.t3 d, select_access.t3 e";
    // Expected counts of: SelectTable, get_database, OwnershipPrefetch, mget_ownerships,
    // get_ownership, TableOwnership, DatabaseOwnership.
    for (user, sql, expected, denied_table) in [
        (database_owner, all_tables, [3, 0, 1, 1, 0, 3, 3], None),
        (grants_only, all_tables, [3, 0, 0, 0, 0, 0, 0], None),
        (mixed, t2_and_t3, [2, 0, 1, 1, 0, 1, 0], None),
        // Ownership is checked after every grant check, then in plan order, so `t` is the
        // first denial and `t2` is never checked.
        (partial, all_tables, [3, 0, 1, 1, 0, 1, 1], Some("t")),
    ] {
        let name = user.name.clone();
        let ctx = user_context(&fixture, user, None).await?;
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
        let trace = SpanContext::random();
        let result = InterpreterFactory::get(ctx, &plan)
            .in_span(Span::root("multi_table_select_access_test", trace))
            .await;
        match denied_table {
            None => {
                result?;
            }
            Some(table) => {
                let error = result.err().expect("query must be denied");
                assert_eq!(
                    error.code(),
                    ErrorCode::PERMISSION_DENIED,
                    "{name}: {error}"
                );
                assert!(
                    error
                        .message()
                        .contains(&format!("'default'.'select_access'.'{table}'")),
                    "{name}: {error}"
                );
            }
        }
        fastrace::flush();
        let spans = spans.lock();
        for (span_name, expected) in [
            "Access.SelectTable",
            "get_database",
            "Access.OwnershipPrefetch",
            "mget_ownerships",
            "get_ownership",
            "Access.TableOwnership",
            "Access.DatabaseOwnership",
        ]
        .into_iter()
        .zip(expected)
        {
            assert_eq!(
                count_spans(&spans, trace, span_name),
                expected,
                "{name}: {span_name}"
            );
        }
    }
    Ok(())
}

/// The fast path reads grants from the role cache. Revoking through the production
/// interpreters must be visible to the next check in an already-authenticated session on the
/// same node, without any manual cache invalidation in the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_revoked_role_grants_are_visible_to_existing_session() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup_table(&fixture).await?;
    // `inner_reader` holds the SELECT grant; `outer_reader` only inherits it.
    for sql in [
        "CREATE ROLE inner_reader",
        "CREATE ROLE outer_reader",
        "GRANT SELECT ON select_access.t TO ROLE inner_reader",
        "GRANT ROLE inner_reader TO ROLE outer_reader",
    ] {
        fixture.execute_command(sql).await?;
    }

    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    let mut user = UserInfo::new_no_auth("role_user", "%");
    user.grants.grant_role("outer_reader".into());
    session.set_authed_user(user, None).await?;
    let select = || async {
        check_access(
            session.create_query_context(&BUILD_INFO).await?,
            "SELECT * FROM select_access.t",
        )
        .await
    };
    let assert_denied = |result: Result<()>| {
        let error = result.unwrap_err();
        assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{error}");
    };

    // Revoke the privilege on the inherited role while the cache is warm.
    select().await?;
    fixture
        .execute_command("REVOKE SELECT ON select_access.t FROM ROLE inner_reader")
        .await?;
    assert_denied(select().await);

    // Re-grant, then revoke the inheritance relationship instead of the privilege.
    fixture
        .execute_command("GRANT SELECT ON select_access.t TO ROLE inner_reader")
        .await?;
    select().await?;
    fixture
        .execute_command("REVOKE ROLE inner_reader FROM ROLE outer_reader")
        .await?;
    assert_denied(select().await);

    // Direct role grant: revoke from the role the user holds directly.
    fixture
        .execute_command("GRANT SELECT ON select_access.t TO ROLE outer_reader")
        .await?;
    select().await?;
    fixture
        .execute_command("REVOKE SELECT ON select_access.t FROM ROLE outer_reader")
        .await?;
    assert_denied(select().await);
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

    // A restricted AUTH ROLE must not activate even a role granted directly to the user.
    let restricted_session = fixture.new_session_with_type(SessionType::Dummy).await?;
    restricted_session
        .set_authed_user(user.clone(), Some("limited".into()))
        .await?;
    restricted_session
        .set_secondary_roles_checked(Some(vec![]))
        .await?;
    let error = restricted_session
        .set_secondary_roles_checked(Some(vec!["reader".into()]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::INVALID_ROLE);
    assert_eq!(restricted_session.get_secondary_roles(), Some(vec![]));

    // A restored list (HTTP session state) is kept, but grants nothing beyond the AUTH ROLE.
    restricted_session.set_secondary_roles_unchecked(Some(vec!["reader".into()]));
    let error = check_access(
        restricted_session.create_query_context(&BUILD_INFO).await?,
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

/// MySQL and Flight SQL sessions authenticate once per connection, so the session must reload
/// the user before each query: a revoke, disabling the user or DROP USER takes effect without
/// reconnecting.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_connection_sessions_reload_user_before_each_query() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup_table(&fixture).await?;
    for sql in [
        "CREATE ROLE reload_reader",
        "CREATE ROLE reload_limited",
        "GRANT SELECT ON select_access.t TO ROLE reload_reader",
    ] {
        fixture.execute_command(sql).await?;
    }
    let tenant = fixture.default_tenant();
    let select = "SELECT * FROM select_access.t";

    for (index, (label, session_type)) in [
        ("MySQL", SessionType::MySQL),
        ("FlightSQL", SessionType::FlightSQL),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("reload_user_{index}");
        for sql in [
            format!("CREATE USER {name} IDENTIFIED BY 'password'"),
            format!("GRANT ROLE reload_reader TO {name}"),
            format!("GRANT ROLE reload_limited TO {name}"),
        ] {
            fixture.execute_command(&sql).await?;
        }
        let user = UserApiProvider::instance()
            .get_user(&tenant, UserIdentity::new(&name, "%"))
            .await?;
        let session = fixture.new_session_with_type(session_type).await?;
        session.set_authed_user(user, None).await?;

        // A revoked secondary role stops taking effect, and takes effect again once granted.
        session.set_current_role_checked("reload_limited").await?;
        session
            .set_secondary_roles_checked(Some(vec!["reload_reader".into()]))
            .await?;
        check_access(session.create_query_context(&BUILD_INFO).await?, select).await?;
        fixture
            .execute_command(&format!("REVOKE ROLE reload_reader FROM {name}"))
            .await?;
        let error = check_access(session.create_query_context(&BUILD_INFO).await?, select)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{label}");
        fixture
            .execute_command(&format!("GRANT ROLE reload_reader TO {name}"))
            .await?;
        check_access(session.create_query_context(&BUILD_INFO).await?, select).await?;

        // A revoked current role falls back to PUBLIC, as on login.
        session.set_current_role_checked("reload_reader").await?;
        session.set_secondary_roles_checked(Some(vec![])).await?;
        fixture
            .execute_command(&format!("REVOKE ROLE reload_reader FROM {name}"))
            .await?;
        let error = check_access(session.create_query_context(&BUILD_INFO).await?, select)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED, "{label}");
        assert_eq!(
            session.get_current_role().map(|role| role.name),
            Some("public".to_string())
        );

        // A disabled user can no longer run queries, and can once enabled again.
        fixture
            .execute_command(&format!("ALTER USER {name} WITH DISABLED = TRUE"))
            .await?;
        let Err(error) = session.create_query_context(&BUILD_INFO).await else {
            panic!("{label}: a disabled user created a query context");
        };
        assert_eq!(error.code(), ErrorCode::AUTHENTICATE_FAILURE, "{label}");
        fixture
            .execute_command(&format!("ALTER USER {name} WITH DISABLED = FALSE"))
            .await?;
        session.create_query_context(&BUILD_INFO).await?;

        // A dropped user can no longer run queries.
        fixture
            .execute_command(&format!("DROP USER {name}"))
            .await?;
        let Err(error) = session.create_query_context(&BUILD_INFO).await else {
            panic!("{label}: a dropped user created a query context");
        };
        assert_eq!(error.code(), ErrorCode::UNKNOWN_USER, "{label}");
    }
    Ok(())
}

/// Login sets `need_change` in memory only: reloading the user keeps it until the password
/// changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reload_user_keeps_need_change_password() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE USER reload_password_user IDENTIFIED BY 'password'")
        .await?;
    let identity = UserIdentity::new("reload_password_user", "%");
    let mut user = UserApiProvider::instance()
        .get_user(&fixture.default_tenant(), identity)
        .await?;
    user.update_auth_need_change_password();
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    session.set_authed_user(user, None).await?;

    let error = check_access(session.create_query_context(&BUILD_INFO).await?, "SELECT 1")
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::NEED_CHANGE_PASSWORD_DENIED);
    assert!(session.get_current_user()?.auth_info.get_need_change());

    fixture
        .execute_command("ALTER USER reload_password_user IDENTIFIED BY 'new-password'")
        .await?;
    check_access(session.create_query_context(&BUILD_INFO).await?, "SELECT 1").await?;
    assert!(!session.get_current_user()?.auth_info.get_need_change());
    Ok(())
}
