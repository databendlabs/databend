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

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_sql::plans::Plan;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_send_notification_planning_is_result_free_and_uncacheable() -> anyhow::Result<()> {
    // No Cloud Control configured: planning must not try to send an RPC.
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner
        .plan_sql("CALL system$send_notification('oncall', 'alert')")
        .await?;
    assert!(!plan.has_result_set());
    assert!(matches!(plan, Plan::Query {
        ignore_result: true,
        ..
    }));
    assert!(!ctx.result_cache_state().cacheable());

    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner
        .plan_sql("SELECT * FROM send_notification('oncall', 'alert')")
        .await?;
    assert!(plan.has_result_set());
    assert!(!ctx.result_cache_state().cacheable());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_send_notification_name_collision_preserves_table_privileges() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let database = "notification_privilege_check";
    fixture
        .execute_command("CREATE ROLE notification_table_owner")
        .await?;
    // Otherwise the fixture creates objects under public, which would let
    // users without grants pass the negative cases through public ownership.
    let owner_session = fixture.default_session();
    let mut owner = owner_session.get_current_user()?;
    owner
        .grants
        .grant_role("notification_table_owner".to_string());
    owner_session.set_authed_user(owner, None).await?;
    owner_session
        .set_current_role_checked("notification_table_owner")
        .await?;
    fixture
        .execute_command(&format!("CREATE DATABASE {database}"))
        .await?;
    for table in [
        "send_notification",
        "fuse_amend",
        "set_cache_capacity",
        "numbers",
    ] {
        fixture
            .execute_command(&format!("CREATE TABLE {database}.{table} (v INT)"))
            .await?;
        for granted in [false, true] {
            let mut user = UserInfo::new_no_auth("notification_table_user", "%");
            if granted {
                user.grants.grant_privileges(
                    &GrantObject::Table(
                        "default".to_string(),
                        database.to_string(),
                        table.to_string(),
                    ),
                    UserPrivilegeSet::from(vec![
                        UserPrivilegeType::Select,
                        UserPrivilegeType::Alter,
                        UserPrivilegeType::Drop,
                    ]),
                );
            }
            let session = fixture.new_session_with_type(SessionType::Dummy).await?;
            session.set_authed_user(user, None).await?;
            for (sql, privilege) in [
                (format!("SELECT * FROM {database}.{table}"), "Select"),
                (
                    format!("ALTER TABLE {database}.{table} ADD COLUMN extra INT"),
                    "Alter",
                ),
                (format!("DROP TABLE {database}.{table}"), "Drop"),
            ] {
                let ctx = session.create_query_context(&BUILD_INFO).await?;
                let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
                let result = InterpreterFactory::get(ctx, &plan).await;
                if granted {
                    result?;
                } else {
                    let err = match result {
                        Ok(_) => panic!("ungranted table access must fail: {sql}"),
                        Err(err) => err,
                    };
                    assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
                    assert!(err.message().contains(&format!("privilege [{privilege}]")));
                    assert!(!err.message().contains("table function"));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_send_notification_function_requires_global_super_privilege() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    let mut user = UserInfo::new_no_auth("notification_table_user", "%");
    // Even a grant using the system/function name must not replace global SUPER.
    user.grants.grant_privileges(
        &GrantObject::Table(
            "default".to_string(),
            "system".to_string(),
            "send_notification".to_string(),
        ),
        UserPrivilegeType::Select.into(),
    );
    session.set_authed_user(user, None).await?;
    for sql in [
        "CALL system$send_notification('oncall', 'alert')",
        "SELECT * FROM send_notification('oncall', 'alert')",
        "SELECT * FROM set_cache_capacity('ALL', '0')",
    ] {
        let ctx = session.create_query_context(&BUILD_INFO).await?;
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
        let err = InterpreterFactory::get(ctx, &plan)
            .await
            .map(|_| ())
            .expect_err("table-function access must require global SUPER");
        assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
        assert!(err.message().contains("privilege [Super]"));
        assert!(err.message().contains("table function"));
    }
    let ctx = session.create_query_context(&BUILD_INFO).await?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("SELECT * FROM numbers(3)")
        .await?;
    InterpreterFactory::get(ctx, &plan).await?;
    Ok(())
}
