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
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextQueryState;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::KillTarget;
use databend_query::sessions::INTERNAL_SESSION_ID_PREFIX;
use databend_query::sessions::QueriesQueueManager;
use databend_query::sessions::QueryEntry;
use databend_query::sessions::Session;
use databend_query::sessions::SessionContext;
use databend_query::sessions::SessionManager;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_queued_query_authorization() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.max_running_queries = 1;
    config.query.common.global_statement_queue = false;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let queue = QueriesQueueManager::instance();
    let holder_ctx = fixture.new_query_ctx().await?;
    let _holder = queue
        .acquire(QueryEntry::create_entry(&holder_ctx, true)?)
        .await?;

    for kind in ["QUERY", "CONNECTION"] {
        for id_type in ["session", "mysql", "query", "query_prefixed"] {
            if kind == "CONNECTION" && id_type.starts_with("query") {
                continue;
            }
            let target = fixture.new_session_with_type(SessionType::MySQL).await?;
            target
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let target_ctx = target.create_query_context(&BUILD_INFO).await?;
            if id_type == "query_prefixed" {
                target_ctx.update_init_query_id(format!("p-{}", uuid::Uuid::new_v4()));
            }
            let mut acquire = Box::pin(queue.acquire(QueryEntry::create_entry(&target_ctx, true)?));
            assert!(futures::poll!(acquire.as_mut()).is_pending());
            assert_eq!(queue.length(), 1);

            let id = match id_type {
                "mysql" => target.get_mysql_conn_id().unwrap().to_string(),
                "query" | "query_prefixed" => target_ctx.get_id(),
                _ => target.get_id(),
            };
            let sql = format!("KILL {} '{}'", kind, id);
            let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
            requester
                .set_authed_user(UserInfo::new_no_auth("u2", "%"), None)
                .await?;
            let ctx = requester.create_query_context(&BUILD_INFO).await?;
            let error = execute_command(ctx, &sql).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
            assert!(error.message().contains("not owner of target"));
            assert_eq!(queue.length(), 1, "unauthorized KILL removed queued query");
            assert!(!target.is_aborting());

            requester
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let ctx = requester.create_query_context(&BUILD_INFO).await?;
            execute_command(ctx, &sql).await?;
            assert_eq!(queue.length(), 0);
            let error = acquire.await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
            assert_eq!(error.message(), "Query canceled by KILL QUERY");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_with_super_from_role() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.execute_command("CREATE ROLE kill_admin").await?;
    fixture
        .execute_command("GRANT SUPER ON *.* TO ROLE kill_admin")
        .await?;

    for (kind, id_type) in [
        ("QUERY", "session"),
        ("CONNECTION", "session"),
        ("QUERY", "query"),
        ("QUERY", "mysql"),
    ] {
        let target = fixture.new_session_with_type(SessionType::MySQL).await?;
        target
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        let target_ctx = target.create_query_context(&BUILD_INFO).await?;

        let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
        let mut user = UserInfo::new_no_auth("u2", "%");
        user.grants.grant_role("kill_admin".to_string());
        requester.set_authed_user(user, None).await?;
        let ctx = requester.create_query_context(&BUILD_INFO).await?;
        let id = match id_type {
            "query" => target_ctx.get_id(),
            "mysql" => target.get_mysql_conn_id().unwrap().to_string(),
            _ => target.get_id(),
        };
        execute_command(ctx, &format!("KILL {} '{}'", kind, id)).await?;
        assert_eq!(target.is_aborting(), kind == "CONNECTION");
        let error = target_ctx
            .get_abort_checker()
            .try_check_aborting()
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        if kind == "QUERY" {
            assert_eq!(error.message(), "Query canceled by KILL QUERY");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_super_respects_tenant_before_queue_removal() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.max_running_queries = 1;
    config.query.common.global_statement_queue = false;
    let fixture = TestFixture::setup_with_config(&config).await?;
    fixture.execute_command("CREATE ROLE kill_admin").await?;
    fixture
        .execute_command("GRANT SUPER ON *.* TO ROLE kill_admin")
        .await?;
    let manager = SessionManager::instance();
    let queue = QueriesQueueManager::instance();
    for role_super in [false, true] {
        for queued in [false, true] {
            for (kind, by_query_id) in [("QUERY", false), ("CONNECTION", false), ("QUERY", true)] {
                let holder_ctx = fixture.new_query_ctx().await?;
                let _holder = if queued {
                    Some(
                        queue
                            .acquire(QueryEntry::create_entry(&holder_ctx, true)?)
                            .await?,
                    )
                } else {
                    None
                };
                let mut target = manager.create_session(SessionType::Local).await?;
                target.set_current_tenant(Tenant::new_literal("other_tenant"))?;
                let target = manager.register_session(target)?;
                target
                    .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                    .await?;
                let target_ctx = target.create_query_context(&BUILD_INFO).await?;
                let mut acquire =
                    Box::pin(queue.acquire(QueryEntry::create_entry(&target_ctx, true)?));
                if queued {
                    assert!(futures::poll!(acquire.as_mut()).is_pending());
                }
                let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
                if role_super {
                    let mut user = UserInfo::new_no_auth("u2", "%");
                    user.grants.grant_role("kill_admin".to_string());
                    requester.set_authed_user(user, None).await?;
                }
                let ctx = requester.create_query_context(&BUILD_INFO).await?;
                let id = if by_query_id {
                    target_ctx.get_id()
                } else {
                    target.get_id()
                };
                let error = execute_command(ctx, &format!("KILL {} '{}'", kind, id))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), ErrorCode::PERMISSION_DENIED);
                assert!(error.message().contains("outside the requester's tenant"));
                assert_eq!(queue.length(), usize::from(queued));
                assert!(!target.is_aborting());
                assert!(target_ctx.get_abort_checker().try_check_aborting().is_ok());
                drop(acquire);
                assert_eq!(queue.length(), 0);
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_client_query_id_formats() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for id in [
        uuid::Uuid::new_v4().simple().to_string(),
        uuid::Uuid::new_v4().to_string(),
        format!("{}.2", uuid::Uuid::new_v4()),
        format!("p-{}", uuid::Uuid::new_v4()),
        "p-".to_string(),
    ] {
        let target = fixture.new_session_with_type(SessionType::MySQL).await?;
        target
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        let target_ctx = target.create_query_context(&BUILD_INFO).await?;
        target_ctx.update_init_query_id(id.clone());
        let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
        requester
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        let ctx = requester.create_query_context(&BUILD_INFO).await?;
        execute_command(ctx, &format!("KILL QUERY '{}'", id)).await?;
        let err = target_ctx
            .get_abort_checker()
            .try_check_aborting()
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::ABORTED_QUERY);
        assert!(err.message().contains("canceled"));
        assert!(!target.is_aborting());
    }
    Ok(())
}

#[test]
fn test_kill_target_parsing() {
    assert_eq!(KillTarget::parse("42"), KillTarget::MySqlConnection(42));
    assert_eq!(KillTarget::parse("00042"), KillTarget::MySqlConnection(42));
    let session = format!("{}{}", INTERNAL_SESSION_ID_PREFIX, uuid::Uuid::new_v4());
    assert_eq!(
        KillTarget::parse(&session),
        KillTarget::SessionOrQuery(&session)
    );
    assert_eq!(KillTarget::parse("p-"), KillTarget::SessionOrQuery("p-"));
    for id in [
        "0123456789abcdef0123456789abcdef",
        "55b05b74-80a6-49fd-b813-3ef25bf0f110.1",
        "55b05b74-80a6-49fd-b813-3ef25bf0f110",
    ] {
        assert_eq!(KillTarget::parse(id), KillTarget::QueryOrSession(id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_legacy_session_and_query_id_precedence() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let manager = SessionManager::instance();
    // Model a session created by an old node, with an unprefixed UUID.
    let legacy_id = uuid::Uuid::new_v4().to_string();
    let legacy = manager.register_session(Session::try_create(
        legacy_id.clone(),
        SessionType::MySQL,
        Box::new(SessionContext::try_create(
            fixture.default_session().get_settings(),
            SessionType::MySQL,
            None,
        )?),
        Some(manager.alloc_mysql_conn_id()),
    )?)?;
    legacy
        .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
        .await?;
    let legacy_ctx = legacy.create_query_context(&BUILD_INFO).await?;
    let target = fixture.new_session_with_type(SessionType::MySQL).await?;
    let target_ctx = target.create_query_context(&BUILD_INFO).await?;
    target_ctx.update_init_query_id(legacy_id.clone());

    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx, &format!("KILL QUERY '{}'", legacy_id)).await?;
    assert!(
        target_ctx
            .clone()
            .get_abort_checker()
            .try_check_aborting()
            .is_err()
    );
    assert!(
        legacy_ctx
            .clone()
            .get_abort_checker()
            .try_check_aborting()
            .is_ok()
    );
    drop(target_ctx);
    drop(target);

    // When no query matches, fall back to the legacy session id for either KILL form.
    for kind in ["QUERY", "CONNECTION"] {
        let ctx = fixture.new_query_ctx().await?;
        execute_command(ctx, &format!("KILL {} '{}'", kind, legacy_id)).await?;
    }
    assert!(legacy.is_aborting());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_connection_rejects_query_id_and_numeric_id_is_connection() -> anyhow::Result<()>
{
    let fixture = TestFixture::setup().await?;
    let target = fixture.new_session_with_type(SessionType::MySQL).await?;
    let target_ctx = target.create_query_context(&BUILD_INFO).await?;
    for query_id in [
        target_ctx.get_id(),
        format!("p-{}", uuid::Uuid::new_v4()),
        "p-".to_string(),
    ] {
        target_ctx.update_init_query_id(query_id.clone());
        let ctx = fixture.new_query_ctx().await?;
        let err = execute_command(ctx, &format!("KILL CONNECTION '{}'", query_id))
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS);
        assert!(
            target_ctx
                .clone()
                .get_abort_checker()
                .try_check_aborting()
                .is_ok()
        );
        assert!(!target.is_aborting());
    }

    target_ctx.update_init_query_id(u32::MAX.to_string());
    let ctx = fixture.new_query_ctx().await?;
    let err = execute_command(ctx, &format!("KILL QUERY '{}'", u32::MAX))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::UNKNOWN_SESSION);
    assert!(target_ctx.get_abort_checker().try_check_aborting().is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_prefix_lookup_priority_and_no_permission_fallback() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let manager = SessionManager::instance();
    for prefixed in [false, true] {
        let id = if prefixed {
            format!("p-{}", uuid::Uuid::new_v4())
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        let sid_target = manager.register_session(Session::try_create(
            id.clone(),
            SessionType::MySQL,
            Box::new(SessionContext::try_create(
                fixture.default_session().get_settings(),
                SessionType::MySQL,
                None,
            )?),
            Some(manager.alloc_mysql_conn_id()),
        )?)?;
        let query_target = fixture.new_session_with_type(SessionType::MySQL).await?;
        // Only the lower-priority match belongs to the requester initially.
        sid_target
            .set_authed_user(
                UserInfo::new_no_auth(if prefixed { "u2" } else { "u1" }, "%"),
                None,
            )
            .await?;
        query_target
            .set_authed_user(
                UserInfo::new_no_auth(if prefixed { "u1" } else { "u2" }, "%"),
                None,
            )
            .await?;
        let sid_ctx = sid_target.create_query_context(&BUILD_INFO).await?;
        let query_ctx = query_target.create_query_context(&BUILD_INFO).await?;
        query_ctx.update_init_query_id(id.clone());
        let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
        requester
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        let ctx = requester.create_query_context(&BUILD_INFO).await?;
        let err = execute_command(ctx, &format!("KILL QUERY '{}'", id))
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
        assert!(
            sid_ctx
                .clone()
                .get_abort_checker()
                .try_check_aborting()
                .is_ok()
        );
        assert!(
            query_ctx
                .clone()
                .get_abort_checker()
                .try_check_aborting()
                .is_ok()
        );

        sid_target
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        query_target
            .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
            .await?;
        let ctx = requester.create_query_context(&BUILD_INFO).await?;
        execute_command(ctx, &format!("KILL QUERY '{}'", id)).await?;
        assert_eq!(
            sid_ctx
                .clone()
                .get_abort_checker()
                .try_check_aborting()
                .is_err(),
            prefixed
        );
        assert_eq!(
            query_ctx
                .clone()
                .get_abort_checker()
                .try_check_aborting()
                .is_err(),
            !prefixed
        );
        assert!(!sid_target.is_aborting());
        assert!(!query_target.is_aborting());

        // CONNECTION always chooses the actual sid, even if an unprefixed query
        // with the same string exists. It never disconnects the query's session.
        let next_query = query_target.create_query_context(&BUILD_INFO).await?;
        next_query.update_init_query_id(id.clone());
        let ctx = requester.create_query_context(&BUILD_INFO).await?;
        execute_command(ctx, &format!("KILL CONNECTION '{}'", id)).await?;
        assert!(sid_target.is_aborting());
        assert!(!query_target.is_aborting());
        assert!(next_query.get_abort_checker().try_check_aborting().is_ok());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_conditional_kill_does_not_cancel_next_query() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    let old = session.create_query_context(&BUILD_INFO).await?;
    let old_id = old.get_id();
    let next = session.create_query_context(&BUILD_INFO).await?;
    assert!(!session.force_kill_query_if_id(&old_id, ErrorCode::aborting()));
    assert!(
        next.clone()
            .get_abort_checker()
            .try_check_aborting()
            .is_ok()
    );
    assert!(session.force_kill_query_if_id(&next.get_id(), ErrorCode::aborting()));
    assert!(next.get_abort_checker().try_check_aborting().is_err());

    // Exercise both orderings of replacing the context slot versus conditional kill.
    for _ in 0..32 {
        let old = session.create_query_context(&BUILD_INFO).await?;
        let old_id = old.get_id();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let kill_session = session.clone();
        let kill_barrier = barrier.clone();
        let kill = std::thread::Builder::new()
            .name(std::thread::current().name().unwrap().to_string())
            .spawn(move || {
                kill_barrier.wait();
                kill_session.force_kill_query_if_id(&old_id, ErrorCode::aborting())
            })?;
        barrier.wait();
        let next = session.create_query_context(&BUILD_INFO).await?;
        kill.join().unwrap();
        assert!(next.get_abort_checker().try_check_aborting().is_ok());
    }
    Ok(())
}
