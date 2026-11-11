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
use databend_common_catalog::table_context::TableContextQueryState;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::UserInfo;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::QueriesQueueManager;
use databend_query::sessions::QueryEntry;
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
        for mysql_id in [false, true] {
            let target = fixture.new_session_with_type(SessionType::MySQL).await?;
            target
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let target_ctx = target.create_query_context(&BUILD_INFO).await?;
            let mut acquire = Box::pin(queue.acquire(QueryEntry::create_entry(&target_ctx, true)?));
            assert!(futures::poll!(acquire.as_mut()).is_pending());
            assert_eq!(queue.length(), 1);

            let id = if mysql_id {
                target.get_mysql_conn_id().unwrap().to_string()
            } else {
                target.get_id()
            };
            let sql = format!("KILL {} '{}'", kind, id);
            let requester = fixture.new_session_with_type(SessionType::MySQL).await?;
            requester
                .set_authed_user(UserInfo::new_no_auth("u2", "%"), None)
                .await?;
            let ctx = requester.create_query_context(&BUILD_INFO).await?;
            let error = execute_command(ctx, &sql).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::UNKNOWN_SESSION);
            let expected = if mysql_id {
                format!("MySQL connection id {} not found session id", id)
            } else {
                format!("Not found session id {}", id)
            };
            assert_eq!(error.message(), expected);
            assert_eq!(queue.length(), 1, "unauthorized KILL removed queued query");
            assert!(!target.is_aborting());

            requester
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let ctx = requester.create_query_context(&BUILD_INFO).await?;
            execute_command(ctx, &sql).await?;
            assert_eq!(queue.length(), 0);
            assert_eq!(acquire.await.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
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

    for kind in ["QUERY", "CONNECTION"] {
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
        execute_command(ctx, &format!("KILL {} '{}'", kind, target.get_id())).await?;
        assert_eq!(target.is_aborting(), kind == "CONNECTION");
        assert!(target_ctx.get_abort_checker().try_check_aborting().is_err());
    }
    Ok(())
}
