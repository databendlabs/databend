// Copyright 2022 Datafuse Labs.
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

use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_query::interpreters::interpreter_plan_sql;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_passwords_are_not_attached_to_query_history() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command(
            "CREATE USER history_user IDENTIFIED WITH sha256_password BY 'old_password'",
        )
        .await?;
    fixture
        .execute_command(
            "CREATE PROCEDURE history_p(password STRING) RETURNS STRING LANGUAGE SQL AS $$ BEGIN \
             EXECUTE IMMEDIATE concat('ALTER USER history_user IDENTIFIED BY ''', :password, ''''); \
             RETURN 'ok'; END; $$",
        )
        .await?;
    // (SQL, succeeds, structure that must remain auditable)
    for (sql, succeeds, kept) in [
        (
            "EXECUTE IMMEDIATE concat('ALTER USER history_user IDENTIFIED BY ', '''history_secret''')",
            true,
            "ALTER USER",
        ),
        // Arguments may be passed on to dynamic SQL.
        (
            "CALL PROCEDURE history_p('history_secret')",
            true,
            "CALL PROCEDURE history_p",
        ),
        // Failed planning must also attach masked history and retain diagnostics.
        (
            "CREATE USER history_user IDNETIFIED BY 'history_secret'",
            false,
            "IDNETIFIED",
        ),
        (
            "EXECUTE IMMEDIATE $$CREATE CONNECTION c STORAGE_TYPE='s3 SECRET_ACCESS_KEY='history_secret'$$",
            false,
            "CREATE CONNECTION",
        ),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        // Use the server planning entry point, which also attaches failed
        // statements; the execution-only test helper does not do that.
        let result = match interpreter_plan_sql(ctx.clone(), sql, false, None).await {
            Ok(_) => execute_command(ctx.clone(), sql).await,
            Err(err) => Err(err),
        };
        assert_eq!(result.is_ok(), succeeds, "{result:?}");
        if let Err(err) = result {
            // As rendered for clients and logs: the excerpt is scrubbed with
            // the recorded values, but still locates the error.
            let err = ctx
                .query_secrets()
                .scrub(&err.display_with_sql(sql).to_string());
            assert!(!err.contains("history_secret"), "{err}");
            assert!(err.contains(kept), "diagnostic must remain: {err}");
        }
        let logged = ctx.get_query_str();
        assert!(!logged.contains("history_secret"), "{logged}");
        assert!(logged.contains("***"), "{sql}: {logged}");
        assert!(logged.contains(kept), "{sql}: {logged}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_s3() -> anyhow::Result<()> {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "bucket".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        disable_credential_loader: true,
        ..Default::default()
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;

    let _ = ctx.get_application_level_data_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_fs() -> anyhow::Result<()> {
    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp".to_string(),
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;
    let _ = ctx.get_application_level_data_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_created_time_propagation() -> anyhow::Result<()> {
    use std::time::Duration;
    use std::time::SystemTime;

    use databend_common_catalog::session_type::SessionType;
    use databend_common_version::BUILD_INFO;
    use databend_query::schedulers::QueryFragmentsActions;
    use databend_query::servers::flight::v1::packets::QueryEnv;
    use databend_query::sessions::QueryContext;
    use databend_query::sessions::TableContextCluster;
    use databend_query::sessions::TableContextQueryIdentity;
    use databend_query::sessions::TableContextSettings;

    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;

    let ctx = session.create_query_context(&BUILD_INFO).await?;
    assert_eq!(
        ctx.get_function_context()?.now,
        chrono::DateTime::<chrono::Utc>::from(ctx.get_query_created_time())
    );
    assert_eq!(
        session.process_info().created_time,
        ctx.get_query_created_time()
    );
    let cluster = ctx.get_cluster();
    drop(ctx);

    // Use an explicit historical timestamp to catch worker-local clock sampling.
    let query_created_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let ctx = session.create_query_context_with_cluster(
        cluster.clone(),
        &BUILD_INFO,
        Some(query_created_time),
    )?;
    assert_eq!(ctx.get_query_created_time(), query_created_time);
    let expected_now: chrono::DateTime<chrono::Utc> = query_created_time.into();
    assert_eq!(ctx.get_function_context()?.now, expected_now);

    let env = QueryFragmentsActions::create(ctx.clone()).get_query_env()?;
    assert_eq!(env.query_created_time, query_created_time);
    let serialized = serde_json::to_value(&env)?;
    let received: QueryEnv = serde_json::from_value(serialized)?;
    let before_worker = SystemTime::now();
    let worker = received.create_query_ctx().await?;
    assert_eq!(worker.get_id(), ctx.get_id());
    assert_eq!(worker.get_query_created_time(), query_created_time);
    assert_eq!(worker.get_function_context()?.now, expected_now);
    let worker_info = worker.get_current_session().process_info();
    assert!(worker_info.created_time >= before_worker);
    assert_ne!(worker_info.created_time, query_created_time);
    let derived = QueryContext::create_from(&worker);
    assert_eq!(derived.get_query_created_time(), query_created_time);
    assert_eq!(derived.get_function_context()?.now, expected_now);
    assert_eq!(worker.get_function_context()?.now, expected_now);

    drop(ctx);
    let next_time = query_created_time + Duration::from_secs(60);
    let next = session.create_query_context_with_cluster(cluster, &BUILD_INFO, Some(next_time))?;
    assert_eq!(next.get_query_created_time(), next_time);
    assert_eq!(
        next.get_function_context()?.now,
        chrono::DateTime::<chrono::Utc>::from(next_time)
    );
    Ok(())
}
