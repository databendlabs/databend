// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

use base64::engine::general_purpose;
use base64::prelude::*;
use common_base::base::get_free_tcp_port;
use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::PasswordHashMethod;
use common_users::CustomClaims;
use common_users::EnsureUser;
use databend_query::auth::AuthMgr;
use databend_query::servers::http::middleware::HTTPSessionEndpoint;
use databend_query::servers::http::middleware::HTTPSessionMiddleware;
use databend_query::servers::http::v1::make_final_uri;
use databend_query::servers::http::v1::make_page_uri;
use databend_query::servers::http::v1::make_state_uri;
use databend_query::servers::http::v1::query_route;
use databend_query::servers::http::v1::ExecuteStateKind;
use databend_query::servers::http::v1::HttpSessionConf;
use databend_query::servers::http::v1::QueryResponse;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::sessions::QueryAffect;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestGlobalServices;
use headers::Header;
use http::HeaderMap;
use jwt_simple::algorithms::RS256KeyPair;
use jwt_simple::algorithms::RSAKeyPairLike;
use jwt_simple::claims::JWTClaims;
use jwt_simple::claims::NoCustomClaims;
use jwt_simple::prelude::Clock;
use poem::http::header;
use poem::http::Method;
use poem::http::StatusCode;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Response;
use poem::Route;
use pretty_assertions::assert_eq;
use tokio::time::sleep;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

use crate::tests::tls_constants::*;

type EndpointType = HTTPSessionEndpoint<Route>;

// TODO(youngsofun): add test for
// 1. query fail after started

async fn expect_end(ep: &EndpointType, result: QueryResponse) -> Result<()> {
    assert!(result.next_uri.is_some(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert!(
        matches!(
            result.state,
            ExecuteStateKind::Succeeded | ExecuteStateKind::Running
        ),
        "{:?}",
        result
    );
    assert!(!result.schema.is_empty(), "{:?}", result);

    let next_uri = result.next_uri.clone().unwrap();
    if next_uri.contains("final") {
        check_final(ep, &next_uri).await?;
    } else {
        let (status, result) = get_uri_checked(ep, &next_uri).await?;
        assert_eq!(status, StatusCode::OK, "{:?}", result);
        assert_eq!(result.data.len(), 0, "{:?}", result);
        let next_uri = result.next_uri.clone().unwrap();
        check_final(ep, &next_uri).await?;
    }
    Ok(())
}

async fn check_final(ep: &EndpointType, final_uri: &str) -> Result<()> {
    let (status, result) = get_uri_checked(ep, final_uri).await?;
    assert!(final_uri.contains("final"), "{:?}", result);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert!(result.next_uri.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_simple_sql() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let sql = "select * from system.tables limit 10";
    let ep = create_endpoint().await?;
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, 1, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result.error);

    let query_id = &result.id;
    let final_uri = make_final_uri(query_id);

    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    assert_eq!(result.next_uri, Some(final_uri.clone()), "{:?}", result);
    assert_eq!(result.data.len(), 10, "{:?}", result);
    assert_eq!(result.schema.len(), 18, "{:?}", result);

    // get state
    let uri = make_state_uri(query_id);
    let (status, result) = get_uri_checked(&ep, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert_eq!(result.next_uri, Some(final_uri.clone()), "{:?}", result);
    assert!(result.schema.is_empty(), "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);

    // get page, support retry
    let page_0_uri = make_page_uri(query_id, 0);
    for _ in 1..3 {
        let (status, result) = get_uri_checked(&ep, &page_0_uri).await?;
        assert_eq!(status, StatusCode::OK, "{:?}", result);
        assert!(result.error.is_none(), "{:?}", result);
        assert_eq!(result.data.len(), 10, "{:?}", result);
        assert_eq!(result.next_uri, Some(final_uri.clone()), "{:?}", result);
        assert!(!result.schema.is_empty(), "{:?}", result);
        assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    }

    // get page not expected
    let page_1_uri = make_page_uri(query_id, 1);
    let response = get_uri(&ep, &page_1_uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{:?}", result);
    let body = response.into_body().into_string().await.unwrap();
    assert_eq!(
        body,
        r#"{"error":{"code":"404","message":"wrong page number 1"}}"#
    );

    // final
    let (status, result) = get_uri_checked(&ep, &final_uri).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.next_uri.is_none(), "{:?}", result);

    let response = get_uri(&ep, &page_0_uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{:?}", result);

    let sql = "show databases";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has only one column
    assert_eq!(result.schema.len(), 1, "{:?}", result);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_show_databases() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let sql = "show databases";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has only one field: name
    assert_eq!(result.schema.len(), 1, "{:?}", result);

    let sql = "show full databases";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has two fields: catalog, name
    assert_eq!(result.schema.len(), 2, "{:?}", result);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_when_finish() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let wait_time_secs = 5;
    let sql = "create table t1(a int)";
    let ep = create_endpoint().await?;
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, wait_time_secs, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    for (sql, state) in [
        ("select * from numbers(1)", ExecuteStateKind::Succeeded),
        ("bad sql", ExecuteStateKind::Failed), // parse fail
        ("select cast(null as boolean)", ExecuteStateKind::Failed), // execute fail at once
        ("create table t1(a int)", ExecuteStateKind::Failed),
    ] {
        let start_time = std::time::Instant::now();
        let (status, result) = post_sql_to_endpoint(&ep, sql, wait_time_secs).await?;
        let duration = start_time.elapsed().as_secs_f64();
        let msg = || format!("{}: {:?}", sql, result);
        assert_eq!(status, StatusCode::OK, "{}", msg());
        assert_eq!(result.state, state, "{}", msg());
        // should not wait until wait_time_secs even if there is no more data
        assert!(
            duration < 5.0,
            "duration {} is too large than expect",
            msg()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_client_query_id() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let wait_time_secs = 5;
    let sql = "select * from numbers(1)";
    let ep = create_endpoint().await?;
    let mut headers = HeaderMap::new();
    headers.insert("x-databend-query-id", "test-query-id".parse().unwrap());
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, wait_time_secs, headers).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.id, "test-query-id");
    Ok(())
}

// ref: query_log not recorded correctly.
// It could be uncommented when we remove SEE_YOU_AGAIN stmt

// #[tokio::test(flavor = "current_thread")]
// async fn test_bad_sql() -> Result<()> {
//     let sql = "bad sql";
//     let ep = create_endpoint();
//     let (status, result) = post_sql_to_endpoint(&ep, sql, 1).await?;
//     assert_eq!(status, StatusCode::OK);
//     assert!(result.error.is_some(), "{:?}", result);
//     assert_eq!(result.data.len(), 0, "{:?}", result);
//     assert!(result.next_uri.is_none(), "{:?}", result);
//     assert_eq!(result.state, ExecuteStateKind::Failed, "{:?}", result);
//     assert!(result.schema.is_none(), "{:?}", result);
//
//     let sql = "select query_text, exception_code, exception_text, stack_trace from system.query_log where log_type=3";
//     let (status, result) = post_sql_to_endpoint(&ep, sql, 1).await?;
//     assert_eq!(status, StatusCode::OK, "{:?}", result);
//     assert_eq!(result.data.len(), 1, "{:?}", result);
//     assert_eq!(
//         result.data[0][0].as_str().unwrap(),
//         "bad sql",
//         "{:?}",
//         result
//     );
//     assert_eq!(
//         result.data[0][1].as_u64().unwrap(),
//         ErrorCode::SyntaxException("").code().to_u64().unwrap(),
//         "{:?}",
//         result
//     );
//
//     assert!(
//         result.data[0][2]
//             .as_str()
//             .unwrap()
//             .to_lowercase()
//             .contains("bad"),
//         "{:?}",
//         result
//     );
//
//     Ok(())
// }

#[tokio::test(flavor = "current_thread")]
async fn test_wait_time_secs() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;
    let ep = create_endpoint().await?;
    let sql = "select sleep(0.001)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 0}});

    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(result.state, ExecuteStateKind::Running, "{:?}", result);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    let query_id = &result.id;
    let next_uri = make_page_uri(query_id, 0);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert_eq!(result.next_uri, Some(next_uri.clone()), "{:?}", result);
    assert!(!result.schema.is_empty(), "{:?}", result);

    let mut uri = make_page_uri(query_id, 0);
    let mut num_row = 0;
    for _ in 1..300 {
        sleep(Duration::from_millis(10)).await;
        let (status, result) = get_uri_checked(&ep, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{:?}", result);
        assert!(result.error.is_none(), "{:?}", result);
        num_row += result.data.len();
        match &result.next_uri {
            Some(next_uri) => {
                assert!(
                    matches!(
                        result.state,
                        ExecuteStateKind::Succeeded | ExecuteStateKind::Running
                    ),
                    "{:?}",
                    result
                );
                uri = next_uri.clone();
            }
            None => {
                assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
                assert!(result.schema.is_empty(), "{:?}", result);
                assert_eq!(num_row, 1, "{:?}", result);
                return Ok(());
            }
        }
    }
    unreachable!("'{}' run for more than 3 secs", sql);
}

#[tokio::test(flavor = "current_thread")]
async fn test_buffer_size() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let rows = 100;
    let ep = create_endpoint().await?;
    let sql = format!("select * from numbers({})", rows);

    for buf_size in [0, 99, 100, 101] {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 1, "max_rows_in_buffer": buf_size}});
        let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
        assert_eq!(
            result.data.len(),
            rows,
            "buf_size={}, result={:?}",
            buf_size,
            result
        );
        assert_eq!(status, StatusCode::OK, "{} {:?}", buf_size, result);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_pagination() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let ep = create_endpoint().await?;
    let sql = "select * from numbers(10)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 1, "max_rows_per_page": 2}, "session": { "settings": {}}});

    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    let query_id = &result.id;
    let next_uri = make_page_uri(query_id, 1);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 2, "{:?}", result);
    assert_eq!(result.next_uri, Some(next_uri), "{:?}", result);
    assert!(!result.schema.is_empty(), "{:?}", result);

    // get page not expected
    let uri = make_page_uri(query_id, 6);
    let response = get_uri(&ep, &uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{:?}", result);
    let body = response.into_body().into_string().await.unwrap();
    assert_eq!(
        body,
        r#"{"error":{"code":"404","message":"wrong page number 6"}}"#
    );

    let mut next_uri = result.next_uri.clone().unwrap();

    for page in 1..5 {
        let (status, result) = get_uri_checked(&ep, &next_uri).await?;
        let msg = || format!("page {}: {:?}", page, result);
        assert_eq!(status, StatusCode::OK, "{:?}", msg());
        assert!(result.error.is_none(), "{:?}", msg());
        assert!(!result.schema.is_empty(), "{:?}", result);
        if page == 5 {
            // get state
            let uri = make_state_uri(query_id);
            let (status, _state_result) = get_uri_checked(&ep, &uri).await?;
            assert_eq!(status, StatusCode::OK);

            expect_end(&ep, result).await?;
        } else {
            assert_eq!(result.data.len(), 2, "{:?}", msg());
            assert!(result.next_uri.is_some(), "{:?}", msg());
            next_uri = result.next_uri.clone().unwrap();
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_http_session() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let ep = create_endpoint().await?;
    let json =
        serde_json::json!({"sql":  "use system", "session": {"keep_server_session_secs": 10}});

    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert!(result.next_uri.is_some(), "{:?}", result);
    assert!(result.schema.is_empty(), "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    let session_id = &result.session_id.unwrap();

    let json = serde_json::json!({"sql": "select database()", "session_id": session_id});
    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 1, "{:?}", result);
    assert_eq!(result.data[0][0], "system", "{:?}", result);

    let json = serde_json::json!({"sql": "select * from x", "session_id": session_id});
    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_some(), "{:?}", result);

    let json = serde_json::json!({"sql": "select 1", "session_id": session_id});
    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 1, "{:?}", result);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_result_timeout() -> Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_result_timeout(1u64)
        .build();

    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let (status, result) = post_sql_to_endpoint(&ep, "select 1", 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    let query_id = result.id.clone();
    let next_uri = make_page_uri(&query_id, 0);
    let response = get_uri(&ep, &next_uri).await;
    assert_eq!(response.status(), StatusCode::OK, "{:?}", result);

    sleep(std::time::Duration::from_secs(2)).await;
    let response = get_uri(&ep, &next_uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{:?}", result);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_system_tables() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;
    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());
    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let sql = "select name from system.tables where database='system' order by name";

    let (status, result) = post_sql_to_endpoint(&ep, sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(!result.data.is_empty(), "{:?}", result);

    let table_names = result
        .data
        .iter()
        .flatten()
        .map(|j| j.as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let skipped = vec![
        "credits", // slow for ci (> 1s) and maybe flaky
        "metrics", // QueryError: "Prometheus recorder is not initialized yet"
        "tracing", // Could be very large.
    ];
    for table_name in table_names {
        if skipped.contains(&table_name.as_str()) {
            continue;
        };
        let sql = format!("select * from system.{}", table_name);
        let (status, result) = post_sql_to_endpoint(&ep, &sql, 1)
            .await
            .map_err(|e| ErrorCode::Internal(format!("system.{}: {}", table_name, e.message())))?;
        let error_message = format!("{}: status={:?}, result={:?}", table_name, status, result);
        assert_eq!(status, StatusCode::OK, "{}", error_message);
        assert!(result.error.is_none(), "{}", error_message);
        assert_eq!(
            result.state,
            ExecuteStateKind::Succeeded,
            "{}",
            error_message
        );
        assert!(result.next_uri.is_some(), "{:?}", result);
        assert!(!result.schema.is_empty(), "{:?}", result);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let route = create_endpoint().await?;

    let sqls = vec![
        ("create table t(a int) engine=fuse", 0, 0),
        ("insert into t(a) values (1),(2)", 0, 2),
        ("select * from t", 2, 0),
    ];

    for (sql, data_len, rows_written) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 3}});
        let (status, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
        assert_eq!(status, StatusCode::OK, "{:?}", result);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.data.len(), data_len, "{:?}", result);
        assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
        assert_eq!(
            result.stats.progresses.write_progress.rows, rows_written,
            "{:?}",
            result
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_log() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let sql = "create table t1(a int)";
    let (status, result) = post_sql_to_endpoint(&ep, sql, 10).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded);
    assert!(result.data.is_empty(), "{:?}", result);
    let result_type_2 = result;

    let (status, result) = post_sql_to_endpoint(&ep, sql, 3).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_some(), "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Failed);
    let result_type_3 = result;

    let sql = "select query_text, query_duration_ms from system.query_log where log_type=2";
    let (status, result) = post_sql_to_endpoint(&ep, sql, 3).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(
        result.data[0][1].as_str().unwrap(),
        result_type_2.stats.running_time_ms.to_string(),
    );

    let sql = "select query_text, exception_code, exception_text, stack_trace, query_duration_ms from system.query_log where log_type=3";
    let (status, result) = post_sql_to_endpoint(&ep, sql, 3).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 1, "{:?}", result);
    assert!(
        result.data[0][0]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("create table"),
        "{:?}",
        result
    );
    assert!(
        result.data[0][2]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("exist"),
        "{:?}",
        result
    );
    assert_eq!(
        result.data[0][1].as_str().unwrap(),
        ErrorCode::TableAlreadyExists("").code().to_string(),
        "{:?}",
        result
    );
    assert_eq!(
        result.data[0][4].as_str().unwrap(),
        result_type_3.stats.running_time_ms.to_string(),
        "{:?}",
        result
    );
    Ok(())
}

// todo(youngsofun): flaky, may timing problem
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn test_query_log_killed() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let sql = "select sleep(2)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 0}});
    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);

    let response = get_uri(&ep, result.kill_uri.as_ref().unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK, "{:?}", result);

    let sql = "select query_text, exception_code, exception_text, stack_trace, query_duration_ms from system.query_log where log_type=4";
    let (status, result) = post_sql_to_endpoint(&ep, sql, 3).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 1, "{:?}", result);
    assert!(
        result.data[0][0].as_str().unwrap().contains("sleep"),
        "{:?}",
        result
    );
    assert!(
        result.data[0][2]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("killed"),
        "{:?}",
        result
    );
    assert_eq!(
        result.data[0][1].as_str().unwrap(),
        ErrorCode::AbortedQuery("").code().to_string(),
        "{:?}",
        result
    );
    Ok(())
}

async fn check_response(response: Response) -> Result<(StatusCode, QueryResponse)> {
    let status = response.status();
    let body = response.into_body().into_string().await.unwrap();
    let result = serde_json::from_str::<QueryResponse>(&body);
    assert!(
        result.is_ok(),
        "body ='{}', result='{:?}'",
        &body,
        result.err()
    );
    Ok((status, result?))
}

async fn get_uri(ep: &EndpointType, uri: &str) -> Response {
    let basic = headers::Authorization::basic("root", "");
    ep.call(
        Request::builder()
            .uri(uri.parse().unwrap())
            .method(Method::GET)
            .typed_header(basic)
            .finish(),
    )
    .await
    .unwrap_or_else(|err| err.into_response())
}

async fn get_uri_checked(ep: &EndpointType, uri: &str) -> Result<(StatusCode, QueryResponse)> {
    let response = get_uri(ep, uri).await;
    check_response(response).await
}

async fn post_sql(sql: &str, wait_time_secs: u64) -> Result<(StatusCode, QueryResponse)> {
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    post_json(&json).await
}

pub async fn create_endpoint() -> Result<EndpointType> {
    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    Ok(Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware))
}

async fn post_json(json: &serde_json::Value) -> Result<(StatusCode, QueryResponse)> {
    let ep = create_endpoint().await?;
    post_json_to_endpoint(&ep, json, HeaderMap::default()).await
}

async fn post_sql_to_endpoint(
    ep: &EndpointType,
    sql: &str,
    wait_time_secs: u64,
) -> Result<(StatusCode, QueryResponse)> {
    post_sql_to_endpoint_new_session(ep, sql, wait_time_secs, HeaderMap::default()).await
}

async fn post_sql_to_endpoint_new_session(
    ep: &EndpointType,
    sql: &str,
    wait_time_secs: u64,
    headers: HeaderMap,
) -> Result<(StatusCode, QueryResponse)> {
    let json = serde_json::json!({ "sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}, "session": { "settings": {}}});
    post_json_to_endpoint(ep, &json, headers).await
}

async fn post_json_to_endpoint(
    ep: &EndpointType,
    json: &serde_json::Value,
    headers: HeaderMap,
) -> Result<(StatusCode, QueryResponse)> {
    let uri = "/v1/query";
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;
    let basic = headers::Authorization::basic("root", "");

    let mut req = Request::builder()
        .uri(uri.parse().unwrap())
        .method(Method::POST)
        .header(header::CONTENT_TYPE, content_type)
        .typed_header(basic)
        .body(body);
    req.headers_mut().extend(headers.into_iter());
    let response = ep
        .call(req)
        .await
        .map_err(|e| ErrorCode::Internal(e.to_string()))?;

    check_response(response).await
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_jwt() -> Result<()> {
    let user_name = "test_user";

    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;

    // Setup mock jwks url
    let config = ConfigBuilder::create()
        .jwt_key_file(format!("http://{}{}", server.address(), json_path))
        .build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let now = Some(Clock::now_since_epoch());
    let claims = JWTClaims {
        issued_at: now,
        expires_at: Some(now.unwrap() + jwt_simple::prelude::Duration::from_secs(10)),
        invalid_before: now,
        audiences: None,
        issuer: None,
        jwt_id: None,
        subject: Some(user_name.to_string()),
        nonce: None,
        custom: NoCustomClaims {},
    };

    let token = key_pair.sign(claims)?;
    let bear = headers::Authorization::bearer(&token).unwrap();
    assert_auth_failure(&ep, bear).await?;
    Ok(())
}

async fn assert_auth_failure(ep: &EndpointType, header: impl Header) -> Result<()> {
    let sql = "select 1";

    let json = serde_json::json!({"sql": sql.to_string()});

    let path = "/v1/query";
    let uri = format!("{}?wait_time_secs={}", path, 3);
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;

    let response = ep
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .header(header::CONTENT_TYPE, content_type)
                .typed_header(header)
                .body(body),
        )
        .await
        .unwrap();

    let status = response.status();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

async fn assert_auth_current_user(
    ep: &EndpointType,
    user_name: &str,
    header: impl Header,
    host: &str,
) -> Result<()> {
    let sql = "select current_user()";

    let json = serde_json::json!({"sql": sql.to_string()});

    let path = "/v1/query";
    let uri = format!("{}?wait_time_secs={}", path, 3);
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;

    let response = ep
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .header(header::CONTENT_TYPE, content_type)
                .typed_header(header)
                .body(body),
        )
        .await
        .unwrap();

    let (_, resp) = check_response(response).await?;
    let v = resp.data;
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 1);
    assert_eq!(
        v[0][0],
        serde_json::Value::String(format!("'{}'@'{}'", user_name, host))
    );
    Ok(())
}

async fn assert_auth_current_role(
    ep: &EndpointType,
    role_name: &str,
    header: impl Header,
) -> Result<()> {
    let sql = "select current_role()";

    let json = serde_json::json!({"sql": sql.to_string()});

    let path = "/v1/query";
    let uri = format!("{}?wait_time_secs={}", path, 3);
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;

    let response = ep
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .header(header::CONTENT_TYPE, content_type)
                .typed_header(header)
                .body(body),
        )
        .await
        .unwrap();

    let (_, resp) = check_response(response).await?;
    let v = resp.data;
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 1);
    assert_eq!(v[0][0], serde_json::Value::String(role_name.to_string()));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_jwt_with_create_user() -> Result<()> {
    let user_name = "user1";

    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;

    // Setup mock jwt
    let config = ConfigBuilder::create()
        .jwt_key_file(format!("http://{}{}", server.address(), json_path))
        .build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());
    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let now = Some(Clock::now_since_epoch());
    let claims = JWTClaims {
        issued_at: now,
        expires_at: Some(now.unwrap() + jwt_simple::prelude::Duration::from_secs(10)),
        invalid_before: now,
        audiences: None,
        issuer: None,
        jwt_id: None,
        subject: Some(user_name.to_string()),
        nonce: None,
        custom: CustomClaims {
            tenant_id: None,
            role: Some("account_admin".to_string()),
            ensure_user: Some(EnsureUser::default()),
        },
    };

    let token = key_pair.sign(claims)?;
    let bearer = headers::Authorization::bearer(&token).unwrap();
    assert_auth_current_user(&ep, user_name, bearer.clone(), "%").await?;
    assert_auth_current_role(&ep, "account_admin", bearer).await?;
    Ok(())
}

// need to support local_addr, but axum_server do not have local_addr callback
#[tokio::test(flavor = "current_thread")]
async fn test_http_handler_tls_server() -> Result<()> {
    let _guard = TestGlobalServices::setup(
        ConfigBuilder::create()
            .http_handler_tls_server_key(TEST_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_SERVER_CERT)
            .build(),
    )
    .await?;
    let address_str = format!("127.0.0.1:{}", get_free_tcp_port());
    let mut srv = HttpHandler::create(HttpHandlerKind::Query);

    let listening = srv.start(address_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/query", TEST_CN_NAME, listening.port());
    let sql = "select * from system.tables limit 10";
    let json = serde_json::json!({"sql": sql.to_string()});
    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let resp = client
        .post(&url)
        .json(&json)
        .basic_auth("root", Some(""))
        .send()
        .await;
    assert!(resp.is_ok(), "{:?}", resp.err());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    let res = resp.json::<QueryResponse>().await;
    assert!(res.is_ok(), "{:?}", res);
    let res = res.unwrap();
    assert!(!res.data.is_empty(), "{:?}", res);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_http_handler_tls_server_failed_case_1() -> Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_SERVER_CERT)
        .build();
    let _guard = TestGlobalServices::setup(config).await?;
    let address_str = format!("127.0.0.1:{}", get_free_tcp_port());
    let mut srv = HttpHandler::create(HttpHandlerKind::Query);

    let listening = srv.start(address_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/query", TEST_CN_NAME, listening.port());
    let sql = "select * from system.tables limit 10";
    let json = serde_json::json!({"sql": sql.to_string()});

    // kick off
    let client = reqwest::Client::builder().build().unwrap();
    let resp = client.post(&url).json(&json).send().await;
    assert!(resp.is_err(), "{:?}", resp.err());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_http_service_tls_server_mutual_tls() -> Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
        .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
        .build();

    let _guard = TestGlobalServices::setup(config.clone()).await?;
    let address_str = format!("127.0.0.1:{}", get_free_tcp_port());
    let mut srv = HttpHandler::create(HttpHandlerKind::Query);
    let listening = srv.start(address_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/query", TEST_CN_NAME, listening.port());
    let sql = "select * from system.tables limit 10";
    let json = serde_json::json!({"sql": sql.to_string()});

    // get identity
    let mut buf = Vec::new();
    File::open(TEST_TLS_CLIENT_KEY)?.read_to_end(&mut buf)?;
    File::open(TEST_TLS_CLIENT_CERT)?.read_to_end(&mut buf)?;
    let pkcs12 = reqwest::Identity::from_pem(&buf).unwrap();
    let mut buf = Vec::new();
    File::open(TEST_TLS_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();
    // kick off
    let client = reqwest::Client::builder()
        .identity(pkcs12)
        .add_root_certificate(cert)
        .build()
        .expect("preconfigured rustls tls");
    let resp = client
        .post(&url)
        .json(&json)
        .basic_auth("root", Some(""))
        .send()
        .await;
    assert!(resp.is_ok(), "{:?}", resp.err());
    let resp = resp.unwrap();
    assert!(resp.status().is_success(), "{:?}", resp);
    let res = resp.json::<QueryResponse>().await;
    assert!(res.is_ok(), "{:?}", res);
    let res = res.unwrap();
    assert!(!res.data.is_empty(), "{:?}", res);
    Ok(())
}

// cannot connect with server unless it have CA signed identity
#[tokio::test(flavor = "current_thread")]
async fn test_http_service_tls_server_mutual_tls_failed() -> Result<()> {
    let _guard = TestGlobalServices::setup(
        ConfigBuilder::create()
            .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
            .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
            .build(),
    )
    .await?;
    let address_str = format!("127.0.0.1:{}", get_free_tcp_port());

    let mut srv = HttpHandler::create(HttpHandlerKind::Query);
    let listening = srv.start(address_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/query", TEST_CN_NAME, listening.port());
    let sql = "select * from system.tables limit 10";
    let json = serde_json::json!({"sql": sql.to_string()});

    let mut buf = Vec::new();
    File::open(TEST_TLS_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();
    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .expect("preconfigured rustls tls");
    let resp = client.post(&url).json(&json).send().await;
    assert!(resp.is_err(), "{:?}", resp.err());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_func_object_keys() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let route = create_endpoint().await?;

    let sqls = vec![
        (
            "CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, obj JSON, var VARIANT) Engine=Fuse;",
            0,
        ),
        (
            "INSERT INTO objects_test1 VALUES (1, parse_json('{\"a\": 1, \"b\": [1,2,3]}'), parse_json('{\"1\": 2}'));",
            0,
        ),
        (
            "SELECT id, object_keys(obj), object_keys(var) FROM objects_test1;",
            1,
        ),
    ];

    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 3}});
        let (status, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.data.len(), data_len);
        assert_eq!(result.state, ExecuteStateKind::Succeeded);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multi_partition() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let route = create_endpoint().await?;

    let sqls = vec![
        ("create table tb2(id int, c1 varchar) Engine=Fuse;", 0),
        ("insert into tb2 values(1, 'mysql'),(1, 'databend')", 0),
        ("insert into tb2 values(2, 'mysql'),(2, 'databend')", 0),
        ("insert into tb2 values(3, 'mysql'),(3, 'databend')", 0),
        ("select * from tb2;", 6),
    ];

    let wait_time_secs = 5;
    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
        let (status, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(
            result.state,
            ExecuteStateKind::Succeeded,
            "SQL '{sql}' not finish after {wait_time_secs} secs"
        );
        assert_eq!(result.data.len(), data_len);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_affect() -> Result<()> {
    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;

    let route = create_endpoint().await?;

    let sqls = vec![
        (
            serde_json::json!({"sql": "set max_threads=1", "session": {"settings": {"max_threads": "6", "timezone": "Asia/Shanghai"}}}),
            Some(QueryAffect::ChangeSettings {
                keys: vec!["max_threads".to_string()],
                values: vec!["1".to_string()],
                is_globals: vec![false],
            }),
            Some(HttpSessionConf {
                database: None,
                keep_server_session_secs: None,
                settings: Some(BTreeMap::from([
                    ("max_threads".to_string(), "1".to_string()),
                    ("timezone".to_string(), "Asia/Shanghai".to_string()),
                ])),
            }),
        ),
        (
            serde_json::json!({"sql":  "create database if not exists db2", "session": {"settings": {"max_threads": "6"}}}),
            None,
            Some(HttpSessionConf {
                database: None,
                keep_server_session_secs: None,
                settings: Some(BTreeMap::from([(
                    "max_threads".to_string(),
                    "6".to_string(),
                )])),
            }),
        ),
        (
            serde_json::json!({"sql":  "use db2", "session": {"settings": {"max_threads": "6"}}}),
            Some(QueryAffect::UseDB {
                name: "db2".to_string(),
            }),
            Some(HttpSessionConf {
                database: Some("db2".to_string()),
                keep_server_session_secs: None,
                settings: Some(BTreeMap::from([(
                    "max_threads".to_string(),
                    "6".to_string(),
                )])),
            }),
        ),
    ];

    for (json, affect, session_conf) in sqls {
        let (status, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
        assert_eq!(status, StatusCode::OK, "{} {:?}", json, result.error);
        assert!(result.error.is_none(), "{} {:?}", json, result.error);
        assert_eq!(result.state, ExecuteStateKind::Succeeded);
        assert_eq!(result.affect, affect);
        assert_eq!(result.session, session_conf);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_configured_user() -> Result<()> {
    let user_name = "conf_user";
    let pass_word = "conf_user_pwd";
    let hash_method = PasswordHashMethod::DoubleSha1;
    let hash_value = hash_method.hash(pass_word.as_bytes());

    let auth_info = AuthInfo::Password {
        hash_value,
        hash_method,
    };
    let config = ConfigBuilder::create()
        .add_user(user_name, auth_info)
        .build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let session_middleware =
        HTTPSessionMiddleware::create(HttpHandlerKind::Query, AuthMgr::instance());

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(session_middleware);

    let basic = headers::Authorization::basic(user_name, pass_word);
    // root user can only login in localhost
    assert_auth_current_user(&ep, user_name, basic, "%").await?;
    Ok(())
}
