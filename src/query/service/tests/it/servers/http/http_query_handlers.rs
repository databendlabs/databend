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

#![allow(clippy::unnecessary_unwrap)]
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

use base64::engine::general_purpose;
use base64::prelude::*;
use databend_common_base::base::get_free_tcp_port;
use databend_common_base::headers::HEADER_VERSION;
use databend_common_config::UserAuthConfig;
use databend_common_config::UserConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use databend_common_users::CustomClaims;
use databend_common_users::EnsureUser;
use databend_common_version::DATABEND_SEMVER;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::servers::admin::v1::instance_status::InstanceStatus;
use databend_query::servers::admin::v1::instance_status::instance_status_handler;
use databend_query::servers::http::error::QueryError;
use databend_query::servers::http::middleware::json_response;
use databend_query::servers::http::v1::ExecuteStateKind;
use databend_query::servers::http::v1::HttpSessionConf;
use databend_query::servers::http::v1::QueryResponseField;
use databend_query::servers::http::v1::QueryStats;
use databend_query::servers::http::v1::catalog;
use databend_query::servers::http::v1::make_page_uri;
use databend_query::servers::http::v1::query_route;
use databend_query::servers::http::v1::roles::ListRolesResponse;
use databend_query::servers::http::v1::users::CreateUserRequest;
use databend_query::servers::http::v1::users::ListUsersResponse;
use databend_query::sessions::QueryAffect;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_storages_common_session::TxnState;
use futures_util::future::try_join_all;
use headers::Header;
use headers::HeaderMapExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header;
use jwt_simple::algorithms::RS256KeyPair;
use jwt_simple::algorithms::RSAKeyPairLike;
use jwt_simple::claims::JWTClaims;
use jwt_simple::claims::NoCustomClaims;
use jwt_simple::prelude::Clock;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Response;
use poem::Route;
use poem::get;
use pretty_assertions::assert_eq;
use serde::Deserialize;
use tokio::time::sleep;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

use crate::tests::tls_constants::*;

type EndpointType = Route;

fn unwrap_data<'a>(data: &'a [Vec<Option<String>>], null_as: &'a str) -> Vec<Vec<&'a str>> {
    data.iter()
        .map(|x| {
            x.iter()
                .map(|y| match y {
                    Some(y) => y.as_str(),
                    None => null_as,
                })
                .collect()
        })
        .collect()
}

struct TestHttpQueryRequest {
    ep: EndpointType,
    json: serde_json::Value,
    auth_header: HeaderValue,
    headers: HeaderMap,
    next_uri: Option<String>,
}

/// Copy for [QueryResponse](databend_query::servers::http::v1::QueryResponse) to deserialize
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct TestQueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub node_id: String,

    pub state: ExecuteStateKind,
    pub session: Option<HttpSessionConf>,
    // only sql query error
    pub error: Option<QueryError>,
    pub warnings: Vec<String>,

    // about results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_result_set: Option<bool>,
    pub schema: Vec<QueryResponseField>,
    pub data: Vec<Vec<Option<String>>>,
    pub affect: Option<QueryAffect>,
    pub result_timeout_secs: Option<u64>,

    pub stats: QueryStats,

    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

impl TestHttpQueryRequest {
    fn new(json: serde_json::Value) -> Self {
        let ep = create_endpoint().unwrap();

        let root_auth_header = {
            let mut headers = HeaderMap::new();
            headers.typed_insert(headers::Authorization::basic("root", ""));
            headers["authorization"].clone()
        };

        Self {
            ep,
            json,
            auth_header: root_auth_header,
            headers: HeaderMap::new(),
            next_uri: None,
        }
    }

    fn with_basic_auth(mut self, username: &str, password: &str) -> Self {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::Authorization::basic(username, password));
        self.auth_header = headers["authorization"].clone();
        self
    }

    // fn with_headers(mut self, headers: HeaderMap) -> Self {
    //    self.headers = headers;
    //    self
    // }

    async fn fetch_begin(&mut self) -> Result<(StatusCode, TestQueryResponse, String)> {
        let (status, resp, body) = self
            .do_request(Method::POST, "/v1/query")
            .await
            .map_err(|e| ErrorCode::Internal(e.to_string()))?;
        self.next_uri = resp.as_ref().and_then(|r| r.next_uri.clone());
        Ok((status, resp.unwrap(), body))
    }

    #[allow(unused)]
    async fn fetch_next(&mut self) -> Result<(StatusCode, Option<TestQueryResponse>, String)> {
        let (status, resp, body) = self
            .do_request(Method::GET, self.next_uri.as_ref().unwrap())
            .await?;
        self.next_uri = resp.as_ref().and_then(|r| r.next_uri.clone());
        Ok((status, resp, body))
    }

    async fn status(&mut self) -> InstanceStatus {
        let req = Request::builder()
            .uri("/v1_status".parse().unwrap())
            .method(Method::GET)
            .finish();
        let response = self
            .ep
            .call(req)
            .await
            .map_err(|e| ErrorCode::Internal(e.to_string()))
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().into_vec().await.unwrap();
        serde_json::from_str::<InstanceStatus>(&String::from_utf8_lossy(&body)).unwrap()
    }

    async fn fetch_total(&mut self) -> Result<TestHttpQueryFetchReply> {
        let mut resps = vec![];

        let (status, resp, _) = self.do_request(Method::POST, "/v1/query").await?;
        self.next_uri = resp.as_ref().and_then(|r| r.next_uri.clone());
        resps.push((status, resp.clone().unwrap()));

        while self.next_uri.is_some() {
            let (status, resp, _) = self
                .do_request(Method::GET, self.next_uri.as_ref().unwrap())
                .await?;
            self.next_uri = resp.as_ref().and_then(|r| r.next_uri.clone());
            if self.next_uri.is_some() {
                resps.push((status, resp.clone().unwrap()));
            }
        }

        Ok(TestHttpQueryFetchReply { resps })
    }

    async fn do_request(
        &self,
        method: Method,
        uri: &str,
    ) -> Result<(StatusCode, Option<TestQueryResponse>, String)> {
        let content_type = "application/json";
        let body = serde_json::to_vec(&self.json).unwrap();

        let mut req = Request::builder()
            .uri(uri.parse().unwrap())
            .method(method)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::AUTHORIZATION, self.auth_header.clone())
            .body(body);
        req.headers_mut().extend(self.headers.clone().into_iter());

        let resp = self
            .ep
            .call(req)
            .await
            .map_err(|e| ErrorCode::Internal(e.to_string()))
            .unwrap();
        assert_eq!(
            resp.header(HEADER_VERSION),
            Some(DATABEND_SEMVER.to_string().as_str())
        );

        let status_code = resp.status();
        let body = resp.into_body().into_string().await.unwrap();
        let query_resp = serde_json::from_str::<TestQueryResponse>(&body)
            .map(Some)
            .unwrap_or_default();

        Ok((status_code, query_resp, body))
    }
}

#[derive(Debug, Clone)]
struct TestHttpQueryFetchReply {
    pub resps: Vec<(StatusCode, TestQueryResponse)>,
}

impl TestHttpQueryFetchReply {
    fn last(&self) -> (StatusCode, TestQueryResponse) {
        self.resps.last().unwrap().clone()
    }

    fn data(&self) -> Vec<Vec<Option<String>>> {
        let mut result = vec![];
        for (_, resp) in &self.resps {
            result.extend(resp.data.clone());
        }
        result
    }

    fn state(&self) -> ExecuteStateKind {
        self.last().1.state
    }

    fn error(&self) -> Option<QueryError> {
        for (_, resp) in &self.resps {
            if let Some(e) = &resp.error {
                return Some(e.clone());
            }
        }
        None
    }
}

// TODO(youngsofun): add test for
// 1. query fail after started

async fn expect_end(ep: &EndpointType, result: TestQueryResponse) -> Result<()> {
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
async fn test_simple_sql() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sql = "select * from system.tables limit 10";
    let ep = create_endpoint()?;
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, 5, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result.error);

    let query_id = &result.id;
    let final_uri = result.final_uri.clone().unwrap();

    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    assert_eq!(result.next_uri, Some(final_uri.clone()), "{:?}", result);
    assert_eq!(result.data.len(), 10, "{:?}", result);
    assert_eq!(result.schema.len(), 31, "{:?}", result);

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

    // client retry
    let page_1_uri = make_page_uri(query_id, 1);
    let (_, result) = get_uri_checked(&ep, &page_1_uri).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert_eq!(result.next_uri, Some(final_uri.clone()), "{:?}", result);

    // get page not expected
    let page_2_uri = make_page_uri(query_id, 2);
    let response = get_uri(&ep, &page_2_uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "{:?}", result);
    let body = response.into_body().into_string().await.unwrap();
    assert_eq!(
        body,
        r#"{"error":{"code":404,"message":"Invalid page number: requested 2, current page is 1"}}"#
    );

    // final
    let (status, result) = get_uri_checked(&ep, &final_uri).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.next_uri.is_none(), "{:?}", result);

    let response = get_uri(&ep, &page_0_uri).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{:?}", result);

    let sql = "show databases";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has only one column
    assert_eq!(result.schema.len(), 1, "{:?}", result);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_show_databases() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sql = "show databases";
    let (status, result) = post_sql(sql, 3).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has only one field: name
    assert_eq!(result.schema.len(), 1, "{:?}", result);

    let sql = "show full databases";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    // has three fields: catalog, owner, name
    assert_eq!(result.schema.len(), 3, "{:?}", result);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_when_finish() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let wait_time_secs = 5;
    let sql = "create table t1(a int)";
    let ep = create_endpoint()?;
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, wait_time_secs, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
    for (sql, state) in [
        ("select * from numbers(1)", ExecuteStateKind::Succeeded),
        ("bad sql", ExecuteStateKind::Failed), // parse fail
        ("select cast(null as boolean)", ExecuteStateKind::Succeeded),
        ("create table t1(a int)", ExecuteStateKind::Failed),
    ] {
        let start_time = std::time::Instant::now();
        let json = serde_json::json!({ "sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}, "session": { "settings": {}}});
        let (status, result) = TestHttpQueryRequest::new(json).fetch_total().await?.last();
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
async fn test_client_query_id() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let wait_time_secs = 5;
    let sql = "select * from numbers(1)";
    let ep = create_endpoint()?;
    let mut headers = HeaderMap::new();
    headers.insert("x-databend-query-id", "testqueryid".parse().unwrap());
    let (status, result) =
        post_sql_to_endpoint_new_session(&ep, sql, wait_time_secs, headers).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.id, "testqueryid");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_client_compatible_query_id() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let wait_time_secs = 5;
    let sql = "select * from numbers(1)";
    let ep = create_endpoint()?;
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
// async fn test_bad_sql() -> anyhow::Result<()> {
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
async fn test_active_sessions() -> anyhow::Result<()> {
    let conf = ConfigBuilder::create()
        .max_active_sessions(2)
        .off_log()
        .build();
    let _fixture = TestFixture::setup_with_config(&conf).await?;
    let ep = create_endpoint()?;
    let sql = "select sleep(1)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 1}});

    let mut handlers = vec![];
    for _ in 0..3 {
        handlers.push(post_json_to_endpoint(&ep, &json, HeaderMap::default()));
    }
    let mut results = try_join_all(handlers)
        .await?
        .into_iter()
        .map(|(_status, resp)| resp.error.map(|e| e.message).unwrap_or_default())
        .collect::<Vec<_>>();
    results.sort();
    let msg = "Failed to upgrade session: Current active sessions (2) has exceeded the max_active_sessions limit (2)";
    let expect = vec!["", "", msg];
    assert_eq!(results, expect);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_wait_time_secs() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let ep = create_endpoint()?;
    let sql = "select sleep(0.001)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 0}});

    let (status, result) = post_json_to_endpoint(&ep, &json, HeaderMap::default()).await?;
    assert_eq!(result.state, ExecuteStateKind::Starting, "{:?}", result);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    let query_id = &result.id;
    let next_uri = make_page_uri(query_id, 0);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.data.len(), 0, "{:?}", result);
    assert_eq!(result.next_uri, Some(next_uri.clone()), "{:?}", result);

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
                        ExecuteStateKind::Succeeded
                            | ExecuteStateKind::Running
                            | ExecuteStateKind::Starting
                    ),
                    "{:?}",
                    result
                );
                uri = next_uri.clone();
            }
            None => {
                assert_eq!(result.state, ExecuteStateKind::Succeeded, "{:?}", result);
                // assert!(result.schema.is_empty(), "{:?}", result);
                assert_eq!(num_row, 1, "{:?}", result);

                return Ok(());
            }
        }
    }
    unreachable!("'{}' run for more than 3 secs", sql);
}

#[tokio::test(flavor = "current_thread")]
async fn test_buffer_size() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let rows = 100;
    let sql = format!("select * from numbers({})", rows);

    for buf_size in [0, 99, 100, 101] {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 1, "max_rows_in_buffer": buf_size}});
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        assert_eq!(
            reply.data().len(),
            rows,
            "buf_size={}, result={:?}",
            buf_size,
            reply,
        );
        assert_eq!(reply.last().0, StatusCode::OK, "{} {:?}", buf_size, reply);
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_pagination() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let ep = create_endpoint()?;
    let sql = "select * from numbers(10)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 6   , "max_rows_per_page": 2}, "session": { "settings": {}}});

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
        r#"{"error":{"code":404,"message":"Invalid page number: requested 6, current page is 1"}}"#
    );

    let mut next_uri = result.next_uri.clone().unwrap();

    for page in 1..5 {
        let (status, result) = get_uri_checked(&ep, &next_uri).await?;
        let msg = || format!("page {}: {:?}", page, result);
        assert_eq!(status, StatusCode::OK, "{:?}", msg());
        assert!(result.error.is_none(), "{:?}", msg());
        assert!(!result.schema.is_empty(), "{:?}", result);
        if page == 5 {
            expect_end(&ep, result).await?;
        } else {
            assert_eq!(result.data.len(), 2, "{:?}", msg());
            assert!(result.next_uri.is_some(), "{:?}", msg());
            next_uri = result.next_uri.clone().unwrap();
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_result_timeout() -> anyhow::Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_result_timeout(5u64)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let json = serde_json::json!({
        "sql": "SELECT * from numbers(5)",
        "pagination": {"wait_time_secs": 5,  "max_rows_per_page": 1,},
        "session": { "settings": {"http_handler_result_timeout_secs": "1"}}}
    );
    let mut req = TestHttpQueryRequest::new(json);
    assert_eq!(req.status().await.running_queries_count, 0);
    let (status, result, _) = req.fetch_begin().await?;
    assert_eq!(req.status().await.running_queries_count, 1);
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 1);

    sleep(std::time::Duration::from_secs(6)).await;
    let status = req.status().await;
    assert_eq!(status.running_queries_count, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_system_tables() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let ep = create_endpoint()?;

    let sql = "select name from system.tables where database='system' order by name";

    let (status, result) = post_sql_to_endpoint(&ep, sql, 3).await?;
    let data = unwrap_data(&result.data, "");
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(!data.is_empty(), "{:?}", result);

    let table_names = data.into_iter().flatten().collect::<Vec<_>>();

    let skipped = [
        "credits", // slow for ci (> 1s) and maybe flaky
        "metrics", // QueryError: "Prometheus recorder is not initialized yet"
        "tasks",   // need to connect grpc server, tested on sqllogic test
        "notifications",
        "notification_history",
        "task_history", // same with tasks
        "tracing",      // Could be very large.
    ];
    for table_name in table_names {
        if skipped.contains(&table_name) {
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
async fn test_insert() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let route = create_endpoint()?;

    let sqls = vec![
        ("create table t(a int) engine=fuse", 0, 0),
        ("insert into t(a) values (1),(2)", 1, 2),
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
async fn test_catalog_apis() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;

    let sql = "create table t1(a int)";
    let (status, result) = post_sql_to_endpoint(&ep, sql, 10).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result);
    assert_eq!(result.state, ExecuteStateKind::Succeeded);

    let response = get_uri(&ep, "/v1/catalog/databases").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: catalog::list_databases::ListDatabasesResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(body.databases.len(), 3);
    assert_eq!(body.databases[0].name, "system");
    assert_eq!(body.databases[1].name, "information_schema");
    assert_eq!(body.databases[2].name, "default");

    let response = get_uri(&ep, "/v1/catalog/databases/default/tables").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: catalog::list_database_tables::ListDatabaseTablesResponse =
        serde_json::from_str(&body).unwrap();
    assert_eq!(body.tables.len(), 1);
    assert_eq!(body.tables[0].name, "t1");

    let response = get_uri(&ep, "/v1/catalog/databases/default/tables/t1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: catalog::get_database_table::GetDatabaseTableResponse =
        serde_json::from_str(&body).unwrap();
    assert_eq!(body.table.unwrap().name, "t1");

    let response = get_uri(&ep, "/v1/catalog/databases/default/tables/t1/fields").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: catalog::list_database_table_fields::ListDatabaseTableFieldsResponse =
        serde_json::from_str(&body).unwrap();
    assert_eq!(body.fields.len(), 1);
    assert_eq!(body.fields[0].name, "a");

    let response = get_uri(&ep, "/v1/catalog/stats").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: catalog::stats::CatalogStatsResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(body.tables, 1);
    assert_eq!(body.databases, 3);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_user_apis() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;

    let req = CreateUserRequest {
        name: "test_user".to_string(),
        hostname: Some("%".to_string()),
        auth_type: Some("double_sha1_password".to_string()),
        auth_string: Some("test_password".to_string()),
        default_role: None,
        default_warehouse: None,
        roles: None,
        grant_all: Some(true),
    };
    let response = post_uri(
        &ep,
        "/v1/users",
        &serde_json::to_value(req).unwrap(),
        HeaderMap::default(),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let response = get_uri(&ep, "/v1/users").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: ListUsersResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(body.users.len(), 1);
    assert_eq!(body.users[0].name, "test_user");
    assert_eq!(body.users[0].hostname, "%");
    assert_eq!(body.users[0].auth_type, "double_sha1_password");
    assert_eq!(body.users[0].disabled, false);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_role_apis() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;

    let response = get_uri(&ep, "/v1/roles").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_string().await.unwrap();
    let body: ListRolesResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(body.roles.len(), 2);
    assert_eq!(body.roles[0].name, BUILTIN_ROLE_ACCOUNT_ADMIN);
    assert_eq!(body.roles[1].name, BUILTIN_ROLE_PUBLIC);
    Ok(())
}

async fn check_response(response: Response) -> Result<(StatusCode, TestQueryResponse)> {
    let status = response.status();
    let body = response.into_body().into_string().await.unwrap();
    let result = serde_json::from_str::<TestQueryResponse>(&body);
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

async fn post_uri(
    ep: &EndpointType,
    uri: &str,
    json: &serde_json::Value,
    headers: HeaderMap,
) -> Result<Response> {
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
    Ok(response)
}

async fn get_uri_checked(ep: &EndpointType, uri: &str) -> Result<(StatusCode, TestQueryResponse)> {
    let response = get_uri(ep, uri).await;
    check_response(response).await
}

async fn post_sql(sql: &str, wait_time_secs: u64) -> Result<(StatusCode, TestQueryResponse)> {
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    post_json(&json).await
}

pub fn create_endpoint() -> Result<EndpointType> {
    Ok(Route::new()
        .nest("/v1", query_route().around(json_response))
        .at("/v1_status", get(instance_status_handler)))
}

async fn post_json(json: &serde_json::Value) -> Result<(StatusCode, TestQueryResponse)> {
    let ep = create_endpoint()?;
    post_json_to_endpoint(&ep, json, HeaderMap::default()).await
}

async fn post_sql_to_endpoint(
    ep: &EndpointType,
    sql: &str,
    wait_time_secs: u64,
) -> Result<(StatusCode, TestQueryResponse)> {
    post_sql_to_endpoint_new_session(ep, sql, wait_time_secs, HeaderMap::default()).await
}

async fn post_sql_to_endpoint_new_session(
    ep: &EndpointType,
    sql: &str,
    wait_time_secs: u64,
    headers: HeaderMap,
) -> Result<(StatusCode, TestQueryResponse)> {
    let json = serde_json::json!({ "sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}, "session": { "settings": {}}});
    post_json_to_endpoint(ep, &json, headers).await
}

async fn post_json_to_endpoint(
    ep: &EndpointType,
    json: &serde_json::Value,
    headers: HeaderMap,
) -> Result<(StatusCode, TestQueryResponse)> {
    let uri = "/v1/query";
    let response = post_uri(ep, uri, json, headers).await?;
    check_response(response).await
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_jwt() -> anyhow::Result<()> {
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
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let ep = create_endpoint()?;

    let now = Clock::now_since_epoch();
    let claims = JWTClaims {
        issued_at: Some(now),
        expires_at: Some(now + jwt_simple::prelude::Duration::from_secs(10)),
        invalid_before: Some(now),
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
    let v = unwrap_data(&resp.data, "");
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
    let v = unwrap_data(&resp.data, "");
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 1);
    assert_eq!(v[0][0], serde_json::Value::String(role_name.to_string()));
    Ok(())
}

async fn assert_auth_current_role_with_role(
    ep: &EndpointType,
    role_name: &str,
    role: &str,
    header: impl Header,
) -> Result<()> {
    let sql = "select current_role()";

    let json = serde_json::json!({"sql": sql.to_string(), "session": {"role": role.to_string()}});

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
    let v = unwrap_data(&resp.data, "");
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 1);
    assert_eq!(v[0][0], serde_json::Value::String(role_name.to_string()));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_jwt_with_create_user() -> anyhow::Result<()> {
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
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let ep = create_endpoint()?;

    let now = Clock::now_since_epoch();
    let claims = JWTClaims {
        issued_at: Some(now),
        expires_at: Some(now + jwt_simple::prelude::Duration::from_secs(10)),
        invalid_before: Some(now),
        audiences: None,
        issuer: None,
        jwt_id: None,
        subject: Some(user_name.to_string()),
        nonce: None,
        custom: CustomClaims {
            tenant_id: None,
            role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
            ensure_user: Some(EnsureUser::default()),
        },
    };

    let token = key_pair.sign(claims)?;
    let bearer = headers::Authorization::bearer(&token).unwrap();
    assert_auth_current_user(&ep, user_name, bearer.clone(), "%").await?;
    assert_auth_current_role(&ep, BUILTIN_ROLE_ACCOUNT_ADMIN, bearer.clone()).await?;
    // assert_auth_current_role_with_restricted_role(&ep, BUILTIN_ROLE_PUBLIC, BUILTIN_ROLE_PUBLIC, bearer).await?;
    assert_auth_current_role_with_role(&ep, BUILTIN_ROLE_PUBLIC, BUILTIN_ROLE_PUBLIC, bearer)
        .await?;

    Ok(())
}

// need to support local_addr, but axum_server do not have local_addr callback
#[tokio::test(flavor = "current_thread")]
async fn test_http_handler_tls_server() -> anyhow::Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_SERVER_CERT)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

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
    let res = resp.json::<TestQueryResponse>().await;
    assert!(res.is_ok(), "{:?}", res);
    let res = res.unwrap();
    assert!(!res.data.is_empty(), "{:?}", res);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_http_handler_tls_server_failed_case_1() -> anyhow::Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_SERVER_CERT)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

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

#[ignore = "remove client cert support for now"]
#[tokio::test(flavor = "current_thread")]
async fn test_http_service_tls_server_mutual_tls() -> anyhow::Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
        .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

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
    let res = resp.json::<TestQueryResponse>().await;
    assert!(res.is_ok(), "{:?}", res);
    let res = res.unwrap();
    assert!(!res.data.is_empty(), "{:?}", res);

    Ok(())
}

// cannot connect with server unless it have CA signed identity
#[ignore = "remove client cert support for now"]
#[tokio::test(flavor = "current_thread")]
async fn test_http_service_tls_server_mutual_tls_failed() -> anyhow::Result<()> {
    let config = ConfigBuilder::create()
        .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
        .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
        .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

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
async fn test_func_object_keys() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sqls = vec![
        (
            "CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, obj JSON, var VARIANT) Engine=Fuse;",
            0,
        ),
        (
            "INSERT INTO objects_test1 VALUES (1, parse_json('{\"a\": 1, \"b\": [1,2,3]}'), parse_json('{\"1\": 2}'));",
            1,
        ),
        (
            "SELECT id, object_keys(obj), object_keys(var) FROM objects_test1;",
            1,
        ),
    ];

    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 3}});
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        let (status, result) = reply.last();
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(reply.data().len(), data_len);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multi_partition() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sqls = vec![
        ("create table tb2(id int, c1 varchar) Engine=Fuse;", 0),
        ("insert into tb2 values(1, 'mysql'),(1, 'databend')", 1),
        ("insert into tb2 values(2, 'mysql'),(2, 'databend')", 1),
        ("insert into tb2 values(3, 'mysql'),(3, 'databend')", 1),
        ("select * from tb2;", 6),
    ];

    let wait_time_secs = 5;
    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        assert!(reply.error().is_none(), "{:?}", reply.error());
        assert_eq!(
            reply.state(),
            ExecuteStateKind::Succeeded,
            "SQL '{sql}' not finish after {wait_time_secs} secs"
        );
        assert_eq!(reply.data().len(), data_len);
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_affect() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sqls = vec![
        (
            serde_json::json!({"sql": "set max_threads=1", "session": {"settings": {"max_threads": "6", "timezone": "Asia/Shanghai"}}}),
            Some(QueryAffect::ChangeSettings {
                keys: vec!["max_threads".to_string()],
                values: vec!["1".to_string()],
                is_globals: vec![false],
            }),
            Some(HttpSessionConf {
                catalog: Some("default".to_string()),
                database: Some("default".to_string()),
                role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
                secondary_roles: None,
                settings: Some(BTreeMap::from([
                    ("max_threads".to_string(), "1".to_string()),
                    ("timezone".to_string(), "Asia/Shanghai".to_string()),
                ])),
                txn_state: Some(TxnState::AutoCommit),
                need_sticky: false,
                need_keep_alive: false,
                internal: None,
            }),
        ),
        (
            serde_json::json!({"sql": "unset timezone", "session": {"settings": {"max_threads": "6", "timezone": "Asia/Shanghai"}}}),
            Some(QueryAffect::ChangeSettings {
                keys: vec!["timezone".to_string()],
                values: vec!["UTC".to_string()],
                // TODO(liyz): consider to return the complete settings after set or unset
                is_globals: vec![false],
            }),
            Some(HttpSessionConf {
                catalog: Some("default".to_string()),
                database: Some("default".to_string()),
                role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
                secondary_roles: None,
                settings: Some(BTreeMap::from([(
                    "max_threads".to_string(),
                    "6".to_string(),
                )])),
                txn_state: Some(TxnState::AutoCommit),
                need_sticky: false,
                need_keep_alive: false,
                internal: None,
            }),
        ),
        (
            serde_json::json!({"sql":  "create database if not exists db2", "session": {"settings": {"max_threads": "6"}}}),
            None,
            Some(HttpSessionConf {
                catalog: Some("default".to_string()),
                database: Some("default".to_string()),
                role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
                secondary_roles: None,
                settings: Some(BTreeMap::from([(
                    "max_threads".to_string(),
                    "6".to_string(),
                )])),
                txn_state: Some(TxnState::AutoCommit),
                need_sticky: false,
                need_keep_alive: false,
                internal: None,
            }),
        ),
        (
            serde_json::json!({"sql":  "use db2", "session": {"settings": {"max_threads": "6"}}}),
            Some(QueryAffect::UseDB {
                name: "db2".to_string(),
            }),
            Some(HttpSessionConf {
                catalog: Some("default".to_string()),
                database: Some("db2".to_string()),
                role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
                secondary_roles: None,
                settings: Some(BTreeMap::from([(
                    "max_threads".to_string(),
                    "6".to_string(),
                )])),
                txn_state: Some(TxnState::AutoCommit),
                need_sticky: false,
                need_keep_alive: false,
                internal: None,
            }),
        ),
        (
            serde_json::json!({"sql": "set global max_threads=2", "session": {"settings": {"max_threads": "4", "timezone": "Asia/Shanghai"}}}),
            Some(QueryAffect::ChangeSettings {
                keys: vec!["max_threads".to_string()],
                values: vec!["2".to_string()],
                is_globals: vec![true],
            }),
            Some(HttpSessionConf {
                catalog: Some("default".to_string()),
                database: Some("default".to_string()),
                role: Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
                secondary_roles: None,
                settings: Some(BTreeMap::from([(
                    "timezone".to_string(),
                    "Asia/Shanghai".to_string(),
                )])),
                txn_state: Some(TxnState::AutoCommit),
                need_sticky: false,
                need_keep_alive: false,
                internal: None,
            }),
        ),
    ];

    for (json, affect, session_conf) in sqls {
        let mut result = TestHttpQueryRequest::new(json.clone())
            .fetch_total()
            .await?
            .last();
        if let Some(s) = result.1.session.as_mut() {
            s.internal = None;
        }
        assert_eq!(result.0, StatusCode::OK, "{} {:?}", json, result.1.error);
        assert!(result.1.error.is_none(), "{} {:?}", json, result.1.error);
        assert_eq!(result.1.state, ExecuteStateKind::Succeeded, "{}", json);
        assert_eq!(result.1.affect, affect, "{}", json);
        let session = result.1.session.map(|s| HttpSessionConf { ..s });

        assert_eq!(session, session_conf, "{}", json);
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_session_secondary_roles() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let route = create_endpoint()?;

    let json = serde_json::json!({"sql":  "SELECT 1", "session": {"secondary_roles": vec!["role1".to_string()]}});
    let (_, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
    assert!(result.error.is_none());
    assert_eq!(result.state, ExecuteStateKind::Succeeded);
    assert_eq!(
        result.session.unwrap().secondary_roles,
        Some(vec!["role1".to_string()])
    );

    let json = serde_json::json!({"sql":  "select 1", "session": {"role": BUILTIN_ROLE_PUBLIC, "secondary_roles": Vec::<String>::new()}});
    let (_, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
    assert!(result.error.is_none());
    assert_eq!(result.state, ExecuteStateKind::Succeeded);
    assert_eq!(result.session.unwrap().secondary_roles, Some(vec![]));

    let json = serde_json::json!({"sql":  "select 1", "session": {"role": BUILTIN_ROLE_PUBLIC}});
    let (_, result) = post_json_to_endpoint(&route, &json, HeaderMap::default()).await?;
    assert!(result.error.is_none());
    assert_eq!(result.state, ExecuteStateKind::Succeeded);
    assert_eq!(result.session.unwrap().secondary_roles, None);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_auth_configured_user() -> anyhow::Result<()> {
    let user_name = "conf_user";
    let pass_word = "conf_user_pwd";
    let hash_method = PasswordHashMethod::DoubleSha1;
    let hash_value = hash_method.hash(pass_word.as_bytes());

    let user_config = UserConfig {
        name: user_name.to_string(),
        auth: UserAuthConfig {
            auth_type: "double_sha1_password".to_string(),
            auth_string: Some(hex::encode(hash_value)),
        },
    };

    let config = ConfigBuilder::create()
        .add_user(user_name, user_config)
        .build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let mut req = TestHttpQueryRequest::new(serde_json::json!({"sql": "select current_user()"}))
        .with_basic_auth(user_name, pass_word);
    let resp = req.fetch_total().await?.data();
    let v = unwrap_data(&resp, "");

    assert_eq!(v.len(), 1);
    assert_eq!(v[0].len(), 1);
    assert_eq!(
        v[0][0],
        serde_json::Value::String(format!("'{}'@'%'", user_name))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_error() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let wait_time_secs = 5;

    let json =
        serde_json::json!({"sql": "begin", "pagination": {"wait_time_secs": wait_time_secs}});
    let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
    let last = reply.last().1;
    let session = last.session.unwrap();

    {
        let mut session = session.clone();
        if let Some(info) = &mut session.internal.as_mut() {
            info.last_node_id = Some("abc".to_string());
        }
        let json = serde_json::json!({
            "sql": "select 1",
            "session": session,
            "pagination": {"wait_time_secs": wait_time_secs}
        });
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        assert_eq!(reply.last().1.error.unwrap().code, 5111u16);
        assert!(reply.last().1.error.unwrap().message.contains("restart"),);
    }

    {
        let mut session = session.clone();
        if let Some(s) = &mut session.internal.as_mut() {
            s.last_node_id = Some("abc".to_string())
        }
        let json = serde_json::json!({
            "sql": "select 1",
            "session": session,
            "pagination": {"wait_time_secs": wait_time_secs}
        });
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        assert_eq!(reply.last().1.error.unwrap().code, 5111u16);
        let message = reply.last().1.error.unwrap().message;
        assert!(
            reply.last().1.error.unwrap().message.contains("restart"),
            "error message={}",
            message
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_timeout() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let wait_time_secs = 5;

    let json = serde_json::json!({"sql": "begin", "session": { "settings": {"idle_transaction_timeout_secs": "1"}}, "pagination": {"wait_time_secs": wait_time_secs}});
    let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
    let last = reply.last().1;
    let session = last.session.unwrap();
    sleep(Duration::from_secs(3)).await;

    let session = session.clone();
    let last_query_id = session
        .internal
        .as_ref()
        .unwrap()
        .last_query_ids
        .first()
        .unwrap()
        .to_string();
    let json = serde_json::json!({
        "sql": "select 1",
        "session": session,
        "pagination": {"wait_time_secs": wait_time_secs}
    });
    let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
    assert_eq!(reply.last().1.error.unwrap().code, 4003u16);
    assert_eq!(
        reply.last().1.error.unwrap().message,
        format!(
            "Transaction timeout: last_query_id {} not found on this server",
            last_query_id
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_has_result_set() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let sqls = vec![
        ("create table tb2(id int, c1 varchar) Engine=Fuse;", false),
        ("insert into tb2 values(1, 'mysql'),(1, 'databend')", true),
        ("select * from tb2;", true),
    ];

    let wait_time_secs = 5;
    for (sql, has_result_set) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
        let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
        assert!(reply.error().is_none(), "{:?}", reply.error());
        assert_eq!(
            reply.state(),
            ExecuteStateKind::Succeeded,
            "SQL '{sql}' not finish after {wait_time_secs} secs"
        );
        assert_eq!(reply.last().1.has_result_set, Some(has_result_set));
    }

    Ok(())
}

// FIXME:
#[ignore]
#[tokio::test(flavor = "current_thread")]
async fn test_max_size_per_page() -> anyhow::Result<()> {
    let _fixture =
        TestFixture::setup_with_config(&ConfigBuilder::create().off_log().config()).await?;

    let sql = "select repeat('1', 1000) as a, repeat('2', 1000) from numbers(10000)";
    let wait_time_secs = 5;
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    let (_, reply, body) = TestHttpQueryRequest::new(json).fetch_begin().await?;
    assert!(reply.error.is_none(), "{:?}", reply.error);
    let target = (4_usize * 1024 * 1024) as f64;
    assert!(
        (0.9..1.1).contains(&(body.len() as f64 / target)),
        "body len {} rows {}",
        body.len(),
        reply.data.len()
    );
    Ok(())
}

// FIXME:
#[ignore]
#[tokio::test(flavor = "current_thread")]
async fn test_max_size_per_page_total_rows() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    // bytes_limit / rows_limit = 1024 * 1024 * 10 / 10000 = 1048.567
    let sql = "select repeat('1', 1050) as a from numbers(20000)";
    let wait_time_secs = 5;
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    let reply = TestHttpQueryRequest::new(json).fetch_total().await?;
    assert!(reply.error().is_none(), "{:?}", reply.error());
    assert_eq!(reply.resps.len(), 6);
    assert_eq!(reply.data().len(), 20000, "{:?}", reply.error());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_response() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    {
        let sql = "select NULL";
        let json = serde_json::json!({"sql": sql.to_string(), "session": {"settings": {"format_null_as_str": "1"}}});
        let mut req = TestHttpQueryRequest::new(json);
        let resp = req.fetch_total().await?.data();

        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].len(), 1);
        assert_eq!(resp[0][0].clone().unwrap_or_default(), "NULL");
    }

    {
        let sql = "select NULL";
        let json = serde_json::json!({"sql": sql.to_string(), "session": {"settings": {"format_null_as_str": "0"}}});
        let mut req = TestHttpQueryRequest::new(json);
        let resp = req.fetch_total().await?.data();

        assert_eq!(resp[0].len(), 1);
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0][0], None);
    }

    Ok(())
}
