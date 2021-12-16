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

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use databend_query::servers::http::v1::make_final_uri;
use databend_query::servers::http::v1::make_page_uri;
use databend_query::servers::http::v1::make_state_uri;
use databend_query::servers::http::v1::query_route;
use databend_query::servers::http::v1::ExecuteStateName;
use databend_query::servers::http::v1::QueryResponse;
use databend_query::servers::HttpHandler;
use databend_query::sessions::SessionManager;
use hyper::header;
use poem::http::Method;
use poem::http::StatusCode;
use poem::middleware::AddDataEndpoint;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Response;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;
use crate::tests::tls_constants::TEST_TLS_CA_CERT;
use crate::tests::tls_constants::TEST_TLS_CLIENT_IDENTITY;
use crate::tests::tls_constants::TEST_TLS_CLIENT_PASSWORD;
use crate::tests::tls_constants::TEST_TLS_SERVER_CERT;
use crate::tests::tls_constants::TEST_TLS_SERVER_KEY;
use crate::tests::SessionManagerBuilder;

// TODO(youngsofun): add test for
// 1. query fail after started

pub fn get_page_uri(query_id: &str, page_no: usize, wait_time: u32) -> String {
    format!(
        "{}?wait_time={}",
        make_page_uri(query_id, page_no),
        wait_time
    )
}

type RouteWithData = AddDataEndpoint<Route, Arc<SessionManager>>;

#[tokio::test]
async fn test_simple_sql() -> Result<()> {
    let sql = "select * from system.tables limit 10";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.data.len(), 10);
    assert_eq!(result.state, ExecuteStateName::Succeeded);
    assert!(result.next_uri.is_none(), "{:?}", result);
    assert!(result.stats.progress.is_some());
    assert!(result.schema.is_some());
    Ok(())
}

#[tokio::test]
async fn test_bad_sql() -> Result<()> {
    let (status, result) = post_sql("bad sql", 1).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(result.error.is_some());
    assert_eq!(result.data.len(), 0);
    assert!(result.next_uri.is_none());
    assert_eq!(result.state, ExecuteStateName::Failed);
    assert!(result.stats.progress.is_none());
    assert!(result.schema.is_none());
    Ok(())
}

#[tokio::test]
async fn test_async() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let route = Route::new().nest("/v1/query", query_route()).data(sessions);
    let sql = "select sleep(2)";
    let json = serde_json::json!({"sql": sql.to_string()});

    let (status, result) = post_json_to_router(&route, &json, 0).await?;
    assert_eq!(status, StatusCode::OK);
    let query_id = result.id;
    let next_uri = make_page_uri(&query_id, 0);
    assert_eq!(result.data.len(), 0);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.next_uri, Some(next_uri));
    assert!(result.stats.progress.is_some());
    assert!(result.schema.is_some());
    assert_eq!(result.state, ExecuteStateName::Running,);

    // get page, support retry
    for _ in 1..3 {
        let uri = get_page_uri(&query_id, 0, 3);

        let (status, result) = get_uri_checked(&route, &uri).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.len(), 1);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(result.next_uri.is_none());
        assert!(result.schema.is_none());
        assert!(result.stats.progress.is_some());
        assert_eq!(result.state, ExecuteStateName::Succeeded);
    }

    // get state
    let uri = make_state_uri(&query_id);
    let (status, result) = get_uri_checked(&route, &uri).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.data.len(), 0);
    assert!(result.next_uri.is_none());
    assert!(result.schema.is_none());
    assert!(result.stats.progress.is_some());
    assert_eq!(result.state, ExecuteStateName::Succeeded);

    // get page not expected
    let uri = get_page_uri(&query_id, 1, 3);
    let response = get_uri(&route, &uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response.into_body().into_string().await.unwrap();
    assert_eq!(body, "wrong page number 1");

    // delete
    let status = delete_query(&route, query_id.clone()).await;
    assert_eq!(status, StatusCode::OK);

    let response = get_uri(&route, &uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_multi_page() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let route = Route::new().nest("/v1/query", query_route()).data(sessions);

    let max_block_size = 10000;
    let num_parts = num_cpus::get();
    let sql = format!("select * from numbers({})", max_block_size * num_parts);

    let json = serde_json::json!({"sql": sql.to_string()});
    let (status, result) = post_json_to_router(&route, &json, 3).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.data.len(), max_block_size);
    let query_id = result.id;
    let mut next_uri = get_page_uri(&query_id, 1, 3);

    for p in 1..(num_parts + 1) {
        let (status, result) = get_uri_checked(&route, &next_uri).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(result.stats.progress.is_some());
        if p == num_parts {
            assert_eq!(result.data.len(), 0);
            assert_eq!(result.next_uri, None);
            assert_eq!(result.state, ExecuteStateName::Succeeded);
        } else {
            assert_eq!(result.data.len(), 10000);
            assert_eq!(result.next_uri, Some(make_page_uri(&query_id, p + 1)));
            next_uri = get_page_uri(&query_id, p + 1, 3);
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_insert() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let route = Route::new().nest("/v1/query", query_route()).data(sessions);

    let sqls = vec![
        ("create table t(a int) engine=fuse", 0),
        ("insert into t(a) values (1),(2)", 0),
        ("select * from t", 2),
    ];

    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string()});
        let (status, result) = post_json_to_router(&route, &json, 3).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.data.len(), data_len);
        assert_eq!(result.state, ExecuteStateName::Succeeded);
    }
    Ok(())
}

async fn delete_query(route: &RouteWithData, query_id: String) -> StatusCode {
    let uri = make_final_uri(&query_id);
    let resp = get_uri(route, &uri).await;
    resp.status()
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

async fn get_uri(route: &RouteWithData, uri: &str) -> Response {
    route
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::GET)
                .finish(),
        )
        .await
}
async fn get_uri_checked(route: &RouteWithData, uri: &str) -> Result<(StatusCode, QueryResponse)> {
    let response = get_uri(route, uri).await;
    check_response(response).await
}

async fn post_sql(sql: &'static str, wait_time: i32) -> Result<(StatusCode, QueryResponse)> {
    let json = serde_json::json!({"sql": sql.to_string()});
    post_json(&json, wait_time).await
}

pub fn create_router() -> RouteWithData {
    let sessions = SessionManagerBuilder::create().build().unwrap();
    Route::new().nest("/v1/query", query_route()).data(sessions)
}

async fn post_json(
    json: &serde_json::Value,
    wait_time: i32,
) -> Result<(StatusCode, QueryResponse)> {
    let router = create_router();
    post_json_to_router(&router, json, wait_time).await
}

async fn post_json_to_router(
    route: &RouteWithData,
    json: &serde_json::Value,
    wait_time: i32,
) -> Result<(StatusCode, QueryResponse)> {
    let path = "/v1/query";
    let uri = format!("{}?wait_time={}", path, wait_time);
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;

    let response = route
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .header(header::CONTENT_TYPE, content_type)
                .body(body),
        )
        .await;

    check_response(response).await
}

// need to support local_addr, but axum_server do not have local_addr callback
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_handler_tls_server() -> Result<()> {
    let address_str = "127.0.0.1:39000";
    let mut srv = HttpHandler::create(
        SessionManagerBuilder::create()
            .http_handler_tls_server_key(TEST_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_SERVER_CERT)
            .build()?,
    );

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
    let resp = client.post(&url).json(&json).send().await;
    assert!(resp.is_ok(), "{:?}", resp.err());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    let res = resp.json::<QueryResponse>().await;
    assert!(res.is_ok());
    let res = res.unwrap();
    assert!(res.data.len() > 0, "{:?}", res);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_handler_tls_server_failed_case_1() -> Result<()> {
    let address_str = "127.0.0.1:39001";
    let mut srv = HttpHandler::create(
        SessionManagerBuilder::create()
            .http_handler_tls_server_key(TEST_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_SERVER_CERT)
            .build()?,
    );

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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_mutual_tls() -> Result<()> {
    let addr_str = "127.0.0.1:39011";
    let mut srv = HttpHandler::create(
        SessionManagerBuilder::create()
            .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
            .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
            .build()?,
    );
    let listening = srv.start(addr_str.parse()?).await?;

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/query", TEST_CN_NAME, listening.port());
    let sql = "select * from system.tables limit 10";
    let json = serde_json::json!({"sql": sql.to_string()});

    // get identity
    let mut buf = Vec::new();
    File::open(TEST_TLS_CLIENT_IDENTITY)?.read_to_end(&mut buf)?;
    let pkcs12 = reqwest::Identity::from_pkcs12_der(&buf, TEST_TLS_CLIENT_PASSWORD).unwrap();
    let mut buf = Vec::new();
    File::open(TEST_TLS_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();
    // kick off
    let client = reqwest::Client::builder()
        .identity(pkcs12)
        .add_root_certificate(cert)
        .build()
        .expect("preconfigured rustls tls");
    let resp = client.post(&url).json(&json).send().await;
    assert!(resp.is_ok(), "{:?}", resp.err());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    let res = resp.json::<QueryResponse>().await;
    assert!(res.is_ok());
    let res = res.unwrap();
    assert!(res.data.len() > 0, "{:?}", res);
    Ok(())
}

// cannot connect with server unless it have CA signed identity
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server_mutual_tls_failed() -> Result<()> {
    let addr_str = "127.0.0.1:39012";
    let mut srv = HttpHandler::create(
        SessionManagerBuilder::create()
            .http_handler_tls_server_key(TEST_TLS_SERVER_KEY)
            .http_handler_tls_server_cert(TEST_TLS_SERVER_CERT)
            .http_handler_tls_server_root_ca_cert(TEST_TLS_CA_CERT)
            .build()?,
    );
    let listening = srv.start(addr_str.parse()?).await?;

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
