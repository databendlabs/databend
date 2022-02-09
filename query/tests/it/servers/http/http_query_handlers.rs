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
use std::time::Duration;

use base64::encode_config;
use base64::URL_SAFE_NO_PAD;
use common_base::tokio;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::UserInfo;
use databend_query::servers::http::v1::make_final_uri;
use databend_query::servers::http::v1::make_page_uri;
use databend_query::servers::http::v1::make_state_uri;
use databend_query::servers::http::v1::middleware::HTTPSessionEndpoint;
use databend_query::servers::http::v1::middleware::HTTPSessionMiddleware;
use databend_query::servers::http::v1::query_route;
use databend_query::servers::http::v1::ExecuteStateName;
use databend_query::servers::http::v1::QueryResponse;
use databend_query::servers::HttpHandler;
use headers::Header;
use httpmock::MockServer;
use hyper::header;
use jwt_simple::algorithms::RS256KeyPair;
use jwt_simple::algorithms::RSAKeyPairLike;
use jwt_simple::claims::JWTClaims;
use jwt_simple::claims::NoCustomClaims;
use jwt_simple::prelude::Clock;
use poem::http::Method;
use poem::http::StatusCode;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Response;
use poem::Route;
use pretty_assertions::assert_eq;
use tokio::time::sleep;

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

type EndpointType = HTTPSessionEndpoint<Route>;

// TODO(youngsofun): add test for
// 1. query fail after started

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
#[ignore]
async fn test_async() -> Result<()> {
    let ep = create_endpoint();
    let sql = "select sleep(0.2)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 0}});

    let (status, result) = post_json_to_endpoint(&ep, &json).await?;
    assert_eq!(status, StatusCode::OK);
    let query_id = result.id;
    let next_uri = make_page_uri(&query_id, 0);
    assert_eq!(result.data.len(), 0);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.next_uri, Some(next_uri));
    assert!(result.stats.progress.is_some());
    assert!(result.schema.is_some());
    assert_eq!(result.state, ExecuteStateName::Running,);
    sleep(Duration::from_millis(300)).await;

    // get page, support retry
    for _ in 1..3 {
        let uri = make_page_uri(&query_id, 0);

        let (status, result) = get_uri_checked(&ep, &uri).await?;
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
    let (status, result) = get_uri_checked(&ep, &uri).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.data.len(), 0);
    assert!(result.next_uri.is_none());
    assert!(result.schema.is_none());
    assert!(result.stats.progress.is_some());
    assert_eq!(result.state, ExecuteStateName::Succeeded);

    // get page not expected
    let uri = make_page_uri(&query_id, 1);
    let response = get_uri(&ep, &uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response.into_body().into_string().await.unwrap();
    assert_eq!(body, "wrong page number 1");

    // delete
    let status = delete_query(&ep, query_id.clone()).await;
    assert_eq!(status, StatusCode::OK);

    let response = get_uri(&ep, &uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_result_timeout() -> Result<()> {
    let session_manager = SessionManagerBuilder::create()
        .http_handler_result_time_out(200u64)
        .build()
        .unwrap();
    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(HTTPSessionMiddleware { session_manager });

    let sql = "select sleep(0.1)";
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 0}});
    let (status, result) = post_json_to_endpoint(&ep, &json).await?;
    assert_eq!(status, StatusCode::OK);
    let query_id = result.id;
    let next_uri = make_page_uri(&query_id, 0);
    assert_eq!(result.next_uri, Some(next_uri.clone()));

    sleep(Duration::from_millis(110)).await;
    let response = get_uri(&ep, &next_uri).await;
    assert_eq!(response.status(), StatusCode::OK);

    sleep(std::time::Duration::from_millis(210)).await;
    let response = get_uri(&ep, &next_uri).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_multi_page() -> Result<()> {
    let session_manager = SessionManagerBuilder::create().build().unwrap();
    let num_parts = session_manager.get_conf().query.num_cpus as usize;
    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(HTTPSessionMiddleware { session_manager });

    let max_block_size = 10000;
    let sql = format!("select * from numbers({})", max_block_size * num_parts);

    let json = serde_json::json!({"sql": sql.to_string(),  "pagination": {"wait_time_secs": 3}});
    let (status, result) = post_json_to_endpoint(&ep, &json).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.data.len(), max_block_size);
    let query_id = result.id;
    let mut next_uri = make_page_uri(&query_id, 1);

    for p in 1..(num_parts + 1) {
        let (status, result) = get_uri_checked(&ep, &next_uri).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(result.stats.progress.is_some());
        if p == num_parts {
            assert_eq!(result.data.len(), 0);
            assert_eq!(result.next_uri, None);
            assert_eq!(result.state, ExecuteStateName::Succeeded);
        } else {
            next_uri = make_page_uri(&query_id, p + 1);
            assert_eq!(result.data.len(), max_block_size);
            assert_eq!(result.next_uri, Some(next_uri.clone()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_insert() -> Result<()> {
    let route = create_endpoint();

    let sqls = vec![
        ("create table t(a int) engine=fuse", 0),
        ("insert into t(a) values (1),(2)", 0),
        ("select * from t", 2),
    ];

    for (sql, data_len) in sqls {
        let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": 3}});
        let (status, result) = post_json_to_endpoint(&route, &json).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.data.len(), data_len);
        assert_eq!(result.state, ExecuteStateName::Succeeded);
    }
    Ok(())
}

async fn delete_query(ep: &EndpointType, query_id: String) -> StatusCode {
    let uri = make_final_uri(&query_id);
    let resp = get_uri(ep, &uri).await;
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

async fn get_uri(ep: &EndpointType, uri: &str) -> Response {
    ep.call(
        Request::builder()
            .uri(uri.parse().unwrap())
            .method(Method::GET)
            .finish(),
    )
    .await
    .unwrap_or_else(|err| err.as_response())
}

async fn get_uri_checked(ep: &EndpointType, uri: &str) -> Result<(StatusCode, QueryResponse)> {
    let response = get_uri(ep, uri).await;
    check_response(response).await
}

async fn post_sql(sql: &str, wait_time_secs: u64) -> Result<(StatusCode, QueryResponse)> {
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    post_json(&json).await
}

pub fn create_endpoint() -> EndpointType {
    let session_manager = SessionManagerBuilder::create().build().unwrap();
    Route::new()
        .nest("/v1/query", query_route())
        .with(HTTPSessionMiddleware { session_manager })
}

async fn post_json(json: &serde_json::Value) -> Result<(StatusCode, QueryResponse)> {
    let ep = create_endpoint();
    post_json_to_endpoint(&ep, json).await
}

async fn post_sql_to_endpoint(
    ep: &EndpointType,
    sql: &str,
    wait_time_secs: u64,
) -> Result<(StatusCode, QueryResponse)> {
    let json = serde_json::json!({"sql": sql.to_string(), "pagination": {"wait_time_secs": wait_time_secs}});
    post_json_to_endpoint(ep, &json).await
}

async fn post_json_to_endpoint(
    ep: &EndpointType,
    json: &serde_json::Value,
) -> Result<(StatusCode, QueryResponse)> {
    let uri = "/v1/query";
    let content_type = "application/json";
    let body = serde_json::to_vec(&json)?;

    let response = ep
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .header(header::CONTENT_TYPE, content_type)
                .body(body),
        )
        .await
        .unwrap();

    check_response(response).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_basic() -> Result<()> {
    let user_name = "user1";
    let password = "password";

    let ep = create_endpoint();
    let sql = format!("create user {} identified by {}", user_name, password);
    post_sql_to_endpoint(&ep, &sql, 1).await?;

    let basic = headers::Authorization::basic(user_name, password);
    test_auth_post(&ep, user_name, basic).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_jwt() -> Result<()> {
    let user_name = "user1";

    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = encode_config(rsa_components.e, URL_SAFE_NO_PAD);
    let n = encode_config(rsa_components.n, URL_SAFE_NO_PAD);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start();
    let path = "/jwks.json";
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path(path);
        then.status(200)
            .header("content-type", "application/json")
            .body(j);
    });
    let jwks_url = format!("http://{}{}", server.address(), path);

    let session_manager = SessionManagerBuilder::create()
        .jwt_key_file(jwks_url)
        .build()
        .unwrap();

    let user_info = UserInfo {
        name: user_name.to_string(),
        hostname: "%".to_string(),
        auth_info: AuthInfo::JWT,
        grants: Default::default(),
        quota: Default::default(),
    };

    let tenant = "test";
    session_manager
        .get_user_manager()
        .add_user(tenant, user_info)
        .await?;

    let ep = Route::new()
        .nest("/v1/query", query_route())
        .with(HTTPSessionMiddleware { session_manager });

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
    test_auth_post(&ep, user_name, bear).await?;
    mock.assert();
    Ok(())
}

async fn test_auth_post(ep: &EndpointType, user_name: &str, header: impl Header) -> Result<()> {
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
        serde_json::Value::String(format!("'{}'@'%'", user_name))
    );
    Ok(())
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
