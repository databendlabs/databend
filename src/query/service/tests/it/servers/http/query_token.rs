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

use std::io::Cursor;
use std::time::Duration;

use arrow_ipc::reader::StreamReader;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use databend_common_base::headers::HEADER_AUTH_METHOD;
use databend_common_base::headers::HEADER_CLIENT_CAPABILITIES;
use databend_common_base::headers::HEADER_NODE_ID;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_QUERY_TOKEN;
use databend_common_base::headers::HEADER_SESSION;
use databend_common_base::headers::HEADER_TENANT;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use headers::HeaderMapExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use poem::Endpoint;
use poem::Request;
use poem::Response;
use poem::Route;
use serde_json::json;

use super::http_query_handlers::TestQueryResponse;
use super::http_query_handlers::create_endpoint;
use super::http_query_handlers::post_sql_to_endpoint_new_session;

fn basic_headers(user: &str, password: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.typed_insert(headers::Authorization::basic(user, password));
    headers
}

fn token_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_AUTH_METHOD, HeaderValue::from_static("query-token"));
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

async fn request(
    ep: &Route,
    method: Method,
    uri: &str,
    headers: HeaderMap,
    body: serde_json::Value,
) -> Response {
    let submission = method == Method::POST && uri == "/v1/query";
    let mut req = Request::builder()
        .method(method)
        .uri(uri.parse().unwrap())
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&body).unwrap());
    req.headers_mut().extend(headers);
    let response = ep.call(req).await.unwrap_or_else(|err| err.into_response());
    if !submission {
        assert!(!response.headers().contains_key(HEADER_QUERY_TOKEN));
    }
    response
}

fn response_token(response: &Response) -> anyhow::Result<Option<String>> {
    let token = response.headers().get(HEADER_QUERY_TOKEN);
    if let Some(token) = token {
        assert_eq!(response.headers()[http::header::CACHE_CONTROL], "no-store");
        for name in [HEADER_QUERY_ID, HEADER_QUERY_TOKEN] {
            assert!(
                response.headers()[http::header::ACCESS_CONTROL_EXPOSE_HEADERS]
                    .to_str()?
                    .split(',')
                    .any(|header| header.trim().eq_ignore_ascii_case(name))
            );
        }
        assert!(token.is_sensitive());
        assert_eq!(token.to_str()?.len(), 64);
    }
    Ok(token
        .map(|token| token.to_str().map(str::to_string))
        .transpose()?)
}

async fn decode_json(response: Response) -> anyhow::Result<TestQueryResponse> {
    let query_token = response_token(&response)?;
    let value: serde_json::Value = response.into_body().into_json().await?;
    assert!(value.get("query_token").is_none());
    let mut result: TestQueryResponse = serde_json::from_value(value)?;
    result.query_token = query_token;
    Ok(result)
}

fn client_session_header(id: &str) -> HeaderValue {
    let json = json!({"id": id, "last_refresh_time": 1}).to_string();
    HeaderValue::from_str(&URL_SAFE.encode(json)).unwrap()
}

async fn start(ep: &Route, headers: HeaderMap) -> anyhow::Result<TestQueryResponse> {
    start_with_timeout(ep, headers, 60).await
}

async fn start_with_timeout(
    ep: &Route,
    headers: HeaderMap,
    timeout_secs: u64,
) -> anyhow::Result<TestQueryResponse> {
    start_query(ep, headers, timeout_secs, "SELECT * FROM numbers(5)").await
}

async fn start_query(
    ep: &Route,
    mut headers: HeaderMap,
    timeout_secs: u64,
    sql: &str,
) -> anyhow::Result<TestQueryResponse> {
    headers
        .entry(http::header::AUTHORIZATION)
        .or_insert_with(|| basic_headers("root", "")[http::header::AUTHORIZATION].clone());
    let response = request(
        ep,
        Method::POST,
        "/v1/query",
        headers,
        json!({
            "sql": sql,
            "pagination": {"wait_time_secs": 5, "max_rows_per_page": 1},
            "session": {"settings": {"http_handler_result_timeout_secs": timeout_secs.to_string()}}
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result = decode_json(response).await?;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.data.len(), 1);
    assert_eq!(result.query_token.as_ref().unwrap().len(), 64);
    Ok(result)
}

async fn admin_sql(ep: &Route, sql: &str) -> anyhow::Result<()> {
    let (status, result) =
        post_sql_to_endpoint_new_session(ep, sql, 5, HeaderMap::default()).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.state.to_string(), "Succeeded");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_followups_and_retries() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let result = start(&ep, HeaderMap::new()).await?;
    let token = result.query_token.as_ref().unwrap();
    let headers = token_headers(token);

    // Authenticate using the path ID, never a caller-supplied query ID header.
    let mut state_headers = headers.clone();
    state_headers.insert(
        HEADER_QUERY_ID,
        HeaderValue::from_static("unrelated-header-id"),
    );
    let state = request(
        &ep,
        Method::GET,
        &format!("/v1/query/{}", result.id),
        state_headers,
        json!(null),
    )
    .await;
    assert_eq!(state.status(), StatusCode::OK);
    let state: serde_json::Value = state.into_body().into_json().await?;
    assert!(state.get("query_token").is_none());

    let page = result.next_uri.as_ref().unwrap();
    // Token delivery is tied to submission, not page number: even page/0 and
    // retries of an authorized page must omit the capability.
    for uri in [format!("/v1/query/{}/page/0", result.id), page.clone()] {
        for _ in 0..2 {
            let response = request(&ep, Method::GET, &uri, headers.clone(), json!(null)).await;
            assert_eq!(response.status(), StatusCode::OK);
            let response: serde_json::Value = response.into_body().into_json().await?;
            assert!(response.get("query_token").is_none());
            let response: TestQueryResponse = serde_json::from_value(response)?;
            assert_eq!(response.data.len(), 1);
        }
    }

    // Retain the capability for the same closed-query period as legacy final
    // retries, rather than inventing a separate token expiration/refresh timer.
    for method in [Method::POST, Method::GET] {
        let response = request(
            &ep,
            method,
            result.final_uri.as_ref().unwrap(),
            headers.clone(),
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response: serde_json::Value = response.into_body().into_json().await?;
        assert!(response.get("query_token").is_none());
    }
    let closed = request(&ep, Method::GET, page, headers, json!(null)).await;
    assert_eq!(closed.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_scope_and_auth_dispatch() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let first = start(&ep, HeaderMap::new()).await?;
    let second = start(&ep, HeaderMap::new()).await?;
    let token = first.query_token.as_ref().unwrap();
    assert_ne!(first.query_token, second.query_token);

    for uri in [
        format!("/v1/query/{}", second.id),
        second.next_uri.clone().unwrap(),
        second.final_uri.clone().unwrap(),
        second.kill_uri.clone().unwrap(),
    ] {
        let response = request(&ep, Method::GET, &uri, token_headers(token), json!(null)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!response.into_body().into_string().await?.contains(token));
    }
    for bad in ["", "not-hex", &"0".repeat(64)] {
        let response = request(
            &ep,
            Method::GET,
            first.next_uri.as_ref().unwrap(),
            token_headers(bad),
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    for uri in [
        "/v1/query",
        "/v1/session/login",
        "/v1/session/refresh",
        "/v1/session/logout",
        "/v1/session/heartbeat",
        "/v1/verify",
    ] {
        let response = request(
            &ep,
            Method::POST,
            uri,
            token_headers(token),
            json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    // This token is not a JWT or a native session/refresh token; dispatch is
    // explicit, and Basic authentication cannot masquerade as a query token.
    // The former spelling is not an alias for the explicit query-token method.
    for method in [None, Some("query")] {
        let mut headers = token_headers(token);
        headers.remove(HEADER_AUTH_METHOD);
        if let Some(method) = method {
            headers.insert(HEADER_AUTH_METHOD, HeaderValue::from_static(method));
        }
        let response = request(
            &ep,
            Method::GET,
            first.next_uri.as_ref().unwrap(),
            headers,
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    for invalid in ["Basic cm9vdDo=", "", "B", "Bearer", "BearerX token"] {
        let mut headers = token_headers(token);
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static(invalid),
        );
        let response = request(
            &ep,
            Method::GET,
            first.next_uri.as_ref().unwrap(),
            headers,
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let mut mixed_case = token_headers(token);
    mixed_case.insert(HEADER_AUTH_METHOD, HeaderValue::from_static("QuErY-ToKeN"));
    mixed_case.insert(
        http::header::AUTHORIZATION,
        format!("bEaReR {token}").parse()?,
    );
    let response = request(
        &ep,
        Method::GET,
        first.stats_uri.as_ref().unwrap(),
        mixed_case,
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut spoofed_id = token_headers(token);
    spoofed_id.insert(HEADER_QUERY_ID, first.id.parse()?);
    let response = request(
        &ep,
        Method::GET,
        second.next_uri.as_ref().unwrap(),
        spoofed_id,
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let mut duplicate = token_headers(token);
    duplicate.append(HEADER_AUTH_METHOD, HeaderValue::from_static("keypair"));
    let response = request(
        &ep,
        Method::GET,
        first.next_uri.as_ref().unwrap(),
        duplicate,
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let mut missing_auth = token_headers(token);
    missing_auth.remove(http::header::AUTHORIZATION);
    let mut duplicate_auth = token_headers(token);
    duplicate_auth.append(
        http::header::AUTHORIZATION,
        basic_headers("root", "")[http::header::AUTHORIZATION].clone(),
    );
    for headers in [missing_auth, duplicate_auth] {
        let response = request(
            &ep,
            Method::GET,
            first.next_uri.as_ref().unwrap(),
            headers,
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    for _ in 0..2 {
        let response = request(
            &ep,
            Method::POST,
            first.kill_uri.as_ref().unwrap(),
            token_headers(token),
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_missing_query_and_routing() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let result = start(&ep, HeaderMap::new()).await?;
    let token = result.query_token.as_ref().unwrap();
    for suffix in ["", "/page/1", "/final", "/kill"] {
        for method in [Method::GET, Method::POST] {
            if method == Method::POST && !matches!(suffix, "/final" | "/kill") {
                continue;
            }
            let response = request(
                &ep,
                method,
                &format!("/v1/query/missing-query{suffix}"),
                token_headers(token),
                json!(null),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let body = response.into_body().into_string().await?;
            assert!(body.contains("not found on node"), "{body}");
            assert!(!body.contains(token));
        }
    }
    let mut wrong_node = token_headers(token);
    wrong_node.insert(
        HEADER_NODE_ID,
        HeaderValue::from_static("another-query-node"),
    );
    let response = request(
        &ep,
        Method::GET,
        result.next_uri.as_ref().unwrap(),
        wrong_node.clone(),
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response
            .into_body()
            .into_string()
            .await?
            .contains("Routing error:")
    );
    let response = request(
        &ep,
        Method::GET,
        "/v1/query/missing-query/page/1",
        wrong_node,
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_after_password_change_and_user_deletion() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    admin_sql(&ep, "CREATE USER query_token_user IDENTIFIED BY 'before'").await?;
    admin_sql(&ep, "GRANT ROLE account_admin TO query_token_user").await?;
    let basic = basic_headers("query_token_user", "before");
    let result = start(&ep, basic.clone()).await?;
    let token = result.query_token.as_ref().unwrap();
    admin_sql(&ep, "ALTER USER query_token_user IDENTIFIED BY 'after'").await?;
    let old = request(
        &ep,
        Method::GET,
        result.next_uri.as_ref().unwrap(),
        basic.clone(),
        json!(null),
    )
    .await;
    assert_eq!(old.status(), StatusCode::UNAUTHORIZED);
    let page = request(
        &ep,
        Method::GET,
        result.next_uri.as_ref().unwrap(),
        token_headers(token),
        json!(null),
    )
    .await;
    assert_eq!(page.status(), StatusCode::OK);

    // Stronger than simply changing the password: even when user lookup can
    // no longer succeed, this already-authorized query remains accessible.
    admin_sql(&ep, "DROP USER query_token_user").await?;
    let state = request(
        &ep,
        Method::GET,
        &format!("/v1/query/{}", result.id),
        token_headers(token),
        json!(null),
    )
    .await;
    assert_eq!(state.status(), StatusCode::OK);
    let page = request(
        &ep,
        Method::GET,
        result.next_uri.as_ref().unwrap(),
        token_headers(token),
        json!(null),
    )
    .await;
    assert_eq!(page.status(), StatusCode::OK);
    let final_response = request(
        &ep,
        Method::POST,
        result.final_uri.as_ref().unwrap(),
        token_headers(token),
        json!(null),
    )
    .await;
    assert_eq!(final_response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_self_password_change_result() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    admin_sql(&ep, "CREATE USER self_change_user IDENTIFIED BY 'before'").await?;
    admin_sql(&ep, "GRANT ROLE account_admin TO self_change_user").await?;
    let response = request(
        &ep,
        Method::POST,
        "/v1/query",
        basic_headers("self_change_user", "before"),
        json!({
            "sql": "ALTER USER USER() IDENTIFIED BY 'after'",
            "pagination": {"wait_time_secs": 0}
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    // Keep the capability as soon as headers arrive, even if the initial body
    // is lost after the SQL has started changing the user's credentials.
    let token = response_token(&response)?.unwrap();
    let initial_page_uri = format!(
        "/v1/query/{}/page/0",
        response.headers()[HEADER_QUERY_ID].to_str()?
    );
    drop(response);
    let response = request(
        &ep,
        Method::GET,
        &initial_page_uri,
        token_headers(&token),
        json!(null),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut result = decode_json(response).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !result.state.is_stopped() {
            let page = request(
                &ep,
                Method::GET,
                result.next_uri.as_ref().unwrap(),
                token_headers(&token),
                json!(null),
            )
            .await;
            assert_eq!(page.status(), StatusCode::OK);
            result = page.into_body().into_json().await?;
        }
        anyhow::Ok(())
    })
    .await??;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.state.to_string(), "Succeeded");
    let final_response = request(
        &ep,
        Method::POST,
        result.final_uri.as_ref().unwrap(),
        token_headers(&token),
        json!(null),
    )
    .await;
    assert_eq!(final_response.status(), StatusCode::OK);
    let body = json!({"sql": "SELECT 1", "pagination": {"wait_time_secs": 5}});
    let old = request(
        &ep,
        Method::POST,
        "/v1/query",
        basic_headers("self_change_user", "before"),
        body.clone(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::UNAUTHORIZED);
    let new = request(
        &ep,
        Method::POST,
        "/v1/query",
        basic_headers("self_change_user", "after"),
        body,
    )
    .await;
    assert_eq!(new.status(), StatusCode::OK);
    let new: TestQueryResponse = new.into_body().into_json().await?;
    assert!(new.error.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_tenant_and_session_isolation() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.query.common.management_mode = true;
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let ep = create_endpoint()?;
    let session = client_session_header("query-token-session");
    let mut headers = basic_headers("root", "");
    headers.insert(
        HEADER_TENANT,
        HeaderValue::from_static("query-token-tenant"),
    );
    headers.insert(
        HEADER_CLIENT_CAPABILITIES,
        HeaderValue::from_static("session_header"),
    );
    headers.insert(HEADER_SESSION, session.clone());
    headers.insert(
        HEADER_QUERY_ID,
        HeaderValue::from_static("query-token-fixed-id"),
    );
    let result = start_query(&ep, headers.clone(), 60, "SHOW SETTINGS").await?;
    let token = result.query_token.as_ref().unwrap();
    let mut auth = token_headers(token);
    auth.insert(
        HEADER_CLIENT_CAPABILITIES,
        HeaderValue::from_static("session_header"),
    );
    auth.insert(HEADER_SESSION, session);
    // Retry a lost submission response before advancing to the next page: only
    // the most recent result page is retained for response retries.
    let body = json!({"sql": "SELECT 1"});
    let retry = request(
        &ep,
        Method::POST,
        "/v1/query",
        headers.clone(),
        body.clone(),
    )
    .await;
    assert_eq!(retry.status(), StatusCode::OK);
    let retry = decode_json(retry).await?;
    assert_eq!(retry.query_token.as_ref(), Some(token));
    let page = result.next_uri.as_ref().unwrap();
    // Tenant comes from the validated query capability when omitted.
    assert_eq!(
        request(&ep, Method::GET, page, auth.clone(), json!(null))
            .await
            .status(),
        StatusCode::OK
    );
    let mut wrong_tenant = auth.clone();
    wrong_tenant.insert(HEADER_TENANT, HeaderValue::from_static("another-tenant"));
    assert_eq!(
        request(&ep, Method::GET, page, wrong_tenant, json!(null))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let mut wrong_session = auth.clone();
    wrong_session.insert(HEADER_SESSION, client_session_header("another-session"));
    assert_eq!(
        request(&ep, Method::GET, page, wrong_session, json!(null))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&ep, Method::GET, page, token_headers(token), json!(null))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );

    // Submission retries must not disclose the capability to a different
    // tenant or client session, even after the original page has advanced.
    let mut wrong_session = headers.clone();
    wrong_session.insert(HEADER_SESSION, client_session_header("another-session"));
    let retry = request(&ep, Method::POST, "/v1/query", wrong_session, body.clone()).await;
    assert_eq!(retry.status(), StatusCode::BAD_REQUEST);
    headers.insert(HEADER_TENANT, HeaderValue::from_static("another-tenant"));
    let retry = request(&ep, Method::POST, "/v1/query", headers, body).await;
    assert_eq!(retry.status(), StatusCode::BAD_REQUEST);

    // Legacy Basic authentication must not disclose the new capability or
    // operate on this query from another tenant either.
    let mut wrong_tenant = auth;
    wrong_tenant.insert(
        http::header::AUTHORIZATION,
        basic_headers("root", "")[http::header::AUTHORIZATION].clone(),
    );
    wrong_tenant.remove(HEADER_AUTH_METHOD);
    wrong_tenant.insert(HEADER_TENANT, HeaderValue::from_static("another-tenant"));
    for uri in [
        result.stats_uri.as_ref().unwrap(),
        result.final_uri.as_ref().unwrap(),
        result.kill_uri.as_ref().unwrap(),
    ] {
        let response = request(&ep, Method::GET, uri, wrong_tenant.clone(), json!(null)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_sandbox_owner_with_legacy_auth() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.query.common.internal_enable_sandbox_tenant = true;
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let ep = create_endpoint()?;
    let basic = basic_headers("root", "");
    let mut session = basic.clone();
    session.insert(
        HEADER_CLIENT_CAPABILITIES,
        HeaderValue::from_static("session_header"),
    );
    session.insert(
        HEADER_SESSION,
        client_session_header("sandbox-legacy-session"),
    );
    let login = request(
        &ep,
        Method::POST,
        "/v1/session/login",
        session.clone(),
        json!({}),
    )
    .await;
    assert_eq!(login.status(), StatusCode::OK);
    let login: serde_json::Value = login.into_body().into_json().await?;
    let token = login["tokens"]["session_token"].as_str().unwrap();
    session.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {token}").parse()?,
    );

    for (method, headers) in [("basic", basic), ("session", session)] {
        // Mirror the HTTP sqllogictest flow: SET returns session settings that
        // are sent back in the next query body, but never on follow-up GETs.
        let response = request(
            &ep,
            Method::POST,
            "/v1/query",
            headers.clone(),
            json!({
                "sql": "SET sandbox_tenant = 'legacy-sandbox'",
                "pagination": {"wait_time_secs": 5}
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let settings_query: TestQueryResponse = response.into_body().into_json().await?;
        assert!(settings_query.error.is_none(), "{:?}", settings_query.error);
        assert_eq!(settings_query.state.to_string(), "Succeeded");
        let restored = settings_query.session.as_ref().unwrap();
        assert_eq!(
            restored.settings.as_ref().unwrap()["sandbox_tenant"],
            "legacy-sandbox"
        );
        let response = request(
            &ep,
            Method::GET,
            settings_query.next_uri.as_ref().unwrap(),
            headers.clone(),
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{method} SET final");
        for close in ["final", "kill"] {
            let mut headers = headers.clone();
            headers.insert(
                HEADER_QUERY_ID,
                format!("sandbox-{method}-{close}").parse()?,
            );
            // This header is ineffective outside management mode, including
            // when sandbox selection comes from the request body's settings.
            headers.insert(
                HEADER_TENANT,
                HeaderValue::from_static("ignored-header-tenant"),
            );
            let body = json!({
                "sql": "SELECT * FROM numbers(5)",
                "pagination": {"wait_time_secs": 5, "max_rows_per_page": 1},
                "session": restored
            });
            let response = request(
                &ep,
                Method::POST,
                "/v1/query",
                headers.clone(),
                body.clone(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let mut result = decode_json(response).await?;
            assert!(result.error.is_none(), "{:?}", result.error);
            let retry = request(&ep, Method::POST, "/v1/query", headers.clone(), body).await;
            assert_eq!(
                retry.status(),
                StatusCode::OK,
                "{method} sandbox submission retry"
            );
            let retry = decode_json(retry).await?;
            assert_eq!(retry.query_token, result.query_token);
            let mut capability = headers.clone();
            capability.extend(token_headers(result.query_token.as_ref().unwrap()));
            let response = request(
                &ep,
                Method::GET,
                result.stats_uri.as_ref().unwrap(),
                capability,
                json!(null),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{method} ignored tenant header with query token"
            );
            let state = request(
                &ep,
                Method::GET,
                result.stats_uri.as_ref().unwrap(),
                headers.clone(),
                json!(null),
            )
            .await;
            assert_eq!(state.status(), StatusCode::OK, "{method} sandbox state");
            let mut rows = result.data.len();
            while result
                .next_uri
                .as_ref()
                .is_some_and(|uri| uri.contains("/page/"))
            {
                let response = request(
                    &ep,
                    Method::GET,
                    result.next_uri.as_ref().unwrap(),
                    headers.clone(),
                    json!(null),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK, "{method} sandbox page");
                result = response.into_body().into_json().await?;
                rows += result.data.len();
            }
            assert_eq!(rows, 5);
            let uri = if close == "final" {
                result.final_uri.as_ref()
            } else {
                result.kill_uri.as_ref()
            }
            .unwrap();
            for verb in [Method::GET, Method::POST] {
                let response = request(&ep, verb, uri, headers.clone(), json!(null)).await;
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "{method} sandbox {close}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_preserves_sandbox_owner() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.query.common.internal_enable_sandbox_tenant = true;
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let ep = create_endpoint()?;
    let response = request(
        &ep,
        Method::POST,
        "/v1/query",
        basic_headers("root", ""),
        json!({
            "sql": "SELECT * FROM numbers(5)",
            "pagination": {"wait_time_secs": 5, "max_rows_per_page": 1},
            "session": {"settings": {"sandbox_tenant": "query-token-sandbox"}}
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result = decode_json(response).await?;
    assert!(result.error.is_none(), "{:?}", result.error);
    let token = result.query_token.as_ref().unwrap();
    let mut headers = token_headers(token);
    headers.insert(
        HEADER_TENANT,
        HeaderValue::from_static("query-token-sandbox"),
    );
    for headers in [headers, token_headers(token)] {
        let page = request(
            &ep,
            Method::GET,
            result.next_uri.as_ref().unwrap(),
            headers,
            json!(null),
        )
        .await;
        assert_eq!(page.status(), StatusCode::OK);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_not_disclosed_to_other_user() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let result = start(&ep, HeaderMap::new()).await?;
    admin_sql(&ep, "CREATE USER another_query_user IDENTIFIED BY 'test'").await?;
    let basic = basic_headers("another_query_user", "test");
    for uri in [
        result.stats_uri.unwrap(),
        result.next_uri.unwrap(),
        result.final_uri.unwrap(),
        result.kill_uri.unwrap(),
    ] {
        let response = request(&ep, Method::GET, &uri, basic.clone(), json!(null)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            !response
                .into_body()
                .into_string()
                .await?
                .contains(result.query_token.as_ref().unwrap())
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_token_expires_with_query_state() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let result = start_with_timeout(&ep, HeaderMap::new(), 1).await?;
    let token = result.query_token.as_ref().unwrap();
    let uri = result.final_uri.as_ref().unwrap();
    assert_eq!(
        request(&ep, Method::POST, uri, token_headers(token), json!(null))
            .await
            .status(),
        StatusCode::OK
    );
    // Finalized queries are retained for min(result_timeout_secs, 30s), then
    // both their result state and their authentication capability disappear.
    for _ in 0..200 {
        let response = request(
            &ep,
            Method::GET,
            &format!("/v1/query/{}", result.id),
            token_headers(token),
            json!(null),
        )
        .await;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        assert_eq!(response.status(), StatusCode::OK);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("query token outlived the finalized query retention period")
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_in_arrow_response_header() -> anyhow::Result<()> {
    async fn decode_arrow(response: Response) -> anyhow::Result<TestQueryResponse> {
        let query_token = response_token(&response)?;
        let reader =
            StreamReader::try_new(Cursor::new(response.into_body().into_vec().await?), None)?;
        let value: serde_json::Value =
            serde_json::from_str(&reader.schema().metadata()["response_header"])?;
        assert!(value.get("query_token").is_none());
        let mut result: TestQueryResponse = serde_json::from_value(value)?;
        result.query_token = query_token;
        Ok(result)
    }

    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let mut headers = basic_headers("root", "");
    headers.insert(
        http::header::ACCEPT,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    headers.insert(
        HEADER_QUERY_ID,
        HeaderValue::from_static("arrow-token-retry"),
    );
    let body = json!({
        "sql": "SELECT * FROM numbers(5)",
        "pagination": {"wait_time_secs": 5, "max_rows_per_page": 1},
        "arrow_result_version_max": 2
    });
    let response = request(
        &ep,
        Method::POST,
        "/v1/query",
        headers.clone(),
        body.clone(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result = decode_arrow(response).await?;
    let token = result.query_token.as_ref().unwrap();
    // Arrow submission retries recover the same capability only via HTTP headers.
    let retry = request(&ep, Method::POST, "/v1/query", headers, body).await;
    assert_eq!(retry.status(), StatusCode::OK);
    let retry = decode_arrow(retry).await?;
    assert_eq!(retry.query_token.as_ref(), Some(token));
    let mut headers = token_headers(token);
    headers.insert(
        http::header::ACCEPT,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    let page = request(
        &ep,
        Method::GET,
        result.next_uri.as_ref().unwrap(),
        headers,
        json!(null),
    )
    .await;
    assert_eq!(page.status(), StatusCode::OK);
    let page = decode_arrow(page).await?;
    assert!(page.query_token.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_header_for_arrow_json_fallback() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let mut headers = basic_headers("root", "");
    headers.insert(
        http::header::ACCEPT,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    let response = request(
        &ep,
        Method::POST,
        "/v1/query",
        headers,
        json!({
            "sql": "SELECT 1 WHERE false",
            "pagination": {"wait_time_secs": 5},
            "arrow_result_version_max": 2
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[http::header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    let result = decode_json(response).await?;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.state.to_string(), "Succeeded");
    assert!(result.data.is_empty());
    assert!(result.query_token.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_query_token_invalid_after_query_manager_reset() -> anyhow::Result<()> {
    let config = ConfigBuilder::create().build();
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let ep = create_endpoint()?;
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_QUERY_ID, HeaderValue::from_static("reused-query-id"));
    let result = start(&ep, headers.clone()).await?;
    // A separately initialized node has no copy of the original node's query
    // state, just like a restarted node. Keep global fixtures on separate named
    // threads rather than replacing the registry of a running server fixture.
    std::thread::Builder::new()
        .name("query-token-fresh-node".to_string())
        .spawn(move || -> anyhow::Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let _fixture = TestFixture::setup().await?;
                let ep = create_endpoint()?;
                let response = request(
                    &ep,
                    Method::GET,
                    result.next_uri.as_ref().unwrap(),
                    token_headers(result.query_token.as_ref().unwrap()),
                    json!(null),
                )
                .await;
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
                let replacement = start(&ep, headers).await?;
                assert_eq!(replacement.id, result.id);
                assert_ne!(replacement.query_token, result.query_token);
                for uri in [
                    replacement.stats_uri.as_ref().unwrap(),
                    replacement.next_uri.as_ref().unwrap(),
                    replacement.final_uri.as_ref().unwrap(),
                    replacement.kill_uri.as_ref().unwrap(),
                ] {
                    let response = request(
                        &ep,
                        Method::GET,
                        uri,
                        token_headers(result.query_token.as_ref().unwrap()),
                        json!(null),
                    )
                    .await;
                    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
                }
                let response = request(
                    &ep,
                    Method::GET,
                    replacement.next_uri.as_ref().unwrap(),
                    token_headers(replacement.query_token.as_ref().unwrap()),
                    json!(null),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK);
                Ok(())
            })
        })?
        .join()
        .expect("fresh-node query-token test panicked")
}
