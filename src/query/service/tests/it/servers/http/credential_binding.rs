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

use databend_common_base::headers::HEADER_AUTH_METHOD;
use databend_common_base::headers::HEADER_CLIENT_CAPABILITIES;
use databend_common_base::headers::HEADER_SESSION;
use databend_common_base::headers::HEADER_TENANT;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use poem::Route;
use serde_json::json;

use super::http_query_handlers::TestQueryResponse;
use super::http_query_handlers::create_endpoint;
use super::query_token::admin_sql;
use super::query_token::basic_headers;
use super::query_token::client_session_header;
use super::query_token::request;
use super::query_token::start;
use super::query_token::start_query;

async fn get(ep: &Route, uri: &str, headers: HeaderMap) -> StatusCode {
    request(ep, Method::GET, uri, headers, json!(null))
        .await
        .status()
}

async fn submit(ep: &Route, headers: HeaderMap) -> StatusCode {
    let body = json!({"sql": "SELECT 1", "pagination": {"wait_time_secs": 5}});
    request(ep, Method::POST, "/v1/query", headers, body)
        .await
        .status()
}

async fn create_admin_user(
    ep: &Route,
    user: &str,
    password: &str,
    extra: &str,
) -> anyhow::Result<()> {
    admin_sql(
        ep,
        &format!("CREATE USER {user} IDENTIFIED BY '{password}' {extra}"),
    )
    .await?;
    admin_sql(ep, &format!("GRANT ROLE account_admin TO {user}")).await
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_survives_password_change() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    create_admin_user(&ep, "binding_user", "before", "").await?;
    let old = basic_headers("binding_user", "before");
    let new = basic_headers("binding_user", "after");
    let result = start(&ep, old.clone()).await?;
    let state_uri = format!("/v1/query/{}", result.id);
    admin_sql(&ep, "ALTER USER binding_user IDENTIFIED BY 'after'").await?;

    // The submission credential keeps working for its own query only.
    assert_eq!(get(&ep, &state_uri, old.clone()).await, StatusCode::OK);
    assert_eq!(
        get(&ep, result.next_uri.as_ref().unwrap(), old.clone()).await,
        StatusCode::OK
    );
    assert_eq!(submit(&ep, old.clone()).await, StatusCode::UNAUTHORIZED);

    // A different credential falls back to ordinary authentication.
    assert_eq!(get(&ep, &state_uri, new.clone()).await, StatusCode::OK);
    let wrong = basic_headers("binding_user", "wrong");
    assert_eq!(get(&ep, &state_uri, wrong).await, StatusCode::UNAUTHORIZED);
    assert_eq!(submit(&ep, new).await, StatusCode::OK);

    // Like query tokens, the binding does not depend on the user still existing.
    admin_sql(&ep, "DROP USER binding_user").await?;
    assert_eq!(get(&ep, &state_uri, old.clone()).await, StatusCode::OK);
    let final_response = request(
        &ep,
        Method::POST,
        result.final_uri.as_ref().unwrap(),
        old,
        json!(null),
    )
    .await;
    assert_eq!(final_response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_is_per_query() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    create_admin_user(&ep, "binding_a", "before", "").await?;
    create_admin_user(&ep, "binding_b", "secret", "").await?;
    let a = basic_headers("binding_a", "before");
    let b = basic_headers("binding_b", "secret");
    let query_a = start(&ep, a.clone()).await?;
    let query_b = start(&ep, b.clone()).await?;
    admin_sql(&ep, "ALTER USER binding_a IDENTIFIED BY 'after'").await?;

    // A's stale credential is bound to A's query, not to B's.
    let uri_b = format!("/v1/query/{}", query_b.id);
    assert_eq!(get(&ep, &uri_b, a.clone()).await, StatusCode::UNAUTHORIZED);
    // B's valid credential still cannot access A's query.
    let uri_a = format!("/v1/query/{}", query_a.id);
    assert_eq!(get(&ep, &uri_a, b).await, StatusCode::UNAUTHORIZED);
    assert_eq!(get(&ep, &uri_a, a).await, StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_includes_requested_tenant() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.query.common.management_mode = true;
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let ep = create_endpoint()?;
    let mut headers = basic_headers("root", "");
    headers.insert(HEADER_TENANT, HeaderValue::from_static("binding-tenant"));
    let result: TestQueryResponse = start_query(&ep, headers.clone(), 60, "SHOW SETTINGS").await?;
    let uri = format!("/v1/query/{}", result.id);
    assert_eq!(get(&ep, &uri, headers.clone()).await, StatusCode::OK);

    headers.insert(HEADER_TENANT, HeaderValue::from_static("another-tenant"));
    assert_eq!(get(&ep, &uri, headers).await, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_avoids_password_lockout() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    admin_sql(
        &ep,
        "CREATE PASSWORD POLICY binding_policy PASSWORD_MAX_RETRIES = 1 PASSWORD_LOCKOUT_TIME_MINS = 10",
    )
    .await?;
    create_admin_user(
        &ep,
        "binding_policy_user",
        "Before12345",
        "WITH SET PASSWORD POLICY = 'binding_policy'",
    )
    .await?;
    let old = basic_headers("binding_policy_user", "Before12345");
    let result = start(&ep, old.clone()).await?;
    admin_sql(
        &ep,
        "ALTER USER binding_policy_user IDENTIFIED BY 'After123456'",
    )
    .await?;

    // Bound polls are not login attempts, so they cannot count as failures.
    let uri = format!("/v1/query/{}", result.id);
    for _ in 0..3 {
        assert_eq!(get(&ep, &uri, old.clone()).await, StatusCode::OK);
    }
    let new = basic_headers("binding_policy_user", "After123456");
    assert_eq!(submit(&ep, new).await, StatusCode::OK);
    Ok(())
}

fn with_session(mut headers: HeaderMap, session_id: Option<&str>) -> HeaderMap {
    headers.insert(
        HEADER_CLIENT_CAPABILITIES,
        HeaderValue::from_static("session_header"),
    );
    if let Some(id) = session_id {
        headers.insert(HEADER_SESSION, client_session_header(id));
    }
    headers
}

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_stays_in_client_session() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    create_admin_user(&ep, "binding_session_user", "before", "").await?;
    let old = basic_headers("binding_session_user", "before");
    let new = basic_headers("binding_session_user", "after");
    let in_session = start(&ep, with_session(old.clone(), Some("binding-s1"))).await?;
    let no_session = start(&ep, old.clone()).await?;
    admin_sql(&ep, "ALTER USER binding_session_user IDENTIFIED BY 'after'").await?;

    let uri = format!("/v1/query/{}", in_session.id);
    let kill_uri = in_session.kill_uri.as_ref().unwrap();
    let s1 = with_session(old.clone(), Some("binding-s1"));
    assert_eq!(get(&ep, &uri, s1).await, StatusCode::OK);
    // A stale credential cannot reach the query from another, new or missing
    // session: it falls back to ordinary authentication and fails.
    for headers in [
        with_session(old.clone(), Some("binding-s2")),
        with_session(old.clone(), None),
        old.clone(),
    ] {
        assert_eq!(
            get(&ep, &uri, headers.clone()).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(get(&ep, kill_uri, headers).await, StatusCode::UNAUTHORIZED);
    }
    let uri = format!("/v1/query/{}", no_session.id);
    let headers = with_session(old.clone(), Some("binding-s1"));
    assert_eq!(get(&ep, &uri, headers).await, StatusCode::UNAUTHORIZED);
    assert_eq!(get(&ep, &uri, old).await, StatusCode::OK);

    // A valid credential keeps the existing cross-session state/kill access.
    let uri = format!("/v1/query/{}", in_session.id);
    let s2 = with_session(new, Some("binding-s2"));
    assert_eq!(get(&ep, &uri, s2.clone()).await, StatusCode::OK);
    assert_eq!(get(&ep, kill_uri, s2).await, StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_with_session_token() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    let login = request(
        &ep,
        Method::POST,
        "/v1/session/login",
        with_session(basic_headers("root", ""), None),
        json!({}),
    )
    .await;
    assert_eq!(login.status(), StatusCode::OK);
    let login: serde_json::Value = login.into_body().into_json().await?;
    let session_id = login["session_id"].as_str().unwrap().to_string();
    let session_token = login["tokens"]["session_token"].as_str().unwrap();
    let refresh_token = login["tokens"]["refresh_token"].as_str().unwrap();
    let in_session = |headers: HeaderMap| with_session(headers, Some(&session_id));

    let result = start(&ep, in_session(bearer(session_token))).await?;
    let uri = format!("/v1/query/{}", result.id);

    // Native session tokens are not bound: polling keeps using ordinary
    // verification, so a refreshed token works and explicit logout revokes.
    let refresh = request(
        &ep,
        Method::POST,
        "/v1/session/refresh",
        in_session(bearer(refresh_token)),
        json!({}),
    )
    .await;
    assert_eq!(refresh.status(), StatusCode::OK);
    let refresh: serde_json::Value = refresh.into_body().into_json().await?;
    let refreshed = refresh["tokens"]["session_token"].as_str().unwrap();
    assert_eq!(
        get(&ep, &uri, in_session(bearer(refreshed))).await,
        StatusCode::OK
    );

    // An explicit query-token request never falls back to the binding, even
    // when Authorization equals the submission credential.
    let mut explicit = in_session(bearer(session_token));
    explicit.insert(HEADER_AUTH_METHOD, HeaderValue::from_static("query-token"));
    assert_eq!(get(&ep, &uri, explicit).await, StatusCode::UNAUTHORIZED);

    let logout = request(
        &ep,
        Method::POST,
        "/v1/session/logout",
        in_session(bearer(session_token)),
        json!({}),
    )
    .await;
    assert_eq!(logout.status(), StatusCode::OK);
    assert_eq!(
        get(&ep, &uri, in_session(bearer(session_token))).await,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_credential_binding_skipped_under_network_policy() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let ep = create_endpoint()?;
    admin_sql(
        &ep,
        "CREATE NETWORK POLICY binding_net ALLOWED_IP_LIST=('127.0.0.0/8')",
    )
    .await?;
    create_admin_user(
        &ep,
        "binding_net_user",
        "before",
        "WITH SET NETWORK POLICY='binding_net'",
    )
    .await?;
    let mut old = basic_headers("binding_net_user", "before");
    old.insert("X-Real-IP", HeaderValue::from_static("127.0.0.1"));
    let result = start(&ep, old.clone()).await?;
    let uri = format!("/v1/query/{}", result.id);
    assert_eq!(get(&ep, &uri, old.clone()).await, StatusCode::OK);
    admin_sql(&ep, "ALTER USER binding_net_user IDENTIFIED BY 'after'").await?;

    // Network policy must be checked on every request, so such queries are
    // not bound and keep the ordinary per-request authentication.
    assert_eq!(get(&ep, &uri, old).await, StatusCode::UNAUTHORIZED);
    Ok(())
}
