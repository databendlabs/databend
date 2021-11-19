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

use common_base::tokio;
use common_exception::Result;
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
use serde_json;

use crate::servers::http::v1::http_query_handlers::make_final_uri;
use crate::servers::http::v1::http_query_handlers::make_page_uri;
use crate::servers::http::v1::http_query_handlers::make_state_uri;
use crate::servers::http::v1::http_query_handlers::query_route;
use crate::servers::http::v1::http_query_handlers::QueryResponse;
use crate::servers::http::v1::query::execute_state::ExecuteStateName;
use crate::sessions::SessionManagerRef;
use crate::tests::SessionManagerBuilder;

// TODO(youngsofun): add test for
// 1. kill without delete
// 2. query fail after started
// 3. get old page other than the last

pub fn get_page_uri(query_id: &str, page_no: usize, wait_time: u32) -> String {
    format!(
        "{}?wait_time={}",
        make_page_uri(query_id, page_no),
        wait_time
    )
}

type RouteWithData = AddDataEndpoint<Route, SessionManagerRef>;

#[tokio::test]
async fn test_simple_sql() -> Result<()> {
    let sql = "select * from system.tables limit 10";
    let (status, result) = post_sql(sql, 1).await?;
    assert_eq!(status, StatusCode::OK, "{:?}", result);
    assert_eq!(result.data.len(), 10);
    assert_eq!(result.state, ExecuteStateName::Succeeded);
    assert!(result.next_uri.is_none(), "{:?}", result);
    assert!(result.error.is_none());
    assert!(result.stats.progress.is_some());
    assert!(result.columns.is_some());
    Ok(())
}

#[tokio::test]
async fn test_bad_sql() -> Result<()> {
    let (status, result) = post_sql("bad sql", 1).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.data.len(), 0);
    assert!(result.next_uri.is_none());
    assert_eq!(result.state, ExecuteStateName::Failed);
    assert!(result.error.is_some());
    assert!(result.stats.progress.is_none());
    assert!(result.columns.is_none());
    Ok(())
}

#[tokio::test]
async fn test_async() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let route = Route::new().nest("/v1/query", query_route()).data(sessions);
    let sql = "select sum(number+1) from numbers(1000000) where number>0 group by number%3";
    let json = serde_json::json!({"sql": sql.to_string()});

    let (status, result) = post_json_to_router(&route, &json, 0).await?;
    assert_eq!(status, StatusCode::OK);
    let query_id = result.id;
    let next_uri = make_page_uri(&query_id, 0);
    assert_eq!(result.data.len(), 0);
    assert_eq!(result.next_uri, Some(next_uri));
    assert!(result.stats.progress.is_some());
    assert!(result.columns.is_some());
    assert!(result.error.is_none());
    assert_eq!(result.state, ExecuteStateName::Running,);

    // get page, support retry
    for _ in 1..2 {
        let uri = get_page_uri(&query_id, 0, 3);

        let (status, result) = get_uri_checked(&route, &uri).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.len(), 3);
        assert!(result.next_uri.is_none());
        assert!(result.columns.is_none());
        assert!(result.error.is_none());
        assert!(result.stats.progress.is_some());
        assert_eq!(result.state, ExecuteStateName::Succeeded);
    }

    // get state
    let uri = make_state_uri(&query_id);
    let (status, result) = get_uri_checked(&route, &uri).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.data.len(), 0);
    assert!(result.next_uri.is_none());
    assert!(result.columns.is_none());
    assert!(result.error.is_none());
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
