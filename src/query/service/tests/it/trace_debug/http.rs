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

use std::env;

use databend_query::servers::admin::v1::instance_status::instance_status_handler;
use databend_query::servers::http::error::QueryError;
use databend_query::servers::http::middleware::json_response;
use databend_query::servers::http::v1::ExecuteStateKind;
use databend_query::servers::http::v1::HttpSessionConf;
use databend_query::servers::http::v1::QueryResponseField;
use databend_query::servers::http::v1::QueryStats;
use databend_query::servers::http::v1::query_route;
use databend_query::sessions::QueryAffect;
use fastrace::collector::SpanRecord;
use fastrace::future::FutureExt;
use fastrace::prelude::*;
use headers::HeaderMapExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use poem::get;
use serde::Deserialize;

use super::infra::TraceDebugRuntimeFlavor;
use super::infra::build_trace_debug_otlp_export;
use super::infra::persist_trace_debug_output;
use super::infra::run_future_in_named_thread;
use super::infra::setup_trace_debug_fixture;
use super::infra::trace_capture_handle;
use super::infra::trace_test_lock;

type EndpointType = Route;

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct TestQueryResponse {
    id: String,
    session_id: Option<String>,
    node_id: String,
    state: ExecuteStateKind,
    session: Option<HttpSessionConf>,
    error: Option<QueryError>,
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    has_result_set: Option<bool>,
    schema: Vec<QueryResponseField>,
    data: Vec<Vec<Option<String>>>,
    affect: Option<QueryAffect>,
    result_timeout_secs: Option<u64>,
    stats: QueryStats,
    stats_uri: Option<String>,
    final_uri: Option<String>,
    next_uri: Option<String>,
    kill_uri: Option<String>,
}

#[derive(Debug, Clone)]
struct TraceDebugConfig {
    sqls: Vec<String>,
    wait_time_secs: u32,
    max_rows_per_page: usize,
    max_pages: usize,
}

impl TraceDebugConfig {
    fn from_env() -> Self {
        Self {
            sqls: parse_trace_debug_sqls(),
            wait_time_secs: parse_trace_debug_env("DATABEND_TRACE_DEBUG_WAIT_SECS", 2),
            max_rows_per_page: parse_trace_debug_env("DATABEND_TRACE_DEBUG_MAX_ROWS_PER_PAGE", 2),
            max_pages: parse_trace_debug_env("DATABEND_TRACE_DEBUG_MAX_PAGES", 16),
        }
    }
}

fn parse_trace_debug_sqls() -> Vec<String> {
    const DEFAULT_SQLS: &[&str] = &["select number from numbers(10)", "show databases"];

    env::var("DATABEND_TRACE_DEBUG_SQLS")
        .ok()
        .map(|sqls| {
            sqls.split(";;")
                .map(str::trim)
                .filter(|sql| !sql.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|sqls| !sqls.is_empty())
        .unwrap_or_else(|| DEFAULT_SQLS.iter().map(|sql| sql.to_string()).collect())
}

fn parse_trace_debug_env<T>(key: &str, default: T) -> T
where T: std::str::FromStr + Copy {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

fn create_endpoint() -> databend_common_exception::Result<EndpointType> {
    Ok(Route::new()
        .nest("/v1", query_route().around(json_response))
        .at("/v1_status", get(instance_status_handler)))
}

fn root_auth_header() -> HeaderValue {
    let mut headers = HeaderMap::new();
    headers.typed_insert(headers::Authorization::basic("root", ""));
    headers["authorization"].clone()
}

async fn send_debug_request(
    ep: &EndpointType,
    body: &[u8],
    method: Method,
    uri: &str,
) -> anyhow::Result<(StatusCode, Option<TestQueryResponse>, String)> {
    let req = Request::builder()
        .uri(uri.parse().unwrap())
        .method(method)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, root_auth_header())
        .body(body.to_vec());

    let resp = ep.call(req).await?;
    let status_code = resp.status();
    let body = resp.into_body().into_string().await?;
    let query_resp = serde_json::from_str::<TestQueryResponse>(&body)
        .map(Some)
        .unwrap_or_default();

    Ok((status_code, query_resp, body))
}

fn record_http_exchange(
    method: &Method,
    uri: &str,
    request_body: &str,
    status: StatusCode,
    response: Option<&TestQueryResponse>,
    body: &str,
) {
    let span = LocalSpan::enter_with_local_parent("databend.trace_debug.http_exchange")
        .with_property(|| ("http.request.method", method.as_str().to_string()))
        .with_property(|| ("url.path", uri.to_string()))
        .with_property(|| ("http.response.status_code", status.as_u16().to_string()))
        .with_property(|| ("http.request.body", request_body.to_string()))
        .with_property(|| ("http.response.body", body.to_string()));

    match response {
        Some(response) => {
            let error_text = response
                .error
                .as_ref()
                .map(|error| format!("{error:?}"))
                .unwrap_or_default();
            let span = span
                .with_property(|| ("query_id", response.id.clone()))
                .with_property(|| ("databend.http.state", format!("{:?}", response.state)))
                .with_property(|| ("databend.http.rows", response.data.len().to_string()))
                .with_property(|| ("databend.http.warnings", response.warnings.join("\n")))
                .with_property(|| {
                    (
                        "databend.http.next_uri",
                        response.next_uri.clone().unwrap_or_default(),
                    )
                })
                .with_property(|| {
                    (
                        "databend.http.final_uri",
                        response.final_uri.clone().unwrap_or_default(),
                    )
                })
                .with_property(|| ("databend.http.error", error_text.clone()));

            if status.is_success() && response.error.is_none() {
                let _span = span.with_property(|| ("span.status_code", "ok"));
            } else {
                let _span = span
                    .with_property(|| ("span.status_code", "error"))
                    .with_property(|| ("span.status_description", error_text));
            }
        }
        None => {
            let _span = span
                .with_property(|| ("span.status_code", "error"))
                .with_property(|| {
                    (
                        "span.status_description",
                        "http response body did not match TestQueryResponse",
                    )
                });
        }
    }
}

async fn dump_http_query_trace(
    sql: &str,
    config: &TraceDebugConfig,
) -> anyhow::Result<Vec<SpanRecord>> {
    let ep = create_endpoint()?;
    let capture = trace_capture_handle();
    capture.reset();

    let sql_text = sql.to_string();
    let root = Span::root("db.query", SpanContext::random())
        .with_property(|| ("db.system", "databend"))
        .with_property(|| ("db.statement", sql_text.clone()))
        .with_property(|| ("databend.trace.entry", "http"))
        .with_property(|| {
            (
                "databend.http.wait_time_secs",
                config.wait_time_secs.to_string(),
            )
        })
        .with_property(|| {
            (
                "databend.http.max_rows_per_page",
                config.max_rows_per_page.to_string(),
            )
        })
        .with_property(|| ("databend.http.max_pages", config.max_pages.to_string()));

    let json = serde_json::json!({
        "sql": sql.to_string(),
        "pagination": {
            "wait_time_secs": config.wait_time_secs,
            "max_rows_per_page": config.max_rows_per_page,
        },
        "session": { "settings": {} }
    });
    let request_body_bytes = serde_json::to_vec(&json)?;
    let request_body_text = String::from_utf8(request_body_bytes.clone())?;

    async {
        let (status, begin_resp, body) =
            send_debug_request(&ep, &request_body_bytes, Method::POST, "/v1/query").await?;
        record_http_exchange(
            &Method::POST,
            "/v1/query",
            &request_body_text,
            status,
            begin_resp.as_ref(),
            &body,
        );

        let mut next_uri = begin_resp.as_ref().and_then(|resp| resp.next_uri.clone());
        let mut fetched_pages = 0usize;
        while let Some(current_next_uri) = next_uri.clone() {
            if fetched_pages >= config.max_pages {
                let _span =
                    LocalSpan::enter_with_local_parent("databend.trace_debug.http_pagination_stop")
                        .with_property(|| ("databend.http.reason", "max_pages_reached"))
                        .with_property(|| ("databend.http.max_pages", config.max_pages.to_string()))
                        .with_property(|| ("databend.http.next_uri", current_next_uri.clone()));
                break;
            }

            let (status, next_resp, body) =
                send_debug_request(&ep, &request_body_bytes, Method::GET, &current_next_uri)
                    .await?;
            record_http_exchange(
                &Method::GET,
                &current_next_uri,
                &request_body_text,
                status,
                next_resp.as_ref(),
                &body,
            );
            next_uri = next_resp.as_ref().and_then(|resp| resp.next_uri.clone());
            fetched_pages += 1;
        }

        Ok::<(), anyhow::Error>(())
    }
    .in_span(root)
    .await?;

    fastrace::flush();
    Ok(capture.snapshot())
}

#[test]
#[ignore = "debug utility: dumps HTTP/query tracing without assertions"]
fn test_dump_http_query_trace_debug() -> anyhow::Result<()> {
    let _guard = trace_test_lock().lock().unwrap();
    run_future_in_named_thread(
        "trace-debug-http",
        TraceDebugRuntimeFlavor::CurrentThread,
        || async move {
            let _fixture = setup_trace_debug_fixture().await?;
            let config = TraceDebugConfig::from_env();
            let mut spans = Vec::new();

            for sql in &config.sqls {
                spans.extend(dump_http_query_trace(sql, &config).await?);
            }

            let export = build_trace_debug_otlp_export(&spans);
            let _path = persist_trace_debug_output("http", &export)?;

            Ok(())
        },
    )
}
