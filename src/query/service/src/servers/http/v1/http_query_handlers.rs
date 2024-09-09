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

use databend_common_base::base::mask_connection_info;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_QUERY_PAGE_ROWS;
use databend_common_base::headers::HEADER_QUERY_STATE;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataSchemaRef;
use databend_common_metrics::http::metrics_incr_http_response_errors_count;
use fastrace::full_name;
use fastrace::prelude::*;
use highway::HighwayHash;
use http::StatusCode;
use log::error;
use log::info;
use log::warn;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::post;
use poem::web::Json;
use poem::web::Path;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;

use super::query::ExecuteStateKind;
use super::query::HttpQueryRequest;
use super::query::HttpQueryResponseInternal;
use super::query::RemoveReason;
use crate::servers::http::error::QueryError;
use crate::servers::http::middleware::EndpointKind;
use crate::servers::http::middleware::HTTPSessionMiddleware;
use crate::servers::http::middleware::MetricsMiddleware;
use crate::servers::http::v1::query::Progresses;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::HttpSessionConf;
use crate::servers::http::v1::StringBlock;
use crate::servers::HttpHandlerKind;
use crate::sessions::QueryAffect;

pub fn make_page_uri(query_id: &str, page_no: usize) -> String {
    format!("/v1/query/{}/page/{}", query_id, page_no)
}

pub fn make_state_uri(query_id: &str) -> String {
    format!("/v1/query/{}", query_id)
}

pub fn make_final_uri(query_id: &str) -> String {
    format!("/v1/query/{}/final", query_id)
}

pub fn make_kill_uri(query_id: &str) -> String {
    format!("/v1/query/{}/kill", query_id)
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct QueryStats {
    #[serde(flatten)]
    pub progresses: Progresses,
    pub running_time_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryResponseField {
    name: String,
    r#type: String,
}

impl QueryResponseField {
    pub fn from_schema(schema: DataSchemaRef) -> Vec<Self> {
        schema
            .fields()
            .iter()
            .map(|f| Self {
                name: f.name().to_string(),
                r#type: f.data_type().wrapped_display(),
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryResponse {
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

    pub stats: QueryStats,

    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

impl QueryResponse {
    pub(crate) fn from_internal(
        id: String,
        r: HttpQueryResponseInternal,
        is_final: bool,
    ) -> impl IntoResponse {
        let state = r.state.clone();
        let (data, next_uri) = if is_final {
            (StringBlock::empty(), None)
        } else {
            match state.state {
                ExecuteStateKind::Running | ExecuteStateKind::Starting => match r.data {
                    None => (StringBlock::empty(), Some(make_state_uri(&id))),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_state_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
                ExecuteStateKind::Failed => (StringBlock::empty(), Some(make_final_uri(&id))),
                ExecuteStateKind::Succeeded => match r.data {
                    None => (StringBlock::empty(), Some(make_final_uri(&id))),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_final_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
            }
        };

        if let Some(err) = &r.state.error {
            metrics_incr_http_response_errors_count(err.name(), err.code());
        }

        let session_id = r.session_id.clone();
        let stats = QueryStats {
            progresses: state.progresses.clone(),
            running_time_ms: state.running_time_ms,
        };
        let rows = data.data.len();

        Json(QueryResponse {
            data: data.into(),
            state: state.state,
            schema: state.schema.clone(),
            session_id: Some(session_id),
            node_id: r.node_id,
            session: r.session,
            stats,
            affect: state.affect,
            warnings: r.state.warnings,
            id: id.clone(),
            next_uri,
            stats_uri: Some(make_state_uri(&id)),
            final_uri: Some(make_final_uri(&id)),
            kill_uri: Some(make_kill_uri(&id)),
            error: r.state.error.map(QueryError::from_error_code),
            has_result_set: r.state.has_result_set,
        })
        .with_header(HEADER_QUERY_ID, id.clone())
        .with_header(HEADER_QUERY_STATE, state.state.to_string())
        .with_header(HEADER_QUERY_PAGE_ROWS, rows)
    }
}

/// final is not ACKed by client, so client should not depend on the final response,
///
/// for server:
/// 1. when response with `next_uri` =  `.../final`, all states should already be delivered to the client the latest in this response.
/// 2. `/final` SHOULD response with nothing but `next_uri` = `null`, BUT to tolerant old clients, we still keep some fields.
///
/// for clients:
/// 1. check `next_uri` before refer to other fields of the response.
///
/// the client in sql logic tests should follow this.
#[poem::handler]
async fn query_final_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    ctx.check_node_id(&query_id)?;
    let root = get_http_tracing_span(full_name!(), ctx, &query_id);
    let _t = SlowRequestLogTracker::new(ctx);
    async {
        info!(
            "{}: got {} request, this query is going to be finally completed.",
            query_id,
            make_final_uri(&query_id)
        );
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager
            .remove_query(
                &query_id,
                &ctx.client_session_id,
                RemoveReason::Finished,
                ErrorCode::ClosedQuery("closed by client"),
            )
            .await?
        {
            Some(query) => {
                let mut response = query.get_response_state_only().await;
                // it is safe to set these 2 fields to None, because client now check for null/None first.
                response.session = None;
                response.state.affect = None;
                Ok(QueryResponse::from_internal(query_id, response, true))
            }
            None => Err(query_id_not_found(&query_id, &ctx.node_id)),
        }
    }
    .in_span(root)
    .await
}

// currently implementation only support kill http query
#[poem::handler]
async fn query_cancel_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    ctx.check_node_id(&query_id)?;
    let root = get_http_tracing_span(full_name!(), ctx, &query_id);
    let _t = SlowRequestLogTracker::new(ctx);
    async {
        info!(
            "{}: got {} request, cancel the query",
            query_id,
            make_kill_uri(&query_id)
        );
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager
            .remove_query(
                &query_id,
                &ctx.client_session_id,
                RemoveReason::Canceled,
                ErrorCode::AbortedQuery("canceled by client"),
            )
            .await?
        {
            Some(_) => Ok(StatusCode::OK),
            None => Err(query_id_not_found(&query_id, &ctx.node_id)),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
async fn query_state_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    ctx.check_node_id(&query_id)?;
    let root = get_http_tracing_span(full_name!(), ctx, &query_id);

    async {
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.get_query(&query_id) {
            Some(query) => {
                query.check_client_session_id(&ctx.client_session_id)?;
                if let Some(reason) = query.check_removed() {
                    Err(query_id_removed(&query_id, reason))
                } else {
                    let response = query.get_response_state_only().await;
                    Ok(QueryResponse::from_internal(query_id, response, false))
                }
            }
            None => Err(query_id_not_found(&query_id, &ctx.node_id)),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
async fn query_page_handler(
    ctx: &HttpQueryContext,
    Path((query_id, page_no)): Path<(String, usize)>,
) -> PoemResult<impl IntoResponse> {
    ctx.check_node_id(&query_id)?;
    let root = get_http_tracing_span(full_name!(), ctx, &query_id);
    let _t = SlowRequestLogTracker::new(ctx);

    async {
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.get_query(&query_id) {
            Some(query) => {
                query.check_client_session_id(&ctx.client_session_id)?;
                if let Some(reason) = query.check_removed() {
                    Err(query_id_removed(&query_id, reason))
                } else {
                    query.update_expire_time(true).await;
                    let resp = query.get_response_page(page_no).await.map_err(|err| {
                        poem::Error::from_string(err.message(), StatusCode::NOT_FOUND)
                    })?;
                    query.update_expire_time(false).await;
                    Ok(QueryResponse::from_internal(query_id, resp, false))
                }
            }
            None => Err(query_id_not_found(&query_id, &ctx.node_id)),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
#[async_backtrace::framed]
pub(crate) async fn query_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<HttpQueryRequest>,
) -> PoemResult<impl IntoResponse> {
    let root = get_http_tracing_span(full_name!(), ctx, &ctx.query_id);
    let _t = SlowRequestLogTracker::new(ctx);

    async {
        let agent_info = ctx.user_agent.as_ref().map(|s|(format!("(from {s})"))).unwrap_or("".to_string());
        let client_session_id_info = ctx.client_session_id.as_ref().map(|s|(format!("(client_session_id={s})"))).unwrap_or("".to_string());
        info!("http query new request{}{}: {}", agent_info, client_session_id_info, mask_connection_info(&format!("{:?}", req)));
        let http_query_manager = HttpQueryManager::instance();
        let sql = req.sql.clone();

        let query = http_query_manager
            .try_create_query(ctx, req.clone())
            .await
            .map_err(|err| err.display_with_sql(&sql));
        match query {
            Ok(query) => {
                query.update_expire_time(true).await;
                // tmp workaround to tolerant old clients
                let resp = query
                    .get_response_page(0)
                    .await
                    .map_err(|err| err.display_with_sql(&sql))
                    .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
                if matches!(resp.state.state, ExecuteStateKind::Failed) {
                    ctx.set_fail();
                }
                let (rows, next_page) = match &resp.data {
                    None => (0, None),
                    Some(p) => (p.page.data.num_rows(), p.next_page_no),
                };
                info!( "http query initial response to http query_id={}, state={:?}, rows={}, next_page={:?}, sql='{}'",
                        &query.id, &resp.state, rows, next_page, mask_connection_info(&sql)
                    );
                query.update_expire_time(false).await;
                Ok(QueryResponse::from_internal(query.id.to_string(), resp, false).into_response())
            }
            Err(e) => {
                error!("http query fail to start sql, error: {:?}", e);
                ctx.set_fail();
                Ok(req.fail_to_start_sql(e).into_response())
            }
        }
    }
        .in_span(root)
        .await
}

pub fn query_route(http_handler_kind: HttpHandlerKind) -> Route {
    // Note: endpoints except /v1/query may change without notice, use uris in response instead
    let rules = [
        ("/", post(query_handler)),
        ("/:id", get(query_state_handler)),
        ("/:id/page/:page_no", get(query_page_handler)),
        (
            "/:id/kill",
            get(query_cancel_handler).post(query_cancel_handler),
        ),
        (
            "/:id/final",
            get(query_final_handler).post(query_final_handler),
        ),
    ];

    let mut route = Route::new();
    for (path, endpoint) in rules.into_iter() {
        let kind = if path == "/" {
            EndpointKind::StartQuery
        } else {
            EndpointKind::PollQuery
        };
        route = route.at(
            path,
            endpoint
                .with(MetricsMiddleware::new(path))
                .with(HTTPSessionMiddleware::create(http_handler_kind, kind)),
        );
    }
    route
}

fn query_id_removed(query_id: &str, remove_reason: RemoveReason) -> PoemError {
    PoemError::from_string(
        format!("query id {query_id} {}", remove_reason),
        StatusCode::BAD_REQUEST,
    )
}

fn query_id_not_found(query_id: &str, node_id: &str) -> PoemError {
    PoemError::from_string(
        format!("query id {query_id} not found on {node_id}"),
        StatusCode::NOT_FOUND,
    )
}

fn query_id_to_trace_id(query_id: &str) -> TraceId {
    let [hash_high, hash_low] = highway::PortableHash::default().hash128(query_id.as_bytes());
    TraceId(((hash_high as u128) << 64) + (hash_low as u128))
}

/// The HTTP query endpoints are expected to be responses within 60 seconds.
/// If it exceeds far from 60 seconds, there might be something wrong, we should
/// log it.
struct SlowRequestLogTracker {
    started_at: std::time::Instant,
    method: String,
    uri: String,
}

impl SlowRequestLogTracker {
    fn new(ctx: &HttpQueryContext) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            method: ctx.http_method.clone(),
            uri: ctx.uri.clone(),
        }
    }
}

impl Drop for SlowRequestLogTracker {
    fn drop(&mut self) {
        drop_guard(move || {
            let elapsed = self.started_at.elapsed();
            if elapsed.as_secs_f64() > 60.0 {
                warn!(
                    "slow http query request on {} {}, elapsed: {:.2}s",
                    self.method,
                    self.uri,
                    elapsed.as_secs_f64()
                );
            }
        })
    }
}

// get_http_tracing_span always return a valid span for tracing
// it will try to decode w3 traceparent and if empty or failed, it will create a new root span and throw a warning
fn get_http_tracing_span(name: &'static str, ctx: &HttpQueryContext, query_id: &str) -> Span {
    if let Some(parent) = ctx.trace_parent.as_ref() {
        let trace = parent.as_str();
        match SpanContext::decode_w3c_traceparent(trace) {
            Some(span_context) => {
                return Span::root(name, span_context)
                    .with_properties(|| ctx.to_fastrace_properties());
            }
            None => {
                warn!("failed to decode trace parent: {}", trace);
            }
        }
    }

    let trace_id = query_id_to_trace_id(query_id);
    Span::root(name, SpanContext::new(trace_id, SpanId(rand::random())))
        .with_properties(|| ctx.to_fastrace_properties())
}
