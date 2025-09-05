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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::mask_connection_info;
use databend_common_base::base::GlobalInstance;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_QUERY_PAGE_ROWS;
use databend_common_base::headers::HEADER_QUERY_STATE;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataSchemaRef;
use databend_common_management::WorkloadGroupResourceManager;
use databend_common_metrics::http::metrics_incr_http_response_errors_count;
use fastrace::func_path;
use fastrace::prelude::*;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use log::error;
use log::info;
use log::warn;
use poem::error::Error as PoemError;
use poem::error::ResponseError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::middleware::CookieJarManager;
use poem::post;
use poem::put;
use poem::web::Json;
use poem::web::Path;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Request;
use poem::Response;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use super::query::ExecuteStateKind;
use super::query::HttpQuery;
use super::query::HttpQueryRequest;
use super::query::HttpQueryResponseInternal;
use super::query::RemoveReason;
use crate::clusters::ClusterDiscovery;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::error::QueryError;
use crate::servers::http::middleware::forward_request_with_body;
use crate::servers::http::middleware::EndpointKind;
use crate::servers::http::middleware::HTTPSessionMiddleware;
use crate::servers::http::middleware::MetricsMiddleware;
use crate::servers::http::v1::catalog::catalog_stats_handler;
use crate::servers::http::v1::catalog::get_database_table_handler;
use crate::servers::http::v1::catalog::list_database_table_fields_handler;
use crate::servers::http::v1::catalog::list_database_tables_handler;
use crate::servers::http::v1::catalog::list_databases_handler;
use crate::servers::http::v1::catalog::search_databases_handler;
use crate::servers::http::v1::catalog::search_tables_handler;
use crate::servers::http::v1::discovery_nodes;
use crate::servers::http::v1::login_handler;
use crate::servers::http::v1::logout_handler;
use crate::servers::http::v1::query::blocks_serializer::BlocksSerializer;
use crate::servers::http::v1::query::Progresses;
use crate::servers::http::v1::refresh_handler;
use crate::servers::http::v1::roles::list_roles_handler;
use crate::servers::http::v1::streaming_load_handler;
use crate::servers::http::v1::upload_to_stage;
use crate::servers::http::v1::users::create_user_handler;
use crate::servers::http::v1::users::list_users_handler;
use crate::servers::http::v1::verify_handler;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::HttpSessionConf;
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

#[derive(Serialize, Debug, Clone)]
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
    pub data: Arc<BlocksSerializer>,
    pub affect: Option<QueryAffect>,
    pub result_timeout_secs: Option<u64>,

    pub stats: QueryStats,

    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

impl QueryResponse {
    pub(crate) fn removed(query_id: &str, remove_reason: RemoveReason) -> impl IntoResponse {
        let id = query_id.to_string();
        let state = match remove_reason {
            RemoveReason::Finished => ExecuteStateKind::Succeeded,
            _ => ExecuteStateKind::Failed,
        };
        Json(QueryResponse {
            id: query_id.to_string(),
            session_id: None,
            node_id: GlobalConfig::instance().query.node_id.clone(),
            state,
            session: None,
            error: None,
            warnings: vec![],
            has_result_set: None,
            schema: vec![],
            data: Arc::new(BlocksSerializer::empty()),
            affect: None,
            result_timeout_secs: None,
            stats: Default::default(),
            stats_uri: None,
            final_uri: None,
            next_uri: None,
            kill_uri: None,
        })
        .with_header(HEADER_QUERY_ID, id.clone())
        .with_header(HEADER_QUERY_STATE, state.to_string())
    }
    pub(crate) fn from_internal(
        id: String,
        r: HttpQueryResponseInternal,
        is_final: bool,
    ) -> (impl IntoResponse, bool) {
        let state = r.state.clone();
        let (data, next_uri) = if is_final {
            (Arc::new(BlocksSerializer::empty()), None)
        } else {
            match state.state {
                ExecuteStateKind::Running | ExecuteStateKind::Starting => match r.data {
                    None => (
                        Arc::new(BlocksSerializer::empty()),
                        Some(make_state_uri(&id)),
                    ),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_state_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
                ExecuteStateKind::Failed => (
                    Arc::new(BlocksSerializer::empty()),
                    Some(make_final_uri(&id)),
                ),
                ExecuteStateKind::Succeeded => match r.data {
                    None => (
                        Arc::new(BlocksSerializer::empty()),
                        Some(make_final_uri(&id)),
                    ),
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
        let rows = data.num_rows();

        let next_is_final = next_uri
            .as_ref()
            .map(|u| u.ends_with("final"))
            .unwrap_or(false);

        let resp = Json(QueryResponse {
            data,
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
            result_timeout_secs: Some(r.result_timeout_secs),
        })
        .with_header(HEADER_QUERY_ID, id.clone())
        .with_header(HEADER_QUERY_STATE, state.state.to_string())
        .with_header(HEADER_QUERY_PAGE_ROWS, rows);
        (resp, next_is_final)
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
    let root = get_http_tracing_span(func_path!(), ctx, &query_id);
    let _t = SlowRequestLogTracker::new(ctx);
    async {
        info!(
            "[HTTP-QUERY] Query {} received final request at {}, completing query execution",
            query_id,
            make_final_uri(&query_id)
        );
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager
            .remove_query(
                &query_id,
                &ctx.client_session_id,
                RemoveReason::Finished,
                ErrorCode::ClosedQuery("Query closed by client"),
            )
            .await?
        {
            Some(query) => {
                let mut response = query
                    .get_response_state_only()
                    .map_err(HttpErrorCode::server_error)?;
                // it is safe to set these 2 fields to None, because client now check for null/None first.
                response.session = None;
                response.state.affect = None;
                Ok(QueryResponse::from_internal(query_id, response, true).0)
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
    let root = get_http_tracing_span(func_path!(), ctx, &query_id);
    let _t = SlowRequestLogTracker::new(ctx);
    async {
        info!(
            "[HTTP-QUERY] Query {} received cancel request at {}, terminating execution",
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
) -> PoemResult<Response> {
    ctx.check_node_id(&query_id)?;
    let root = get_http_tracing_span(func_path!(), ctx, &query_id);

    async {
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.get_query(&query_id) {
            Some(query) => {
                if let Some(reason) = query.check_removed() {
                    Ok(QueryResponse::removed(&query_id, reason).into_response())
                } else {
                    let response = query
                        .get_response_state_only()
                        .map_err(HttpErrorCode::server_error)?;
                    Ok(QueryResponse::from_internal(query_id, response, false)
                        .0
                        .into_response())
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

    let http_query_manager = HttpQueryManager::instance();

    let Some(query) = http_query_manager.get_query(&query_id) else {
        return Err(query_id_not_found(&query_id, &ctx.node_id));
    };

    ctx.try_refresh_worksheet_session().await.ok();

    let query_mem_stat = query.query_mem_stat.clone();

    let query_page_handle = {
        let query_id = query_id.clone();
        async move {
            if query.user_name != ctx.user_name {
                return Err(poem::error::Error::from_string(
                    format!(
                        "[HTTP-QUERY] Authentication error: query {} expected user {}, but got {}",
                        query_id, query.user_name, ctx.user_name
                    ),
                    StatusCode::UNAUTHORIZED,
                ));
            }

            query.check_client_session_id(&ctx.client_session_id)?;
            if let Some(reason) = query.check_removed() {
                log::info!(
                    "[HTTP-QUERY] /query/{}/page/{} - query is removed (reason: {})",
                    query_id,
                    page_no,
                    reason
                );
                Err(query_id_removed(&query_id, reason))
            } else {
                query.update_expire_time(true).await;
                let resp = query.get_response_page(page_no).await.map_err(|err| {
                    log::info!(
                        "[HTTP-QUERY] /query/{}/page/{} - get response page error (reason: {})",
                        query_id,
                        page_no,
                        err.message()
                    );
                    poem::Error::from_string(
                        format!("[HTTP-QUERY] {}", err.message()),
                        StatusCode::NOT_FOUND,
                    )
                })?;
                query.update_expire_time(false).await;
                let (resp, next_is_final) = QueryResponse::from_internal(query_id, resp, false);
                if next_is_final {
                    query.wait_for_final()
                }
                Ok(resp)
            }
        }
    };

    let query_page_handle = {
        let root = get_http_tracing_span(func_path!(), ctx, &query_id);
        let _t = SlowRequestLogTracker::new(ctx);
        query_page_handle.in_span(root)
    };

    let query_page_handle = {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.mem_stat = query_mem_stat;
        tracking_payload.query_id = Some(query_id.clone());
        let _tracking_guard = ThreadTracker::tracking(tracking_payload);
        ThreadTracker::tracking_future(query_page_handle)
    };

    query_page_handle.await
}

#[poem::handler]
#[async_backtrace::framed]
#[fastrace::trace]
pub(crate) async fn query_handler(
    ctx: &HttpQueryContext,
    Json(mut req): Json<HttpQueryRequest>,
) -> PoemResult<impl IntoResponse> {
    let session = ctx.session.clone();

    let query_handle = async {
        let agent_info = ctx
            .user_agent
            .as_ref()
            .map(|s| format!("(from {s})"))
            .unwrap_or("".to_string());

        let client_session_id_info = ctx
            .client_session_id
            .as_ref()
            .map(|s| format!("(client_session_id={s})"))
            .unwrap_or("".to_string());
        info!(
            "[HTTP-QUERY] New query request{}{}: {}",
            agent_info,
            client_session_id_info,
            mask_connection_info(&format!("{:?}", req))
        );
        let sql = req.sql.clone();

        match HttpQuery::try_create(ctx, req.clone()).await {
            Err(err) => {
                let err = err.display_with_sql(&sql);
                error!("[HTTP-QUERY] Failed to start SQL query, error: {:?}", err);
                ctx.set_fail();
                Ok(req.fail_to_start_sql(err).into_response())
            }
            Ok(mut query) => {
                if let Err(err) = query.start_query(sql.clone()).await {
                    let err = err.display_with_sql(&sql);
                    error!("[HTTP-QUERY] Failed to start SQL query, error: {:?}", err);
                    ctx.set_fail();
                    return Ok(req.fail_to_start_sql(err).into_response());
                }

                let http_query_manager = HttpQueryManager::instance();
                let query = http_query_manager.add_query(query).await;

                query.update_expire_time(true).await;
                // tmp workaround to tolerant old clients
                let resp = query
                    .get_response_page(0)
                    .await
                    .map_err(|err| err.display_with_sql(&sql))
                    .map_err(|err| {
                        poem::Error::from_string(
                            format!("[HTTP-QUERY] {}", err.message()),
                            StatusCode::NOT_FOUND,
                        )
                    })?;

                if matches!(resp.state.state, ExecuteStateKind::Failed) {
                    ctx.set_fail();
                }

                let (rows, next_page) = match &resp.data {
                    None => (0, None),
                    Some(p) => (p.page.data.num_rows(), p.next_page_no),
                };
                info!("[HTTP-QUERY] Initial response for query_id={}, state={:?}, rows={}, next_page={:?}, sql='{}'",
                        &query.id, &resp.state, rows, next_page, mask_connection_info(&sql)
                    );
                query.update_expire_time(false).await;
                let (resp, next_is_final) =
                    QueryResponse::from_internal(query.id.to_string(), resp, false);
                if next_is_final {
                    query.wait_for_final()
                }
                Ok(resp.into_response())
            }
        }
    };

    let query_handle = {
        let root = get_http_tracing_span(func_path!(), ctx, &ctx.query_id);
        let _t = SlowRequestLogTracker::new(ctx);
        query_handle.in_span(root)
    };

    let query_handle = {
        let mut tracking_workload_group = None;
        let mut parent_mem_stat = ParentMemStat::StaticRef(&GLOBAL_MEM_STAT);

        if let Some(workload_id) = session.get_current_workload_group() {
            let mgr = GlobalInstance::get::<Arc<WorkloadGroupResourceManager>>();

            let workload_group = match mgr.get_workload(&workload_id).await {
                Ok(workload_group) => workload_group,
                Err(error) => {
                    return Ok(HttpErrorCode::error_code(error).as_response());
                }
            };

            log::info!(
                "[Workload-Group] attach workload group {}({}) for query {}, quotas: {:?}",
                workload_group.meta.name,
                workload_group.meta.id,
                ctx.query_id,
                workload_group.meta.quotas
            );

            parent_mem_stat = ParentMemStat::Normal(workload_group.mem_stat.clone());
            tracking_workload_group = Some(workload_group);
        } else if let Ok(user) = session.get_current_user() {
            log::info!(
                "[Workload-Group] The user {} does not have a workload group.",
                user.name
            );
        } else {
            log::info!(
                "[Workload-Group] The query {:?} does not have a workload group.",
                session.get_current_query_id()
            );
        }

        let name = Some(ctx.query_id.clone());
        let query_mem_stat = MemStat::create_child(name, 0, parent_mem_stat);
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(ctx.query_id.clone());
        tracking_payload.mem_stat = Some(query_mem_stat.clone());
        tracking_payload.workload_group_resource = tracking_workload_group;
        let _tracking_guard = ThreadTracker::tracking(tracking_payload);
        ThreadTracker::tracking_future(query_handle)
    };

    query_handle.await
}

#[derive(Deserialize, Serialize, Debug)]
struct HeartBeatRequest {
    node_to_queries: HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Serialize)]
struct HeartBeatResponse {
    queries_to_remove: Vec<String>,
}

/// /v1/session/heartbeat are used for 2 purpose:
/// 1. heartbeat to avoid session token/temp table expire
/// 2. heartbeat to avoid result timeout of queries in this session
#[poem::handler]
#[async_backtrace::framed]
pub async fn heartbeat_handler(
    ctx: &HttpQueryContext,
    req: &Request,
    Json(body): Json<HeartBeatRequest>,
) -> poem::error::Result<impl IntoResponse> {
    let local_id = GlobalConfig::instance().query.node_id.clone();
    let mut queries_to_remove = vec![];
    let mut nodes_to_forwards = vec![];
    for (node_id, queries) in body.node_to_queries {
        if node_id == local_id {
            queries_to_remove.extend(HttpQueryManager::instance().on_heartbeat(queries));
        } else if let Some(node) = ClusterDiscovery::instance()
            .find_node_by_id(&node_id)
            .await
            .map_err(HttpErrorCode::server_error)?
        {
            let mut node_to_queries = HashMap::new();
            node_to_queries.insert(node_id.to_string(), queries);
            let body = HeartBeatRequest { node_to_queries };
            let body = serde_json::to_vec(&body).unwrap();
            nodes_to_forwards.push((node, body));
        } else {
            queries_to_remove.extend(queries)
        }
    }

    let num_task = nodes_to_forwards.len();
    if num_task > 0 {
        let mut tasks = Vec::with_capacity(num_task);
        let uri = req.uri().to_string();
        let method = req.method();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let agent = format!("databend-query/{}", ctx.version.semantic);
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_str(&agent).unwrap(),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            req.headers()
                .get(http::header::AUTHORIZATION)
                .expect("heartbeat request should contain auth header")
                .to_owned(),
        );
        for (node, body) in nodes_to_forwards {
            let uri = uri.clone();
            let method = method.clone();
            let headers = headers.clone();

            tasks.push(async move {
                match forward_request_with_body(node, &uri, body, method, headers).await {
                    Ok(mut resp) => {
                        if resp.status() == StatusCode::OK {
                            Some(
                                resp.take_body()
                                    .into_json::<HeartBeatResponse>()
                                    .await
                                    .unwrap(),
                            )
                        } else {
                            warn!("[HTTP-QUERY] Heartbeat forward failed: {:?}", resp);
                            None
                        }
                    }
                    Err(e) => {
                        warn!("[HTTP-QUERY] Heartbeat forward error: {:?}", e);
                        None
                    }
                }
            });
        }
        let settings = ctx.session.get_settings();
        let num_threads = num_task.max(
            settings
                .get_max_threads()
                .map_err(HttpErrorCode::server_error)? as usize,
        );
        let responses = execute_futures_in_parallel(
            tasks,
            num_threads,
            num_threads * 2,
            "forward_heartbeat".to_owned(),
        )
        .await
        .map_err(HttpErrorCode::server_error)?;
        for response in responses.into_iter().flatten() {
            queries_to_remove.extend(response.queries_to_remove);
        }
    }

    Ok(Json(HeartBeatResponse { queries_to_remove }).into_response())
}

pub fn query_route() -> Route {
    // Note: endpoints except /v1/query may change without notice, use uris in response instead
    let rules = [
        ("/query", post(query_handler), EndpointKind::StartQuery),
        (
            "/query/:id",
            get(query_state_handler),
            EndpointKind::PollQuery,
        ),
        (
            "/query/:id/page/:page_no",
            get(query_page_handler),
            EndpointKind::PollQuery,
        ),
        (
            "/query/:id/kill",
            get(query_cancel_handler).post(query_cancel_handler),
            EndpointKind::PollQuery,
        ),
        (
            "/query/:id/final",
            get(query_final_handler).post(query_final_handler),
            EndpointKind::PollQuery,
        ),
        ("/session/login", post(login_handler), EndpointKind::Login),
        (
            "/session/logout",
            post(logout_handler),
            EndpointKind::Logout,
        ),
        (
            "/session/refresh",
            post(refresh_handler),
            EndpointKind::Refresh,
        ),
        (
            "/session/heartbeat",
            post(heartbeat_handler),
            EndpointKind::HeartBeat,
        ),
        ("/verify", post(verify_handler), EndpointKind::Verify),
        (
            "/upload_to_stage",
            put(upload_to_stage),
            EndpointKind::UploadToStage,
        ),
        (
            "/discovery_nodes",
            get(discovery_nodes),
            EndpointKind::SystemInfo,
        ),
        (
            "/catalog/databases",
            get(list_databases_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/databases/:database/tables",
            get(list_database_tables_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/databases/:database/tables/:table",
            get(get_database_table_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/databases/:database/tables/:table/fields",
            get(list_database_table_fields_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/search/tables",
            post(search_tables_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/search/databases",
            post(search_databases_handler),
            EndpointKind::Catalog,
        ),
        (
            "/catalog/stats",
            get(catalog_stats_handler),
            EndpointKind::Catalog,
        ),
        (
            "/users",
            get(list_users_handler).post(create_user_handler),
            EndpointKind::Metadata,
        ),
        (
            "/streaming_load",
            put(streaming_load_handler),
            EndpointKind::StreamingLoad,
        ),
        ("/roles", get(list_roles_handler), EndpointKind::Metadata),
    ];

    let mut route = Route::new();
    for (path, endpoint, kind) in rules.into_iter() {
        route = route.at(
            path,
            endpoint
                .with(MetricsMiddleware::new(path))
                .with(HTTPSessionMiddleware::create(HttpHandlerKind::Query, kind))
                .with(CookieJarManager::new()),
        );
    }

    route
}

fn query_id_removed(query_id: &str, remove_reason: RemoveReason) -> PoemError {
    PoemError::from_string(
        format!("[HTTP-QUERY] Query ID {query_id} {}", remove_reason),
        StatusCode::BAD_REQUEST,
    )
}

fn query_id_not_found(query_id: &str, node_id: &str) -> PoemError {
    PoemError::from_string(
        format!("[HTTP-QUERY] Query ID {query_id} not found on node {node_id}"),
        StatusCode::NOT_FOUND,
    )
}

fn query_id_to_trace_id(query_id: &str) -> TraceId {
    let uuid = Uuid::parse_str(query_id).unwrap_or_else(|_| Uuid::now_v7());
    TraceId(uuid.as_u128())
}

/// The HTTP query endpoints are expected to be responses within 60 seconds.
/// If it exceeds far from 60 seconds, there might be something wrong, we should
/// log it.
pub(crate) struct SlowRequestLogTracker {
    started_at: std::time::Instant,
    method: String,
    uri: String,
}

impl SlowRequestLogTracker {
    pub(crate) fn new(ctx: &HttpQueryContext) -> Self {
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
                    "[HTTP-QUERY] Slow request detected on {} {}, elapsed time: {:.2}s",
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
pub(crate) fn get_http_tracing_span(
    name: &'static str,
    ctx: &HttpQueryContext,
    query_id: &str,
) -> Span {
    if let Some(parent) = ctx.trace_parent.as_ref() {
        let trace = parent.as_str();
        match SpanContext::decode_w3c_traceparent(trace) {
            Some(span_context) => {
                return Span::root(name, span_context)
                    .with_properties(|| ctx.to_fastrace_properties());
            }
            None => {
                warn!("[HTTP-QUERY] Failed to decode trace parent: {}", trace);
            }
        }
    }

    let trace_id = query_id_to_trace_id(query_id);
    Span::root(name, SpanContext::new(trace_id, SpanId(rand::random())))
        .with_properties(|| ctx.to_fastrace_properties())
}
