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

use common_exception::ErrorCode;
use common_expression::DataSchemaRef;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::http::StatusCode;
use poem::post;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use tracing::error;
use tracing::info;

use super::query::ExecuteStateKind;
use super::query::HttpQueryRequest;
use super::query::HttpQueryResponseInternal;
use crate::servers::http::v1::query::Progresses;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::HttpSessionConf;
use crate::servers::http::v1::JsonBlock;
use crate::sessions::QueryAffect;
const HEADER_QUERY_ID: &str = "X-DATABEND-QUERY-ID";
const HEADER_QUERY_STATE: &str = "X-DATABEND-QUERY-STATE";
const HEADER_QUERY_PAGE_ROWS: &str = "X-DATABEND-QUERY-PAGE-ROWS";

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

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
}

impl QueryError {
    fn from_error_code(e: &ErrorCode) -> Self {
        QueryError {
            code: e.code(),
            message: e.message(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct QueryStats {
    #[serde(flatten)]
    pub progresses: Progresses,
    pub running_time_ms: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponseField {
    name: String,
    r#type: String,
}

impl QueryResponseField {
    fn from_schema(schema: DataSchemaRef) -> Vec<Self> {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub session: Option<HttpSessionConf>,
    pub schema: Vec<QueryResponseField>,
    pub data: Vec<Vec<JsonValue>>,
    pub state: ExecuteStateKind,
    // only sql query error
    pub error: Option<QueryError>,
    pub stats: QueryStats,
    pub affect: Option<QueryAffect>,
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
            (JsonBlock::empty(), None)
        } else {
            match state.state {
                ExecuteStateKind::Running => match r.data {
                    None => (JsonBlock::empty(), Some(make_state_uri(&id))),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_state_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
                ExecuteStateKind::Failed => (JsonBlock::empty(), Some(make_final_uri(&id))),
                ExecuteStateKind::Succeeded => match r.data {
                    None => (JsonBlock::empty(), Some(make_final_uri(&id))),
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

        let schema = data.schema().clone();
        let session_id = r.session_id.clone();
        let stats = QueryStats {
            progresses: state.progresses.clone(),
            running_time_ms: state.running_time_ms,
        };
        let rows = data.data.len();
        Json(QueryResponse {
            data: data.into(),
            state: state.state,
            schema: QueryResponseField::from_schema(schema),
            session_id: Some(session_id),
            session: r.session,
            stats,
            affect: state.affect,
            id: id.clone(),
            next_uri,
            stats_uri: Some(make_state_uri(&id)),
            final_uri: Some(make_final_uri(&id)),
            kill_uri: Some(make_kill_uri(&id)),
            error: r.state.error.as_ref().map(QueryError::from_error_code),
        })
        .with_header(HEADER_QUERY_ID, id.clone())
        .with_header(HEADER_QUERY_STATE, state.state.to_string())
        .with_header(HEADER_QUERY_PAGE_ROWS, rows)
    }

    pub(crate) fn fail_to_start_sql(err: &ErrorCode) -> impl IntoResponse {
        Json(QueryResponse {
            id: "".to_string(),
            stats: QueryStats::default(),
            state: ExecuteStateKind::Failed,
            affect: None,
            data: vec![],
            schema: vec![],
            session_id: None,
            session: None,
            next_uri: None,
            stats_uri: None,
            final_uri: None,
            kill_uri: None,
            error: Some(QueryError::from_error_code(err)),
        })
    }
}

#[poem::handler]
async fn query_final_handler(
    _ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let http_query_manager = HttpQueryManager::instance();
    match http_query_manager.remove_query(&query_id).await {
        Some(query) => {
            let mut response = query.get_response_state_only().await;
            if response.state.state == ExecuteStateKind::Running {
                return Err(PoemError::from_string(
                    format!("query {} is still running, can not final it", query_id),
                    StatusCode::BAD_REQUEST,
                ));
            }
            Ok(QueryResponse::from_internal(query_id, response, true))
        }
        None => Err(query_id_not_found(query_id)),
    }
}

// currently implementation only support kill http query
#[poem::handler]
async fn query_cancel_handler(
    _ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    let http_query_manager = HttpQueryManager::instance();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            query.kill().await;
            http_query_manager.remove_query(&query_id).await;
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

#[poem::handler]
async fn query_state_handler(
    _ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let http_query_manager = HttpQueryManager::instance();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            let response = query.get_response_state_only().await;
            Ok(QueryResponse::from_internal(query_id, response, false))
        }
        None => Err(query_id_not_found(query_id)),
    }
}

#[poem::handler]
async fn query_page_handler(
    _ctx: &HttpQueryContext,
    Path((query_id, page_no)): Path<(String, usize)>,
) -> PoemResult<impl IntoResponse> {
    let http_query_manager = HttpQueryManager::instance();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            query.update_expire_time(true).await;
            let resp = query
                .get_response_page(page_no)
                .await
                .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
            query.update_expire_time(false).await;
            Ok(QueryResponse::from_internal(query_id, resp, false))
        }
        None => Err(query_id_not_found(query_id)),
    }
}

#[poem::handler]
#[async_backtrace::framed]
pub(crate) async fn query_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<HttpQueryRequest>,
) -> PoemResult<impl IntoResponse> {
    info!("receive http query: {:?}", req);
    let http_query_manager = HttpQueryManager::instance();
    let sql = req.sql.clone();

    let query = http_query_manager
        .try_create_query(ctx, req)
        .await
        .map_err(|err| err.display_with_sql(&sql));
    match query {
        Ok(query) => {
            query.update_expire_time(true).await;
            let resp = query
                .get_response_page(0)
                .await
                .map_err(|err| err.display_with_sql(&sql))
                .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
            let (rows, next_page) = match &resp.data {
                None => (0, None),
                Some(p) => (p.page.data.num_rows(), p.next_page_no),
            };
            info!(
                "initial response to http query_id={}, state={:?}, rows={}, next_page={:?}, sql='{}'",
                &query.id, &resp.state, rows, next_page, sql
            );
            query.update_expire_time(false).await;
            Ok(QueryResponse::from_internal(query.id.to_string(), resp, false).into_response())
        }
        Err(e) => {
            error!("Fail to start sql, Error: {:?}", e);
            Ok(QueryResponse::fail_to_start_sql(&e).into_response())
        }
    }
}

pub fn query_route() -> Route {
    // Note: endpoints except /v1/query may change without notice, use uris in response instead
    Route::new()
        .at("/", post(query_handler))
        .at("/:id", get(query_state_handler))
        .at("/:id/page/:page_no", get(query_page_handler))
        .at(
            "/:id/kill",
            get(query_cancel_handler).post(query_cancel_handler),
        )
        .at(
            "/:id/final",
            get(query_final_handler).post(query_final_handler),
        )
}

fn query_id_not_found(query_id: String) -> PoemError {
    PoemError::from_string(
        format!("query id not found {}", query_id),
        StatusCode::NOT_FOUND,
    )
}
