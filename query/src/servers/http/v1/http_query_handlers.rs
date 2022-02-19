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

use std::sync::Arc;

use common_base::ProgressValues;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_meta_types::UserInfo;
use common_tracing::tracing;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::http::StatusCode;
use poem::post;
use poem::web::Data;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;

use super::query::ExecuteStateName;
use super::query::HttpQueryRequest;
use super::query::HttpQueryResponseInternal;
use super::JsonBlockRef;
use crate::sessions::SessionManager;

pub fn make_page_uri(query_id: &str, page_no: usize) -> String {
    format!("/v1/query/{}/page/{}", query_id, page_no)
}

pub fn make_state_uri(query_id: &str) -> String {
    format!("/v1/query/{}", query_id)
}

pub fn make_final_uri(query_id: &str) -> String {
    format!("/v1/query/{}/kill?delete=true", query_id)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
    pub backtrace: Option<String>,
    // TODO(youngsofun): add other info more friendly to client
}

impl QueryError {
    fn from_error_code(e: &ErrorCode) -> Self {
        QueryError {
            code: e.code(),
            message: e.message(),
            backtrace: e.backtrace().map(|b| b.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct QueryStats {
    pub progress: Option<ProgressValues>,
    pub wall_time_ms: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub id: String,
    pub schema: Option<DataSchemaRef>,
    pub data: JsonBlockRef,
    pub state: ExecuteStateName,
    // only sql query error
    pub error: Option<QueryError>,
    pub stats: QueryStats,
    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
}

impl QueryResponse {
    pub(crate) fn from_internal(id: String, r: HttpQueryResponseInternal) -> QueryResponse {
        let (data, next_url) = match &r.data {
            Some(d) => (
                d.page.data.clone(),
                d.next_page_no.map(|n| make_page_uri(&id, n)),
            ),
            None => (Arc::new(vec![]), None),
        };
        let columns = r.initial_state.as_ref().and_then(|v| v.schema.clone());
        let stats = QueryStats {
            progress: r.state.progress.clone(),
            wall_time_ms: r.state.wall_time_ms,
        };
        QueryResponse {
            data,
            state: r.state.state,
            schema: columns,
            stats,
            id: id.clone(),
            next_uri: next_url,
            stats_uri: Some(make_state_uri(&id)),
            final_uri: Some(make_final_uri(&id)),
            error: r.state.error.as_ref().map(QueryError::from_error_code),
        }
    }

    pub(crate) fn fail_to_start_sql(id: String, err: &ErrorCode) -> QueryResponse {
        QueryResponse {
            id,
            stats: QueryStats::default(),
            state: ExecuteStateName::Failed,
            data: Arc::new(vec![]),
            schema: None,
            next_uri: None,
            stats_uri: None,
            final_uri: None,
            error: Some(QueryError::from_error_code(err)),
        }
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct CancelParams {
    delete: Option<bool>,
}

#[poem::handler]
async fn query_cancel_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
    Query(params): Query<CancelParams>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            query.kill().await;
            if params.delete.unwrap_or(false) {
                http_query_manager.remove_query(&query_id).await;
            }
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

#[poem::handler]
async fn query_state_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
    Path(query_id): Path<String>,
) -> PoemResult<Json<QueryResponse>> {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            let response = query.get_response_state_only().await;
            Ok(Json(QueryResponse::from_internal(query_id, response)))
        }
        None => Err(query_id_not_found(query_id)),
    }
}

#[poem::handler]
async fn query_page_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
    Path((query_id, page_no)): Path<(String, usize)>,
) -> PoemResult<Json<QueryResponse>> {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query(&query_id).await {
        Some(query) => {
            query.clear_expire_time().await;
            let resp = query
                .get_response_page(page_no, false)
                .await
                .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
            query.update_expire_time().await;
            Ok(Json(QueryResponse::from_internal(query_id, resp)))
        }
        None => Err(query_id_not_found(query_id)),
    }
}

#[poem::handler]
pub(crate) async fn query_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
    user_info: Data<&UserInfo>,
    Json(req): Json<HttpQueryRequest>,
) -> PoemResult<Json<QueryResponse>> {
    tracing::info!("receive http query: {:?}", req);
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    let query_id = http_query_manager.next_query_id();
    let query = http_query_manager
        .try_create_query(&query_id, req, session_manager, &user_info)
        .await;

    match query {
        Ok(query) => {
            let resp = query
                .get_response_page(0, true)
                .await
                .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
            query.update_expire_time().await;
            Ok(Json(QueryResponse::from_internal(
                query.id.to_string(),
                resp,
            )))
        }
        Err(e) => Ok(Json(QueryResponse::fail_to_start_sql(query_id, &e))),
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
}

fn query_id_not_found(query_id: String) -> PoemError {
    PoemError::from_string(
        format!("query id not found {}", query_id),
        StatusCode::NOT_FOUND,
    )
}
