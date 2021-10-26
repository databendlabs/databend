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

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::ProgressValues;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use hyper::http::header;
use poem::get;
use poem::http::StatusCode;
use poem::post;
use poem::web::Data;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use poem::IntoResponse;
use poem::Response;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::block_to_json::JsonBlockRef;
use crate::servers::http::v1::query::execute_state::HttpQueryRequest;
use crate::servers::http::v1::query::http_query::HttpQueryResponseInternal;
use crate::servers::http::v1::query::result_data_manager::Wait;
use crate::sessions::SessionManagerRef;

pub fn make_page_uri(query_id: &str, page_no: usize) -> String {
    format!("/v1/query/{}/page/{}", query_id, page_no)
}

pub fn make_state_uri(query_id: &str) -> String {
    format!("/v1/query/{}", query_id)
}

pub fn make_delete_uri(query_id: &str) -> String {
    format!("/v1/query/{}/kill?delete=true", query_id)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub id: Option<String>,
    pub columns: Option<DataSchemaRef>,
    pub data: JsonBlockRef,
    pub next_uri: Option<String>,
    pub state_uri: Option<String>,
    pub delete_uri: Option<String>,
    // TODO(youngsofun): consider better response for error
    // 1. another json format for request error
    // 2. return 400 for some situation
    // 3. more detail from ErrorCode (does not support Serialization))
    // 4. use error code to ease the handling in client program
    pub request_error: Option<String>,
    pub query_error: Option<String>,
    pub query_state: Option<String>,
    pub query_progress: Option<ProgressValues>,
}

impl QueryResponse {
    fn from_internal(id: String, result: &Result<HttpQueryResponseInternal>) -> QueryResponse {
        match result {
            Ok(r) => {
                let (data, next_url) = match &r.data {
                    Some(d) => (
                        d.page.data.clone(),
                        d.next_page_no.map(|n| make_page_uri(&id, n)),
                    ),
                    None => (Arc::new(vec![]), None),
                };
                let columns = match &r.initial_state {
                    Some(v) => v.schema.clone(),
                    None => None,
                };
                QueryResponse {
                    data,
                    columns,
                    id: Some(id.clone()),
                    next_uri: next_url,
                    state_uri: Some(make_state_uri(&id)),
                    delete_uri: Some(make_delete_uri(&id)),
                    query_error: r.state.error.clone(),
                    query_progress: r.state.progress.clone(),
                    query_state: Some(r.state.state.to_string()),
                    request_error: None,
                }
            }
            Err(e) => QueryResponse::simple_error(Some(id), e.message()),
        }
    }

    fn not_found(query_id: String) -> QueryResponse {
        QueryResponse::simple_error(None, format!("query id not found {}", query_id))
    }

    fn simple_error(id: Option<String>, message: String) -> QueryResponse {
        QueryResponse {
            id,
            data: Arc::new(vec![]),
            query_error: None,
            columns: None,
            query_progress: None,
            next_uri: None,
            state_uri: None,
            delete_uri: None,
            query_state: None,
            request_error: Some(message),
        }
    }
}

impl IntoResponse for QueryResponse {
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self).unwrap();
        // TODO(youngsofun): when should we return other status code here?
        let status = StatusCode::OK;
        let content_type = "application/json";
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct CancelParams {
    delete: Option<bool>,
}

#[poem::handler]
async fn query_cancel_handler(
    sessions_extension: Data<&SessionManagerRef>,
    Query(params): Query<CancelParams>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query_by_id(&query_id).await {
        Some(query) => {
            query.kill().await;
            if params.delete.unwrap_or(false) {
                http_query_manager.remove_query_by_id(&query_id).await;
            }
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

#[poem::handler]
async fn query_state_handler(
    sessions_extension: Data<&SessionManagerRef>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query_by_id(&query_id).await {
        Some(query) => {
            let response = query.get_response_state_only().await;
            QueryResponse::from_internal(query_id, &Ok(response))
        }
        None => QueryResponse::not_found(query_id),
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct PageParams {
    // for now, only used for test
    wait_time: Option<i32>,
}

impl PageParams {
    fn get_wait_type(&self) -> Wait {
        let t = self.wait_time.unwrap_or(10);
        match t.cmp(&0) {
            Ordering::Greater => Wait::Deadline(Instant::now() + Duration::from_secs(t as u64)),
            Ordering::Equal => Wait::Async,
            Ordering::Less => Wait::Sync,
        }
    }
}

#[poem::handler]
async fn query_page_handler(
    sessions_extension: Data<&SessionManagerRef>,
    Query(params): Query<PageParams>,
    Path((query_id, page_no)): Path<(String, usize)>,
) -> impl IntoResponse {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    match http_query_manager.get_query_by_id(&query_id).await {
        Some(query) => {
            let wait_type = params.get_wait_type();
            let result = query.get_response_page(page_no, &wait_type, false).await;
            QueryResponse::from_internal(query_id, &result)
        }
        None => QueryResponse::not_found(query_id),
    }
}

#[poem::handler]
pub(crate) async fn query_handler(
    sessions_extension: Data<&SessionManagerRef>,
    Query(params): Query<PageParams>,
    Json(req): Json<HttpQueryRequest>,
) -> impl IntoResponse {
    log::info!("receive http query: {:?} {:?}", req, params);
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    let query = http_query_manager.create_query(req, session_manager).await;
    match query {
        Ok(query) => {
            let wait_type = params.get_wait_type();
            let result = query.get_response_page(0, &wait_type, true).await;
            QueryResponse::from_internal(query.id.to_string(), &result)
        }
        Err(e) => QueryResponse::simple_error(None, e.message()),
    }
}

pub fn query_route() -> Route {
    // Note: endpoints except /v1/query may change without notice, use uris in response instead
    Route::new()
        .at("/", post(query_handler))
        .at("/:id", get(query_state_handler))
        .at("/:id/page/:page_no", get(query_page_handler))
        .at("/:id/kill", get(query_cancel_handler))
}
