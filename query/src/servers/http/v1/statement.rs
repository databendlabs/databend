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

use common_io::prelude::FormatSettings;
use hyper::StatusCode;
use poem::error::Result as PoemResult;
use poem::post;
use poem::web::Json;
use poem::web::Query;
use poem::Endpoint;
use poem::Route;
use serde::Deserialize;

use super::query::HttpQueryRequest;
use super::query::HttpSession;
use super::query::HttpSessionConf;
use super::query::PaginationConf;
use super::HttpQueryContext;
use super::QueryResponse;

#[derive(Deserialize)]
pub struct StatementHandlerParams {
    db: Option<String>,
}

#[poem::handler]
pub async fn statement_handler(
    ctx: &HttpQueryContext,
    sql: String,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Json<QueryResponse>> {
    let http_query_manager = ctx.session_mgr.get_http_query_manager();
    let query_id = http_query_manager.next_query_id();
    let session = HttpSessionConf {
        database: params.db.filter(|x| !x.is_empty()),
        max_idle_time: None,
    };
    let req = HttpQueryRequest {
        sql,
        session: HttpSession::New(session),
        pagination: PaginationConf { wait_time_secs: -1 },
    };
    let query = http_query_manager
        .try_create_query(&query_id, ctx, req)
        .await;
    // TODO(veeupup): get query_ctx's format_settings here
    let format = FormatSettings::default();
    match query {
        Ok(query) => {
            let resp = query
                .get_response_page(0, &format)
                .await
                .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
            http_query_manager.remove_query(&query_id).await;
            Ok(Json(QueryResponse::from_internal(query_id, resp)))
        }
        Err(e) => Ok(Json(QueryResponse::fail_to_start_sql(query_id, &e))),
    }
}

pub fn statement_router() -> impl Endpoint {
    Route::new().at("/", post(statement_handler))
}
