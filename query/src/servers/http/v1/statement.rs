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

use poem::error::NotFound;
use poem::error::Result as PoemResult;
use poem::post;
use poem::web::Data;
use poem::web::Json;
use poem::web::Query;
use poem::Endpoint;
use poem::Route;
use serde::Deserialize;

use crate::servers::http::v1::query::HttpQuery;
use crate::servers::http::v1::query::HttpQueryRequest;
use crate::servers::http::v1::query::HttpSessionConf;
use crate::servers::http::v1::query::Wait;
use crate::servers::http::v1::QueryResponse;
use crate::sessions::SessionManager;

#[derive(Deserialize)]
pub struct StatementHandlerParams {
    db: Option<String>,
}

#[poem::handler]
pub async fn statement_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
    sql: String,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Json<QueryResponse>> {
    let session_manager = sessions_extension.0;
    let http_query_manager = session_manager.get_http_query_manager();
    let query_id = http_query_manager.next_query_id();
    let session = HttpSessionConf {
        database: params.db.filter(|x| !x.is_empty()),
    };
    let req = HttpQueryRequest { sql, session };
    let query = HttpQuery::try_create(query_id.clone(), req, session_manager).await;

    match query {
        Ok(query) => {
            let resp = query
                .get_response_page(0, &Wait::Sync, true)
                .await
                .map_err(|err| NotFound(err.message()))?;
            Ok(Json(QueryResponse::from_internal(query_id, resp)))
        }
        Err(e) => Ok(Json(QueryResponse::fail_to_start_sql(query_id, &e))),
    }
}

pub fn statement_router() -> impl Endpoint {
    Route::new().at("/", post(statement_handler))
}
