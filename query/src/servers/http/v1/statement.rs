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

use std::collections::HashMap;
use std::convert::Infallible;

use axum::body::Bytes;
use axum::body::Full;
use axum::extract::Extension;
use axum::extract::Query;
use axum::handler::post;
use axum::http::header;
use axum::http::Response;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::BoxRoute;
use axum::Router;
use common_base::ProgressValues;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use serde_json::Value as JsonValue;
use uuid;

use super::block_to_json::block_to_json;
use crate::interpreters::InterpreterFactory;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize, Debug)]
pub struct HttpQueryResult {
    id: String,
    #[serde(rename = "nextUri")] // to be compatible with presto
    pub next_uri: Option<String>,
    pub data: Option<Vec<Vec<JsonValue>>>,
    pub columns: Option<DataSchemaRef>,
    // TODO(youngsofun): add more info in error (ErrorCode does not support Serialize)
    pub error: Option<String>,
    pub stats: Option<ProgressValues>,
}

impl HttpQueryResult {
    fn create(id: String) -> HttpQueryResult {
        HttpQueryResult {
            id,
            next_uri: None,
            error: None,
            data: None,
            columns: None,
            stats: None,
        }
    }
}

impl IntoResponse for HttpQueryResult {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        let body = Full::from(serde_json::to_vec(&self).unwrap());
        // TODO(youngsofun): when should we return other status code here?
        let status = StatusCode::OK;
        let content_type = "application/javascript";
        //let content_type = header::HeaderValue::from_str("application/javascript").unwrap();
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
            .unwrap()
    }
}

struct HttpQuery {
    id: String,
    sql: String,
    state: Option<HttpQueryState>,
    db: Option<String>,
}

struct HttpQueryState {
    #[allow(dead_code)]
    session: SessionRef,
    context: DatabendQueryContextRef,
    data_stream: SendableDataBlockStream,
    schema: DataSchemaRef,
}

// TODO(youngsofun): add a HttpQueryManager in SessionManger to support async query.
impl HttpQuery {
    fn new(id: String, sql: String, db: Option<String>) -> HttpQuery {
        HttpQuery {
            id,
            sql,
            state: None,
            db,
        }
    }

    async fn start(&mut self, session_manager: SessionManagerRef) -> Result<HttpQueryState> {
        let session = session_manager.create_session("http-statement")?;
        let ctx = session.create_context().await?;
        if self.db.is_some() && !self.db.clone().unwrap().is_empty() {
            ctx.set_current_database(self.db.clone().unwrap())?;
        }
        ctx.attach_query_str(&self.sql);
        let plan = PlanParser::create(ctx.clone()).build_from_sql(&self.sql)?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let data_stream = interpreter.execute().await?;
        let state = HttpQueryState {
            session,
            data_stream,
            context: ctx.clone(),
            schema: plan.schema(),
        };
        Ok(state)
    }

    async fn initial_result(&mut self, session_manager: SessionManagerRef) -> HttpQueryResult {
        let state = self.start(session_manager).await;
        let mut result = HttpQueryResult::create(self.id.clone());
        match state {
            Ok(st) => {
                self.state = Some(st);
                result = self.state.as_mut().unwrap().fill_result_sync(result).await;
            }
            Err(err) => {
                result.error = Some(err.message());
            }
        }
        result
    }

    #[allow(dead_code)]
    fn kill(&self) {
        self.state.as_ref().unwrap().kill()
    }
}

impl HttpQueryState {
    async fn collect_all(&mut self) -> Result<Vec<Vec<JsonValue>>> {
        let mut results: Vec<Vec<Vec<JsonValue>>> = Vec::new();
        while let Some(block) = self.data_stream.next().await {
            results.push(block_to_json(block.unwrap())?);
        }
        Ok(results.concat())
    }

    async fn fill_result_sync(&mut self, mut result: HttpQueryResult) -> HttpQueryResult {
        let data = self.collect_all().await.unwrap();
        result.data = Some(data);
        result.columns = Some(self.schema.clone());
        result.stats = Some(self.context.get_and_reset_progress_value());
        result
    }

    #[allow(dead_code)]
    fn kill(&self) {
        self.session.force_kill_session();
    }
}

pub(crate) async fn statement_handler(
    sessions_extension: Extension<SessionManagerRef>,
    sql: String,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_manager = sessions_extension.0;
    let query_id = uuid::Uuid::new_v4().to_string();
    let db = params.get("db");
    let mut query = HttpQuery::new(query_id, sql, db.cloned());
    query.initial_result(session_manager).await
}

pub fn statement_router() -> Router<BoxRoute> {
    Router::new().route("/", post(statement_handler)).boxed()
}
