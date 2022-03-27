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

use std::borrow::Cow;
use std::sync::Arc;

use async_stream::stream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_types::UserInfo;
use common_planners::PlanNode;
use common_streams::NDJsonSourceBuilder;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_tracing::tracing;
use futures::StreamExt;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::post;
use poem::web::Data;
use poem::web::Query;
use poem::Body;
use poem::Endpoint;
use poem::Route;
use serde::Deserialize;

use crate::interpreters::InterpreterFactory;
use crate::servers::http::formats::tsv_output::block_to_tsv;
use crate::servers::http::formats::Format;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::sql::PlanParser;

// https://clickhouse.com/docs/en/interfaces/http/

const FORMAT_JSON_EACH_ROW: &str = "JSONEachRow";

#[derive(Deserialize)]
pub struct StatementHandlerParams {
    query: String,
}

fn supported_formats() -> String {
    vec![FORMAT_JSON_EACH_ROW].join("|")
}

async fn execute(
    ctx: Arc<QueryContext>,
    plan: PlanNode,
    input_stream: Option<SendableDataBlockStream>,
) -> Result<Body> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
    // Write Start to query log table.
    let _ = interpreter
        .start()
        .await
        .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

    let data_stream = interpreter.execute(input_stream).await?;
    let mut data_stream = ctx.try_create_abortable(data_stream)?;

    let stream = stream! {
           while let Some(block) = data_stream.next().await {
            match block{
                Ok(block) => {
                    yield(block_to_tsv(&block))
                },
                Err(err) => yield(Err(err)),
            };
        }
    };

    Ok(Body::from_bytes_stream(stream))
}

#[poem::handler]
pub async fn clickhouse_handler_get(
    sessions_extension: Data<&Arc<SessionManager>>,
    user_info: Data<&UserInfo>,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Body> {
    let session_manager = sessions_extension.0;
    let session = session_manager
        .create_session(SessionType::ClickHouseHttpHandler)
        .await
        .map_err(InternalServerError)?;
    session.set_current_user(user_info.0.clone());

    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let mut sql = params.query;
    let plan = PlanParser::parse(context.clone(), &sql)
        .await
        .map_err(BadRequest)?;

    if matches!(plan, PlanNode::Insert(_)) {
        return Err(BadRequest(ErrorCode::SyntaxException(
            "not allow insert in GET",
        )));
    }
    context.attach_query_str(&sql);
    execute(context, plan, None)
        .await
        .map_err(InternalServerError)
}

fn try_parse_insert_formatted(sql: &str) -> Result<Option<(Format, Vec<DfStatement>)>> {
    if let Ok((statements, _)) = DfParser::parse_sql(sql) {
        if statements.is_empty() {
            return Ok(None);
        }
        if statements.len() > 1 {
            return Err(ErrorCode::SyntaxException("Only support single query"));
        }
        let statement = &statements[0];
        if let DfStatement::InsertQuery(ref insert) = statement {
            if let Some(format) = &insert.format {
                match format.as_str() {
                    FORMAT_JSON_EACH_ROW => return Ok(Some((Format::TSV, statements))),
                    // "" for "insert into my_table values;"
                    "" => {}
                    _ => {
                        return Err(ErrorCode::SyntaxException(format!(
                            "format {} not supported; only support: {}",
                            format,
                            supported_formats()
                        )))
                    }
                };
            }
        };
    }
    Ok(None)
}

// parse query params first to avoid tokenize the body part when it is a insert with format.
async fn parse(
    sql: &str,
    body: Body,
) -> PoemResult<(Option<(Format, Body)>, Cow<'_, str>, Vec<DfStatement>)> {
    if let Some((format, statements)) = try_parse_insert_formatted(sql).map_err(BadRequest)? {
        Ok((Some((format, body)), sql.into(), statements))
    } else {
        let body = body.into_string().await.map_err(BadRequest)?;
        let sql = format!("{}\n{}", sql, body);
        let (statements, _) = DfParser::parse_sql(&sql).map_err(BadRequest)?;
        Ok((None, sql.into(), statements))
    }
}

#[poem::handler]
pub async fn clickhouse_handler_post(
    sessions_extension: Data<&Arc<SessionManager>>,
    user_info: Data<&UserInfo>,
    body: Body,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Body> {
    let mut sql = params.query;
    let (format, sql, statements) = parse(&sql, body).await?;

    let session_manager = sessions_extension.0;
    let session = session_manager
        .create_session(SessionType::ClickHouseHttpHandler)
        .await
        .map_err(InternalServerError)?;
    session.set_current_user(user_info.0.clone());

    let ctx = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let plan = PlanParser::build_plan(statements, ctx.clone())
        .await
        .map_err(InternalServerError)?;
    ctx.attach_query_str(&sql);

    if let Some((format, body)) = format {
        let input_stream = match format {
            Format::TSV => build_ndjson_stream(&plan, body).await.map_err(BadRequest)?,
        };
        execute(ctx, plan, Some(input_stream))
            .await
            .map_err(InternalServerError)
    } else {
        execute(ctx, plan, None).await.map_err(InternalServerError)
    }
}

async fn build_ndjson_stream(plan: &PlanNode, body: Body) -> Result<SendableDataBlockStream> {
    let builder = NDJsonSourceBuilder::create(plan.schema());
    let cursor = std::io::Cursor::new(
        body.into_vec()
            .await
            .map_err_to_code(ErrorCode::BadBytes, || "fail to read body")?,
    );
    let source = builder.build(cursor)?;
    SourceStream::new(Box::new(source)).execute().await
}

pub fn clickhouse_router() -> impl Endpoint {
    Route::new().at(
        "/",
        post(clickhouse_handler_post).get(clickhouse_handler_get),
    )
}
