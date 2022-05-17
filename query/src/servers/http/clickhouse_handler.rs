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

use async_stream::stream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_io::prelude::FormatSettings;
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
use poem::web::Query;
use poem::Body;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;
use serde::Deserialize;

use crate::formats::output_format::OutputFormat;
use crate::formats::output_format::OutputFormatType;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::StreamSource;
use crate::pipelines::new::SourcePipeBuilder;
use crate::servers::http::formats::Format;
use crate::servers::http::v1::HttpQueryContext;
use crate::sessions::QueryContext;
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
    let _ = interpreter
        .start()
        .await
        .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));
    let data_stream: SendableDataBlockStream =
        if ctx.get_settings().get_enable_new_processor_framework()? != 0
            && ctx.get_cluster().is_empty()
        {
            let output_port = OutputPort::create();
            let stream_source =
                StreamSource::create(ctx.clone(), input_stream, output_port.clone())?;
            let mut source_pipe_builder = SourcePipeBuilder::create();
            source_pipe_builder.add_source(output_port, stream_source);
            let _ = interpreter
                .set_source_pipe_builder(Option::from(source_pipe_builder))
                .map_err(|e| tracing::error!("interpreter.set_source_pipe_builder.error: {:?}", e));
            interpreter.execute(None).await?
        } else {
            interpreter.execute(input_stream).await?
        };
    let mut data_stream = ctx.try_create_abortable(data_stream)?;
    let format_setting = ctx.get_format_settings()?;
    let fmt = OutputFormatType::Tsv;
    let mut output_format = fmt.with_default_setting();
    let stream = stream! {
        while let Some(block) = data_stream.next().await {
            match block{
                Ok(block) => {
                    yield output_format.serialize_block(&block, &format_setting);
                },
                Err(err) => yield(Err(err)),
            };
        }

        yield output_format.finalize();

        let _ = interpreter
            .finish()
            .await
            .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));
    };

    Ok(Body::from_bytes_stream(stream))
}

#[poem::handler]
pub async fn clickhouse_handler_get(
    ctx: &HttpQueryContext,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Body> {
    let session = ctx
        .create_session(SessionType::ClickHouseHttpHandler)
        .await
        .map_err(InternalServerError)?;

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

fn try_parse_insert_formatted(
    sql: &str,
    typ: SessionType,
) -> Result<Option<(Format, Vec<DfStatement>)>> {
    if let Ok((statements, _)) = DfParser::parse_sql(sql, typ) {
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
                    FORMAT_JSON_EACH_ROW => return Ok(Some((Format::NDJson, statements))),
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

#[poem::handler]
pub async fn clickhouse_handler_post(
    ctx: &HttpQueryContext,
    body: Body,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<Body> {
    let session = ctx
        .create_session(SessionType::ClickHouseHttpHandler)
        .await
        .map_err(InternalServerError)?;

    let ctx = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let mut sql = params.query;

    // Insert into format sql
    let (plan, input_stream) = if let Some((format, statements)) =
        try_parse_insert_formatted(&sql, ctx.get_current_session().get_type())
            .map_err(BadRequest)?
    {
        let plan = PlanParser::build_plan(statements, ctx.clone())
            .await
            .map_err(InternalServerError)?;
        ctx.attach_query_str(&sql);

        let input_stream = match format {
            Format::NDJson => build_ndjson_stream(&plan, body).await.map_err(BadRequest)?,
        };
        (plan, Some(input_stream))
    } else {
        // Other sql
        let body = body.into_string().await.map_err(BadRequest)?;
        let sql = format!("{}\n{}", sql, body);
        let (statements, _) =
            DfParser::parse_sql(&sql, ctx.get_current_session().get_type()).map_err(BadRequest)?;

        let plan = PlanParser::build_plan(statements, ctx.clone())
            .await
            .map_err(InternalServerError)?;
        ctx.attach_query_str(&sql);

        (plan, None)
    };

    execute(ctx, plan, input_stream)
        .await
        .map_err(InternalServerError)
}

async fn build_ndjson_stream(plan: &PlanNode, body: Body) -> Result<SendableDataBlockStream> {
    // TODO(veeupup): HTTP with global session tz
    let builder = NDJsonSourceBuilder::create(plan.schema(), FormatSettings::default());
    let cursor = futures::io::Cursor::new(
        body.into_vec()
            .await
            .map_err_to_code(ErrorCode::BadBytes, || "fail to read body")?,
    );
    let source = builder.build(cursor)?;
    SourceStream::new(Box::new(source)).execute().await
}

pub fn clickhouse_router() -> impl Endpoint {
    Route::new()
        .at(
            "/",
            post(clickhouse_handler_post).get(clickhouse_handler_get),
        )
        .with(poem::middleware::Compression)
}
